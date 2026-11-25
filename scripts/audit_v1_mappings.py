#!/usr/bin/env python3
# Copyright The Linux Foundation and each contributor to LFX.
# SPDX-License-Identifier: MIT
# /// script
# requires-python = ">=3.12"
# dependencies = ["nats-py[nkeys]==2.17.0", "psycopg[binary]==3.3.9"]
# ///

"""Read-only legacy NATS/Postgres mapping audit (lfx-self-serve-ops#1649).

Run with uv run --no-project scripts/audit_v1_mappings.py --help.
Only aggregate counts go to stdout. Treat the output directory as sensitive.
"""

import argparse
import asyncio
import base64
import hashlib
import json
import logging
import math
import os
import sqlite3
import sys
import tempfile
from collections import Counter
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path

STREAM = "KV_v1-mappings"
PREFIX = "$KV.v1-mappings."
FILTER = PREFIX + ">"
BATCH_SIZE = 1000

# Explicit policies, not user-supplied prefix exclusions. Tombstones never
# qualify for a missing-key exemption. Unknown families always need review.
# Sources: pending_markers.go (consumed/expired markers are hard-deleted),
# AGENTS.md / lfx_v1_client.go (retired organization cache and lock).
NON_REQUIRED = {
    "pending.": "consumed or expired echo-suppression marker",
    "v1_org.": "retired organization cache; lookups now use the v1 database",
    "v1_org_lock.": "retired organization cache lock",
}
DURABLE = (
    "project.sfid.",
    "project.uid.",
    "committee.sfid.",
    "committee.uid.",
    "committee_member.sfid.",
    "committee_member.uid.",
    "committee_member.record_sfid.",
    "formation-lead-reconcile.",
    "create_attempt.committee.",
    "create_attempt.committee_member.",
    "create_deleted.committee.",
    "create_deleted.committee_member.",
    "create_scope.committee.",
    "cleanup_attempt.committee_member.",
)
# These are known, but absence/changed state is NOT automatically exempt:
# workspace state is needed for association deletes; old cursors and other
# families need an operator decision rather than a blanket cache exemption.
REVIEW_FAMILIES = ("workspace.uid.", "v1-user.username.", "v1-user.email.", "backfill.")
INVESTIGATE = {
    "missing_postgres",
    "missing_tombstone",
    "value_changed",
    "nats_tombstone_postgres_live",
}


class AuditError(Exception):
    """A safe, fixed error message that does not contain source data."""


def now():
    return datetime.now(UTC).isoformat()


def family_for(key):
    for prefix in (*DURABLE, *NON_REQUIRED, *REVIEW_FAMILIES):
        if key.startswith(prefix) and len(key) > len(prefix):
            return prefix
    return "unknown"


def classify(nats_state, nats_value, pg_state, pg_value, family):
    if nats_state in (None, "deleted"):
        if pg_state == "tombstone":
            return "postgres_only_tombstone"
        if pg_state == "live":
            return "postgres_only_live"
        return "native_delete_absent"
    if nats_state == "tombstone":
        if pg_state is None:
            return "missing_tombstone"
        return (
            "match_tombstone"
            if pg_state == "tombstone"
            else "nats_tombstone_postgres_live"
        )
    if pg_state == "tombstone":
        return "postgres_tombstoned_since_nats"
    if pg_state is None:
        return (
            "expected_operational_absence"
            if family in NON_REQUIRED
            else "missing_postgres"
        )
    return "match_live" if nats_value == pg_value else "value_changed"


def needs_review(status, family):
    return family == "unknown" or status in INVESTIGATE


def open_stage(path):
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    db.execute(
        "PRAGMA cache_size = -65536"
    )  # 64 MiB page cache, not the entire bucket.
    db.execute("PRAGMA temp_store = FILE")
    db.executescript("""
        CREATE TABLE nats (
            key TEXT PRIMARY KEY, value BLOB NOT NULL, state TEXT NOT NULL,
            seq INTEGER NOT NULL
        ) WITHOUT ROWID;
        CREATE TABLE pg (
            key TEXT PRIMARY KEY, value BLOB NOT NULL, state TEXT NOT NULL,
            version INTEGER NOT NULL, updated_at TEXT NOT NULL
        ) WITHOUT ROWID;
        CREATE TABLE rechecked (
            key TEXT PRIMARY KEY, changed INTEGER NOT NULL
        ) WITHOUT ROWID;
    """)
    return db


def stage_nats(db, key, value, state, seq):
    # Resolve LWW first, INCLUDING DEL/PURGE. Filtering deletes before this
    # upsert could resurrect an older PUT from retained stream history.
    db.execute(
        """
        INSERT INTO nats VALUES (?, ?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET
            value=excluded.value, state=excluded.state, seq=excluded.seq
        WHERE excluded.seq > nats.seq
    """,
        (key, value, state, seq),
    )


async def comparison_rows(db):
    # Page INPUT keys before joining, not the anti-join output. LIMIT on
    # unmatched rows alone can still scan every PG key without returning a
    # row when both stores match, starving heartbeats and cancellation.
    queries = (
        (
            "nats",
            """
        SELECT n.key, n.state AS ns, n.value AS nv, n.seq,
               p.state AS ps, p.value AS pv, p.version, p.updated_at
        FROM nats n LEFT JOIN pg p ON p.key=n.key
        WHERE n.key >= ? AND n.key <= ?
        ORDER BY n.key
        """,
        ),
        (
            "pg",
            """
        SELECT p.key, NULL AS ns, NULL AS nv, NULL AS seq,
               p.state AS ps, p.value AS pv, p.version, p.updated_at
        FROM pg p LEFT JOIN nats n ON n.key=p.key WHERE n.key IS NULL
        AND p.key >= ? AND p.key <= ?
        ORDER BY p.key
        """,
        ),
    )
    for source, query in queries:
        last_key = None
        while True:
            # source is one of the two fixed local table names above, never
            # user input. Keyset paging uses the primary key, not OFFSET.
            if last_key is None:
                keys = db.execute(
                    f"SELECT key FROM {source} ORDER BY key LIMIT ?", (BATCH_SIZE,)
                ).fetchall()
            else:
                keys = db.execute(
                    f"SELECT key FROM {source} WHERE key > ? ORDER BY key LIMIT ?",
                    (last_key, BATCH_SIZE),
                ).fetchall()
            if not keys:
                break
            last_key = keys[-1]["key"]
            rows = db.execute(query, (keys[0]["key"], last_key)).fetchall()
            for row in rows:
                yield row
            # This executes even for PG pages with NO unmatched rows. Both
            # callers inherit bounded work/yields from this shared iterator.
            await asyncio.sleep(0)


def row_status(row):
    family = family_for(row["key"])
    return family, classify(row["ns"], row["nv"], row["ps"], row["pv"], family)


def stream_fingerprint(info):
    # Consumer activity is irrelevant. Config includes max_age/TTL policies;
    # sequence/count/size changes and recreation invalidate the baseline.
    return {
        "created": info.created.isoformat() if info.created else None,
        "config": info.config.as_dict(),
        "messages": info.state.messages,
        "bytes": info.state.bytes,
        "first_seq": info.state.first_seq,
        "last_seq": info.state.last_seq,
        "num_deleted": info.state.num_deleted,
        "lost": info.state.lost.as_dict() if info.state.lost else None,
    }


def check_stream(before, after, visits):
    if before["messages"] == 0:
        raise AuditError(
            "Legacy stream is empty; cannot establish a migration baseline"
        )
    if before["lost"] or before != after or visits != before["messages"]:
        raise AuditError(
            "Legacy stream changed, lost data, or scan coverage is incomplete"
        )


async def collect_nats(db, js, info, workers):
    from nats.js.errors import NotFoundError

    end = info.state.last_seq + 1
    width = max(1, (info.state.last_seq + workers - 1) // workers)
    visits = 0

    async def scan(start, stop):
        nonlocal visits
        seq = start
        while seq < stop:
            try:
                msg = await js.get_msg(STREAM, seq=seq, subject=FILTER, next=True)
            except NotFoundError as exc:
                # Only JetStream's message-not-found code means clean EOF.
                # Stream-not-found and other 404s must fail the audit.
                if exc.err_code == 10037:
                    break
                raise AuditError(
                    "Legacy stream disappeared during enumeration"
                ) from None
            if msg.seq is None or msg.seq < seq:
                raise AuditError("Invalid sequence returned during enumeration")
            if msg.seq >= stop:
                break
            if not msg.subject or not msg.subject.startswith(PREFIX):
                raise AuditError("Unexpected subject in legacy stream")
            key = msg.subject[len(PREFIX) :]
            if not key:
                raise AuditError("Empty legacy mapping key")
            value = msg.data or b""
            operation = (msg.headers or {}).get("KV-Operation", "PUT")
            if operation not in ("PUT", "DEL", "PURGE"):
                raise AuditError("Unknown legacy KV operation")
            state = (
                "deleted"
                if operation in ("DEL", "PURGE")
                else ("tombstone" if value == b"!del" else "live")
            )
            stage_nats(db, key, value, state, msg.seq)
            visits += 1
            if visits % BATCH_SIZE == 0:
                db.commit()
            seq = msg.seq + 1

    # TaskGroup cancels siblings on any error; no partial scan can pass.
    async with asyncio.TaskGroup() as group:
        for start in range(1, end, width):
            group.create_task(scan(start, min(start + width, end)))
    db.commit()
    return visits


def pg_record(row):
    key, value, tombstoned, version, updated_at = row
    if not key:
        raise AuditError("Empty Postgres mapping key")
    return (
        key,
        value.encode("utf-8"),
        "tombstone" if tombstoned else "live",
        version,
        updated_at.isoformat(),
    )


async def collect_postgres(db, conn):
    import psycopg

    async with conn.transaction():
        # No startup helper/schema.Apply: this transaction can only read.
        await conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        clock = await conn.execute("SELECT transaction_timestamp()")
        snapshot_at = (await clock.fetchone())[0]
        async with conn.cursor(name="v1_mapping_audit") as cursor:
            await cursor.execute("""
                SELECT mapping_key, mapping_value, tombstoned, version, updated_at
                FROM public.v1_mappings
            """)
            count = 0
            while rows := await cursor.fetchmany(BATCH_SIZE):
                try:
                    db.executemany(
                        "INSERT INTO pg VALUES (?, ?, ?, ?, ?)",
                        (pg_record(row) for row in rows),
                    )
                except (UnicodeError, psycopg.DataError):
                    raise AuditError("Invalid Postgres mapping data") from None
                db.commit()
                count += len(rows)
                await asyncio.sleep(0)
    return {"snapshot_at": snapshot_at.isoformat(), "rows": count}


async def recheck_postgres(db, conn):
    # Separate short read-only transactions use CURRENT state, not the old
    # repeatable-read snapshot. Keep the initial finding, flag any race, and
    # require a new audit rather than silently declaring a changed row fixed.
    async def check_batch(batch):
        async with conn.transaction():
            await conn.execute("SET TRANSACTION READ ONLY")
            cursor = await conn.execute(
                """
                SELECT mapping_key, mapping_value, tombstoned, version, updated_at
                FROM public.v1_mappings WHERE mapping_key = ANY(%s)
            """,
                ([row["key"] for row in batch],),
            )
            rows = await cursor.fetchall()
        current = {row[0]: pg_record(row) for row in rows}
        for old in batch:
            previous = (
                None
                if old["ps"] is None
                else (
                    old["key"],
                    old["pv"],
                    old["ps"],
                    old["version"],
                    old["updated_at"],
                )
            )
            db.execute(
                "INSERT INTO rechecked VALUES (?, ?)",
                (old["key"], current.get(old["key"]) != previous),
            )
        db.commit()

    batch = []
    async for row in comparison_rows(db):
        family, status = row_status(row)
        if needs_review(status, family):
            batch.append(row)
            if len(batch) == BATCH_SIZE:
                await check_batch(batch)
                batch = []
    if batch:
        await check_batch(batch)


async def write_report(db, path):
    totals, families = Counter(), {}
    unresolved = races = 0
    # Caller creates a private directory; exclusive mode prevents overwrites.
    with path.open("x", encoding="utf-8") as out:
        async for row in comparison_rows(db):
            family, status = row_status(row)
            review = needs_review(status, family)
            checked = (
                db.execute(
                    "SELECT changed FROM rechecked WHERE key=?", (row["key"],)
                ).fetchone()
                if review
                else None
            )
            if review and checked is None:
                raise AuditError("Candidate recheck is incomplete")
            changed = bool(checked[0]) if checked else False
            unresolved += int(review)
            races += int(changed)
            totals[status] += 1
            families.setdefault(family, Counter())[status] += 1
            if status in ("match_live", "match_tombstone") and not review:
                continue
            # Dedicated protected audit artifact, never stdout/general logs.
            # Keys are needed to investigate #1649; values stay in disposable
            # staging only. Base64 preserves identifiers without CSV injection.
            record = {
                "key_base64": base64.b64encode(row["key"].encode()).decode(),
                "family": family,
                "status": status,
                "requires_review": review,
                "nats_state": row["ns"],
                "nats_sequence": row["seq"],
                "postgres_state": row["ps"],
                "postgres_version": row["version"],
                "postgres_updated_at": row["updated_at"],
                "changed_during_recheck": changed,
                "policy": NON_REQUIRED.get(family),
            }
            out.write(json.dumps(record) + "\n")
    return {
        "counts": dict(totals),
        "families": families,
        "requires_review": unresolved,
        "changed_during_recheck": races,
    }


async def audit(args, summary):
    import nats
    import psycopg

    # Never let library connection errors print credential-bearing URLs.
    async def silent_error(_error):
        pass

    options = {
        "servers": os.environ["NATS_URL"].split(","),
        "name": "v1-mappings-retirement-audit",
        "connect_timeout": args.timeout,
        "allow_reconnect": False,
        "error_cb": silent_error,
    }
    if os.environ.get("NATS_CREDS"):
        options["user_credentials"] = os.environ["NATS_CREDS"]
    nc = await nats.connect(**options)
    try:
        summary["phase"] = "nats_scan"
        js = nc.jetstream(timeout=args.timeout, prefix=args.js_api_prefix)
        before_info = await js.stream_info(STREAM)
        before = stream_fingerprint(before_info)
        # Store only safe coverage fields in summary, not server configuration.
        summary["nats"] = {
            key: before[key]
            for key in ("messages", "bytes", "first_seq", "last_seq", "created")
        }
        if not before["messages"]:
            raise AuditError(
                "Legacy stream is empty; cannot establish a migration baseline"
            )
        # Private run-specific scratch directory, automatically removed on all
        # exits. SQLite temp files are directed here too (set before opening).
        with tempfile.TemporaryDirectory(
            prefix="mapping-audit-", dir=args.output_dir
        ) as scratch:
            os.environ["SQLITE_TMPDIR"] = scratch
            with closing(open_stage(Path(scratch) / "stage.sqlite")) as db:
                visits = await collect_nats(db, js, before_info, args.workers)
                summary["nats"]["visits"] = visits
                check_stream(
                    before, stream_fingerprint(await js.stream_info(STREAM)), visits
                )
                # Independent of the application config; no writable clients,
                # subscriptions, pools or schema migrations are initialized.
                summary["phase"] = "postgres_snapshot"
                async with await psycopg.AsyncConnection.connect(
                    os.environ["V1_MAPPINGS_DATABASE_URL"],
                    autocommit=True,
                    connect_timeout=max(1, math.ceil(args.timeout)),
                    application_name="v1-mappings-retirement-audit",
                    options=(
                        "-c default_transaction_read_only=on "
                        f"-c statement_timeout={math.ceil(args.timeout * 1000)}"
                    ),
                ) as conn:
                    summary["postgres"] = await collect_postgres(db, conn)
                    summary["phase"] = "postgres_recheck"
                    await recheck_postgres(db, conn)
                partial = args.output_dir / "discrepancies.partial.jsonl"
                summary["phase"] = "report"
                summary.update(await write_report(db, partial))
                summary["phase"] = "nats_final_validation"
                check_stream(
                    before, stream_fingerprint(await js.stream_info(STREAM)), visits
                )
                partial.rename(args.output_dir / "discrepancies.jsonl")
    finally:
        await nc.close()


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--environment", required=True, choices=("dev", "prod"))
    parser.add_argument(
        "--cutover-at",
        required=True,
        help="Known Postgres-only cutover, ISO-8601 with timezone (context only)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="NEW private directory on a disk with ample scratch space",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=2,
        help="Concurrent NATS requests, 1..16 (default: 2)",
    )
    parser.add_argument(
        "--timeout", type=float, default=120, help="Per-operation timeout in seconds"
    )
    parser.add_argument(
        "--js-api-prefix",
        default="$JS.API",
        help="JetStream API prefix, including domain if required",
    )
    args = parser.parse_args(argv)
    try:
        cutover = datetime.fromisoformat(args.cutover_at)
        if cutover.tzinfo is None or cutover > datetime.now(UTC):
            raise ValueError
    except ValueError:
        parser.error("--cutover-at must be a past ISO-8601 timestamp with timezone")
    args.cutover_at = cutover.isoformat()
    if (
        not 1 <= args.workers <= 16
        or not math.isfinite(args.timeout)
        or args.timeout <= 0
    ):
        parser.error(
            "--workers must be 1..16 and --timeout must be positive and finite"
        )
    if not all(os.environ.get(key) for key in ("NATS_URL", "V1_MAPPINGS_DATABASE_URL")):
        parser.error(
            "NATS_URL and V1_MAPPINGS_DATABASE_URL must be supplied "
            "through the environment"
        )
    return args


def main(argv=None):
    args = parse_args(argv)
    os.umask(0o077)
    logging.disable(logging.CRITICAL)
    try:
        args.output_dir.mkdir(mode=0o700, parents=False, exist_ok=False)
    except OSError:
        print("Cannot create a new private output directory", file=sys.stderr)
        return 2
    summary = {
        "complete": False,
        "environment": args.environment,
        "cutover_at": args.cutover_at,
        "started_at": now(),
        "workers": args.workers,
        "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "phase": "connect",
    }
    code = 2
    try:
        asyncio.run(audit(args, summary))
        summary["complete"] = True
        summary["phase"] = "finished"
        code = 1 if summary["requires_review"] else 0
    # Catch all driver errors so their text cannot expose credentials or DSNs.
    except (Exception, KeyboardInterrupt) as exc:  # noqa: BLE001
        summary["error"] = (
            str(exc)
            if isinstance(exc, AuditError)
            else "Audit failed or interrupted; results are incomplete"
        )
    summary["finished_at"] = now()
    summary["exit_code"] = code
    try:
        with (args.output_dir / "summary.json").open("x", encoding="utf-8") as out:
            json.dump(summary, out, indent=2)
            out.write("\n")
    except OSError:
        print("Cannot save audit summary; audit is incomplete", file=sys.stderr)
        return 2
    # Summary is strictly aggregate/allowlisted: never source identifiers,
    # raw values, connection strings or arbitrary exception messages.
    print(json.dumps(summary, sort_keys=True))
    return code


if __name__ == "__main__":
    sys.exit(main())

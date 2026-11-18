# Copyright The Linux Foundation and each contributor to LFX.
# SPDX-License-Identifier: MIT

import asyncio
import base64
import builtins
import io
import json
import os
import tempfile
import unittest
from contextlib import asynccontextmanager, redirect_stderr, redirect_stdout
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import audit_v1_mappings as audit

STAMP = datetime(2026, 1, 1, tzinfo=UTC)


class FakePostgres:
    def __init__(self, rows):
        self.rows = rows
        self.commands = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    @asynccontextmanager
    async def transaction(self):
        self.commands.append("BEGIN")
        yield
        self.commands.append("COMMIT")

    async def execute(self, query, params=None):
        self.commands.append(query.strip())
        if "transaction_timestamp()" in query:

            async def fetchone():
                return (STAMP,)

            return SimpleNamespace(fetchone=fetchone)
        if "ANY" in query:

            async def fetchall():
                return [row for row in self.rows if row[0] in params[0]]

            return SimpleNamespace(fetchall=fetchall)
        return None

    @asynccontextmanager
    async def cursor(self, name):
        self.commands.append(name)
        remaining = list(self.rows)

        async def fetchmany(size):
            batch = remaining[:size]
            del remaining[:size]
            return batch

        yield SimpleNamespace(execute=self.execute, fetchmany=fetchmany)


class ClassificationTests(unittest.TestCase):
    def test_state_matrix(self):
        cases = [
            ("live", b"old", "live", b"old", "match_live"),
            ("live", b"", "live", b"", "match_live"),
            ("live", b"", None, None, "missing_postgres"),
            ("live", b"old", "live", b"new", "value_changed"),
            ("live", b"old", "tombstone", b"", "postgres_tombstoned_since_nats"),
            ("live", b"old", None, None, "missing_postgres"),
            ("tombstone", b"!del", "tombstone", b"", "match_tombstone"),
            ("tombstone", b"!del", None, None, "missing_tombstone"),
            ("tombstone", b"!del", "live", b"new", "nats_tombstone_postgres_live"),
            (None, None, "tombstone", b"", "postgres_only_tombstone"),
            (None, None, "live", b"new", "postgres_only_live"),
            ("deleted", b"", "tombstone", b"", "postgres_only_tombstone"),
            ("deleted", b"", "live", b"new", "postgres_only_live"),
            ("deleted", b"", None, None, "native_delete_absent"),
        ]
        for ns, nv, ps, pv, expected in cases:
            with self.subTest(expected=expected, ns=ns, ps=ps):
                self.assertEqual(
                    audit.classify(ns, nv, ps, pv, "project.sfid."), expected
                )

    def test_operational_exemptions_do_not_exempt_tombstones(self):
        for family in audit.NON_REQUIRED:
            self.assertEqual(
                audit.classify("live", b"", None, None, family),
                "expected_operational_absence",
            )
            self.assertEqual(
                audit.classify("tombstone", b"!del", None, None, family),
                "missing_tombstone",
            )
        for family in audit.REVIEW_FAMILIES:
            self.assertEqual(
                audit.classify("live", b"", None, None, family), "missing_postgres"
            )

    def test_unknowns_are_not_silently_excluded_or_exposed_in_family_counts(self):
        for key in ("unrecognized.testuser01@example.test", "project.sfid.", "pending"):
            self.assertEqual(audit.family_for(key), "unknown")
        for status in ("match_live", "postgres_only_live", "native_delete_absent"):
            self.assertTrue(audit.needs_review(status, "unknown"))
        self.assertFalse(audit.needs_review("postgres_only_tombstone", "project.sfid."))


class StagingTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db = audit.open_stage(Path(self.tmp.name) / "stage.sqlite")
        self.addCleanup(self.db.close)

    def add_pg(self, key, value=b"value", state="live", version=1):
        self.db.execute(
            "INSERT INTO pg VALUES (?, ?, ?, ?, ?)",
            (key, value, state, version, STAMP.isoformat()),
        )
        self.db.commit()

    async def test_lww_handles_cross_worker_order_and_native_delete_history(self):
        for seq, state, value in (
            (10, "live", b"old"),
            (30, "deleted", b""),
            (20, "live", b"stale"),
        ):
            audit.stage_nats(self.db, "project.sfid.a", value, state, seq)
        row = await anext(audit.comparison_rows(self.db))
        self.assertEqual((row["seq"], row["ns"]), (30, "deleted"))
        self.assertEqual(audit.row_status(row)[1], "native_delete_absent")
        audit.stage_nats(self.db, "project.sfid.a", b"!del", "tombstone", 40)
        self.assertEqual(
            (await anext(audit.comparison_rows(self.db)))["ns"], "tombstone"
        )

    async def test_full_join_preserves_empty_values_and_pg_only_tombstones(self):
        audit.stage_nats(self.db, "project.sfid.match", b"", "live", 1)
        audit.stage_nats(self.db, "project.sfid.missing", b"value", "live", 2)
        self.add_pg("project.sfid.match", b"")
        self.add_pg("project.uid.deleted", b"", "tombstone")
        statuses = {
            row["key"]: audit.row_status(row)[1]
            async for row in audit.comparison_rows(self.db)
        }
        self.assertEqual(
            statuses,
            {
                "project.sfid.match": "match_live",
                "project.sfid.missing": "missing_postgres",
                "project.uid.deleted": "postgres_only_tombstone",
            },
        )

    async def test_comparison_pages_cover_mixed_keys_once_at_boundaries(self):
        nats_keys = {f"project.sfid.{index:02d}" for index in range(0, 13, 2)}
        pg_keys = {f"project.sfid.{index:02d}" for index in range(3, 15)}
        for index, key in enumerate(sorted(nats_keys), start=1):
            audit.stage_nats(self.db, key, b"value", "live", index)
        for key in sorted(pg_keys):
            self.add_pg(key)
        for size in (1, 2, 3, 7, 30):
            with self.subTest(page_size=size), patch.object(audit, "BATCH_SIZE", size):
                rows = [row async for row in audit.comparison_rows(self.db)]
                self.assertEqual(len(rows), len(nats_keys | pg_keys))
                statuses = {row["key"]: audit.row_status(row)[1] for row in rows}
                self.assertEqual(set(statuses), nats_keys | pg_keys)
                for key in nats_keys | pg_keys:
                    expected = (
                        "match_live"
                        if key in nats_keys & pg_keys
                        else (
                            "missing_postgres"
                            if key in nats_keys
                            else "postgres_only_live"
                        )
                    )
                    self.assertEqual(statuses[key], expected)

    async def test_postgres_snapshot_is_read_only_and_streamed(self):
        conn = FakePostgres([("project.sfid.a", "", False, 8, STAMP)])
        with patch.object(audit, "BATCH_SIZE", 1):
            result = await audit.collect_postgres(self.db, conn)
        self.assertEqual(result["rows"], 1)
        self.assertIn(
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY", conn.commands
        )
        self.assertTrue(
            any("FROM public.v1_mappings" in query for query in conn.commands)
        )
        self.assertEqual((await anext(audit.comparison_rows(self.db)))["pv"], b"")
        self.assertFalse(
            any(
                query.startswith(("INSERT", "UPDATE", "DELETE", "CREATE"))
                for query in conn.commands
            )
        )

    async def test_recheck_detects_creation_update_and_hard_deletion(self):
        for index, name in enumerate(
            ("created", "updated", "removed", "unchanged"), start=1
        ):
            audit.stage_nats(self.db, "project.sfid." + name, b"nats", "live", index)
        self.add_pg("project.sfid.updated", b"before")
        self.add_pg("project.sfid.removed", b"before")
        conn = FakePostgres(
            [
                ("project.sfid.created", "new", False, 20, STAMP),
                ("project.sfid.updated", "after", False, 21, STAMP),
            ]
        )
        with patch.object(audit, "BATCH_SIZE", 1):
            await audit.recheck_postgres(self.db, conn)
        changed = dict(self.db.execute("SELECT key, changed FROM rechecked"))
        self.assertEqual(
            changed,
            {
                "project.sfid.created": 1,
                "project.sfid.updated": 1,
                "project.sfid.removed": 1,
                "project.sfid.unchanged": 0,
            },
        )
        self.assertIn("SET TRANSACTION READ ONLY", conn.commands)
        path = Path(self.tmp.name) / "report.jsonl"
        summary = await audit.write_report(self.db, path)
        self.assertEqual(summary["requires_review"], 4)
        self.assertEqual(summary["changed_during_recheck"], 3)
        report = [json.loads(line) for line in path.read_text().splitlines()]
        self.assertEqual(len(report), 4)
        self.assertEqual(
            base64.b64decode(report[0]["key_base64"]).decode(), "project.sfid.created"
        )
        self.assertNotIn("before", path.read_text())

    async def test_report_refuses_missing_recheck_and_overwrites(self):
        audit.stage_nats(self.db, "project.sfid.a", b"value", "live", 1)
        path = Path(self.tmp.name) / "report.jsonl"
        with self.assertRaises(audit.AuditError):
            await audit.write_report(self.db, path)
        with self.assertRaises(FileExistsError):
            await audit.write_report(self.db, path)

    async def test_matching_unknown_family_still_requires_review(self):
        audit.stage_nats(self.db, "unknown.testuser01", b"same", "live", 1)
        self.add_pg("unknown.testuser01", b"same")
        conn = FakePostgres([("unknown.testuser01", "same", False, 1, STAMP)])
        await audit.recheck_postgres(self.db, conn)
        summary = await audit.write_report(
            self.db, Path(self.tmp.name) / "report.jsonl"
        )
        self.assertEqual(summary["requires_review"], 1)
        self.assertEqual(summary["families"], {"unknown": {"match_live": 1}})


class ScanTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db = audit.open_stage(Path(self.tmp.name) / "stage.sqlite")
        self.addCleanup(self.db.close)

    @staticmethod
    def message(seq, key="project.sfid.a", data=b"", operation=None):
        return SimpleNamespace(
            seq=seq,
            subject=audit.PREFIX + key,
            data=data,
            headers={"KV-Operation": operation} if operation else None,
        )

    async def test_parallel_sequence_ranges_cover_once_and_normalize_states(self):
        from nats.js.errors import NotFoundError

        messages = [
            self.message(1, data=b"old"),
            self.message(3, operation="DEL"),
            self.message(6, "project.sfid.b", b"!del"),
            self.message(7, "project.sfid.empty", None),
            self.message(10, "project.sfid.purged", operation="PURGE"),
        ]
        calls = []

        async def get_msg(stream, **kwargs):
            calls.append(kwargs)
            self.assertEqual(stream, audit.STREAM)
            await asyncio.sleep(0)  # Interleave workers deterministically.
            for msg in messages:
                if msg.seq >= kwargs["seq"]:
                    return msg
            raise NotFoundError(err_code=10037)

        js = SimpleNamespace(get_msg=get_msg)
        info = SimpleNamespace(state=SimpleNamespace(last_seq=10))
        visits = await audit.collect_nats(self.db, js, info, 3)
        self.assertEqual(visits, 5)
        rows = {row["key"]: row async for row in audit.comparison_rows(self.db)}
        self.assertEqual(rows["project.sfid.a"]["ns"], "deleted")
        self.assertEqual(rows["project.sfid.b"]["ns"], "tombstone")
        self.assertEqual(rows["project.sfid.empty"]["nv"], b"")
        self.assertEqual(rows["project.sfid.purged"]["ns"], "deleted")
        self.assertTrue(
            all(call["next"] and call["subject"] == audit.FILTER for call in calls)
        )

    async def test_stream_not_found_and_timeout_are_not_clean_eof(self):
        from nats.js.errors import NotFoundError

        for error in (NotFoundError(err_code=10059), TimeoutError()):

            async def fail(*_args, error=error, **_kwargs):
                raise error

            with self.assertRaises(builtins.ExceptionGroup):
                await audit.collect_nats(
                    self.db,
                    SimpleNamespace(get_msg=fail),
                    SimpleNamespace(state=SimpleNamespace(last_seq=5)),
                    2,
                )

    async def test_invalid_operation_subject_and_regressing_sequence_fail_closed(self):
        for msg in (
            self.message(1, operation="OTHER"),
            SimpleNamespace(seq=1, subject="unrelated", headers=None, data=b""),
            self.message(0),
        ):

            async def get_msg(*_args, msg=msg, **_kwargs):
                return msg

            with self.assertRaises(builtins.ExceptionGroup):
                await audit.collect_nats(
                    self.db,
                    SimpleNamespace(get_msg=get_msg),
                    SimpleNamespace(state=SimpleNamespace(last_seq=1)),
                    1,
                )

    def test_coverage_checks(self):
        before = {"messages": 2, "lost": None, "last_seq": 5, "bytes": 20}
        audit.check_stream(before, dict(before), 2)
        for after, visits in (
            (dict(before), 1),
            ({**before, "bytes": 21}, 2),
            ({**before, "last_seq": 6}, 2),
        ):
            with self.assertRaises(audit.AuditError):
                audit.check_stream(before, after, visits)
        with self.assertRaises(audit.AuditError):
            audit.check_stream({"messages": 0}, {"messages": 0}, 0)
        with self.assertRaises(audit.AuditError):
            audit.check_stream({**before, "lost": {"msgs": [1]}}, dict(before), 2)


class CliTests(unittest.TestCase):
    def test_invalid_args_and_missing_connections_fail_before_creating_output(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "audit"
            base = [
                "--environment",
                "dev",
                "--cutover-at",
                STAMP.isoformat(),
                "--output-dir",
                str(target),
            ]
            env = {
                "NATS_URL": "nats://127.0.0.1:4222",
                "V1_MAPPINGS_DATABASE_URL": "postgresql://localhost/audit",
            }
            for extra in (
                ["--workers", "0"],
                ["--timeout", "nan"],
                ["--cutover-at", "2026-01-01"],
                ["--cutover-at", "9999-01-01T00:00:00Z"],
            ):
                with (
                    patch.dict(os.environ, env, clear=True),
                    redirect_stderr(io.StringIO()),
                    self.assertRaises(SystemExit) as caught,
                ):
                    audit.parse_args(base + extra)
                self.assertEqual(caught.exception.code, 2)
                self.assertFalse(target.exists())
            with (
                patch.dict(os.environ, {}, clear=True),
                redirect_stderr(io.StringIO()),
                self.assertRaises(SystemExit) as caught,
            ):
                audit.parse_args(base)
            self.assertEqual(caught.exception.code, 2)

    def test_failure_redacts_driver_error_and_creates_private_incomplete_summary(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "audit"
            out = io.StringIO()
            args = SimpleNamespace(
                output_dir=target,
                environment="dev",
                cutover_at=STAMP.isoformat(),
                workers=1,
            )

            async def fail(*_args):
                raise RuntimeError("credential-secret testuser01@example.test")

            mask = os.umask(0o077)
            try:
                with (
                    patch.object(audit, "parse_args", return_value=args),
                    patch.object(audit, "audit", fail),
                    redirect_stdout(out),
                ):
                    self.assertEqual(audit.main([]), 2)
            finally:
                os.umask(mask)
            summary = json.loads((target / "summary.json").read_text())
            self.assertFalse(summary["complete"])
            self.assertNotIn("credential-secret", out.getvalue())
            self.assertNotIn("testuser01", out.getvalue())
            self.assertEqual(target.stat().st_mode & 0o777, 0o700)
            self.assertEqual((target / "summary.json").stat().st_mode & 0o777, 0o600)

    def test_success_and_findings_have_distinct_exit_codes(self):
        for findings, expected in ((0, 0), (1, 1)):
            with tempfile.TemporaryDirectory() as directory:
                args = SimpleNamespace(
                    output_dir=Path(directory) / "audit",
                    environment="dev",
                    cutover_at=STAMP.isoformat(),
                    workers=1,
                )

                async def succeed(_args, summary, findings=findings):
                    summary["requires_review"] = findings

                mask = os.umask(0o077)
                try:
                    with (
                        patch.object(audit, "parse_args", return_value=args),
                        patch.object(audit, "audit", succeed),
                        redirect_stdout(io.StringIO()),
                    ):
                        self.assertEqual(audit.main([]), expected)
                finally:
                    os.umask(mask)
                self.assertTrue(
                    json.loads((args.output_dir / "summary.json").read_text())[
                        "complete"
                    ]
                )


class OrchestrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_pipeline_with_fake_remote_clients_and_changed_stream(self):
        from nats.js.api import StreamConfig, StreamInfo, StreamState

        for changed in (False, True):
            with (
                self.subTest(changed=changed),
                tempfile.TemporaryDirectory() as directory,
            ):
                out = Path(directory)
                args = SimpleNamespace(
                    output_dir=out,
                    workers=1,
                    timeout=1,
                    js_api_prefix="$JS.fixture.API",
                )
                before = StreamInfo(
                    config=StreamConfig(name=audit.STREAM, subjects=[audit.FILTER]),
                    state=StreamState(
                        messages=1, bytes=50, first_seq=1, last_seq=1, consumer_count=0
                    ),
                    created=STAMP,
                )
                after = StreamInfo(
                    config=before.config,
                    state=StreamState(
                        messages=2 if changed else 1,
                        bytes=50,
                        first_seq=1,
                        last_seq=2 if changed else 1,
                        consumer_count=8,
                    ),
                    created=STAMP,
                )
                js = SimpleNamespace(
                    stream_info=AsyncMock(side_effect=[before, before, after]),
                    get_msg=AsyncMock(
                        return_value=SimpleNamespace(
                            seq=1,
                            subject=audit.PREFIX + "project.sfid.a",
                            data=b"old",
                            headers=None,
                        )
                    ),
                )
                nc = SimpleNamespace(
                    jetstream=lambda js=js, **_kwargs: js, close=AsyncMock()
                )
                conn = FakePostgres([("project.sfid.a", "", True, 90, STAMP)])
                summary = {}
                with (
                    patch.dict(
                        os.environ,
                        {
                            "NATS_URL": "nats://127.0.0.1:4222",
                            "V1_MAPPINGS_DATABASE_URL": "postgresql://localhost/audit",
                            "NATS_CREDS": "/fixture/test.creds",
                        },
                    ),
                    patch("nats.connect", AsyncMock(return_value=nc)) as connect_nats,
                    patch(
                        "psycopg.AsyncConnection.connect", AsyncMock(return_value=conn)
                    ) as connect_pg,
                ):
                    if changed:
                        with self.assertRaises(audit.AuditError):
                            await audit.audit(args, summary)
                    else:
                        await audit.audit(args, summary)
                    self.assertIn(
                        "default_transaction_read_only=on",
                        connect_pg.call_args.kwargs["options"],
                    )
                    self.assertFalse(connect_nats.call_args.kwargs["allow_reconnect"])
                    self.assertEqual(
                        connect_nats.call_args.kwargs["user_credentials"],
                        "/fixture/test.creds",
                    )
                nc.close.assert_awaited_once()
                self.assertFalse(any(out.glob("mapping-audit-*")))
                self.assertEqual(summary["nats"]["visits"], 1)
                self.assertEqual(summary["requires_review"], 0)
                self.assertEqual((out / "discrepancies.jsonl").exists(), not changed)
                self.assertEqual(
                    (out / "discrepancies.partial.jsonl").exists(), changed
                )
                self.assertNotIn("config", summary["nats"])

    async def test_long_local_passes_yield_for_heartbeats_and_cancellation(self):
        with tempfile.TemporaryDirectory() as directory:
            db = audit.open_stage(Path(directory) / "stage.sqlite")
            self.addCleanup(db.close)
            for index in range(10):
                key = f"project.sfid.{index}"
                audit.stage_nats(db, key, b"value", "live", index + 1)
                db.execute(
                    "INSERT INTO pg VALUES (?, ?, ?, ?, ?)",
                    (key, b"value", "live", 1, STAMP.isoformat()),
                )
            db.commit()
            ticks = []
            asyncio.get_running_loop().call_soon(ticks.append, "recheck")
            with patch.object(audit, "BATCH_SIZE", 1):
                await audit.recheck_postgres(db, FakePostgres([]))
            self.assertIn("recheck", ticks)
            asyncio.get_running_loop().call_soon(ticks.append, "report")
            with patch.object(audit, "BATCH_SIZE", 1):
                await audit.write_report(db, Path(directory) / "report.jsonl")
            self.assertIn("report", ticks)
            task = asyncio.create_task(
                audit.write_report(db, Path(directory) / "interrupted.jsonl")
            )
            asyncio.get_running_loop().call_soon(task.cancel)
            with (
                patch.object(audit, "BATCH_SIZE", 1),
                self.assertRaises(asyncio.CancelledError),
            ):
                await task


class PagingTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db = audit.open_stage(Path(self.tmp.name) / "stage.sqlite")
        self.addCleanup(self.db.close)
        for index in range(21):
            key = f"project.sfid.{index:04d}"
            audit.stage_nats(self.db, key, b"value", "live", index + 1)
            self.db.execute(
                "INSERT INTO pg VALUES (?, ?, ?, ?, ?)",
                (key, b"value", "live", index + 1, STAMP.isoformat()),
            )
        self.db.commit()

    async def run_pass(self, name):
        if name == "recheck":
            conn = FakePostgres([])
            await audit.recheck_postgres(self.db, conn)
            self.assertEqual(conn.commands, [])
        else:
            summary = await audit.write_report(
                self.db, Path(self.tmp.name) / f"{name}.jsonl"
            )
            self.assertEqual(summary["counts"], {"match_live": 21})

    async def test_each_empty_antijoin_input_page_yields_in_both_passes(self):
        # All PG keys match: the anti-join returns ZERO rows. Schedule an
        # event-loop callback from each SQL execution and ensure it runs before
        # the next input page, not merely during the earlier NATS-side pass.
        for name in ("recheck", "report"):
            queries, ticks, interleaved = [], [], []

            def trace(sql, interleaved=interleaved, ticks=ticks, queries=queries):
                if "FROM pg p LEFT JOIN nats n" in sql:
                    interleaved.append(len(ticks) == len(queries))
                    queries.append(sql)
                    asyncio.get_running_loop().call_soon(ticks.append, None)

            self.db.set_trace_callback(trace)
            try:
                with patch.object(audit, "BATCH_SIZE", 5):
                    await self.run_pass(name)
            finally:
                self.db.set_trace_callback(None)
            self.assertEqual(len(queries), 5)
            self.assertEqual(len(ticks), 5)
            self.assertTrue(all(interleaved))
            self.assertTrue(
                all("p.key >=" in sql and "p.key <=" in sql for sql in queries)
            )

    async def test_cancellation_stops_after_first_empty_antijoin_page_in_both_passes(
        self,
    ):
        for name in ("recheck", "report"):
            pages = []
            task = asyncio.create_task(self.run_pass(name))

            def trace(sql, task=task, pages=pages):
                if "FROM pg p LEFT JOIN nats n" in sql:
                    pages.append(sql)
                    asyncio.get_running_loop().call_soon(task.cancel)

            self.db.set_trace_callback(trace)
            try:
                with (
                    patch.object(audit, "BATCH_SIZE", 5),
                    self.assertRaises(asyncio.CancelledError),
                ):
                    await task
            finally:
                self.db.set_trace_callback(None)
            self.assertEqual(len(pages), 1)


class CredentialsTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_client_reads_credentials_and_signs_server_nonce(self):
        import nats
        import nkeys

        # Disposable synthetic key material, never real credentials or logs.
        seed = nkeys.encode_seed(os.urandom(32), nkeys.PREFIX_BYTE_USER)
        keypair = nkeys.from_seed(seed)
        nonce = "fixture-server-nonce"
        authenticated = asyncio.get_running_loop().create_future()
        finished = asyncio.get_running_loop().create_future()

        async def handshake(reader, writer):
            try:
                info = {
                    "server_id": "fixture",
                    "version": "2.12.6",
                    "proto": 1,
                    "auth_required": True,
                    "nonce": nonce,
                }
                writer.write(b"INFO " + json.dumps(info).encode() + b"\r\n")
                await writer.drain()
                line = await asyncio.wait_for(reader.readline(), timeout=2)
                self.assertTrue(line.startswith(b"CONNECT "))
                payload = json.loads(line[len(b"CONNECT ") :])
                self.assertEqual(payload["jwt"], "fixture-user-jwt")
                signature = base64.urlsafe_b64decode(payload["sig"] + "===")
                self.assertTrue(keypair.verify(nonce.encode(), signature))
                self.assertEqual(
                    await asyncio.wait_for(reader.readline(), timeout=2), b"PING\r\n"
                )
                writer.write(b"PONG\r\n")
                await writer.drain()
                authenticated.set_result(None)
                await reader.read()
            except Exception as exc:
                if not authenticated.done():
                    authenticated.set_exception(exc)
                else:
                    raise
            finally:
                writer.close()
                await writer.wait_closed()
                finished.set_result(None)

        try:
            with tempfile.TemporaryDirectory() as directory:
                creds = Path(directory) / "fixture.creds"
                with creds.open("xb") as out:
                    creds.chmod(0o600)
                    out.write(
                        b"-----BEGIN NATS USER JWT-----\nfixture-user-jwt\n"
                        b"------END NATS USER JWT------\n"
                        b"-----BEGIN USER NKEY SEED-----\n" + seed + b"\n"
                        b"------END USER NKEY SEED------\n"
                    )
                async with await asyncio.start_server(
                    handshake, "127.0.0.1", 0
                ) as server:
                    port = server.sockets[0].getsockname()[1]
                    # Do not mock nats.connect: this is the dependency-sensitive
                    # credentials-file setup, parsing and signing code path.
                    nc = await asyncio.wait_for(
                        nats.connect(
                            f"nats://127.0.0.1:{port}",
                            user_credentials=str(creds),
                            connect_timeout=1,
                            allow_reconnect=False,
                        ),
                        timeout=3,
                    )
                    try:
                        await asyncio.wait_for(authenticated, timeout=2)
                        self.assertTrue(nc.is_connected)
                    finally:
                        await nc.close()
                    await asyncio.wait_for(finished, timeout=2)
        finally:
            keypair.wipe()


if __name__ == "__main__":
    unittest.main()

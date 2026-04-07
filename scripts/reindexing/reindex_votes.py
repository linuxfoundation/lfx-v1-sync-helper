#!/usr/bin/env python3
# Copyright The Linux Foundation and each contributor to LFX.
# SPDX-License-Identifier: MIT
"""
Votes Reindexer

Queries OpenSearch for all objects of type "vote" and "vote_response" in the
resources index, then reindexes each in the v1-objects NATS KV bucket:
  - vote         -> itx-poll.<object_id>
  - vote_response -> itx-poll-vote.<object_id>

All itx-poll entries are reindexed before itx-poll-vote entries.
"""

import argparse
import asyncio
import sys
from dataclasses import dataclass, field
from typing import List

import httpx
from nats.aio.client import Client as NATS
from nats.js.errors import KeyNotFoundError


OPENSEARCH_URL = "http://localhost:9200"
INDEX = "resources"
PAGE_SIZE = 500
KV_BUCKET = "v1-objects"


@dataclass
class Stats:
    total: int = 0
    reindexed: int = 0
    missing: int = 0
    errors: int = 0
    missing_keys: List[str] = field(default_factory=list)


def fetch_all_by_type(client: httpx.Client, object_type: str) -> List[str]:
    """Fetch all object_ids of the given type from OpenSearch using scroll pagination."""
    ids: List[str] = []
    scroll_id = None

    try:
        response = client.post(
            f"{OPENSEARCH_URL}/{INDEX}/_search",
            params={"scroll": "1m"},
            json={
                "query": {"term": {"object_type": object_type}},
                "size": PAGE_SIZE,
                "_source": ["object_id"],
                "sort": ["_doc"],
            },
        )
        response.raise_for_status()
        data = response.json()

        scroll_id = data.get("_scroll_id")
        hits = data.get("hits", {}).get("hits", [])

        while hits:
            for hit in hits:
                oid = (hit.get("_source") or {}).get("object_id")
                if oid:
                    ids.append(oid)

            if not scroll_id:
                break

            response = client.post(
                f"{OPENSEARCH_URL}/_search/scroll",
                json={"scroll": "1m", "scroll_id": scroll_id},
            )
            response.raise_for_status()
            data = response.json()

            scroll_id = data.get("_scroll_id")
            hits = data.get("hits", {}).get("hits", [])
    finally:
        if scroll_id:
            try:
                client.delete(
                    f"{OPENSEARCH_URL}/_search/scroll",
                    json={"scroll_id": [scroll_id]},
                )
            except Exception:
                pass

    return ids


async def reindex_entry(kv, kv_prefix: str, item_id: str, dry_run: bool, stats: Stats):
    """Fetch entry from NATS KV and write it back to trigger reindex."""
    kv_key = f"{kv_prefix}.{item_id}"
    stats.total += 1

    try:
        entry = await kv.get(kv_key)

        if not dry_run:
            await kv.put(kv_key, entry.value)
            stats.reindexed += 1
            print(f"  reindexed: {kv_key}")
        else:
            print(f"  found: {kv_key}")

    except KeyNotFoundError:
        print(f"  MISSING: {kv_key}")
        stats.missing += 1
        stats.missing_keys.append(item_id)
    except Exception as e:
        print(f"  ERROR {kv_key}: {e}")
        stats.errors += 1


async def run(nats_url: str, dry_run: bool):
    print(f"\n{'=' * 60}")
    print("Votes Reindexer")
    print(f"Mode       : {'DRY RUN' if dry_run else 'REINDEX'}")
    print(f"{'=' * 60}\n")

    # Fetch all IDs from OpenSearch first
    with httpx.Client(timeout=30) as client:
        print("--- Fetching 'vote' objects from OpenSearch ---")
        vote_ids = fetch_all_by_type(client, "vote")
        print(f"  Found {len(vote_ids)} vote(s)")

        print("--- Fetching 'vote_response' objects from OpenSearch ---")
        vote_response_ids = fetch_all_by_type(client, "vote_response")
        print(f"  Found {len(vote_response_ids)} vote_response(s)")

    # Connect to NATS
    nc = NATS()
    await nc.connect(servers=[nats_url])
    js = nc.jetstream()
    kv = await js.key_value(bucket=KV_BUCKET)
    print(f"\nConnected to NATS at {nats_url}")

    poll_stats = Stats()
    vote_stats = Stats()

    try:
        # Reindex itx-poll entries first
        print(f"\n--- Reindexing itx-poll ({len(vote_ids)} entries) ---")
        for oid in vote_ids:
            await reindex_entry(kv, "itx-poll", oid, dry_run, poll_stats)

        # Then reindex itx-poll-vote entries
        print(f"\n--- Reindexing itx-poll-vote ({len(vote_response_ids)} entries) ---")
        for oid in vote_response_ids:
            await reindex_entry(kv, "itx-poll-vote", oid, dry_run, vote_stats)

    finally:
        await nc.close()

    print_summary({"itx-poll": poll_stats, "itx-poll-vote": vote_stats}, dry_run)


def print_summary(stats_by_prefix: dict, dry_run: bool):
    print(f"\n{'=' * 60}")
    print("SUMMARY REPORT")
    print(f"{'=' * 60}\n")

    grand_total = grand_missing = grand_reindexed = grand_errors = 0

    for prefix, s in stats_by_prefix.items():
        grand_total += s.total
        grand_missing += s.missing
        grand_reindexed += s.reindexed
        grand_errors += s.errors

        status = "OK" if s.missing == 0 and s.errors == 0 else "ISSUES"
        print(f"{prefix}:")
        print(f"  Total checked : {s.total}")
        print(f"  Missing       : {s.missing}")
        if not dry_run:
            print(f"  Reindexed     : {s.reindexed}")
        if s.errors:
            print(f"  Errors        : {s.errors}")
        print(f"  Status        : {status}")
        if s.missing_keys:
            shown = s.missing_keys[:10]
            print(f"  Missing keys  : {', '.join(str(k) for k in shown)}")
            if len(s.missing_keys) > 10:
                print(f"    ... and {len(s.missing_keys) - 10} more")
        print()

    print(f"{'=' * 60}")
    print("TOTALS:")
    print(f"  Total checked : {grand_total}")
    print(f"  Missing       : {grand_missing}")
    if not dry_run:
        print(f"  Reindexed     : {grand_reindexed}")
    if grand_errors:
        print(f"  Errors        : {grand_errors}")
    overall = (
        "ALL OK"
        if grand_missing == 0 and grand_errors == 0
        else "SOME MISSING OR ERRORS"
    )
    print(f"\nOverall Status: {overall}")
    print(f"{'=' * 60}\n")


def main():
    parser = argparse.ArgumentParser(
        description="Reindex votes and vote responses from OpenSearch into NATS KV"
    )
    parser.add_argument(
        "--nats-url",
        default="nats://localhost:4222",
        help="NATS server URL (default: nats://localhost:4222)",
    )
    parser.add_argument(
        "--reindex",
        action="store_true",
        help="Reindex mode — write entries back. "
        "Without this flag, runs dry-run.",
    )
    args = parser.parse_args()

    try:
        asyncio.run(run(nats_url=args.nats_url, dry_run=not args.reindex))
    except KeyboardInterrupt:
        print("\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

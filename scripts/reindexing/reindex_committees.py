#!/usr/bin/env python3
# Copyright The Linux Foundation and each contributor to LFX.
# SPDX-License-Identifier: MIT
"""
Committee Service Reindexer

Given a project_id (CLI arg) and TOKEN (env var), this script:
1. Fetches all committees for the project from the LF project-service API
2. Reindexes each committee in the v1-objects NATS KV bucket
   (key: platform-collaboration__c.<committee_id>)
3. For each committee, fetches all members from the API
4. Reindexes each member in the v1-objects NATS KV bucket
   (key: platform-community__c.<member_id>)
5. Prints a summary report
"""

import argparse
import asyncio
import os
import sys
from dataclasses import dataclass, field
from typing import Dict, List

import httpx
from nats.aio.client import Client as NATS


# API_BASE = "https://api-gw.platform.linuxfoundation.org/project-service/v2"
API_BASE = "https://api-gw.dev.platform.linuxfoundation.org/project-service/v2"
KV_BUCKET = "v1-objects"
COMMITTEE_KV_PREFIX = "platform-collaboration__c"
MEMBER_KV_PREFIX = "platform-community__c"
PAGE_SIZE = 100


@dataclass
class Stats:
    total: int = 0
    reindexed: int = 0
    missing: int = 0
    errors: int = 0
    missing_keys: List[str] = field(default_factory=list)


class CommitteeReindexer:
    def __init__(self, token: str, nats_url: str, dry_run: bool = True):
        self.token = token
        self.nats_url = nats_url
        self.dry_run = dry_run
        self.nc = None
        self.kv = None
        self.stats: Dict[str, Stats] = {}

    def _stats(self, key: str) -> Stats:
        if key not in self.stats:
            self.stats[key] = Stats()
        return self.stats[key]

    async def connect(self):
        self.nc = NATS()
        await self.nc.connect(servers=[self.nats_url])
        js = self.nc.jetstream()
        self.kv = await js.key_value(bucket=KV_BUCKET)
        print(f"Connected to NATS at {self.nats_url}")

    async def disconnect(self):
        if self.nc:
            await self.nc.close()

    def _get_headers(self) -> dict:
        return {"Authorization": f"Bearer {self.token}"}

    def _fetch_all_pages(self, client: httpx.Client, url: str) -> List[dict]:
        """Fetch all pages from a paginated API endpoint."""
        items: List[dict] = []
        offset = 0

        while True:
            response = client.get(
                url,
                params={"pageSize": PAGE_SIZE, "offset": offset},
                headers=self._get_headers(),
            )
            response.raise_for_status()
            data = response.json()

            # The API returns a list directly or wrapped in a data field
            page_items = (
                data
                if isinstance(data, list)
                else data.get("Data", data.get("data", []))
            )
            if not page_items:
                break

            items.extend(page_items)

            # If we got fewer items than the page size, we're done
            if len(page_items) < PAGE_SIZE:
                break

            offset += PAGE_SIZE

        return items

    async def reindex_entry(self, kv_prefix: str, item_id: str):
        """Fetch entry from NATS KV and write it back to trigger reindex."""
        kv_key = f"{kv_prefix}.{item_id}"
        s = self._stats(kv_prefix)
        s.total += 1

        try:
            assert self.kv is not None, "NATS KV not connected"
            entry = await self.kv.get(kv_key)
            if entry is None:
                print(f"  MISSING: {kv_key}")
                s.missing += 1
                s.missing_keys.append(item_id)
                return

            if not self.dry_run:
                await self.kv.put(kv_key, entry.value)
                s.reindexed += 1
                print(f"  reindexed: {kv_key}")
            else:
                print(f"  found: {kv_key}")

        except Exception as e:
            print(f"  ERROR {kv_key}: {e}")
            s = self._stats(kv_prefix)
            s.missing += 1
            s.errors += 1

    async def run(self, project_id: str):
        print(f"\n{'=' * 60}")
        print("Committee Reindexer")
        print(f"Project ID : {project_id}")
        print(f"Mode       : {'DRY RUN' if self.dry_run else 'REINDEX'}")
        print(f"{'=' * 60}\n")

        await self.connect()
        try:
            with httpx.Client(timeout=30) as client:
                # Fetch all committees for the project
                committees_url = f"{API_BASE}/projects/{project_id}/committees"
                print("--- Fetching committees from API ---")
                committees = self._fetch_all_pages(client, committees_url)
                print(f"  Found {len(committees)} committee(s)")

                for committee in committees:
                    committee_id = committee.get("ID") or committee.get("id")
                    if not committee_id:
                        print(f"  WARNING: committee has no ID: {committee}")
                        continue

                    # Reindex the committee
                    await self.reindex_entry(COMMITTEE_KV_PREFIX, committee_id)

                    # Fetch all members for this committee
                    members_url = (
                        f"{API_BASE}/projects/{project_id}"
                        f"/committees/{committee_id}/members"
                    )
                    members = self._fetch_all_pages(client, members_url)
                    print(f"  Committee {committee_id}: {len(members)} member(s)")

                    for member in members:
                        member_id = member.get("ID") or member.get("id")
                        if not member_id:
                            print(f"  WARNING: member has no ID: {member}")
                            continue
                        await self.reindex_entry(MEMBER_KV_PREFIX, member_id)

            self.print_summary()

        finally:
            await self.disconnect()

    def print_summary(self):
        print(f"\n{'=' * 60}")
        print("SUMMARY REPORT")
        print(f"{'=' * 60}\n")

        grand_total = grand_missing = grand_reindexed = grand_errors = 0

        for prefix in [COMMITTEE_KV_PREFIX, MEMBER_KV_PREFIX]:
            if prefix not in self.stats:
                continue
            s = self.stats[prefix]
            grand_total += s.total
            grand_missing += s.missing
            grand_reindexed += s.reindexed
            grand_errors += s.errors

            status = "OK" if s.missing == 0 and s.errors == 0 else "ISSUES"
            print(f"{prefix}:")
            print(f"  Total checked : {s.total}")
            print(f"  Missing       : {s.missing}")
            if not self.dry_run:
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
        if not self.dry_run:
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
        description="Reindex project committees and their members in NATS KV"
    )
    parser.add_argument("project_id", help="Project ID to reindex")
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

    token = os.environ.get("TOKEN")
    if not token:
        print("ERROR: TOKEN environment variable is required", file=sys.stderr)
        sys.exit(1)

    reindexer = CommitteeReindexer(
        token=token,
        nats_url=args.nats_url,
        dry_run=not args.reindex,
    )

    try:
        asyncio.run(reindexer.run(args.project_id))
    except KeyboardInterrupt:
        print("\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

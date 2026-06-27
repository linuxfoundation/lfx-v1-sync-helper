#!/usr/bin/env python3
# Copyright The Linux Foundation and each contributor to LFX.
# SPDX-License-Identifier: MIT
"""
Groups.io Service Reindexer

Given a project_id, this script:
1. Looks up the groupsio service entry in itx-groupsio-v2-service
2. Finds all subgroups/mailing lists via itx-groupsio-v2-subgroup (parent_id_index)
3. Reindexes each subgroup entry in the v1-objects NATS KV bucket
4. For each subgroup, finds all members via itx-groupsio-v2-member (group_id_index)
5. Reindexes each member entry in the v1-objects NATS KV bucket
6. Prints a summary report
"""

import argparse
import asyncio
import sys
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import boto3
from boto3.dynamodb.conditions import Key
from nats.aio.client import Client as NATS
from nats.js.errors import KeyNotFoundError


SERVICE_TABLE = "itx-groupsio-v2-service"
SUBGROUP_TABLE = "itx-groupsio-v2-subgroup"
MEMBER_TABLE = "itx-groupsio-v2-member"
KV_BUCKET = "v1-objects"


@dataclass
class TableStats:
    total: int = 0
    reindexed: int = 0
    missing: int = 0
    errors: int = 0
    missing_keys: List[str] = field(default_factory=list)


class GroupsioReindexer:
    def __init__(self, nats_url: str, dry_run: bool = True):
        self.nats_url = nats_url
        self.dry_run = dry_run
        self.dynamodb = boto3.resource("dynamodb")
        self.nc = None
        self.kv = None
        self.stats: Dict[str, TableStats] = {}

    def _stats(self, table: str) -> TableStats:
        if table not in self.stats:
            self.stats[table] = TableStats()
        return self.stats[table]

    async def connect(self):
        self.nc = NATS()
        await self.nc.connect(servers=[self.nats_url])
        js = self.nc.jetstream()
        self.kv = await js.key_value(bucket=KV_BUCKET)
        print(f"Connected to NATS at {self.nats_url}")

    async def disconnect(self):
        if self.nc:
            await self.nc.close()

    def _query_all(
        self, table_name: str, index: str, key_field: str, key_value: str
    ) -> List[dict]:
        """Query a DynamoDB index, handling pagination."""
        table = self.dynamodb.Table(table_name)
        items = []
        kwargs = dict(
            IndexName=index,
            KeyConditionExpression=Key(key_field).eq(key_value),
        )
        response = table.query(**kwargs)
        items.extend(response.get("Items", []))
        while "LastEvaluatedKey" in response:
            response = table.query(
                **kwargs, ExclusiveStartKey=response["LastEvaluatedKey"]
            )
            items.extend(response.get("Items", []))
        return items

    def _get_item(self, table_name: str, key: dict) -> Optional[dict]:
        table = self.dynamodb.Table(table_name)
        response = table.get_item(Key=key)
        return response.get("Item")

    async def reindex_entry(self, table_name: str, item_id: str):
        """Fetch entry from NATS KV and write it back to trigger reindex."""
        kv_key = f"{table_name}.{item_id}"
        s = self._stats(table_name)
        s.total += 1

        try:
            assert self.kv is not None, "NATS KV not connected"
            entry = await self.kv.get(kv_key)

            if not self.dry_run:
                await self.kv.put(kv_key, entry.value)
                s.reindexed += 1
                print(f"  reindexed: {kv_key}")
            else:
                print(f"  found: {kv_key}")

        except KeyNotFoundError:
            print(f"  MISSING: {kv_key}")
            s.missing += 1
            s.missing_keys.append(item_id)
        except Exception as e:
            print(f"  ERROR {kv_key}: {e}")
            s.errors += 1

    async def run(self, project_id: str):
        print(f"\n{'=' * 60}")
        print("Groups.io Reindexer")
        print(f"Project ID : {project_id}")
        print(f"Mode       : {'DRY RUN' if self.dry_run else 'REINDEX'}")
        print(f"{'=' * 60}\n")

        await self.connect()
        try:
            # Step 1: find the service entry for this project
            print(f"--- Looking up service in {SERVICE_TABLE} ---")
            service_items = self._query_all(
                SERVICE_TABLE, "project_id_index", "project_id", project_id
            )
            if not service_items:
                print(f"ERROR: No groupsio service found for project_id={project_id}")
                return

            # There should be one service per project, but handle multiples gracefully
            for service_item in service_items:
                group_service_id = service_item.get("group_service_id")
                if not group_service_id:
                    print(
                        f"WARNING: service item has no group_service_id: {service_item}"
                    )
                    continue

                print(f"  Found service: group_service_id={group_service_id}")
                await self.reindex_entry(SERVICE_TABLE, group_service_id)

                # Step 2: find all subgroups for this service via parent_id
                print(f"\n--- Finding subgroups in {SUBGROUP_TABLE} ---")
                subgroup_items = self._query_all(
                    SUBGROUP_TABLE, "parent_id_index", "parent_id", group_service_id
                )
                print(f"  Found {len(subgroup_items)} subgroup(s)")

                for subgroup in subgroup_items:
                    group_id = subgroup.get("group_id")
                    if not group_id:
                        print(f"  WARNING: subgroup item has no group_id: {subgroup}")
                        continue

                    # Step 3: reindex the subgroup entry
                    await self.reindex_entry(SUBGROUP_TABLE, group_id)

                    # Step 4: find all members for this subgroup
                    member_items = self._query_all(
                        MEMBER_TABLE, "group_id_index", "group_id", group_id
                    )

                    # Step 5: reindex each member
                    for member in member_items:
                        member_id = member.get("member_id")
                        if not member_id:
                            print(f"  WARNING: member item has no member_id: {member}")
                            continue
                        await self.reindex_entry(MEMBER_TABLE, member_id)

            self.print_summary()

        finally:
            await self.disconnect()

    def print_summary(self):
        print(f"\n{'=' * 60}")
        print("SUMMARY REPORT")
        print(f"{'=' * 60}\n")

        grand_total = grand_missing = grand_reindexed = grand_errors = 0

        for table_name in [SERVICE_TABLE, SUBGROUP_TABLE, MEMBER_TABLE]:
            if table_name not in self.stats:
                continue
            s = self.stats[table_name]
            grand_total += s.total
            grand_missing += s.missing
            grand_reindexed += s.reindexed
            grand_errors += s.errors

            status = "OK" if s.missing == 0 and s.errors == 0 else "ISSUES"
            print(f"{table_name}:")
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
        description="Reindex Groups.io service, subgroups, and members in NATS KV"
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

    reindexer = GroupsioReindexer(nats_url=args.nats_url, dry_run=not args.reindex)

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

#!/usr/bin/env python3
# Copyright The Linux Foundation and each contributor to LFX.
# SPDX-License-Identifier: MIT
"""
DynamoDB to NATS KV Reindexer

This script checks DynamoDB tables for a given project_id and verifies that
corresponding entries exist in the NATS KV bucket. It can operate in two modes:
- Dry-run: Report which entries are missing from NATS KV
- Reindex: Trigger reindexing by updating the NATS KV entry with its current value
"""

import argparse
import asyncio
import sys
from typing import Dict, List, Set, Optional
from dataclasses import dataclass

import boto3
from boto3.dynamodb.conditions import Key
from nats.aio.client import Client as NATS


# Table configuration
@dataclass
class TableConfig:
    name: str
    primary_key: str
    project_id_field: str
    project_id_index: str


@dataclass
class SecondaryTableConfig:
    name: str
    primary_key: str
    parent_table: str
    parent_key_field: str
    parent_key_index: Optional[str] = (
        None  # None if parent_key_field is the primary key
    )


# Primary tables with project_id indices
PRIMARY_TABLES = [
    TableConfig(
        name="itx-poll",
        primary_key="poll_id",
        project_id_field="project_id",
        project_id_index="project_id_index",
    ),
    TableConfig(
        name="itx-zoom-meetings-v2",
        primary_key="meeting_id",
        project_id_field="proj_id",
        project_id_index="proj_id_index",
    ),
    TableConfig(
        name="itx-zoom-past-meetings",
        primary_key="meeting_and_occurrence_id",
        project_id_field="proj_id",
        project_id_index="proj_id_index",
    ),
]

# Secondary tables with their relationships
SECONDARY_TABLES = [
    SecondaryTableConfig(
        name="itx-poll-vote",
        primary_key="vote_id",
        parent_table="itx-poll",
        parent_key_field="poll_id",
        parent_key_index="poll_id_index",
    ),
    SecondaryTableConfig(
        name="itx-zoom-meetings-mappings-v2",
        primary_key="id",
        parent_table="itx-zoom-meetings-v2",
        parent_key_field="meeting_id",
        parent_key_index="meeting_id_index",
    ),
    SecondaryTableConfig(
        name="itx-zoom-meetings-registrants-v2",
        primary_key="registrant_id",
        parent_table="itx-zoom-meetings-v2",
        parent_key_field="meeting_id",
        parent_key_index="meeting_id_index",
    ),
    SecondaryTableConfig(
        name="itx-zoom-meetings-invite-responses-v2",
        primary_key="id",
        parent_table="itx-zoom-meetings-v2",
        parent_key_field="meeting_id",
        parent_key_index="meeting_id_index",
    ),
    SecondaryTableConfig(
        name="itx-zoom-meetings-attachments-v2",
        primary_key="id",
        parent_table="itx-zoom-meetings-v2",
        parent_key_field="meeting_id",
        parent_key_index="meeting_id_index",
    ),
    SecondaryTableConfig(
        name="itx-zoom-past-meetings-mappings",
        primary_key="id",
        parent_table="itx-zoom-past-meetings",
        parent_key_field="meeting_and_occurrence_id",
        parent_key_index="meeting_and_occurrence_id_index",
    ),
    SecondaryTableConfig(
        name="itx-zoom-past-meetings-attendees",
        primary_key="id",
        parent_table="itx-zoom-past-meetings",
        parent_key_field="meeting_and_occurrence_id",
        parent_key_index="meeting_and_occurrence_id_index",
    ),
    SecondaryTableConfig(
        name="itx-zoom-past-meetings-invitees",
        primary_key="invitee_id",
        parent_table="itx-zoom-past-meetings",
        parent_key_field="meeting_and_occurrence_id",
        parent_key_index="meeting_and_occurrence_id_index",
    ),
    SecondaryTableConfig(
        name="itx-zoom-past-meetings-recordings",
        primary_key="meeting_and_occurrence_id",
        parent_table="itx-zoom-past-meetings",
        parent_key_field="meeting_and_occurrence_id",
        parent_key_index=None,  # Primary key, not an index
    ),
    SecondaryTableConfig(
        name="itx-zoom-past-meetings-summaries",
        primary_key="id",
        parent_table="itx-zoom-past-meetings",
        parent_key_field="meeting_and_occurrence_id",
        parent_key_index="meeting_and_occurrence_id_index",
    ),
    SecondaryTableConfig(
        name="itx-zoom-past-meetings-attachments",
        primary_key="id",
        parent_table="itx-zoom-past-meetings",
        parent_key_field="meeting_and_occurrence_id",
        parent_key_index="meeting_and_occurrence_id_index",
    ),
]


class ReindexStats:
    def __init__(self):
        self.tables: Dict[str, Dict] = {}

    def init_table(self, table_name: str):
        if table_name not in self.tables:
            self.tables[table_name] = {
                "total": 0,
                "missing": 0,
                "reindexed": 0,
                "errors": 0,
                "missing_keys": [],
            }

    def add_total(self, table_name: str, count: int = 1):
        self.init_table(table_name)
        self.tables[table_name]["total"] += count

    def add_missing(self, table_name: str, key: str):
        self.init_table(table_name)
        self.tables[table_name]["missing"] += 1
        self.tables[table_name]["missing_keys"].append(key)

    def add_reindexed(self, table_name: str):
        self.init_table(table_name)
        self.tables[table_name]["reindexed"] += 1

    def add_error(self, table_name: str):
        self.init_table(table_name)
        self.tables[table_name]["errors"] += 1


class DynamoDBNATSReindexer:
    def __init__(self, nats_url: str, dry_run: bool = True):
        self.nats_url = nats_url
        self.dry_run = dry_run
        self.dynamodb = boto3.resource("dynamodb")
        self.nc = None
        self.js = None
        self.kv = None
        self.stats = ReindexStats()

    async def connect_nats(self):
        """Connect to NATS and get KV bucket"""
        self.nc = NATS()
        await self.nc.connect(servers=[self.nats_url])
        self.js = self.nc.jetstream()
        self.kv = await self.js.key_value(bucket="v1-objects")
        print(f"✓ Connected to NATS at {self.nats_url}")

    async def disconnect_nats(self):
        """Disconnect from NATS"""
        if self.nc:
            await self.nc.close()

    def query_primary_table(self, config: TableConfig, project_id: str) -> List[Dict]:
        """Query a primary table for all entries with the given project_id"""
        table = self.dynamodb.Table(config.name)
        items = []

        print(f"Querying {config.name} for project_id={project_id}...")

        response = table.query(
            IndexName=config.project_id_index,
            KeyConditionExpression=Key(config.project_id_field).eq(project_id),
        )
        items.extend(response.get("Items", []))

        # Handle pagination
        while "LastEvaluatedKey" in response:
            response = table.query(
                IndexName=config.project_id_index,
                KeyConditionExpression=Key(config.project_id_field).eq(project_id),
                ExclusiveStartKey=response["LastEvaluatedKey"],
            )
            items.extend(response.get("Items", []))

        print(f"  Found {len(items)} items in {config.name}")
        return items

    def query_secondary_table(
        self, config: SecondaryTableConfig, parent_keys: Set[str]
    ) -> List[Dict]:
        """Query a secondary table for all entries matching parent keys"""
        if not parent_keys:
            return []

        table = self.dynamodb.Table(config.name)
        items = []

        print(f"Querying {config.name} for {len(parent_keys)} parent keys...")

        # If parent_key_index is None, the parent_key_field is the primary key
        # Use batch_get_item instead of query
        if config.parent_key_index is None:
            parent_keys_list = list(parent_keys)
            # Batch get items in chunks of 100 (DynamoDB limit)
            for i in range(0, len(parent_keys_list), 100):
                batch = parent_keys_list[i:i + 100]
                keys = [{config.primary_key: pk} for pk in batch]

                response = self.dynamodb.batch_get_item(
                    RequestItems={config.name: {"Keys": keys}}
                )
                items.extend(response.get("Responses", {}).get(config.name, []))

                # Handle unprocessed keys
                while response.get("UnprocessedKeys"):
                    response = self.dynamodb.batch_get_item(
                        RequestItems=response["UnprocessedKeys"]
                    )
                    items.extend(response.get("Responses", {}).get(config.name, []))
        else:
            # Query for each parent key using the index
            for parent_key in parent_keys:
                response = table.query(
                    IndexName=config.parent_key_index,
                    KeyConditionExpression=Key(config.parent_key_field).eq(parent_key),
                )
                items.extend(response.get("Items", []))

                # Handle pagination
                while "LastEvaluatedKey" in response:
                    response = table.query(
                        IndexName=config.parent_key_index,
                        KeyConditionExpression=Key(config.parent_key_field).eq(
                            parent_key
                        ),
                        ExclusiveStartKey=response["LastEvaluatedKey"],
                    )
                    items.extend(response.get("Items", []))

        print(f"  Found {len(items)} items in {config.name}")
        return items

    async def check_and_reindex_entry(
        self, table_name: str, primary_key_value: str
    ) -> bool:
        """Check if entry exists in NATS KV and reindex if needed"""
        kv_key = f"{table_name}.{primary_key_value}"
        self.stats.add_total(table_name)

        try:
            # Try to get the entry from KV bucket
            assert self.kv is not None, "NATS KV not connected"
            entry = await self.kv.get(kv_key)

            if entry is None:
                print(f"  ✗ Missing: {kv_key}")
                self.stats.add_missing(table_name, primary_key_value)
                return False

            # Entry exists - trigger reindex if not in dry-run mode
            if not self.dry_run:
                # Update with the same value to trigger reindex
                await self.kv.put(kv_key, entry.value)
                self.stats.add_reindexed(table_name)
                print(f"  ✓ Reindexed: {kv_key}")

            return True

        except Exception as e:
            print(f"  ✗ Error checking {kv_key}: {e}")
            self.stats.add_error(table_name)
            return False

    async def process_primary_table(
        self, config: TableConfig, project_id: str
    ) -> Set[str]:
        """Process a primary table and return the set of primary keys"""
        items = self.query_primary_table(config, project_id)
        primary_keys = set()

        for item in items:
            primary_key_value = item.get(config.primary_key)
            if primary_key_value:
                primary_keys.add(primary_key_value)
                await self.check_and_reindex_entry(config.name, primary_key_value)

        return primary_keys

    async def process_secondary_table(
        self, config: SecondaryTableConfig, parent_keys: Set[str]
    ):
        """Process a secondary table"""
        items = self.query_secondary_table(config, parent_keys)

        for item in items:
            primary_key_value = item.get(config.primary_key)
            if primary_key_value:
                await self.check_and_reindex_entry(config.name, primary_key_value)

    async def run(self, project_id: str):
        """Main execution flow"""
        print(f"\n{'=' * 60}")
        print(f"Project ID: {project_id}")
        print(f"Mode: {'DRY RUN' if self.dry_run else 'REINDEX'}")
        print(f"{'=' * 60}\n")

        await self.connect_nats()

        try:
            # Store parent keys for secondary table lookups
            parent_keys_by_table = {}

            # Process primary tables
            print("\n--- Processing Primary Tables ---\n")
            for config in PRIMARY_TABLES:
                primary_keys = await self.process_primary_table(config, project_id)
                parent_keys_by_table[config.name] = primary_keys

            # Process secondary tables
            print("\n--- Processing Secondary Tables ---\n")
            for sec_config in SECONDARY_TABLES:
                parent_keys = parent_keys_by_table.get(sec_config.parent_table, set())
                await self.process_secondary_table(sec_config, parent_keys)

            # Print summary
            self.print_summary()

        finally:
            await self.disconnect_nats()

    def print_summary(self):
        """Print summary report"""
        print(f"\n{'=' * 60}")
        print("SUMMARY REPORT")
        print(f"{'=' * 60}\n")

        all_good = True
        total_items = 0
        total_missing = 0
        total_reindexed = 0
        total_errors = 0

        for table_name in sorted(self.stats.tables.keys()):
            stats = self.stats.tables[table_name]
            total_items += stats["total"]
            total_missing += stats["missing"]
            total_reindexed += stats["reindexed"]
            total_errors += stats["errors"]

            status = (
                "✓ OK" if stats["missing"] == 0 and stats["errors"] == 0 else "✗ ISSUES"
            )
            if stats["missing"] > 0 or stats["errors"] > 0:
                all_good = False

            print(f"{table_name}:")
            print(f"  Total entries: {stats['total']}")
            print(f"  Missing from NATS: {stats['missing']}")
            if not self.dry_run:
                print(f"  Reindexed: {stats['reindexed']}")
            if stats["errors"] > 0:
                print(f"  Errors: {stats['errors']}")
            print(f"  Status: {status}")

            # Show missing keys in dry-run mode
            if self.dry_run and stats["missing"] > 0:
                print(f"  Missing keys: {', '.join(stats['missing_keys'][:10])}")
                if len(stats["missing_keys"]) > 10:
                    print(f"    ... and {len(stats['missing_keys']) - 10} more")

            print()

        print(f"{'=' * 60}")
        print("TOTALS:")
        print(f"  Total entries checked: {total_items}")
        print(f"  Missing from NATS: {total_missing}")
        if not self.dry_run:
            print(f"  Reindexed: {total_reindexed}")
        if total_errors > 0:
            print(f"  Errors: {total_errors}")
        status = (
            "✓ ALL ENTRIES HAVE NATS KV MATCHES"
            if all_good
            else "✗ SOME ENTRIES MISSING OR ERRORS"
        )
        print(f"\nOverall Status: {status}")
        print(f"{'=' * 60}\n")


def main():
    parser = argparse.ArgumentParser(
        description="Check and reindex DynamoDB entries in NATS KV bucket"
    )
    parser.add_argument("project_id", help="Project ID to search for")
    parser.add_argument(
        "--nats-url",
        default="nats://localhost:4222",
        help="NATS server URL (default: nats://localhost:4222)",
    )
    parser.add_argument(
        "--reindex",
        action="store_true",
        help="Reindex mode - trigger reindexing for entries "
        "in NATS KV. Without this flag, runs in dry-run mode.",
    )

    args = parser.parse_args()

    # By default, run in dry-run mode unless --reindex is specified
    dry_run = not args.reindex

    reindexer = DynamoDBNATSReindexer(nats_url=args.nats_url, dry_run=dry_run)

    try:
        asyncio.run(reindexer.run(args.project_id))
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nError: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

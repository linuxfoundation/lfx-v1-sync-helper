#!/usr/bin/env python3
# Copyright The Linux Foundation and each contributor to LFX.
# SPDX-License-Identifier: MIT
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "boto3>=1.34.0",
#   "httpx>=0.27.0",
#   "nats-py>=2.7.0",
# ]
# ///
"""
Project Onboarding Script

Consolidates the manual onboarding steps 2-5 into a single command:
  2. Replay project KV entries in v1-objects to trigger reprocessing
  3. Verify (and optionally create) v1-mappings entries
  4. Reindex committees and their members
  5. Reindex DynamoDB resources (meetings, polls, etc.)

Usage:
  python scripts/onboard_project.py <slug> [dev|staging|prod] [options]

Environment variables:
  LFX_API_TOKEN   Bearer token for LFX API calls (required)
  NATS_URL        NATS server URL (overridden by --nats-url)
"""

import argparse
import asyncio
import os
import sys
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

import boto3
from boto3.dynamodb.conditions import Key
import httpx
from nats.aio.client import Client as NATS

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ENV_HOSTS = {
    "dev": "api-gw.dev.platform.linuxfoundation.org",
    "staging": "api-gw.staging.platform.linuxfoundation.org",
    "prod": "api-gw.platform.linuxfoundation.org",
}

V1_OBJECTS_BUCKET = "v1-objects"
V1_MAPPINGS_BUCKET = "v1-mappings"
PROJECT_KV_PREFIX = "salesforce-project__c"
COMMITTEE_KV_PREFIX = "platform-collaboration__c"
MEMBER_KV_PREFIX = "platform-community__c"
PAGE_SIZE = 100

# DynamoDB primary tables (project_id index)
_PRIMARY_TABLES = [
    {"name": "itx-poll", "primary_key": "poll_id", "project_id_field": "project_id", "index": "project_id_index"},
    {"name": "itx-zoom-meetings-v2", "primary_key": "meeting_id", "project_id_field": "proj_id", "index": "proj_id_index"},
    {"name": "itx-zoom-past-meetings", "primary_key": "meeting_and_occurrence_id", "project_id_field": "proj_id", "index": "proj_id_index"},
]

# DynamoDB secondary tables (join via parent primary keys)
_SECONDARY_TABLES = [
    {"name": "itx-poll-vote", "primary_key": "vote_id", "parent_table": "itx-poll", "parent_key_field": "poll_id", "parent_key_index": "poll_id_index"},
    {"name": "itx-zoom-meetings-mappings-v2", "primary_key": "id", "parent_table": "itx-zoom-meetings-v2", "parent_key_field": "meeting_id", "parent_key_index": "meeting_id_index"},
    {"name": "itx-zoom-meetings-registrants-v2", "primary_key": "registrant_id", "parent_table": "itx-zoom-meetings-v2", "parent_key_field": "meeting_id", "parent_key_index": "meeting_id_index"},
    {"name": "itx-zoom-meetings-invite-responses-v2", "primary_key": "id", "parent_table": "itx-zoom-meetings-v2", "parent_key_field": "meeting_id", "parent_key_index": "meeting_id_index"},
    {"name": "itx-zoom-meetings-attachments-v2", "primary_key": "id", "parent_table": "itx-zoom-meetings-v2", "parent_key_field": "meeting_id", "parent_key_index": "meeting_id_index"},
    {"name": "itx-zoom-past-meetings-mappings", "primary_key": "id", "parent_table": "itx-zoom-past-meetings", "parent_key_field": "meeting_and_occurrence_id", "parent_key_index": "meeting_and_occurrence_id_index"},
    {"name": "itx-zoom-past-meetings-attendees", "primary_key": "id", "parent_table": "itx-zoom-past-meetings", "parent_key_field": "meeting_and_occurrence_id", "parent_key_index": "meeting_and_occurrence_id_index"},
    {"name": "itx-zoom-past-meetings-invitees", "primary_key": "invitee_id", "parent_table": "itx-zoom-past-meetings", "parent_key_field": "meeting_and_occurrence_id", "parent_key_index": "meeting_and_occurrence_id_index"},
    {"name": "itx-zoom-past-meetings-recordings", "primary_key": "meeting_and_occurrence_id", "parent_table": "itx-zoom-past-meetings", "parent_key_field": "meeting_and_occurrence_id", "parent_key_index": None},
    {"name": "itx-zoom-past-meetings-summaries", "primary_key": "id", "parent_table": "itx-zoom-past-meetings", "parent_key_field": "meeting_and_occurrence_id", "parent_key_index": "meeting_and_occurrence_id_index"},
    {"name": "itx-zoom-past-meetings-attachments", "primary_key": "id", "parent_table": "itx-zoom-past-meetings", "parent_key_field": "meeting_and_occurrence_id", "parent_key_index": "meeting_and_occurrence_id_index"},
]


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class Project:
    sfid: str
    slug: str
    depth: int = 0


@dataclass
class PhaseStats:
    total: int = 0
    found: int = 0
    missing: int = 0
    written: int = 0
    errors: int = 0
    missing_keys: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# LFX API Client
# ---------------------------------------------------------------------------

class LFXAPIClient:
    """Queries the LFX project-service v1 API for the project tree."""

    def __init__(self, token: str, env: str):
        host = ENV_HOSTS[env]
        self.base_v1 = f"https://{host}/project-service/v1"
        self.base_v2 = f"https://{host}/project-service/v2"
        self.headers = {"Authorization": f"Bearer {token}"}

    def _fetch_all_pages(self, client: httpx.Client, url: str, params: dict) -> List[dict]:
        """Fetch all pages from a paginated endpoint."""
        items: List[dict] = []
        offset = 0
        while True:
            p = {**params, "pageSize": PAGE_SIZE, "offset": offset}
            resp = client.get(url, params=p, headers=self.headers)
            resp.raise_for_status()
            data = resp.json()
            page = data.get("Data") or data.get("data") or []
            if not page:
                break
            items.extend(page)
            if len(page) < PAGE_SIZE:
                break
            offset += PAGE_SIZE
        return items

    def _fetch_project_node(self, slug: str, client: httpx.Client) -> Optional[dict]:
        """Fetch the raw project node by slug from the direct lookup endpoint."""
        resp = client.get(f"{self.base_v1}/projects/{slug}", headers=self.headers)
        resp.raise_for_status()
        node = resp.json()
        node = node.get("Data") or node.get("data") or node
        if isinstance(node, list):
            node = node[0] if node else {}
        return node if node.get("ID") or node.get("id") else None

    def _walk_tree(self, node: dict, depth: int, client: httpx.Client) -> List[Project]:
        """
        Recursively walk a project node, fetching each child individually
        to retrieve its own children (the API only returns one level deep).
        Skips nodes with Funding == "Unfunded".
        """
        if node.get("Funding") == "Unfunded":
            return []
        if node.get("Category") == "Working Group":
            return []
        if node.get("Status") == "Archived":
            return []
        results: List[Project] = []
        sfid = node.get("ID") or node.get("id") or ""
        slug = node.get("Slug") or node.get("slug") or ""
        if sfid and slug:
            results.append(Project(sfid=sfid, slug=slug, depth=depth))
        for child in node.get("Projects") or []:
            child_slug = child.get("Slug") or child.get("slug") or ""
            if not child_slug:
                continue
            full_child = self._fetch_project_node(child_slug, client)
            if full_child:
                results.extend(self._walk_tree(full_child, depth + 1, client))
        return results

    def fetch_project_tree(
        self,
        slug: str,
        recurse: bool = True,
    ) -> List[Project]:
        """
        Fetch the root project and optionally its descendants.

        Uses GET /projects/{slug} for each node since the API only returns
        one level of children at a time.
        When recurse=False only the root project is returned.
        """
        with httpx.Client(timeout=30) as client:
            node = self._fetch_project_node(slug, client)
            if not node:
                return []

            if not recurse:
                sfid = node.get("ID") or node.get("id") or ""
                s = node.get("Slug") or node.get("slug") or ""
                return [Project(sfid=sfid, slug=s)] if sfid and s else []

            return self._walk_tree(node, 0, client)

    def fetch_committees(self, project_id: str) -> List[dict]:
        """Fetch all committees for a project from the v2 API."""
        url = f"{self.base_v2}/projects/{project_id}/committees"
        with httpx.Client(timeout=30) as client:
            items: List[dict] = []
            offset = 0
            while True:
                resp = client.get(
                    url,
                    params={"pageSize": PAGE_SIZE, "offset": offset},
                    headers=self.headers,
                )
                resp.raise_for_status()
                data = resp.json()
                page = (
                    data
                    if isinstance(data, list)
                    else data.get("Data") or data.get("data") or []
                )
                if not page:
                    break
                items.extend(page)
                if len(page) < PAGE_SIZE:
                    break
                offset += PAGE_SIZE
            return items

    def fetch_committee_members(self, project_id: str, committee_id: str) -> List[dict]:
        """Fetch all members for a committee."""
        url = f"{self.base_v2}/projects/{project_id}/committees/{committee_id}/members"
        with httpx.Client(timeout=30) as client:
            items: List[dict] = []
            offset = 0
            while True:
                resp = client.get(
                    url,
                    params={"pageSize": PAGE_SIZE, "offset": offset},
                    headers=self.headers,
                )
                resp.raise_for_status()
                data = resp.json()
                page = (
                    data
                    if isinstance(data, list)
                    else data.get("Data") or data.get("data") or []
                )
                if not page:
                    break
                items.extend(page)
                if len(page) < PAGE_SIZE:
                    break
                offset += PAGE_SIZE
            return items


# ---------------------------------------------------------------------------
# NATS Manager
# ---------------------------------------------------------------------------

class NATSManager:
    """Async context manager: connect/disconnect and expose KV buckets."""

    def __init__(self, nats_url: str):
        self.nats_url = nats_url
        self.nc: Optional[NATS] = None
        self.v1_objects = None
        self.v1_mappings = None

    async def __aenter__(self):
        self.nc = NATS()
        await self.nc.connect(servers=[self.nats_url])
        js = self.nc.jetstream()
        self.v1_objects = await js.key_value(bucket=V1_OBJECTS_BUCKET)
        self.v1_mappings = await js.key_value(bucket=V1_MAPPINGS_BUCKET)
        print(f"Connected to NATS at {self.nats_url}")
        return self

    async def __aexit__(self, *_):
        if self.nc:
            await self.nc.close()


# ---------------------------------------------------------------------------
# Phase 2: Project KV Replayer
# ---------------------------------------------------------------------------

class ProjectReplayer:
    """Get project KV entries from v1-objects and write them back."""

    def __init__(self, nats: NATSManager, dry_run: bool):
        self.nats = nats
        self.dry_run = dry_run
        self.stats = PhaseStats()

    async def replay(self, project: Project):
        kv_key = f"{PROJECT_KV_PREFIX}.{project.sfid}"
        self.stats.total += 1
        try:
            entry = await self.nats.v1_objects.get(kv_key)
            if entry is None:
                print(f"  MISSING: {kv_key}")
                self.stats.missing += 1
                self.stats.missing_keys.append(project.sfid)
                return
            self.stats.found += 1
            if not self.dry_run:
                await self.nats.v1_objects.put(kv_key, entry.value)
                self.stats.written += 1
                print(f"  replayed: {kv_key}")
            else:
                print(f"  found: {kv_key} (dry-run)")
        except Exception as e:
            print(f"  ERROR {kv_key}: {e}")
            self.stats.errors += 1

    async def run(self, projects: List[Project]):
        print(f"\n{'=' * 60}")
        print("Phase 2: Project KV Replay")
        print(f"{'=' * 60}")
        for p in projects:
            await self.replay(p)


# ---------------------------------------------------------------------------
# Phase 3: Mapping Verifier
# ---------------------------------------------------------------------------

class MappingVerifier:
    """Check v1-mappings for project.sfid.* and project.uid.* entries."""

    def __init__(self, nats: NATSManager, dry_run: bool, fix_mappings: bool):
        self.nats = nats
        self.dry_run = dry_run
        self.fix_mappings = fix_mappings
        self.stats = PhaseStats()

    async def verify(self, project: Project):
        sfid_key = f"project.sfid.{project.sfid}"
        self.stats.total += 1
        try:
            entry = await self.nats.v1_mappings.get(sfid_key)
            if entry is None or not entry.value:
                print(f"  MISSING mapping: {sfid_key}")
                self.stats.missing += 1
                self.stats.missing_keys.append(sfid_key)
                # --fix-mappings is intentionally not implemented here:
                # creating mappings requires knowing the v2 UUID which is only
                # established after a successful project sync via the API.
                if self.fix_mappings:
                    print(f"    NOTE: Cannot auto-create mapping for {sfid_key} — "
                          "replay the KV entry first to let the service create it.")
            else:
                uid = entry.value.decode(errors="replace")
                print(f"  OK: {sfid_key} -> {uid}")
                self.stats.found += 1
                # Also verify reverse mapping
                uid_key = f"project.uid.{uid}"
                rev = await self.nats.v1_mappings.get(uid_key)
                if rev is None or not rev.value:
                    print(f"    MISSING reverse mapping: {uid_key}")
                    self.stats.missing += 1
                    self.stats.missing_keys.append(uid_key)
                else:
                    print(f"    OK reverse: {uid_key} -> {rev.value.decode(errors='replace')}")
        except Exception as e:
            print(f"  ERROR checking {sfid_key}: {e}")
            self.stats.errors += 1

    async def run(self, projects: List[Project]):
        print(f"\n{'=' * 60}")
        print("Phase 3: Mapping Verification")
        print(f"{'=' * 60}")
        for p in projects:
            await self.verify(p)


# ---------------------------------------------------------------------------
# Phase 4: Committee Reindexer
# ---------------------------------------------------------------------------

class CommitteeReindexer:
    """Reindex committee and member KV entries (parents before children)."""

    def __init__(self, api: LFXAPIClient, nats: NATSManager, dry_run: bool):
        self.api = api
        self.nats = nats
        self.dry_run = dry_run
        self.stats: Dict[str, PhaseStats] = {
            COMMITTEE_KV_PREFIX: PhaseStats(),
            MEMBER_KV_PREFIX: PhaseStats(),
        }

    async def _reindex_entry(self, kv_prefix: str, item_id: str):
        kv_key = f"{kv_prefix}.{item_id}"
        s = self.stats[kv_prefix]
        s.total += 1
        try:
            entry = await self.nats.v1_objects.get(kv_key)
            if entry is None:
                print(f"    MISSING: {kv_key}")
                s.missing += 1
                s.missing_keys.append(item_id)
                return
            s.found += 1
            if not self.dry_run:
                await self.nats.v1_objects.put(kv_key, entry.value)
                s.written += 1
                print(f"    reindexed: {kv_key}")
            else:
                print(f"    found: {kv_key} (dry-run)")
        except Exception as e:
            print(f"    ERROR {kv_key}: {e}")
            s.errors += 1

    async def run(self, projects: List[Project]):
        print(f"\n{'=' * 60}")
        print("Phase 4: Committee Reindex")
        print(f"{'=' * 60}")
        # Sort parents before children (lower depth first)
        ordered = sorted(projects, key=lambda p: p.depth)
        for proj in ordered:
            print(f"\n  Project {proj.slug} ({proj.sfid})")
            try:
                committees = self.api.fetch_committees(proj.sfid)
            except Exception as e:
                print(f"    ERROR fetching committees: {e}")
                continue
            print(f"  Found {len(committees)} committee(s)")
            for committee in committees:
                cid = committee.get("ID") or committee.get("id")
                if not cid:
                    continue
                await self._reindex_entry(COMMITTEE_KV_PREFIX, cid)
                try:
                    members = self.api.fetch_committee_members(proj.sfid, cid)
                except Exception as e:
                    print(f"    ERROR fetching members for {cid}: {e}")
                    continue
                print(f"    Committee {cid}: {len(members)} member(s)")
                for member in members:
                    mid = member.get("ID") or member.get("id")
                    if not mid:
                        continue
                    await self._reindex_entry(MEMBER_KV_PREFIX, mid)


# ---------------------------------------------------------------------------
# Phase 5: DynamoDB Reindexer
# ---------------------------------------------------------------------------

class DynamoDBReindexer:
    """Reindex DynamoDB table entries in v1-objects KV."""

    def __init__(self, nats: NATSManager, dry_run: bool):
        self.nats = nats
        self.dry_run = dry_run
        self.dynamodb = boto3.resource("dynamodb")
        self.stats: Dict[str, PhaseStats] = {}

    def _stats(self, table_name: str) -> PhaseStats:
        if table_name not in self.stats:
            self.stats[table_name] = PhaseStats()
        return self.stats[table_name]

    def _query_primary(self, cfg: dict, project_id: str) -> List[dict]:
        table = self.dynamodb.Table(cfg["name"])
        items: List[dict] = []
        resp = table.query(
            IndexName=cfg["index"],
            KeyConditionExpression=Key(cfg["project_id_field"]).eq(project_id),
        )
        items.extend(resp.get("Items", []))
        while "LastEvaluatedKey" in resp:
            resp = table.query(
                IndexName=cfg["index"],
                KeyConditionExpression=Key(cfg["project_id_field"]).eq(project_id),
                ExclusiveStartKey=resp["LastEvaluatedKey"],
            )
            items.extend(resp.get("Items", []))
        return items

    def _query_secondary(self, cfg: dict, parent_keys: Set[str]) -> List[dict]:
        if not parent_keys:
            return []
        table = self.dynamodb.Table(cfg["name"])
        items: List[dict] = []
        if cfg["parent_key_index"] is None:
            # Parent key field is the primary key — use batch_get_item
            keys_list = list(parent_keys)
            for i in range(0, len(keys_list), 100):
                batch = [{cfg["primary_key"]: k} for k in keys_list[i:i + 100]]
                resp = self.dynamodb.batch_get_item(
                    RequestItems={cfg["name"]: {"Keys": batch}}
                )
                items.extend(resp.get("Responses", {}).get(cfg["name"], []))
                while resp.get("UnprocessedKeys"):
                    resp = self.dynamodb.batch_get_item(
                        RequestItems=resp["UnprocessedKeys"]
                    )
                    items.extend(resp.get("Responses", {}).get(cfg["name"], []))
        else:
            for pk in parent_keys:
                resp = table.query(
                    IndexName=cfg["parent_key_index"],
                    KeyConditionExpression=Key(cfg["parent_key_field"]).eq(pk),
                )
                items.extend(resp.get("Items", []))
                while "LastEvaluatedKey" in resp:
                    resp = table.query(
                        IndexName=cfg["parent_key_index"],
                        KeyConditionExpression=Key(cfg["parent_key_field"]).eq(pk),
                        ExclusiveStartKey=resp["LastEvaluatedKey"],
                    )
                    items.extend(resp.get("Items", []))
        return items

    async def _check_reindex(self, table_name: str, pk_value: str):
        kv_key = f"{table_name}.{pk_value}"
        s = self._stats(table_name)
        s.total += 1
        try:
            entry = await self.nats.v1_objects.get(kv_key)
            if entry is None:
                print(f"  MISSING: {kv_key}")
                s.missing += 1
                s.missing_keys.append(pk_value)
                return
            s.found += 1
            if not self.dry_run:
                await self.nats.v1_objects.put(kv_key, entry.value)
                s.written += 1
                print(f"  reindexed: {kv_key}")
        except Exception as e:
            print(f"  ERROR {kv_key}: {e}")
            s.errors += 1

    async def run(self, projects: List[Project]):
        print(f"\n{'=' * 60}")
        print("Phase 5: DynamoDB Reindex")
        print(f"{'=' * 60}")
        for proj in projects:
            print(f"\n  Project {proj.slug} ({proj.sfid})")
            parent_keys_by_table: Dict[str, Set[str]] = {}
            for cfg in _PRIMARY_TABLES:
                print(f"  Querying {cfg['name']}...")
                try:
                    items = self._query_primary(cfg, proj.sfid)
                except Exception as e:
                    print(f"    ERROR: {e}")
                    continue
                print(f"    Found {len(items)} item(s)")
                pks: Set[str] = set()
                for item in items:
                    pk = str(item.get(cfg["primary_key"], ""))
                    if pk:
                        pks.add(pk)
                        await self._check_reindex(cfg["name"], pk)
                parent_keys_by_table[cfg["name"]] = pks

            for cfg in _SECONDARY_TABLES:
                parent_keys = parent_keys_by_table.get(cfg["parent_table"], set())
                if not parent_keys:
                    continue
                print(f"  Querying {cfg['name']}...")
                try:
                    items = self._query_secondary(cfg, parent_keys)
                except Exception as e:
                    print(f"    ERROR: {e}")
                    continue
                print(f"    Found {len(items)} item(s)")
                for item in items:
                    pk = str(item.get(cfg["primary_key"], ""))
                    if pk:
                        await self._check_reindex(cfg["name"], pk)


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

class OnboardingOrchestrator:
    def __init__(
        self,
        slug: str,
        env: str,
        nats_url: str,
        dry_run: bool,
        fix_mappings: bool,
        recurse: bool,
        skip_committees: bool,
        skip_dynamodb: bool,
    ):
        self.slug = slug
        self.env = env
        self.nats_url = nats_url
        self.dry_run = dry_run
        self.fix_mappings = fix_mappings
        self.recurse = recurse
        self.skip_committees = skip_committees
        self.skip_dynamodb = skip_dynamodb

    async def run(self, token: str):
        print(f"\n{'=' * 60}")
        print("LFX Project Onboarding")
        print(f"  Slug    : {self.slug}")
        print(f"  Env     : {self.env}")
        print(f"  Mode    : {'DRY RUN' if self.dry_run else 'WRITE'}")
        print(f"  Recurse : {self.recurse}")
        print(f"{'=' * 60}\n")

        api = LFXAPIClient(token=token, env=self.env)

        # Phase 1: Resolve project tree
        print("Phase 1: Resolving project tree...")
        projects = api.fetch_project_tree(slug=self.slug, recurse=self.recurse)
        if not projects:
            print(f"ERROR: No projects found for slug={self.slug!r} in {self.env}", file=sys.stderr)
            sys.exit(1)
        print(f"  Found {len(projects)} project(s):")
        for p in sorted(projects, key=lambda x: x.depth):
            indent = "  " * p.depth
            print(f"    {indent}{p.slug} ({p.sfid})")

        async with NATSManager(self.nats_url) as nats:
            # Phase 2: Replay project KV entries
            replayer = ProjectReplayer(nats=nats, dry_run=self.dry_run)
            await replayer.run(projects)

            # Phase 3: Verify mappings
            verifier = MappingVerifier(
                nats=nats,
                dry_run=self.dry_run,
                fix_mappings=self.fix_mappings,
            )
            await verifier.run(projects)

            # Phase 4: Reindex committees
            if not self.skip_committees:
                reindexer = CommitteeReindexer(
                    api=api, nats=nats, dry_run=self.dry_run
                )
                await reindexer.run(projects)
            else:
                print("\n[skipped] Phase 4: Committee Reindex")
                reindexer = None

            # Phase 5: DynamoDB reindex
            if not self.skip_dynamodb:
                ddb = DynamoDBReindexer(nats=nats, dry_run=self.dry_run)
                await ddb.run(projects)
            else:
                print("\n[skipped] Phase 5: DynamoDB Reindex")
                ddb = None

        # Summary
        self._print_summary(
            projects=projects,
            replayer=replayer,
            verifier=verifier,
            committee_reindexer=reindexer,
            ddb_reindexer=ddb,
        )

    def _print_summary(
        self,
        projects: List[Project],
        replayer: ProjectReplayer,
        verifier: MappingVerifier,
        committee_reindexer: Optional[CommitteeReindexer],
        ddb_reindexer: Optional[DynamoDBReindexer],
    ):
        print(f"\n{'=' * 60}")
        print("SUMMARY REPORT")
        print(f"{'=' * 60}\n")
        print(f"Projects resolved: {len(projects)}")

        def _print_stats(label: str, s: PhaseStats):
            status = "OK" if s.missing == 0 and s.errors == 0 else "ISSUES"
            print(f"\n{label}:")
            print(f"  Total   : {s.total}")
            print(f"  Found   : {s.found}")
            print(f"  Missing : {s.missing}")
            if not self.dry_run:
                print(f"  Written : {s.written}")
            if s.errors:
                print(f"  Errors  : {s.errors}")
            print(f"  Status  : {status}")
            if s.missing_keys:
                shown = s.missing_keys[:10]
                print(f"  Missing keys: {', '.join(str(k) for k in shown)}")
                if len(s.missing_keys) > 10:
                    print(f"    ... and {len(s.missing_keys) - 10} more")

        _print_stats("Project KV Replay", replayer.stats)
        _print_stats("Mapping Verification", verifier.stats)

        if committee_reindexer:
            for prefix, s in committee_reindexer.stats.items():
                _print_stats(f"Committees ({prefix})", s)

        if ddb_reindexer:
            for tname, s in ddb_reindexer.stats.items():
                _print_stats(f"DynamoDB ({tname})", s)

        print(f"\n{'=' * 60}\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Onboard a project into LFX One (consolidates steps 2-5)"
    )
    parser.add_argument("slug", help="Project slug to onboard (e.g. agentic-ai-foundation)")
    parser.add_argument(
        "--env",
        default="dev",
        choices=["dev", "staging", "prod"],
        help="LFX environment (default: dev)",
    )
    parser.add_argument(
        "--nats-url",
        default=os.environ.get("NATS_URL", "nats://localhost:4222"),
        help="NATS server URL (default: nats://localhost:4222 or $NATS_URL)",
    )
    parser.add_argument(
        "--reindex",
        action="store_true",
        help="Write KV entries back (default: dry-run, reports only)",
    )
    parser.add_argument(
        "--fix-mappings",
        action="store_true",
        help="Attempt to create missing v1-mappings entries (limited support)",
    )
    parser.add_argument(
        "--skip-committees",
        action="store_true",
        help="Skip Phase 4: committee reindex",
    )
    parser.add_argument(
        "--skip-dynamodb",
        action="store_true",
        help="Skip Phase 5: DynamoDB reindex",
    )
    parser.add_argument(
        "--no-recurse",
        action="store_true",
        help="Only process the top-level project, not its children",
    )
    args = parser.parse_args()

    token = os.environ.get("LFX_API_TOKEN")
    if not token:
        print("ERROR: LFX_API_TOKEN environment variable is required", file=sys.stderr)
        sys.exit(1)

    orchestrator = OnboardingOrchestrator(
        slug=args.slug,
        env=args.env,
        nats_url=args.nats_url,
        dry_run=not args.reindex,
        fix_mappings=args.fix_mappings,
        recurse=not args.no_recurse,
        skip_committees=args.skip_committees,
        skip_dynamodb=args.skip_dynamodb,
    )

    try:
        asyncio.run(orchestrator.run(token))
    except KeyboardInterrupt:
        print("\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

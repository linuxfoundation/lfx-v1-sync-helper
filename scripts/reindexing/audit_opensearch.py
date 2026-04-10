#!/usr/bin/env python3
# Copyright The Linux Foundation and each contributor to LFX.
# SPDX-License-Identifier: MIT
"""
OpenSearch / NATS KV Audit

For each object type, fetches all documents from OpenSearch and checks whether
a corresponding entry exists in the NATS KV bucket. Reports documents that are
in OpenSearch but missing from NATS KV.

Object types checked:
  - committee       → KV bucket: committees        (key: <uuid>)
  - committee_member → KV bucket: committee-members (key: <uuid>)

By default runs in dry-run mode. Pass --delete to remove stale OpenSearch
documents that have no corresponding NATS KV entry.

Required env vars:
  OPENSEARCH_URL   — e.g. https://opensearch.example.com:9200
                     Credentials via --os-user / --os-password

Optional env vars:
  OPENSEARCH_USER
  OPENSEARCH_PASSWORD
"""

import argparse
import asyncio
import os
import sys
from dataclasses import dataclass, field
from functools import partial
from typing import List, Optional, Tuple
from urllib.parse import urlparse

from nats.aio.client import Client as NATS
from nats.js.errors import KeyNotFoundError
from opensearchpy import OpenSearch


SCROLL_SIZE = 500
SCROLL_TTL = "2m"

OBJECT_TYPES = [
    ("committee", "committees"),
    ("committee_member", "committee-members"),
]


@dataclass
class Stats:
    total: int = 0
    in_nats: int = 0
    missing_in_nats: int = 0
    deleted: int = 0
    errors: int = 0
    missing_ids: List[str] = field(default_factory=list)


def _parse_opensearch_url(
    url: str, user: Optional[str], password: Optional[str]
) -> Tuple[str, Optional[str], Optional[str]]:
    """Return (host_url, user, password) — creds from URL take precedence."""
    parsed = urlparse(url)
    if parsed.username:
        user = parsed.username
    if parsed.password:
        password = parsed.password
    # Rebuild without embedded creds
    host = f"{parsed.scheme}://{parsed.hostname}"
    if parsed.port:
        host = f"{host}:{parsed.port}"
    if parsed.path and parsed.path != "/":
        host = f"{host}{parsed.path}"
    return host, user, password


class OpenSearchNATSAuditor:
    def __init__(
        self,
        opensearch_url: str,
        os_user: Optional[str],
        os_password: Optional[str],
        os_index: str,
        nats_url: str,
        dry_run: bool = True,
    ):
        host, user, password = _parse_opensearch_url(
            opensearch_url, os_user, os_password
        )

        auth = (user, password) if user and password else None
        self.os = OpenSearch(
            hosts=[host],
            http_auth=auth,
            use_ssl=host.startswith("https"),
            verify_certs=True,
        )
        self.os_index = os_index
        self.nats_url = nats_url
        self.dry_run = dry_run
        self.nc = None
        self.js = None

    async def connect_nats(self):
        self.nc = NATS()
        await self.nc.connect(servers=[self.nats_url])
        self.js = self.nc.jetstream()
        print(f"Connected to NATS at {self.nats_url}")

    async def _os(self, fn, *args, **kwargs):
        """Run a sync OpenSearch call in a thread so it doesn't block the event loop."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, partial(fn, *args, **kwargs))

    async def disconnect(self):
        self.os.close()
        if self.nc:
            await self.nc.close()

    async def _get_kv(self, bucket: str):
        assert self.js is not None, "NATS JetStream not connected"
        return await self.js.key_value(bucket=bucket)

    async def scroll_opensearch(self, object_type: str) -> List[dict]:
        """Return all documents of the given object_type from OpenSearch."""
        query = {"query": {"term": {"object_type": object_type}}}
        docs = []

        resp = await self._os(
            self.os.search,
            index=self.os_index,
            body=query,
            scroll=SCROLL_TTL,
            size=SCROLL_SIZE,
            _source=["id", "object_type"],
        )
        scroll_id = resp.get("_scroll_id")

        while True:
            hits = resp["hits"]["hits"]
            if not hits:
                break
            docs.extend(hits)
            if len(hits) < SCROLL_SIZE:
                break
            resp = await self._os(
                self.os.scroll, scroll_id=scroll_id, scroll=SCROLL_TTL
            )
            scroll_id = resp.get("_scroll_id")

        if scroll_id:
            try:
                await self._os(self.os.clear_scroll, scroll_id=scroll_id)
            except Exception:
                pass

        return docs

    async def audit_type(self, object_type: str, kv_bucket: str) -> Stats:
        s = Stats()
        print(f"\n--- {object_type} → KV bucket: {kv_bucket} ---")

        print(
            f"  Fetching all '{object_type}' documents from "
            f"OpenSearch index '{self.os_index}'..."
        )
        docs = await self.scroll_opensearch(object_type)
        s.total = len(docs)
        print(f"  Found {s.total} document(s) in OpenSearch")

        if s.total == 0:
            return s

        kv = await self._get_kv(kv_bucket)

        for doc in docs:
            doc_id = doc["_id"]  # OpenSearch document _id
            # Also check the `id` field in _source for the uuid
            source_id = doc.get("_source", {}).get("id") or doc_id

            # Strip to bare UUID (last segment if namespaced)
            uuid = source_id.split(":")[-1] if ":" in source_id else source_id

            try:
                await kv.get(uuid)
                s.in_nats += 1
            except KeyNotFoundError:
                s.missing_in_nats += 1
                s.missing_ids.append(doc_id)

                if self.dry_run:
                    print(f"  [DRY RUN] would delete: {doc_id} (uuid={uuid})")
                else:
                    try:
                        await self._os(self.os.delete, index=self.os_index, id=doc_id)
                        s.deleted += 1
                        print(f"  deleted: {doc_id} (uuid={uuid})")
                    except Exception as e:
                        s.errors += 1
                        print(f"  ERROR deleting {doc_id}: {e}")
            except Exception as e:
                s.errors += 1
                print(f"  ERROR checking {doc_id}: {e}")

        return s

    async def run(self):
        print(f"\n{'=' * 60}")
        print("OpenSearch / NATS KV Audit")
        print(f"Index : {self.os_index}")
        print(f"Mode  : {'DRY RUN' if self.dry_run else 'DELETE'}")
        print(f"{'=' * 60}")

        await self.connect_nats()

        all_stats: List[Tuple[str, str, Stats]] = []
        try:
            for object_type, kv_bucket in OBJECT_TYPES:
                stats = await self.audit_type(object_type, kv_bucket)
                all_stats.append((object_type, kv_bucket, stats))
        finally:
            await self.disconnect()

        self._print_summary(all_stats)

    def _print_summary(self, all_stats: List[Tuple[str, str, Stats]]):
        print(f"\n{'=' * 60}")
        print("SUMMARY REPORT")
        print(f"{'=' * 60}\n")

        grand_total = grand_in_nats = grand_missing = grand_deleted = grand_errors = 0

        for object_type, kv_bucket, s in all_stats:
            grand_total += s.total
            grand_in_nats += s.in_nats
            grand_missing += s.missing_in_nats
            grand_deleted += s.deleted
            grand_errors += s.errors

            status = "OK" if s.missing_in_nats == 0 and s.errors == 0 else "ISSUES"
            print(f"{object_type} (bucket: {kv_bucket}):")
            print(f"  Total in OpenSearch   : {s.total}")
            print(f"  Present in NATS KV    : {s.in_nats}")
            print(f"  Missing in NATS KV    : {s.missing_in_nats}")
            if not self.dry_run:
                print(f"  Deleted from OpenSearch: {s.deleted}")
            if s.errors:
                print(f"  Errors                : {s.errors}")
            print(f"  Status                : {status}")
            if s.missing_ids:
                shown = s.missing_ids[:10]
                print(f"  Missing IDs           : {', '.join(shown)}")
                if len(s.missing_ids) > 10:
                    print(f"    ... and {len(s.missing_ids) - 10} more")
            print()

        print(f"{'=' * 60}")
        print("TOTALS:")
        print(f"  Total in OpenSearch   : {grand_total}")
        print(f"  Present in NATS KV    : {grand_in_nats}")
        print(f"  Missing in NATS KV    : {grand_missing}")
        if not self.dry_run:
            print(f"  Deleted from OpenSearch: {grand_deleted}")
        if grand_errors:
            print(f"  Errors                : {grand_errors}")
        overall = (
            "ALL OK"
            if grand_missing == 0 and grand_errors == 0
            else "STALE DOCUMENTS FOUND"
        )
        print(f"\nOverall Status: {overall}")
        print(f"{'=' * 60}\n")


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Audit OpenSearch against NATS KV — report (and optionally delete) "
            "documents in OpenSearch with no NATS KV entry."
        )
    )
    parser.add_argument(
        "--opensearch-url",
        default=os.environ.get("OPENSEARCH_URL", "http://localhost:9200"),
        help="OpenSearch URL (default: $OPENSEARCH_URL or http://localhost:9200)",
    )
    parser.add_argument(
        "--os-user",
        default=os.environ.get("OPENSEARCH_USER"),
        help="OpenSearch username (default: $OPENSEARCH_USER)",
    )
    parser.add_argument(
        "--os-password",
        default=os.environ.get("OPENSEARCH_PASSWORD"),
        help="OpenSearch password (default: $OPENSEARCH_PASSWORD)",
    )
    parser.add_argument(
        "--os-index",
        default=os.environ.get("OPENSEARCH_INDEX", "resources"),
        help="OpenSearch index to query (default: $OPENSEARCH_INDEX or resources)",
    )
    parser.add_argument(
        "--nats-url",
        default="nats://localhost:4222",
        help="NATS server URL (default: nats://localhost:4222)",
    )
    parser.add_argument(
        "--delete",
        action="store_true",
        help="Delete stale OpenSearch documents. "
        "Without this flag, runs in dry-run mode.",
    )
    args = parser.parse_args()

    auditor = OpenSearchNATSAuditor(
        opensearch_url=args.opensearch_url,
        os_user=args.os_user,
        os_password=args.os_password,
        os_index=args.os_index,
        nats_url=args.nats_url,
        dry_run=not args.delete,
    )

    try:
        asyncio.run(auditor.run())
    except KeyboardInterrupt:
        print("\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

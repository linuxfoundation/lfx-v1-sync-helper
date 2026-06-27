#!/usr/bin/env python3
# Copyright The Linux Foundation and each contributor to LFX.
# SPDX-License-Identifier: MIT
"""
Past Meeting Reindexer

Fetches all v1_past_meeting objects from OpenSearch, then for each one
updates the corresponding NATS KV entry in v1-objects
(key: itx-zoom-past-meetings.<meeting_and_occurrence_id>).

Required env vars:
  OPENSEARCH_URL   — e.g. https://opensearch.example.com:9200

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
OBJECT_TYPE = "v1_past_meeting"
KV_BUCKET = "v1-objects"
KV_PREFIX = "itx-zoom-past-meetings"


@dataclass
class Stats:
    total: int = 0
    reindexed: int = 0
    missing: int = 0
    errors: int = 0
    missing_keys: List[str] = field(default_factory=list)


def _parse_opensearch_url(
    url: str, user: Optional[str], password: Optional[str]
) -> Tuple[str, Optional[str], Optional[str]]:
    parsed = urlparse(url)
    if parsed.username:
        user = parsed.username
    if parsed.password:
        password = parsed.password
    host = f"{parsed.scheme}://{parsed.hostname}"
    if parsed.port:
        host = f"{host}:{parsed.port}"
    if parsed.path and parsed.path != "/":
        host = f"{host}{parsed.path}"
    return host, user, password


class PastMeetingReindexer:
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
        self.kv = None
        self.stats = Stats()

    async def connect(self):
        self.nc = NATS()
        await self.nc.connect(servers=[self.nats_url])
        js = self.nc.jetstream()
        self.kv = await js.key_value(bucket=KV_BUCKET)
        print(f"Connected to NATS at {self.nats_url}")

    async def disconnect(self):
        self.os.close()
        if self.nc:
            await self.nc.close()

    async def _os(self, fn, *args, **kwargs):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, partial(fn, *args, **kwargs))

    async def scroll_opensearch(self) -> List[dict]:
        """Return all v1_past_meeting documents from OpenSearch."""
        query = {"query": {"term": {"object_type": OBJECT_TYPE}}}
        docs = []

        resp = await self._os(
            self.os.search,
            index=self.os_index,
            body=query,
            scroll=SCROLL_TTL,
            size=SCROLL_SIZE,
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

    async def reindex_entry(self, meeting_and_occurrence_id: str):
        kv_key = f"{KV_PREFIX}.{meeting_and_occurrence_id}"
        self.stats.total += 1

        try:
            assert self.kv is not None, "NATS KV not connected"
            entry = await self.kv.get(kv_key)

            if not self.dry_run:
                await self.kv.put(kv_key, entry.value)
                self.stats.reindexed += 1
                print(f"  reindexed: {kv_key}")
            else:
                print(f"  found: {kv_key}")

        except KeyNotFoundError:
            print(f"  MISSING: {kv_key}")
            self.stats.missing += 1
            self.stats.missing_keys.append(meeting_and_occurrence_id)
        except Exception as e:
            print(f"  ERROR {kv_key}: {e}")
            self.stats.errors += 1

    async def run(self):
        print(f"\n{'=' * 60}")
        print("Past Meeting Reindexer")
        print(f"Index  : {self.os_index}")
        print(f"Mode   : {'DRY RUN' if self.dry_run else 'REINDEX'}")
        print(f"{'=' * 60}\n")

        await self.connect()
        try:
            print(f"--- Fetching '{OBJECT_TYPE}' documents from OpenSearch ---")
            docs = await self.scroll_opensearch()
            print(f"  Found {len(docs)} document(s)\n")

            for doc in docs:
                # _id is of the form "v1_past_meeting:<meeting_and_occurrence_id>"
                doc_id = doc["_id"]
                meeting_and_occurrence_id = (
                    doc_id.split(":", 1)[1] if ":" in doc_id else doc_id
                )
                await self.reindex_entry(meeting_and_occurrence_id)

            self.print_summary()
        finally:
            await self.disconnect()

    def print_summary(self):
        s = self.stats
        print(f"\n{'=' * 60}")
        print("SUMMARY REPORT")
        print(f"{'=' * 60}\n")
        print(f"Total checked : {s.total}")
        print(f"Missing       : {s.missing}")
        if not self.dry_run:
            print(f"Reindexed     : {s.reindexed}")
        if s.errors:
            print(f"Errors        : {s.errors}")
        if s.missing_keys:
            shown = s.missing_keys[:10]
            print(f"Missing keys  : {', '.join(shown)}")
            if len(s.missing_keys) > 10:
                print(f"  ... and {len(s.missing_keys) - 10} more")
        overall = (
            "ALL OK" if s.missing == 0 and s.errors == 0 else "SOME MISSING OR ERRORS"
        )
        print(f"\nOverall Status: {overall}")
        print(f"{'=' * 60}\n")


def main():
    parser = argparse.ArgumentParser(
        description="Reindex v1_past_meeting objects from OpenSearch into NATS KV"
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
        "--reindex",
        action="store_true",
        help="Reindex mode — write entries back. "
        "Without this flag, runs dry-run.",
    )
    args = parser.parse_args()

    reindexer = PastMeetingReindexer(
        opensearch_url=args.opensearch_url,
        os_user=args.os_user,
        os_password=args.os_password,
        os_index=args.os_index,
        nats_url=args.nats_url,
        dry_run=not args.reindex,
    )

    try:
        asyncio.run(reindexer.run())
    except KeyboardInterrupt:
        print("\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

# Copyright The Linux Foundation and each contributor to LFX.
# SPDX-License-Identifier: MIT

import unittest
from typing import Any, cast
from unittest import mock

import msgspec
from nats.js.kv import KeyValue

import target_nats_kv


class FakeKV:
    def __init__(self) -> None:
        self.puts: list[tuple[str, bytes]] = []

    async def put(self, key: str, value: bytes) -> None:
        self.puts.append((key, value))


def schema_message(stream: str, key_properties: list[str]) -> str:
    return msgspec.json.encode(
        {
            "type": "SCHEMA",
            "stream": stream,
            "schema": {
                "type": "object",
                "properties": {
                    "workspace_id": {"type": ["string", "null"]},
                    "project_id": {"type": ["string", "null"]},
                    "updated_at": {"type": ["string", "null"]},
                },
            },
            "key_properties": key_properties,
            "bookmark_properties": ["updated_at"],
        }
    ).decode()


def record_message(stream: str, record: dict[str, Any]) -> str:
    return msgspec.json.encode(
        {
            "type": "RECORD",
            "stream": stream,
            "record": record,
        }
    ).decode()


class PersistMessagesTests(unittest.IsolatedAsyncioTestCase):
    async def test_composite_key_properties_are_joined_into_stable_key(self) -> None:
        kv = FakeKV()
        stream = "platform-organization_workspace_project"
        record = {
            "workspace_id": "03099013-f6d4-453b-888d-6fd9bf35fa8b",
            "project_id": "bdf801de-d748-4b72-b64a-f0a26bec68a3:vllm",
            "updated_at": "2026-06-29T10:00:00Z",
        }
        messages = [
            schema_message(stream, ["workspace_id", "project_id"]),
            record_message(stream, record),
        ]

        with mock.patch.object(
            target_nats_kv,
            "next_singer_message",
            return_value=messages,
        ):
            await target_nats_kv.persist_messages(
                cast(KeyValue, kv),
                key_prefix="",
                refresh_mode="full",
                validate_records=True,
                use_msgpack=False,
            )

        self.assertEqual(len(kv.puts), 1)
        key, value = kv.puts[0]
        self.assertEqual(
            key,
            (
                "platform-organization_workspace_project."
                "03099013-f6d4-453b-888d-6fd9bf35fa8b-"
                "bdf801de-d748-4b72-b64a-f0a26bec68a3:vllm"
            ),
        )
        decoded = msgspec.json.decode(value)
        self.assertEqual(decoded["workspace_id"], record["workspace_id"])
        self.assertEqual(decoded["project_id"], record["project_id"])
        self.assertIn("_sdc_received_at", decoded)

    async def test_composite_key_component_with_subject_wildcard_is_skipped(
        self,
    ) -> None:
        kv = FakeKV()
        stream = "platform-organization_workspace_project"
        messages = [
            schema_message(stream, ["workspace_id", "project_id"]),
            record_message(
                stream,
                {
                    "workspace_id": "03099013-f6d4-453b-888d-6fd9bf35fa8b",
                    "project_id": "bad>project",
                    "updated_at": "2026-06-29T10:00:00Z",
                },
            ),
        ]

        with mock.patch.object(
            target_nats_kv,
            "next_singer_message",
            return_value=messages,
        ):
            await target_nats_kv.persist_messages(
                cast(KeyValue, kv),
                key_prefix="",
                refresh_mode="full",
                validate_records=True,
                use_msgpack=False,
            )

        self.assertEqual(kv.puts, [])


if __name__ == "__main__":
    unittest.main()

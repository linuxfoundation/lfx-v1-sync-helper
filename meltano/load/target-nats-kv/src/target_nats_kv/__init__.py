#!/usr/bin/env python3

# Copyright The Linux Foundation and each contributor to LFX.
# SPDX-License-Identifier: MIT

import argparse
import asyncio
import datetime
import io
import sys
from collections.abc import Iterator
from pathlib import Path

import jsonschema
import msgspec
import nats
import simplejson
import singer
from adjust_precision_for_schema import adjust_decimal_precision_for_schema
from nats.js.kv import KeyValue
from singer.messages import (
    ActivateVersionMessage,
    RecordMessage,
    SchemaMessage,
    StateMessage,
)

logger = singer.get_logger()


def emit_state(state: dict | None) -> None:
    """Emit the state to stdout in JSON format."""
    if state is None:
        return
    state_encoder = msgspec.json.Encoder(decimal_format="number")
    line = state_encoder.encode(state).decode("utf-8")
    logger.debug("Emitting state %s", line)
    sys.stdout.write(f"{line}\n")
    sys.stdout.flush()


def next_singer_message() -> Iterator[str]:
    """
    Read UTF-8 encoded Singer messages from stdin.

    This function is a generator that reads lines from stdin and yields them
    one by one.
    """
    input_messages = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8")
    yield from input_messages


async def persist_messages(
    kv_client: KeyValue,
    key_prefix: str,
    refresh_mode: str,
    validate_records: bool = True,
    use_msgpack: bool = False,
) -> dict | None:
    """
    Process Singer messages.

    Singer messages are read from stdin. Schema-validated records are published to
    a NATS JetStream key/value bucket. State messages are captured and returned
    to the sender.

    Args:
        use_msgpack: If True, encode values as msgpack instead of JSON.
    """
    state = None
    schemas: dict[str, dict] = {}
    key_properties: dict[str, list[str]] = {}
    bookmarks: dict[str, (list[str] | None)] = {}
    validators: dict[str, jsonschema.Draft4Validator] = {}
    json_encoder = msgspec.json.Encoder(decimal_format="number")
    msgpack_encoder = msgspec.msgpack.Encoder(decimal_format="number")
    json_decoder = msgspec.json.Decoder()
    msgpack_decoder = msgspec.msgpack.Decoder()

    for message in next_singer_message():
        try:
            o = singer.parse_message(message)
        except simplejson.JSONDecodeError:
            logger.error("Unable to parse: %s", repr(message))
            raise

        if isinstance(o, RecordMessage):
            stream = o.stream
            if stream not in schemas:
                raise Exception(
                    f"A record for stream {stream}"
                    "was encountered before a corresponding schema"
                )

            if stream not in key_properties or len(key_properties[stream]) != 1:
                logger.warning(
                    (
                        "Ignoring record for stream %s because stream "
                        "needs exactly 1 configured key property"
                    ),
                    stream,
                )
                continue

            if key_properties[stream][0] not in o.record:
                logger.warning(
                    (
                        "Ignoring record for stream %s missing "
                        "configured key property %s"
                    ),
                    stream,
                    key_properties[stream][0],
                )
                continue

            primary_key_value = str(o.record[key_properties[stream][0]])
            # Jetstream allows any character in the subject/key except the nul
            # character, space, ., * and >, and it cannot start with "$".
            if len(primary_key_value) == 0:
                logger.warning(
                    "Ignoring record for stream %s with empty primary key",
                    stream,
                )
                continue
            if primary_key_value[0] == "$":
                logger.warning(
                    "Ignoring record for stream %s with primary key starting with $",
                    stream,
                )
                continue
            for char in primary_key_value:
                if char in [" ", ".", "*", ">", "\0"]:
                    logger.warning(
                        (
                            "Ignoring record for stream %s with primary key "
                            "containing invalid character %s"
                        ),
                        stream,
                        repr(char),
                    )
                    continue

            if validate_records:
                try:
                    validators[stream].validate(o.record)
                except jsonschema.ValidationError as e:
                    logger.warning(
                        (
                            "Ignoring record %s for stream %s that fails "
                            "schema validation: %s at .%s"
                        ),
                        primary_key_value,
                        stream,
                        e.message,
                        ".".join(str(i) for i in e.relative_path),
                    )
                    continue

            key = f"{key_prefix}{stream}.{primary_key_value}"

            if o.time_extracted is not None and "_sdc_extracted_at" not in o.record:
                o.record["_sdc_extracted_at"] = o.time_extracted.isoformat()

            if "_sdc_received_at" not in o.record:
                o.record["_sdc_received_at"] = datetime.datetime.now(
                    tz=datetime.UTC
                ).isoformat()

            bookmark_attr = None
            if stream in bookmarks:
                bookmark = bookmarks[stream]
                if bookmark is not None and len(bookmark) > 0:
                    bookmark_attr = bookmark[0]
            if refresh_mode != "full":
                # Compare data using either the "bookmark" or a data
                # comparison.
                try:
                    # Try to fetch current value.
                    current = await kv_client.get(key)
                    if current.value is None or current.value == b"":
                        logger.warning(
                            "Unexpected empty existing value for stream %s with key %s",
                            stream,
                            primary_key_value,
                        )
                        continue
                    # Always try both formats when parsing existing data since
                    # we don't know what format was used when the data was
                    # originally stored.
                    try:
                        current_record = msgpack_decoder.decode(current.value)
                    except (msgspec.DecodeError, ValueError):
                        # Fallback to JSON if msgpack fails.
                        current_record = json_decoder.decode(current.value)

                    # Check if the record was deleted.
                    if "_sdc_deleted_at" in current_record:
                        logger.warning(
                            (
                                "Skipping record for stream %s with key %s due to "
                                "target having been deleted."
                            ),
                            stream,
                            primary_key_value,
                        )
                        continue

                    should_update = False
                    if bookmark_attr is not None:
                        # Compare bookmark values as strings to determine if the
                        # new record should be refreshed based on the refresh mode.
                        # Note, "newer" (which is the default) is implicit in the
                        # "or" clause of this condition (if it's not "full" or
                        # "same", it must be "newer").
                        if bookmark_attr in o.record:
                            bookmark_source = o.record[bookmark_attr]
                        else:
                            bookmark_source = ""

                        if bookmark_attr in current_record:
                            bookmark_target = current_record[bookmark_attr]
                        else:
                            bookmark_target = ""

                        if (
                            refresh_mode == "same"
                            and str(bookmark_source) >= str(bookmark_target)
                        ) or str(bookmark_source) > str(bookmark_target):
                            should_update = True
                    elif refresh_mode == "same":
                        # If there is no bookmark, "same" means always update.
                        should_update = True
                    else:
                        # No bookmark, and refresh_mode=="newer" (by process of
                        # elimination), so, compare the data itself.
                        target_record_filtered = {
                            k: v
                            for k, v in current_record.items()
                            if not k.startswith("_sdc_")
                        }
                        source_record_filtered = {
                            k: v
                            for k, v in o.record.items()
                            if not k.startswith("_sdc_")
                        }
                        if target_record_filtered != source_record_filtered:
                            should_update = True

                    if should_update:
                        # Update with revision
                        if use_msgpack:
                            value = msgpack_encoder.encode(o.record)
                        else:
                            value = json_encoder.encode(o.record)
                        await kv_client.update(
                            key=key,
                            value=value,
                            last=current.revision,
                        )
                        continue
                    else:
                        logger.debug(
                            (
                                "Skipping record for stream %s with key %s due to "
                                "not needing update."
                            ),
                            stream,
                            primary_key_value,
                        )
                        continue

                except nats.js.errors.KeyNotFoundError:
                    # Key doesn't exist, create it.
                    if use_msgpack:
                        value = msgpack_encoder.encode(o.record)
                    else:
                        value = json_encoder.encode(o.record)
                    await kv_client.create(
                        key=key,
                        value=value,
                    )
            else:
                # User has requested "full" sync, so use "put" without
                # data checks.
                if use_msgpack:
                    value = msgpack_encoder.encode(o.record)
                else:
                    value = json_encoder.encode(o.record)
                await kv_client.put(
                    key=key,
                    value=value,
                )

            state = None
        elif isinstance(o, StateMessage):
            logger.debug("Setting state to %s", repr(o.value))
            state = o.value
        elif isinstance(o, SchemaMessage):
            stream = o.stream
            schemas[stream] = o.schema
            adjust_decimal_precision_for_schema(schemas[stream])
            validators[stream] = jsonschema.Draft4Validator(o.schema)
            bookmarks[stream] = o.bookmark_properties
            key_properties[stream] = o.key_properties
        elif isinstance(o, ActivateVersionMessage):
            logger.warning(
                "ACTIVATE_VERSION is unsupported (%s -> %s)", o.stream, o.version
            )
        else:
            logger.warning("Unknown message: %s", repr(o))

    return state


async def run(config: dict) -> dict | None:
    """
    Async entry point after parsing arguments.

    Returns Singer state.
    """
    url = config.get("url", "nats://localhost:4222")
    user = config.get("user", None)
    password = config.get("password", None)
    token = config.get("token", None)
    user_credentials = config.get("creds", None)
    bucket = config.get("bucket", "singer")
    key_prefix = config.get("key_prefix", "")
    refresh_mode = config.get("refresh_mode", "")
    validate_records = config.get("validate_records", True)
    use_msgpack = config.get("msgpack", False)
    if user_credentials is not None:
        user_credentials = Path(user_credentials)

    nats_client = await nats.connect(
        url,
        allow_reconnect=False,  # Does not work?
        max_reconnect_attempts=1,
        user=user,
        password=password,
        token=token,
        user_credentials=user_credentials,
    )

    js_context = nats_client.jetstream()
    kv_client = await js_context.key_value(bucket)

    state = await persist_messages(
        kv_client,
        key_prefix,
        refresh_mode,
        validate_records,
        use_msgpack,
    )
    return state


def main() -> None:
    """Main script entry point."""
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="Config file")
    args = parser.parse_args()

    if args.config:
        config_decoder = msgspec.json.Decoder()
        with open(args.config, "rb") as input_json:
            config = config_decoder.decode(input_json.read())
    else:
        config = {}

    state = asyncio.run(run(config))
    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == "__main__":
    main()

# dynamodb-stream-consumer

Reads DynamoDB Streams and publishes each change record as a JSON message to a
NATS JetStream stream. It is the DynamoDB equivalent of the `wal-listener`
sidecar used for PostgreSQL: downstream consumers (e.g. `lfx-v1-sync-helper`)
subscribe to the NATS stream and act on the events.

## Architecture

```
DynamoDB table(s)
      │  (DynamoDB Streams API – polling)
      ▼
dynamodb-stream-consumer
      │  publishes JSON to NATS JetStream
      ▼
  Stream: dynamodb_streams
  Subject: dynamodb_streams.<table_name>
      │
      ▼
lfx-v1-sync-helper (or any other consumer)
```

One goroutine is started per configured table. Each goroutine runs a shard
discovery loop and spawns an additional goroutine for every open shard. Shard
lists are refreshed periodically to detect splits as DynamoDB scales.

### Checkpointing

After each record is successfully published to NATS, its DynamoDB sequence
number is written to a NATS KV bucket (`dynamodb-stream-checkpoints`) under the
key `{table_name}.{shard_id}`. On restart the consumer resumes from
`AFTER_SEQUENCE_NUMBER` so no records are skipped.

### Deduplication

Each NATS message carries a `Nats-Msg-Id` header set to the DynamoDB sequence
number. JetStream deduplicates within its configured window (default 2 minutes),
so a crash-and-restart that re-publishes in-flight records will not produce
duplicates on the NATS side.

## NATS resources

The service creates both resources on startup if they do not already exist.

### JetStream stream

| Setting | Value |
|---|---|
| Name | `dynamodb_streams` |
| Subjects | `dynamodb_streams.>` |
| Retention | Limits |
| Max age | 14 days |
| Storage | File |
| Compression | S2 |

Pre-create with the NATS CLI:

```bash
nats stream add dynamodb_streams \
  --subjects "dynamodb_streams.>" \
  --retention limits \
  --max-age 14d \
  --storage file \
  --compression s2 \
  --dupe-window 2m \
  --defaults
```

### KV bucket (checkpoints)

| Setting | Value |
|---|---|
| Bucket | `dynamodb-stream-checkpoints` |
| History | 1 |
| Storage | File |

Pre-create with the NATS CLI:

```bash
nats kv add dynamodb-stream-checkpoints --history 1 --storage file
```

## Event format

Each message payload is a JSON object:

```json
{
  "event_id": "abc123",
  "event_name": "INSERT",
  "table_name": "my-table",
  "sequence_number": "000000000000000000001",
  "approximate_creation_time": "2026-01-01T00:00:00Z",
  "new_image": {
    "pk": "some-key",
    "field": "value"
  },
  "old_image": null
}
```

`event_name` is one of `INSERT`, `MODIFY`, or `REMOVE`. `new_image` is `null`
for `REMOVE` events; `old_image` is `null` for `INSERT` events. DynamoDB
attribute types are converted to native JSON types (strings, numbers, booleans,
arrays, objects).

### Subject naming

Dots in DynamoDB table names are replaced with underscores in the NATS subject:

| Table name | NATS subject |
|---|---|
| `my-table` | `dynamodb_streams.my-table` |
| `my.table` | `dynamodb_streams.my_table` |

## Configuration

All configuration is via environment variables.

| Variable | Default | Description |
|---|---|---|
| `DYNAMODB_TABLES` | *(required)* | Comma-separated list of DynamoDB table names |
| `AWS_REGION` | `us-west-2` | AWS region |
| `AWS_ASSUME_ROLE_ARN` | *(unset)* | IAM role ARN to assume via STS for cross-account DynamoDB access |
| `NATS_URL` | `nats://localhost:4222` | NATS server URL |
| `NATS_STREAM_NAME` | `dynamodb_streams` | JetStream stream name |
| `NATS_SUBJECT_PREFIX` | `dynamodb_streams` | Subject prefix |
| `CHECKPOINT_BUCKET` | `dynamodb-stream-checkpoints` | NATS KV bucket for checkpoints |
| `START_FROM_LATEST` | `false` | If `true`, new shards start from `LATEST` instead of `TRIM_HORIZON` |
| `POLL_INTERVAL_MS` | `1000` | Milliseconds to wait between polls when a shard is caught up |
| `SHARD_REFRESH_INTERVAL_SEC` | `10` | Seconds between shard discovery runs per table |
| `PORT` | `8080` | Health check HTTP port |
| `BIND` | `*` | Interface to bind the health check server on |
| `DEBUG` | `false` | Enable debug logging |

AWS credentials are resolved via the standard AWS credential chain. When
`AWS_ASSUME_ROLE_ARN` is set, those credentials are used to assume the specified
role via STS, enabling cross-account DynamoDB access.

## Health checks

| Endpoint | Description |
|---|---|
| `GET /livez` | Always `200 OK` while the process is running |
| `GET /readyz` | `200 OK` when the NATS connection is ready; `503` otherwise |

## Building

```bash
go build -o dynamodb-stream-consumer ./cmd/dynamodb-stream-consumer
```

Or with Docker (multi-arch):

```bash
docker build -f docker/Dockerfile.dynamodb-stream-consumer -t dynamodb-stream-consumer .
```

## Observing events

Subscribe to all tables:

```bash
nats sub "dynamodb_streams.>"
```

Subscribe to one table:

```bash
nats sub "dynamodb_streams.my_table"
```

View checkpoints:

```bash
nats kv ls dynamodb-stream-checkpoints
nats kv get dynamodb-stream-checkpoints <table_name>.<shard_id>
```

## DynamoDB Streams prerequisites

Streams must be enabled on each table with `NEW_AND_OLD_IMAGES` (recommended)
or `NEW_IMAGE` view type.


# Adding a new DynamoDB table to the replication pipeline

This guide walks through every place you need to touch when adding a new DynamoDB table to the Meltano batch-replication pipeline and the real-time DynamoDB stream consumer. Use `surveymonkey-surveys` as the worked example throughout.

## Overview of the pipeline

```
DynamoDB table
    │
    ├─► Meltano (tap-dynamodb)          ← batch / incremental load
    │       └─► target-nats-kv          ← writes to "v1-objects" KV bucket
    │
    └─► dynamodb-stream-consumer        ← real-time CDC stream
            └─► ingest_dynamodb.go      ← writes to "v1-objects" KV bucket
                    │
                    └─► handlers.go     ← KV watcher dispatches to v2 service handlers
```

Both paths write records to the same NATS KV bucket (`v1-objects`). The `v1-sync-helper` service watches that bucket and forwards records to the appropriate v2 microservice.

---

## Step-by-step checklist

### 1. `meltano/meltano.yml` — add to the extractor tables list

Add the table name to the `tables:` list under `tap-dynamodb`:

```yaml
- name: tap-dynamodb
  config:
    tables:
      - surveymonkey-surveys   # ← add here
```

Then add a `replication-key` entry in the `metadata:` block. The key must match the timestamp field that DynamoDB uses to track updates:

```yaml
metadata:
  "*":
    replication-key: modified_at   # default for most tables
  surveymonkey-surveys:
    replication-key: date_modified  # ← table-specific override
```

If the table has no timestamp field, set `replication-key: ""`. The NATS target will still write records but cannot skip already-current entries.

### 2. `charts/lfx-v1-sync-helper/values.yaml` — add to `DYNAMODB_TABLES`

The stream consumer reads this comma-separated list to know which tables to consume from DynamoDB Streams:

```yaml
DYNAMODB_TABLES:
  value: "itx-zoom-meetings-v2,...,surveymonkey-surveys"
```

This controls which tables the `dynamodb-stream-consumer` subscribes to in real time.

### 3. `manifests/dynamodb-list-tables-job.yaml` — add to the access-check loop

This Kubernetes Job verifies that the pod has IAM access to each table before deploying. Add the table name to the space-separated list in the loop variable so the access check runs against it.

### 4. `cmd/lfx-v1-sync-helper/handlers.go` — add KV dispatch case

The `kvHandler` function dispatches incoming KV updates to the right handler based on the table name prefix of the key. Add the new table to the appropriate `case` in both the **put** and **delete** switch blocks.

**For tables handled by an external v2 service** (the service directly consumes the KV bucket itself, e.g. `lfx-v2-survey-service`):

```go
// In handleKVPut:
case "itx-surveys", "itx-survey-responses", "surveymonkey-surveys":
    logger.With("key", key).DebugContext(ctx, "survey record, handled by lfx-v2-survey-service")
    return false

// In handleKVDelete:
case "itx-surveys", "itx-survey-responses", "surveymonkey-surveys":
    logger.With("key", key).DebugContext(ctx, "survey record deleted, handled by lfx-v2-survey-service")
    return false
```

**For tables where `v1-sync-helper` itself calls a v2 API**, add a call to a new handler function.

If the table is not yet handled (you want records ingested into KV but processing deferred), add a debug log case so records are acknowledged without error.

### 5. `cmd/lfx-v1-sync-helper/ingest_dynamodb.go` — extend timestamp comparison if needed

The `shouldDynamoDBUpdate` function guards against a batch-load record overwriting a newer real-time stream event. It checks these fields in order:

1. `modified_at`
2. `last_modified_at`
3. `date_modified`

If the new table uses a **different** timestamp field name, extend the fallback chain in `shouldDynamoDBUpdate`:

```go
// Example: if your table uses "updated_at"
if newModifiedAt == "" {
    newModifiedAt = getTimestampString(newData, "updated_at")
}
```

Add a corresponding test case in `cmd/lfx-v1-sync-helper/ingest_dynamodb_test.go`.

If your table already uses one of the three fields above, no change is needed here.

---

## Worked example: `surveymonkey-surveys`

Here is every change made when this table was added:

| File | Change |
|---|---|
| `meltano/meltano.yml` | Added `surveymonkey-surveys` to `tables:` list; added `replication-key: date_modified` override in `metadata:` |
| `charts/lfx-v1-sync-helper/values.yaml` | Added `surveymonkey-surveys` to `DYNAMODB_TABLES` env var |
| `manifests/dynamodb-list-tables-job.yaml` | Added `surveymonkey-surveys` to the access-check loop |
| `cmd/lfx-v1-sync-helper/handlers.go` | Added to the `"itx-surveys", "itx-survey-responses", "surveymonkey-surveys"` case in both put and delete dispatch |
| `cmd/lfx-v1-sync-helper/ingest_dynamodb.go` | Extended `shouldDynamoDBUpdate` to fall back to `date_modified` after `modified_at` and `last_modified_at` |
| `cmd/lfx-v1-sync-helper/ingest_dynamodb_test.go` | Added `TestShouldDynamoDBUpdate` test cases for `date_modified` |

### Why `date_modified` required an extra change

Most tables use `modified_at` as their update timestamp. The `surveymonkey-surveys` table uses `date_modified` instead. Without the extension to `shouldDynamoDBUpdate`, a DynamoDB stream event arriving after a Meltano batch load could have overwritten newer data (or vice versa), because neither `modified_at` nor `last_modified_at` would be found in the record, causing the guard to default to "always update".

# target-nats-kv

A Singer.io target for streaming records to a NATS JetStream key/value bucket.

## Features

- Streams Singer records to NATS JetStream KV buckets
- Supports both JSON and MessagePack encoding
- Automatic format detection when reading existing data
- Schema validation
- Incremental sync with bookmark support
- Configurable refresh modes

## Configuration

The target supports the following configuration options:

### Connection Settings

- `url` (string): NATS server URL (default: `nats://localhost:4222`)
- `user` (string): NATS username (optional)
- `password` (string): NATS password (optional, sensitive)
- `token` (string): NATS token (optional)
- `creds` (file): Path to NATS credentials file (optional)

### Bucket Settings

- `bucket` (string): Name of the NATS KV bucket (default: `singer`)
- `key_prefix` (string): Prefix to add to all keys (optional)

### Data Format

- `msgpack` (boolean): Use MessagePack encoding instead of JSON (default: `false`)

When `msgpack` is `true`, records will be stored in MessagePack format for more efficient serialization. When reading existing data, the target automatically detects the format (MessagePack or JSON) regardless of the current configuration setting.

### Sync Behavior

- `refresh_mode` (string): How to handle existing records
  - `newer` (default): Only update if the new record is newer
  - `same`: Update if the new record is same age or newer
  - `full`: Always update (overwrite existing data)

- `validate_records` (boolean): Validate records against schema (default: `true`)

## Usage

### Basic Usage

```bash
# Stream data with JSON encoding (default)
tap-something | target-nats-kv --config config.json

# Stream data with MessagePack encoding
tap-something | target-nats-kv --config config-msgpack.json
```

### Configuration Examples

#### JSON Mode (Default)
```json
{
  "url": "nats://localhost:4222",
  "bucket": "my-data",
  "msgpack": false
}
```

#### MessagePack Mode
```json
{
  "url": "nats://localhost:4222",
  "bucket": "my-data",
  "msgpack": true
}
```

## Data Format Compatibility

The target maintains full backward and forward compatibility:

- When reading existing data, it automatically detects whether the data is stored as JSON or MessagePack
- You can switch between JSON and MessagePack modes without data migration
- Mixed format buckets are supported (some keys in JSON, others in MessagePack)

## Key Structure

Keys in the NATS KV bucket follow this pattern:
```
{key_prefix}{stream_name}.{primary_key_value}
```

For example:
- Stream: `users`
- Primary key: `12345`
- Key prefix: `app-`
- Resulting key: `app-users.12345`

## Requirements

- Python 3.11+
- NATS JetStream server with KV bucket support
- Singer-compatible tap as data source

## Dependencies

- `nats-py`: NATS client library
- `singer-python`: Singer message format support
- `msgpack`: MessagePack serialization
- `jsonschema`: Schema validation
- `simplejson`: JSON processing

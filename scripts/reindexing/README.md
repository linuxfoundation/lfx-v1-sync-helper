# Reindexing Scripts

Scripts for reindexing v1 project resources into the v2 sync pipeline via DynamoDB and NATS KV.

## Prerequisites

- Python 3.12+
- AWS credentials with DynamoDB read access
- NATS server with JetStream enabled and access to the `v1-objects` KV bucket

## Setup

```sh
uv sync
```

Or with pip:

```sh
pip install -r requirements.txt
```

## Scripts

Detailed documentation for each script is forthcoming. See the individual script files for usage and `--help` flags.

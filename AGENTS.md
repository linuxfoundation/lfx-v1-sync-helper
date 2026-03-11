# AGENTS.md

This file provides essential information for AI agents working on the LFX v1 Sync Helper codebase. It focuses on development workflows, architecture understanding, and build processes needed for making code changes.

## Repository Overview

The LFX v1 Sync Helper enables data synchronization between LFX v1 and LFX One (v2) platforms with three main components:

1. **Meltano ETL Pipeline**: Python-based data extraction and loading (DynamoDB → NATS KV)
2. **v1-sync-helper Service**: Go microservice for data synchronization (NATS KV → LFX One APIs)
3. **Helm Charts**: Kubernetes deployment manifests

## Architecture Overview

### Data Flows
```text
LFX v1 Sources → Meltano → NATS KV → v1-sync-helper → LFX One APIs
```

### Key Components
- **DynamoDB (Meetings)** + **PostgreSQL (Projects/Committees)** → **Meltano** → **NATS KV Bucket (`v1-objects`)**
- **NATS KV Watcher** → **v1-sync-helper** → **LFX One Project/Committee Services**
- **JWT Authentication** via Heimdall impersonation for secure API calls
- **ID Mappings** stored in NATS KV bucket (`v1-mappings`)
- **Data Encoding** supports both JSON and MessagePack formats with automatic detection

## Repository Structure

```text
lfx-v1-sync-helper/
├── meltano/                   # Python ETL pipeline
│   ├── meltano.yml            # Main Meltano configuration
│   └── load/target-nats-kv/   # Custom NATS KV target plugin
├── cmd/lfx-v1-sync-helper/    # Go microservice source
├── charts/lfx-v1-sync-helper/ # Helm deployment charts (Chart.yaml version is dynamic on release)
├── docker/                    # Docker build configurations
│   ├── Dockerfile.v1-sync-helper  # Go service container
│   └── Dockerfile.meltano         # Python ETL container
├── .github/workflows/         # CI/CD pipelines
├── Makefile                   # Build automation
└── pyproject.toml             # Python dependency management (uv)
```

## Development Workflow

### Local Setup

1. **Initialize Python environment:**
   ```bash
   uv sync
   ```

2. **Verify Meltano installation:**
   ```bash
   cd meltano
   uv run meltano dragon
   ```

3. **Build Go service:**
   ```bash
   make build
   ```

4. **Run all checks:**
   ```bash
   make check  # Runs fmt, vet, lint
   ```

## Build System (Makefile)

### Container Build Targets

| Target                        | Description                                             |
|-------------------------------|---------------------------------------------------------|
| `all`                         | Complete build pipeline (clean, deps, fmt, lint, build) |
| `build`                       | Compile optimized Go binary                             |
| `debug`                       | Build with debug symbols and race detection             |
| `clean`                       | Clean build artifacts                                   |
| `check`                       | Run formatting, vetting, and linting                    |
| `docker-build-v1-sync-helper` | Build Go service container                              |
| `docker-build-meltano`        | Build Python ETL container                              |
| `docker-build-all`            | Build both containers                                   |
| `docker-run-v1-sync-helper`   | Run Go service container (requires .env)                |
| `docker-run-meltano`          | Run Meltano container (shows dragon)                    |

### Container Configuration

- **v1-sync-helper Image:** `ghcr.io/linuxfoundation/lfx-v1-sync-helper/v1-sync-helper:latest`
- **Meltano Image:** `ghcr.io/linuxfoundation/lfx-v1-sync-helper/meltano:latest`

## Container Builds

### v1-sync-helper (Go Service)
- **Multi-stage build** with Chainguard base images
- **Multi-architecture:** linux/amd64, linux/arm64
- **Security:** Non-root execution, minimal attack surface
- **Build:** `make docker-build-v1-sync-helper`

### Meltano (Python ETL)
- **uv-based dependency management** with locked dependencies
- **Multi-stage build** for optimal image size
- **ADR-0001 compliant** Python containerization
- **Build:** `make docker-build-meltano`
- **Entry:** `ENTRYPOINT ["meltano"]` with flexible command support

## Code Architecture

### Go Service (v1-sync-helper)

#### Key Implementation Patterns
1. **KV Bucket Watcher**: Watches NATS KV bucket instead of direct message consumption
2. **Direct API Calls**: Routes data via LFX One API services
3. **JWT Authentication**: Reuses Heimdall's signing key for secure API calls
4. **Mapping Storage**: Maintains v1-to-v2 ID mappings in NATS KV

#### User Impersonation Logic
- **Machine Users** (`@clients` suffix): Principal `{client_id}@clients`, Subject `{client_id}`
- **Regular Users**: Lookup via LFX v1 User Service API, 6-hour cache with 10-minute refresh
- **Fallback**: `v1_sync_helper@clients` when lookup fails

#### Scaling Architecture
- **JetStream Pull Consumer** with delivery groups for horizontal scaling
- **Non-ephemeral consumer** for reliability
- **Load-balanced message processing** across instances

#### Data Format Handling
- **Automatic Detection**: Tries MessagePack first, falls back to JSON
- **Backward Compatible**: Can read both JSON and MessagePack encoded data
- **Format Agnostic**: Processing logic unchanged regardless of encoding format

### Python ETL (Meltano)

#### Configuration Structure
- **`meltano.yml`**: Main project configuration
- **Environment-specific settings**: dev, staging, prod
- **Custom target plugin**: `load/target-nats-kv/` for NATS KV integration

#### Data Sources
- **DynamoDB**: Meetings data extraction
- **PostgreSQL**: Projects and committees data
- **NATS KV**: Target for all extracted data

#### Data Format Support
- **JSON** (default): Standard JSON encoding for record storage
- **MessagePack**: Compact binary serialization with `msgpack: true` configuration (Meltano) or `USE_MSGPACK=true` (WAL handler)
- **Automatic Detection**: Both Go service and Python plugin automatically detect format when reading existing data

## CI/CD Integration

### GitHub Actions Workflows

1. **publish-main.yaml**: Builds on main branch push
   - Uses `ko` for efficient Go v1-sync-helper builds
   - Multi-architecture support (linux/amd64, linux/arm64)
   - SBOM generation
   - Tags: `{commit-sha}`, `development`

2. **publish-release.yaml**: Tagged release builds
   - **publish-v1-sync-helper**: Go service build using ko
   - **publish-meltano**: Python/Meltano Docker build (depends on v1-sync-helper)
   - **release-helm-chart**: Helm chart publishing (depends on both containers)
   - **create-ghcr-helm-provenance**: SLSA provenance for Helm chart
   - **create-meltano-provenance**: SLSA provenance for Meltano container
   - Multi-architecture support for v1-sync-helper (linux/amd64, linux/arm64)
   - Single architecture for Meltano (linux/amd64)
   - Artifact signing with Cosign
   - Complete SLSA provenance generation
   - Sequential execution: v1-sync-helper → meltano → helm-chart

3. **mega-linter.yml**: Code quality enforcement
   - Cupcake flavor (Go + Python)
   - Security scanning
   - License header validation

4. **license-header-check.yml**: Copyright validation

## Development Guidelines

### Go Code Standards
- Follow standard Go conventions
- Use structured JSON logging

### Python Code Standards
- Use `uv` for dependency management
- Follow Meltano best practices
- Maintain `pyproject.toml` and `uv.lock` consistency
- Environment-based configuration

### Data Serialization
- **target-nats-kv** supports both JSON and MessagePack encoding
- Set `msgpack: true` in Meltano configuration to enable MessagePack
- Set `USE_MSGPACK=true` environment variable for WAL handler to use MessagePack
- Boolean environment variables accept truthy values: "true", "yes", "t", "y", "1" (case-insensitive)
- Automatic format detection when reading existing data for compatibility
- Go service handles both formats transparently
- WAL handler respects the same encoding configuration as Meltano for consistency

### Container Standards
- Multi-stage builds for size optimization
- Non-root execution for security
- Chainguard base images when possible
- Selective file copying with .dockerignore

## Debugging and Monitoring

### Health Endpoints
- **`/livez`**: Liveness probe
- **`/readyz`**: Readiness probe with NATS connectivity check

### Logging Structure
JSON-formatted logs with consistent fields:
- `key`: KV bucket key being processed
- `operation`: KV operation type (PUT, DELETE)
- `slug`/`sfid`: Object identifiers
- `project_uid`/`committee_uid`: Generated v2 UUIDs
- `username`: Extracted from v1 `lastmodifiedbyid`

### Debug Mode
Enable with `DEBUG=true` environment variable for detailed operation logs.

## Adding New PostgreSQL Tables to Replication

When adding a new table from PostgreSQL to the `v1-objects` NATS KV replication pipeline, three places must be updated:

### 1. `meltano/meltano.yml` — Meltano backfill extractor

Add a `select` entry for the table under the `tap-postgres` extractor (use a wildcard, e.g. `myschema-mytable.*`), add the schema to `filter_schemas` if not already present, and add a `metadata` entry specifying `INCREMENTAL` replication with the appropriate replication key (`lastmodifieddate` or `systemmodstamp`).

After editing, or if the file was last written by the Meltano CLI (which uses non-standard sequence indentation), reformat it with prettier before committing:

```bash
npx prettier --write meltano/meltano.yml
```

### 2. `charts/lfx-v1-sync-helper/values.yaml` — WAL listener table filter

Add the table to `walListener.config.listener.filter.tables` with `insert`, `update`, and `delete` operations. Use only the bare table name (no schema prefix), since wal-listener filters on `item.Table` alone (e.g. `Account`, not `salesforce_b2b.Account`). Table names are case-sensitive.

### 3. PostgreSQL `wal-listener` publication — per-environment, ad hoc

The `wal-listener` publication on the PostgreSQL server is managed manually and must be updated in each environment (dev, staging, prod) by running:

```sql
ALTER PUBLICATION "wal-listener" ADD TABLE myschema."MyTable";
```

This is not managed by Helm or any IaC — it must be applied directly against the `sfdc` database in each environment. Verify the current publication contents with:

```sql
SELECT schemaname, tablename
FROM pg_publication_tables
WHERE pubname = 'wal-listener'
ORDER BY schemaname, tablename;
```

Note: The replication slot is named `lfx_v2` (not `wal-listener`). The publication and slot names differ.

### 4. `cmd/lfx-v1-sync-helper/handlers.go` — suppress unknown-object warnings (optional)

If the new table's records should only be stored in KV for downstream consumption (no v2 API side-effects), add the key prefix (e.g. `"myschema-mytable"`) as an explicit `case` in both `handleKVPut` and `handleResourceDelete` with a debug-level log statement. This prevents spurious "unknown object type" warnings in the logs.

## Contributing Workflow

1. **Code Changes**: Follow language-specific standards
2. **Formatting**: Use `make check` for Go code formatting
3. **Container Builds**: Test with `make docker-build-all`
4. **CI Validation**: Ensure MegaLinter and license checks pass

This documentation focuses specifically on the technical aspects needed for codebase development and modification.

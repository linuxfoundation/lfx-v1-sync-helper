# AGENTS.md

This file provides essential information for AI agents working on the LFX v1 Sync Helper codebase. It focuses on development workflows, architecture understanding, and build processes needed for making code changes.

> **Important: invoke `/lfx-skills:lfx` for any cross-repo task or "where does X live" question.** Routes to owning repos and pulls in their CLAUDE.md, skills, and rules. If `/lfx-skills:lfx` is not found, install with `/plugin marketplace add linuxfoundation/lfx-skills` then `/plugin install lfx-skills@lfx-skills`.

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
- **DynamoDB** + **PostgreSQL (Projects/Committees)** → **Meltano** → **NATS KV Bucket (`v1-objects`)**
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
- **DynamoDB**: Data extraction
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

When adding a new table from PostgreSQL to the `v1-objects` NATS KV replication pipeline, four places must be updated:

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

### 4. `tap-postgres-catalog` ConfigMap — per-environment, ad hoc

The PostgreSQL Meltano CronJob runs with `--catalog /catalogs/tap-postgres/catalog.json`, so `meltano.yml` changes are not picked up at runtime until the manually managed `tap-postgres-catalog` ConfigMap is regenerated in each environment.

After updating `meltano/meltano.yml`, regenerate the per-environment ConfigMap, review the generated catalog for the new stream(s), and apply it manually to the target namespace.

Repeat for staging and prod with the matching Kubernetes context, AWS profile, and PostgreSQL credentials. This ConfigMap is intentionally not managed by Helm.

## One-shot Backfill Commands

The following CLI flags each perform their work and then exit without starting the NATS subscriber. All are mutually exclusive. Use `--dry-run` with any of them to preview changes without writing (note: `--rebuild-user-secondary-indexes` does not support `--dry-run`).

### `EnumerateLiveSubjects` — common stream-walk abstraction (`nats_enumerate.go`)

All one-shot backfill and reindex jobs that need to scan a KV-backed JetStream stream use the shared `EnumerateLiveSubjects` function. It creates a headers-only ephemeral pull consumer with `DeliverAllPolicy` (O(1) creation), walks all messages in the filtered stream, applies client-side last-write-wins dedup, and returns `map[string]struct{}` of subjects whose latest revision is not a DEL/PURGE tombstone.

**Any new backfill or reindex pass that needs to enumerate KV bucket keys should use `EnumerateLiveSubjects` rather than implementing its own consumer loop.** Callers that need payloads should do point reads (`KV.Get`) after enumeration.

Key design decisions:
- **`cons.Info()` with generous timeout** — checks `NumPending==0` after each batch as the authoritative end-of-stream signal. This is a correctness requirement: on sparse streams, relying on empty-batch termination alone can silently return incomplete results. Timeout defaults to 120s (configurable via `WithInfoTimeout`) to accommodate large buckets.
- **No full-body walk** — callers do point reads after enumeration (same pattern the user reindex already uses at larger scale).
- **Options**: `WithFetchMaxWait` (default 120s from `defaultNATSFetchMaxWait`), `WithBatchSize` (default 512), `WithInfoTimeout` (default 120s), `WithLogger`.
- **Ephemeral consumer lifecycle**: `MemoryStorage: true`, `InactiveThreshold: 5m`, explicitly deleted in defer.

### Auth0 Management API enumeration — pattern for user-centric backfills (`backfill_email_profile.go`)

Backfills whose outer loop is over **Auth0 users** (rather than NATS KV keys) use the Auth0 Management API `Search()` call with a Lucene query instead of `EnumerateLiveSubjects`. Use this pattern when the canonical source of iteration is the Auth0 user set, not a NATS KV bucket.

Key design decisions:
- **Connection filter**: `Username-Password-Authentication` only — social/enterprise connections are not v1 platform accounts.
- **Sort**: `updated_at:1` (ascending) so the cursor advances monotonically and recent changes are processed last.
- **Resumable cursor**: the `updated_at` value of the last processed user is stored as a plain string in the `v1-mappings` KV bucket (e.g. `backfill.alternate-emails.cursor`). On the next run an inclusive range query `[cursor TO *]` re-processes the last user from the previous run; all per-user operations are idempotent.
- **`--limit N`**: caps Auth0 users fetched per run (default 1000). Multiple runs advance the cursor through the full user population.
- **No `EnumerateLiveSubjects`**: NATS KV is used for side-lookups (e.g. fetching the v1 SFID mapping) and cursor storage, not as the enumeration source.

### `--backfill-acs-project [--dry-run]` (`ingest_acs_project.go`)

The `--backfill-acs-project` flag runs the project grants pass (`backfillACSProjectGrants`), which backfills ACS legacy user grants into v2 project settings:

- **SFID source**: `EnumerateLiveSubjects` on `KV_v1-mappings` with filter `$KV.v1-mappings.project.sfid.*`, followed by point reads to get the SFID → v2 UID mapping value.
- **ACS query**: `GET /acs/v1/api/grantusers?object_type=project&object_id={sfid}&rolename=admin,viewer,meetings-coordinator` (paginated). `admin` → `Writers`; `viewer` → `Auditors`; `meetings-coordinator` → `MeetingCoordinators`.
- **Settings API**: `GetProjectSettings` / `UpdateProjectSettings` via Goa project-service client.
- **Merge**: additive-only; existing v2-only entries are logged as "extra" but never removed.
- **Dry-run**: add `--dry-run` to preview without writing.
- **Summary log fields**: `processed`, `errors`.
- **Manifest**: `manifests/backfill-acs-job.yaml`.

### `--backfill-acs-org [--dry-run]` (`ingest_acs_org.go`)

Backfills ACS legacy org grants into v2 b2b_org settings:

- **SFID source**: `EnumerateLiveSubjects` on `KV_v1-objects` with filter `$KV.v1-objects.salesforce_b2b-Account.*`, followed by point reads and `isLiveMemberOrgAccount` filtering. Skips records where `IsDeleted=true` or `IsMember__c!=true`.
- **UID resolution**: `sfutil.Normalize18(sfid)` — as of LFXV2-2049 the b2b_org UID is the 18-char normalized SFID. No network call.
- **ACS query**: `GET /acs/v1/api/grantusers?object_type=organization&object_id={sfid}&rolename=company-admin,viewer` (paginated). `company-admin` → `writer`; `viewer` → `auditor`.
- **Settings API**: raw HTTP `GET`/`PUT /b2b_orgs/{uid}/settings` via `client_members.go`. Requires `MEMBER_SERVICE_URL` env var.
- **Merge**: additive-only; existing v2-only entries are logged as "extra" but never removed.
- **Dry-run**: add `--dry-run` to preview without writing.
- **Summary log fields**: `orgs_total`, `orgs_changed`, `writers_added`, `auditors_added`, `orgs_skipped`, `errors`.

### `--backfill-alternate-emails [--limit N] [--dry-run]` (`backfill_email_profile.go`)

Iterates Auth0 users (Username-Password-Authentication connection only), sorted by `updated_at` ascending, and links any v1 verified alternate emails not yet linked as Auth0 email-connection identities.

- **Cursor**: stored at `v1-mappings` key `backfill.alternate-emails.cursor` (updated_at of last processed user). Re-run to advance. Uses an inclusive range query so the last user of the previous run is re-processed on the next run; all operations are idempotent.
- **Per-user flow**: resolves v1 SFID via username secondary index → fetches alternate email SFIDs from `v1-mappings` → calls `linkEmailIdentity` for each verified, active, non-primary email.
- **`--limit N`** (default 1000): caps users processed per run.
- **Summary log fields**: `users_processed`, `emails_linked`, `emails_skipped`, `errors`.
- **Manifest**: `manifests/backfill-alternate-emails-job.yaml`.

### `--backfill-profiles [--limit N] [--dry-run]` (`backfill_email_profile.go`)

Iterates Auth0 users (same connection filter and sort), syncs v1 profile fields (name, title, address, org, etc.) to Auth0 `user_metadata` via `syncProfileToAuth0`. No-ops when nothing has changed.

- **Cursor**: stored at `v1-mappings` key `backfill.profiles.cursor`. Same inclusive-cursor behavior as `--backfill-alternate-emails`.
- **`--limit N`** (default 1000): caps users processed per run.
- **Summary log fields**: `users_processed`, `users_updated`, `users_skipped`, `errors`.
- **Manifest**: `manifests/backfill-profiles-job.yaml`.
- **Replaces** the removed `PROFILE_SYNC_BACKFILL` environment variable.

### `--sync-user <username> [--dry-run]` (`backfill_email_profile.go`)

Performs a full sync (profile + alternate emails) for a single user identified by their Auth0 username (the part after `auth0|`). Useful for debugging or targeted re-sync without a full backfill run.

### 4. `cmd/lfx-v1-sync-helper/handlers.go` — suppress unknown-object warnings (optional)

If the new table's records should only be stored in KV for downstream consumption (no v2 API side-effects), add the key prefix (e.g. `"myschema-mytable"`) as an explicit `case` in both `handleKVPut` and `handleResourceDelete` with a debug-level log statement. This prevents spurious "unknown object type" warnings in the logs.

## Contributing Workflow

1. **Code Changes**: Follow language-specific standards
2. **Formatting**: Use `make check` for Go code formatting
3. **Container Builds**: Test with `make docker-build-all`
4. **CI Validation**: Ensure MegaLinter and license checks pass

This documentation focuses specifically on the technical aspects needed for codebase development and modification.

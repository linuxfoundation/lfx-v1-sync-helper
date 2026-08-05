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

#### v1-mappings Store (`mapping_store*.go`) — LFXV2-2985

The v1-mappings backing store is behind a `MappingStore` port so callers stay identical across the KV → Postgres migration. The store surface mirrors `jetstream.KeyValue` (Get / Put / Update / Create / Delete with revision-based optimistic concurrency), and every online call site that previously used `mappingsKV.<op>` will be routed through the package-global `mappingStore` variable.

Backends:

- **`kvMappingStore`** (`mapping_store_kv.go`) — thin adapter over the existing `jetstream.KeyValue` bucket. Translates jetstream sentinel errors (`ErrKeyNotFound`, `ErrKeyExists`, JS API code 10071 "wrong last sequence") to the port-level sentinels (`ErrKeyNotFound`, `ErrKeyExists`, `ErrRevisionMismatch`). Uses the existing `isRevisionMismatchError` helper for CAS mismatch detection.
- **`pgMappingStore`** (`mapping_store_pg.go`) — pgx over the `v1_mappings` table. Translates the KV tombstone sentinel (`[]byte("!del")`) to the `tombstoned` boolean column on write and re-materialises it on read, so `isTombstonedMapping(entry.Value)` fires identically across backends. Update uses `UPDATE ... WHERE version=$expected RETURNING version` (zero rows → `ErrRevisionMismatch`); Create uses `INSERT ... ON CONFLICT DO NOTHING RETURNING version` (zero rows → `ErrKeyExists`); Put unconditionally upserts and bumps `version` on conflict.
- **`dualMappingStore`** (`mapping_store_dual.go`) — the safe steady state during rollout. Reads Postgres, falls back to KV on `ErrKeyNotFound` (logs a warn-level `dual-store read served from KV fallback` event so operators can measure drift). Writes go KV-first, PG-second: if KV fails the whole op fails and PG is untouched (so a rollback to `V1MappingsStoreModeKV` sees a consistent state); if PG fails after KV succeeded the op is still reported successful and drift is logged at error level.

Mode selection (`V1_MAPPINGS_STORE_MODE`, default `dual`):

- **`kv`**: adapter only, no pgxpool opened. Pre-migration behaviour bit-for-bit; used as a rollback target.
- **`dual`**: opens the pgxpool, applies `internal/schema/schema.sql`, wraps both backends in `dualMappingStore`. Default.
- **`postgres`**: opens the pgxpool and returns `pgMappingStore` directly. Used once the KV bucket is ready to be decommissioned.

Boot ordering (`main.go`): NATS + KV bucket handles → `initMappingStore(ctx, cfg, mappingsKV)` (which may open pgxpool + `schema.Apply`) → subscriptions. `pgPool` is a package-global closed in graceful shutdown; nil in `kv` mode.

**Adding a new caller.** Import the sentinel errors from mapping_store.go, use `mappingStore.<op>` in place of `mappingsKV.<op>`, and switch `err == jetstream.ErrKeyNotFound` checks to `errors.Is(err, ErrKeyNotFound)`. The `entry.Value()` method call becomes the `entry.Value` field access. The `lookup_handler.go` migration is the reference example.

**Chart wiring.** The Postgres cluster is provisioned by `charts/lfx-v1-sync-helper/templates/database.yaml`, selected via `.Values.database.mode` (`external` | `database` | `cluster+database`). The app deployment forwards CNPG operator-managed `<clusterName>-app` Secret keys (or external Secret keys, or a single-key `DATABASE_URL`) as env vars and the service composes the libpq DSN in-process via `Config.ResolveDatabaseURL` — never as a literal env-var value, to avoid leaking the password through `kubectl describe pod`. When `database.mode=external` with no `secretName`, the deployment injects `V1_MAPPINGS_STORE_MODE=kv` automatically so a chart install without Postgres wiring still boots. Setting `.Values.app.environment.V1_MAPPINGS_STORE_MODE.value` overrides that safety fallback.

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

### `ScanSubjectData` — stream scan abstraction for all KV enumeration (`nats_scan.go`)

All one-shot backfill and reindex jobs that need to scan a KV-backed JetStream stream use `ScanSubjectData`. It uses sequential `stream.GetMsg` calls with `jetstream.WithGetMsgSubject` (`next_by_subj` API): each call is an independent NATS request-reply asking the server for the first message at seq ≥ N matching the subject filter.

**Why not an ephemeral consumer?** Both `KV_v1-objects` (54M sequences, 35.6M tombstones) and `KV_v1-mappings` (34M sequences) in prod are too large for consumer-based enumeration. A `DeliverAllPolicy` consumer streams all sequences through a single connection, saturating NATS server CPU (~357%), preventing heartbeat delivery, and causing `nats: no heartbeat received` failures.

**Signature**: `ScanSubjectData(ctx, js, streamName, subjectFilter, opTimeout) (map[string][]byte, error)`

Returns `map[string][]byte` (subject → latest payload) with LWW applied; tombstoned subjects are excluded. Callers get payloads directly — no second-pass point reads needed.

Key design decisions:
- **No consumer** — no heartbeat, no consumer lifecycle, no CPU spike from bulk streaming.
- **LWW via seq order** — messages are visited ascending; last write per subject overwrites earlier ones.
- **`ErrMsgNotFound` = end of stream** — the clean signal that no further messages match the filter.
- **Per-call deadline** — each `GetMsg` call uses `context.WithTimeout(ctx, opTimeout)`. Pass the appropriate per-call timeout: `cfg.NATSFetchMaxWait` (default 120s) for backfill scans, `cfg.ReindexNATSOpTimeout` (default 30s) for reindex scans. Avoid the SDK's 5s default — it is too short for in-cluster use.
- **Wildcard subjects** — the `next_by_subj` NATS server API accepts NATS subject wildcards.

**Streaming variant — `ScanSubjectDataStreamRange(ctx, js, streamName, subjectFilter, startSeq, endSeq, opTimeout, cb) (visits int, tombstoned int, err error)`**

Used only for jobs whose full result set is too large to fit in memory as a `map[string][]byte` (currently just `--backfill-v1-mappings-to-postgres`; the KV_v1-mappings bucket is ~5.8 GiB / ~38M subjects in prod). Key differences from `ScanSubjectData`:

- **Callback-based** — visits are delivered to `cb(subject, data, seq, deleted)` as they are read; nothing is retained in-scanner. Memory is O(1) per scanner.
- **No LWW resolved in-scanner** — the sink receives every visit including DEL/PURGE, tagged with the JetStream seq and a `deleted` flag. Callers implement LWW via `DISTINCT ON (subject) ORDER BY seq DESC` (or equivalent).
- **Half-open [startSeq, endSeq) range** — lets callers partition [1, `maxSeq`] across N concurrent workers for wall-clock parallelism. `endSeq=0` means unbounded (scan to end).
- Use `ScanSubjectData` when the result set fits comfortably in memory; use `ScanSubjectDataStreamRange` when it does not or when you want to parallelise across the sequence space.

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

- **SFID source**: `ScanSubjectData` on `KV_v1-mappings` with filter `$KV.v1-mappings.project.sfid.*`. Returns subject → payload map directly; no separate point reads.
- **ACS query**: `GET /acs/v1/api/grantusers?object_type=project&object_id={sfid}&rolename=admin,viewer,meetings-coordinator` (paginated). `admin` → `Writers`; `viewer` → `Auditors`; `meetings-coordinator` → `MeetingCoordinators`.
- **Settings API**: `GetProjectSettings` / `UpdateProjectSettings` via Goa project-service client.
- **Merge**: additive-only; existing v2-only entries are logged as "extra" but never removed.
- **Dry-run**: add `--dry-run` to preview without writing.
- **Summary log fields**: `processed`, `errors`.
- **Manifest**: `manifests/backfill-acs-job.yaml`.

### `--backfill-acs-org [--dry-run]` (`ingest_acs_org.go`)

Backfills ACS legacy org grants into v2 b2b_org settings:

- **SFID source**: `ScanSubjectData` on `KV_v1-objects` with filter `$KV.v1-objects.salesforce_b2b-Account.*`. Returns subject → payload map directly; each payload passed through `isLiveMemberOrgAccount` to filter deleted/non-member records.
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

### `--backfill-v1-mappings-to-postgres [--dry-run]` (`backfill_v1_mappings_pg.go`)

One-shot copy of the entire `v1-mappings` NATS KV bucket into the Postgres `v1_mappings` table. First step of the LFXV2-2985 migration off NATS KV; runs before the online dual-write / cutover paths land.

Designed for the prod scale (~5.8 GiB / ~38M subjects). Streaming scan + parallel scanners + `COPY` into a staging table + single DISTINCT ON upsert. Wall-clock is dominated by NATS `next_by_subj` round-trip time and scales roughly with `maxSeq / workers`; measure on a representative snapshot before sizing job timeouts. The `elapsed` field in the summary log records actuals.

- **Config**: uses `LoadReindexConfig()` (NATS + timeouts only) plus Postgres env: `DATABASE_URL` OR the composed set (`PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`). Password composition uses `url.UserPassword` to percent-encode special characters and to avoid embedding the CNPG-generated password as a literal substring in the pod spec.
- **Tuning env**: `BACKFILL_V1_MAPPINGS_WORKERS` (default 8, clamped [1, 64]) sets scanner concurrency; `BACKFILL_V1_MAPPINGS_BATCH_SIZE` (default 50000) sets rows-per-COPY-flush. Higher worker counts trade wall-clock for NATS server CPU — the prod incident that motivated this ticket showed the server saturates around 350% CPU with consumer-based enumeration, so leave headroom.
- **Schema**: `internal/schema/schema.sql` is embedded and applied at pool-init time via `schema.Apply` — pods bootstrap the table idempotently under a `pg_advisory_xact_lock` + `SET LOCAL statement_timeout='60s'` guard, mirroring lfx-v2-newsletter-service.
- **KV scan**: `ScanSubjectDataStreamRange` on `KV_v1-mappings` with filter `$KV.v1-mappings.>` and per-op timeout `cfg.NATSFetchMaxWait` (default 120s). The stream's [1, `maxSeq`] sequence space is partitioned into N disjoint half-open ranges; each worker drives an independent `next_by_subj` scan and streams visits through a shared channel to the writer goroutine. LWW is resolved in Postgres, not in-scanner, so no `map[string][]byte` snapshot is ever built in memory.
- **Tombstones**: app-level `!del` sentinel PUTs → written as `tombstoned=true, mapping_value=''`; native NATS DEL/PURGE → carried through as `deleted=true` rows in staging and excluded from the final table via `WHERE NOT deleted` on the DISTINCT ON winner.
- **Postgres load**: staging table `v1_mappings_staging` is `UNLOGGED` (no WAL) and dropped-and-recreated per run. Writer streams batches through `pgx.CopyFrom` (a single wire round-trip per batch instead of per-row parse+plan; typically materially faster than batched INSERTs, but the exact ratio is workload-dependent). After scanners finish, one `INSERT ... SELECT DISTINCT ON (mapping_key) ... FROM staging WHERE NOT deleted ORDER BY mapping_key, seq DESC ON CONFLICT DO UPDATE` resolves LWW into `v1_mappings`.
- **Dry-run**: `--dry-run` skips staging creation, `CopyFrom`, and the final upsert but preserves all scan/classification counters so operators can validate row counts before writing.
- **Summary log fields**: `visits`, `live`, `tombstoned`, `empty`, `native_del`, `staged`, `inserted_rows`, `batches`, `workers`, `max_seq`, `elapsed`, `dry_run`.
- **Idempotency caveat**: on re-runs against an already-populated `v1_mappings`, subjects whose latest KV revision is a native NATS DEL/PURGE will NOT be removed from Postgres (the WHERE NOT deleted filter excludes them from the INSERT but does not issue a DELETE). This is safe for the initial cutover (empty target) and for the pending online mutation path (LFXV2-2985 follow-ups) where the online DELETE handler owns removals.
- **Manifest**: `manifests/backfill-v1-mappings-to-postgres-job.yaml`.

### 4. `cmd/lfx-v1-sync-helper/handlers.go` — suppress unknown-object warnings (optional)

If the new table's records should only be stored in KV for downstream consumption (no v2 API side-effects), add the key prefix (e.g. `"myschema-mytable"`) as an explicit `case` in both `handleKVPut` and `handleResourceDelete` with a debug-level log statement. This prevents spurious "unknown object type" warnings in the logs.

## Contributing Workflow

1. **Code Changes**: Follow language-specific standards
2. **Formatting**: Use `make check` for Go code formatting
3. **Container Builds**: Test with `make docker-build-all`
4. **CI Validation**: Ensure MegaLinter and license checks pass

This documentation focuses specifically on the technical aspects needed for codebase development and modification.

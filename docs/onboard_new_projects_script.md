# Onboarding Script — `scripts/onboard_project.py`

Automates steps 2–5 of [onboarding a new project](./onboarding-new-project.md) into a
single command. Step 1 (adding the project to the allowlist) requires patching
the `lfx-v1-sync-helper-allowlists` ConfigMap — see the manual guide for details.

## Prerequisites

- [uv](https://docs.astral.sh/uv/) installed
- `LFX_API_TOKEN` — bearer token for the LFX API gateway
- NATS access — either port-forwarded locally or reachable via `NATS_URL`
- AWS credentials in the environment (for Phase 5 DynamoDB queries)

## Quick start

```sh
# Dry-run (default) — reports what would be done, writes nothing
export LFX_API_TOKEN=<token>
uv run scripts/onboard_project.py agentic-ai-foundation

# Write mode — replays KV entries and reindexes
uv run scripts/onboard_project.py agentic-ai-foundation --reindex

# Target a specific environment
uv run scripts/onboard_project.py agentic-ai-foundation --env staging --reindex
```

uv automatically resolves the script's inline dependencies (`boto3`, `httpx`,
`nats-py`) into an isolated environment on first run — no `pip install` needed.

## What the script does

The script runs five sequential phases:

| Phase | Description |
|-------|-------------|
| 1 | **Resolve project tree** — queries the LFX API for the root project and all its active/funded/membership descendants |
| 2 | **Replay project KV entries** — reads each `salesforce-project__c.<sfid>` entry from `v1-objects` and writes it back to trigger reprocessing |
| 3 | **Verify mappings** — checks `project.sfid.<sfid>` and `project.uid.<uid>` in `v1-mappings`; reports any that are missing |
| 4 | **Reindex committees** — fetches committees and their members via the project-service v2 API and replays their `v1-objects` KV entries (parents before children) |
| 5 | **Reindex DynamoDB** — queries polls, meetings, and past meetings by project SFID and replays their `v1-objects` KV entries, including all secondary tables (votes, registrants, attendees, etc.) |

## Options

```
usage: onboard_project.py [-h] [--env {dev,staging,prod}] [--nats-url URL]
                           [--reindex] [--fix-mappings]
                           [--skip-committees] [--skip-dynamodb] [--no-recurse]
                           slug

positional arguments:
  slug                  Project slug to onboard (e.g. agentic-ai-foundation)

options:
  --env {dev,staging,prod}
                        LFX environment (default: dev)
  --nats-url URL        NATS server URL (default: nats://localhost:4222 or $NATS_URL)
  --reindex             Write KV entries back; without this flag runs dry-run only
  --fix-mappings        Report guidance when mappings are missing (auto-creation
                        is not possible — the service must create them via replay)
  --skip-committees     Skip Phase 4: committee reindex
  --skip-dynamodb       Skip Phase 5: DynamoDB reindex
  --no-recurse          Only process the top-level project, not its children
```

## Environment variables

| Variable | Required | Description |
|----------|----------|-------------|
| `LFX_API_TOKEN` | Yes | Bearer token for LFX API calls |
| `NATS_URL` | No | NATS server URL; overridden by `--nats-url` (default: `nats://localhost:4222`) |
| AWS credentials | Phase 5 only | Standard boto3 credential chain (`AWS_PROFILE`, `AWS_ACCESS_KEY_ID`, etc.) |

## Typical workflow

### 1. Add the project to the allowlist

Before running the script, add the project slug to the
`lfx-v1-sync-helper-allowlists` ConfigMap and trigger a rolling restart:

```sh
kubectl edit configmap -n lfx lfx-v1-sync-helper-allowlists
```

Add the slug to the appropriate list (`PROJECT_ALLOWLIST` for the root project
only, `PROJECT_FAMILY_ALLOWLIST` to include all descendants), then restart the
deployment:

```sh
kubectl rollout restart -n lfx deployment/lfx-v1-sync-helper-app
```

See [onboarding-new-project.md §1](./onboarding-new-project.md#1-add-to-allow-list)
for details on which list to use.

### 2. Port-forward NATS (if running locally)

```sh
kubectl port-forward -n lfx svc/lfx-platform-nats 4222:4222
```

### 3. Dry-run first

```sh
export LFX_API_TOKEN=<token>
uv run scripts/onboard_project.py <slug> --env dev
```

Review the output — confirm the expected project tree is resolved and check for
any `MISSING` entries in phases 2–5.

### 4. Run with `--reindex`

```sh
uv run scripts/onboard_project.py <slug> --env dev --reindex
```

### 5. Check the summary

The summary at the end of the run reports totals per phase. Any `ISSUES` status
indicates missing or errored entries that need follow-up.

### 6. Verify mappings after replay

After `--reindex`, wait a few seconds for the sync-helper to process the
replayed entries, then re-run in dry-run mode (or run with `--skip-committees
--skip-dynamodb`) to confirm mappings now exist:

```sh
uv run scripts/onboard_project.py <slug> --env dev --skip-committees --skip-dynamodb
```

## Missing mappings

Phase 3 checks but cannot create mappings. Mappings are created by the
`v1-sync-helper` service as a side-effect of processing a KV entry. If a
mapping is missing after replay:

1. Confirm the project slug is in the allowlist and the service has restarted.
2. Re-run with `--reindex` to replay the KV entry again.
3. Check `v1-sync-helper` logs for errors during project processing.

## Skipping phases

Use skip flags for targeted re-runs after a partial failure:

```sh
# Only re-run committee and DynamoDB reindex (skip project replay and mapping check)
uv run scripts/onboard_project.py <slug> --reindex --skip-committees

# Only check mappings
uv run scripts/onboard_project.py <slug> --skip-committees --skip-dynamodb
```

## Single project (no children)

To onboard only the root project without processing its child projects:

```sh
uv run scripts/onboard_project.py <slug> --reindex --no-recurse
```

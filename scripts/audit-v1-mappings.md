<!-- Copyright The Linux Foundation and each contributor to LFX. -->
<!-- SPDX-License-Identifier: MIT -->

# Audit legacy v1-mappings against Postgres

`audit_v1_mappings.py` is a standalone, **read-only** retirement audit for
`lfx-self-serve-ops#1649`. It does not initialize application clients, apply
schema, create consumers/buckets, or repair mappings. Never use the migration
backfill to repair an audit finding: stale NATS data can resurrect mappings or
overwrite newer Postgres state.

## Run

Use `uv` and Python 3.12+. The script declares its own pinned dependencies;
it does not install Meltano or alter the project's Python lockfile.
The `nats-py[nkeys]` extra supplies credentials-file nonce signing support.

Supply credentials through your approved secret workflow, not command-line
arguments or committed files:

- `NATS_URL`: explicit server URL(s), comma separated; no implicit localhost.
- `NATS_CREDS`: optional NATS credentials-file path.
- `V1_MAPPINGS_DATABASE_URL`: explicit libpq connection string/URL for the
  mapping database, **not** the Salesforce replica (`V1_DB_*`). Configure TLS
  verification as required by the environment.

Use a Postgres role with only SELECT on `public.v1_mappings`, and NATS
credentials allowing stream-info/message-get requests and replies, but not
writes, consumers, stream creation or deletion. The connection and all
Postgres transactions additionally request read-only operation. Read-only
transactions are defense in depth, not a substitute for restricted roles.

```bash
uv run --no-project scripts/audit_v1_mappings.py \
  --environment dev \
  --cutover-at '<actual environment-specific ISO-8601 cutover time>' \
  --output-dir /tmp/opencode/mapping-audit-dev-run1
```

The cutover timestamp is required context, **not** an automatic correctness
rule. Obtain it from deployment evidence; do not guess. `--environment` is an
operator label, not endpoint verification: independently confirm that both
connections address the intended environment. Run dev before production.
For a JetStream domain/custom API prefix, supply `--js-api-prefix` (default
`$JS.API`). The NATS connection is named `v1-mappings-retirement-audit` so
observers can distinguish this tool from application dependencies.

Default NATS concurrency is 2 (`--workers 1..16`); start conservatively and
watch server load. `--timeout` defaults to 120 seconds per NATS request and
Postgres statement. The Postgres snapshot is streamed in a repeatable-read,
read-only transaction, which may be long-lived; schedule accordingly. Runtime
is driven by retained-message count and network latency, not just unique keys.

## Disk and privacy

The output directory must **not already exist**. Its parent must exist. The
tool creates it with mode 0700, and files with mode 0600. It uses disk-backed
SQLite staging there, including temp files, with a bounded 64 MiB page cache.
Allow substantial headroom beyond both source datasets for indexes/journals.
No production scratch tables are created. Use approved encrypted local
storage; do not use a synced/shared directory.

Scratch staging contains full keys/values and is removed on success and
handled failure/interruption. SIGKILL or machine failure can leave a private
`mapping-audit-*` directory behind: remove it manually after confirming that
the run is no longer active. File deletion is not secure erasure.

`summary.json` and stdout contain only aggregate counts, fixed family names,
coverage metadata, the script checksum, and safe failure messages. They do not
contain source keys/values, URLs, credentials, or raw driver exceptions.
`discrepancies.jsonl` is **sensitive**: `key_base64` reversibly encodes the
complete original key for investigation (it is not anonymization). It omits
mapping values but includes states and revision/update metadata. Unknown
families are aggregated as `unknown`, never as arbitrary source key prefixes.
Restrict access to all output, retain only as long as needed for this audit,
then delete it. Post only reviewed, sanitized summaries on the private ticket;
do not upload raw reports to public GitHub or general application logs.

## Interpretation

The NATS scan resolves the highest sequence for each key, including native
DEL/PURGE, before comparing states. Empty PUTs remain legitimate entries.
NATS `!del` corresponds to Postgres `tombstoned=true`; the value of a tombstoned
Postgres row is irrelevant. Revisions from the two stores are **not comparable**.

| Classification | Meaning/action |
| --- | --- |
| `match_live`, `match_tombstone` | Same effective state; known-family matches are counted but omitted from detail output. |
| `missing_postgres` | Legacy live key absent in Postgres: investigate whether still required. |
| `missing_tombstone` | Legacy deletion guard absent in Postgres: investigate. |
| `value_changed` | Both live, values differ: explain using current ownership/workflow evidence, not timestamps alone. |
| `nats_tombstone_postgres_live` | Possible legitimate recreation or lost guard: investigate. |
| `postgres_tombstoned_since_nats` | Expected suppression/deletion relative to stale NATS; never restore from NATS. |
| `postgres_only_tombstone` | Expected Postgres tombstone without a live NATS entry. |
| `postgres_only_live` | Post-cutover creation or recreation is possible; informational, not automatic loss. |
| `native_delete_absent` | Legacy native deletion and no Postgres row; no mapping required by that baseline. |
| `expected_operational_absence` | Explicit policy exemption for an absent non-tombstoned transient/retired key. |

Only `pending.*` (consumed/expired suppression markers) and retired
`v1_org.*`/`v1_org_lock.*` caches have missing-live-key exemptions, with reasons
in the report. They **do not** exempt missing `!del` guards. Workspace state,
user secondary indexes, and backfill cursors are recognized but not silently
exempted. Durable mapping/create/cleanup fences and formation-reconcile records
are checked. Every unknown family requires review even when the values match;
extend policy only after inspecting its owning code and deployed usage.

Before final reporting, findings requiring investigation are re-read from
Postgres in bounded batches. `changed_during_recheck` flags concurrent changes;
the initial classification is retained and still requires review/rerun. This
is not a global atomic snapshot of both systems, or a guarantee against writes
after recheck. Ordinary expected divergence is not point-read again.

Both recheck and reporting walk the immutable local snapshots in bounded
primary-key pages. Input keys are paged before joining/filtering; the event
loop yields after every page, even when a Postgres page has no unmatched keys,
so fully matching datasets still service NATS heartbeats and cancellation.

NATS must remain unchanged throughout capture and comparison. Start/end
configuration, creation time, sequence bounds, counts, size, lost-data state,
and total scanned messages must agree. Unexpected activity, empty/missing
streams, incomplete enumeration or connection failure fail closed. These
checks cannot recover historical NATS entries already purged/expired before
the audit: results apply only to the retained baseline. Coordinate a stable
legacy bucket (including expiry/purge policies) before using this as evidence.

Exit codes:

- **0:** complete, no unresolved findings within this audit's policy.
- **1:** complete, investigation required (including unknown families/races).
- **2:** incomplete/failed; never use as retirement evidence. A partially
  written detail report retains the `.partial.jsonl` suffix.

Neither code 0 nor explained differences alone authorize bucket deletion.
The ticket's deployed-reader/writer inventory, reversible denial test, and
post-retirement workflow checks are separate gates. There is no repair mode.

## Tests

```bash
uv run --no-project \
  --with 'nats-py[nkeys]==2.17.0' --with 'psycopg[binary]==3.3.9' \
  python -m unittest discover -s scripts -p 'test_*.py'
```

Tests use synthetic data and fake clients, plus a real NATS-client credentials
handshake against a loopback test endpoint that verifies nonce signing. No
dev/production access or real credentials are used.

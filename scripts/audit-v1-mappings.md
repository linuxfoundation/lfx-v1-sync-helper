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
  --output-dir "$HOME/mapping-audit-dev-run1"
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

## Run inside Kubernetes

When the database is not reachable locally, use the manually managed
[`audit-v1-mappings-job.yaml`](../manifests/audit-v1-mappings-job.yaml).
It ships **suspended**, uses a pinned Python/uv image and the merged script's
checksum, and defaults to one NATS worker. It does not modify the application,
create database roles, grant privileges, or copy application credentials.
Use the same manifest for dev or production. Always keep an explicit context
and namespace; validate dev results before an approved production run.

Select the environment before using the commands below:

```bash
export AUDIT_ENVIRONMENT='<dev-or-prod>'
export KUBE_CONTEXT="lfx-v2-${AUDIT_ENVIRONMENT}"
export AUDIT_RUN_ID='run1'
export REMOTE_REPORT_DIR="/audit/${AUDIT_RUN_ID}"
export REPORT_DIR="$HOME/mapping-audit-${AUDIT_ENVIRONMENT}-${AUDIT_RUN_ID}"
```

Replace the placeholder with `dev` or `prod`. Set the manifest's
`AUDIT_ENVIRONMENT` and `AUDIT_RUN_ID` to the same values: `kubectl apply`
does **not** substitute shell variables in YAML. Use a fresh run ID for each
retry, with only letters, digits, underscores or hyphens. The Job derives its
output directory from that ID; remote and local retrieval paths above use the
same ID. The environment is a report label, not endpoint verification.
Check that the context, database credentials, NATS endpoint and CA all belong
to the intended environment. Resource names are environment-neutral and
isolated by the selected cluster/namespace.

`REPORT_DIR` is the local destination for retrieved reports. Choose approved
encrypted storage with an existing parent directory; `$HOME` is a portable
starting location, not a guarantee of encryption. The destination itself must
be new. Retrieval stops if it already exists, preserving earlier evidence.

### Prepare prerequisites

Before applying, review the 50 Gi PVC request. This is conservative headroom,
not a measured environment-specific requirement. Select a disk-backed storage
class and confirm its encryption and reclaim policy in the target cluster;
the manifest uses `gp3` as a starting value. The volume holds scratch data and
reports, and incurs storage cost until deleted. No
automatic TTL deletes evidence. Kubernetes access to this namespace/PVC must
be restricted because the data can identify users.

Through the approved secret workflow, provision or identify a SELECT-only
database account and create `audit-v1-mappings-readonly` in
`v1-sync-helper`, with keys `host`, `port`, `username`, `password`, and `dbname`.
The target must be **that environment's `v1_mappings` database**. Do not copy
the existing `v1-sync-helper-secrets` application credentials or grant permissions
as part of applying this manifest. Reusing an application account requires a
separate explicit decision. Never put secret values in a manifest, command
argument, terminal output, or chat.

The Job maps those fields to libpq's standard `PG*` environment variables.
Its `V1_MAPPINGS_DATABASE_URL` contains only TLS settings: libpq picks up the
connection fields from the environment. No password-bearing URL is composed
or stored in the Pod spec. The script still enforces read-only transactions.

From an approved database CA bundle, create a dedicated audit ConfigMap:

```bash
kubectl --context "$KUBE_CONTEXT" -n v1-sync-helper create configmap \
  audit-v1-mappings-db-ca --from-file=ca.crt=/path/to/trusted-db-ca.pem
```

The Job requires `sslmode=verify-full`: use the actual database DNS hostname
and correct CA chain (the appropriate RDS trust bundle if this is RDS).
Do not disable verification to work around a missing certificate.

From the checkout containing merged PR #224's script, create its ConfigMap
and make it immutable. The manifest checks the SHA-256 before execution; if
the script changes later, deliberately update the checksum and versioned
ConfigMap name together rather than silently accepting different code.

```bash
kubectl --context "$KUBE_CONTEXT" -n v1-sync-helper create configmap \
  audit-v1-mappings-script-92b19af \
  --from-file=audit_v1_mappings.py=scripts/audit_v1_mappings.py
kubectl --context "$KUBE_CONTEXT" -n v1-sync-helper patch configmap \
  audit-v1-mappings-script-92b19af --type merge -p '{"immutable":true}'
```

Set `AUDIT_CUTOVER_AT` in the manifest to the selected environment's actual
Postgres-only cutover timestamp, and verify both endpoints. Leave
`suspend: true` while preparing.
The Job needs DNS/NATS/database connectivity and HTTPS egress to install the
script's declared dependencies. It does not need a Kubernetes API token,
Auth0, Heimdall, or AWS workload credentials. If NATS requires authentication,
mount a restricted credentials Secret and configure `NATS_CREDS` before launch;
the checked-in NATS URL is an in-cluster service address, not proof of which
environment it reaches.

### Start and monitor

```bash
kubectl --context "$KUBE_CONTEXT" -n v1-sync-helper apply \
  -f manifests/audit-v1-mappings-job.yaml
kubectl --context "$KUBE_CONTEXT" -n v1-sync-helper patch job \
  audit-v1-mappings --type merge -p '{"spec":{"suspend":false}}'
kubectl --context "$KUBE_CONTEXT" -n v1-sync-helper logs -f \
  job/audit-v1-mappings -c audit
```

There are no automatic retries. Exit 1 (a complete audit with findings) marks
the Kubernetes Job **Failed**; use `summary.json` to distinguish findings from
an incomplete audit. If initialization fails, a summary may not exist. Do not
wait solely for the Job's Complete condition: inspect the pod's termination
state for either success or failure. Deleting a running Job stops it; treat
that run as incomplete and preserve its PVC until any needed evidence is saved.

The Job has a **48-hour overall active deadline** (`activeDeadlineSeconds:
172800`), separate from the script's 120-second per-operation timeout. Review
and adjust the total budget before applying: a large single-worker scan can
take many hours, and the budget includes startup, scanning and comparison.
If the deadline expires, Kubernetes stops the pod and marks the Job failed
with `DeadlineExceeded`. Treat that run as incomplete; a final summary may
not have been written. The deadline does **not** delete the PVC, release the
need for report retrieval/cleanup, or authorize using partial results as
retirement evidence.

### Retrieve results

First save any needed Job logs and **confirm the audit pod is Succeeded or
Failed, not Running**. Delete the completed Job to release the PVC, then apply
the separate read-only reader. The PVC has no Job owner reference and survives
this deletion. This avoids trying to exec/copy from a terminated container.
The reader has no database credentials and runs under the audit's UID so it
can read private reports. An RWO volume may need time to detach/attach between
nodes.

Before deleting the Job, verify that its configured run ID matches the local
selection. A mismatch stops the workflow instead of retrieving an earlier
run's reports. Keep these variables for subsequent copy attempts after the
Job is gone; do not repeat this preparation block against an already deleted
Job.

```bash
(
  set -e
  job_run_id=$(kubectl --context "$KUBE_CONTEXT" -n v1-sync-helper get job \
    audit-v1-mappings -o \
    'jsonpath={.spec.template.spec.containers[0].env[?(@.name=="AUDIT_RUN_ID")].value}')
  if [ "$job_run_id" != "$AUDIT_RUN_ID" ]; then
    echo 'Job/local AUDIT_RUN_ID mismatch; verify the selected run' >&2
    exit 1
  fi
  kubectl --context "$KUBE_CONTEXT" -n v1-sync-helper delete job \
    audit-v1-mappings --cascade=foreground --wait=true
  kubectl --context "$KUBE_CONTEXT" -n v1-sync-helper apply \
    -f manifests/audit-v1-mappings-results-pod.yaml
  kubectl --context "$KUBE_CONTEXT" -n v1-sync-helper wait \
    --for=condition=Ready pod/audit-v1-mappings-results --timeout=180s
)
```

Define the copy helper below, then choose **one** of the completed/incomplete
flows that follow. It verifies all selected files exist before creating the
local destination. A missing file or an exec failure therefore leaves no new
directory. It accepts only the three named report artifacts, never raw SQLite
scratch, and refuses an existing local directory.

```bash
fetch_audit_reports() (
  set -e
  umask 077
  if [ "$REMOTE_REPORT_DIR" != "/audit/$AUDIT_RUN_ID" ]; then
    echo 'Stale remote path; rerun environment/run selection first' >&2
    exit 1
  fi
  kubectl --context "$KUBE_CONTEXT" -n v1-sync-helper exec \
    audit-v1-mappings-results -- python -c '
import sys
from pathlib import Path
root = Path(sys.argv[1])
names = sys.argv[2:]
allowed = {"summary.json", "discrepancies.jsonl", "discrepancies.partial.jsonl"}
if not names or any(name not in allowed for name in names):
    sys.exit("Select only named audit report artifacts")
if not all((root / name).is_file() for name in names):
    sys.exit("Selected reports unavailable; inspect the run before copying")
' "$REMOTE_REPORT_DIR" "$@"
  mkdir -m 700 "$REPORT_DIR"
  for report in "$@"; do
    kubectl --context "$KUBE_CONTEXT" -n v1-sync-helper cp \
      "audit-v1-mappings-results:$REMOTE_REPORT_DIR/$report" \
      "$REPORT_DIR/$report"
  done
)
```

**Completed audit (exit 0 or 1):** copy both final reports. Exit 1 means
findings, not interruption, even though Kubernetes labels the Job Failed.

```bash
fetch_audit_reports summary.json discrepancies.jsonl
```

**Incomplete/interrupted audit:** first list only the approved artifacts that
exist. This outputs filenames, not report contents or raw staging data.

```bash
kubectl --context "$KUBE_CONTEXT" -n v1-sync-helper exec \
  audit-v1-mappings-results -- python -c '
import sys
from pathlib import Path
root = Path(sys.argv[1])
if not root.is_dir():
    sys.exit("Run directory unavailable; verify the run ID and saved logs")
for name in ("summary.json", "discrepancies.jsonl", "discrepancies.partial.jsonl"):
    if (root / name).is_file():
        print(name)
' "$REMOTE_REPORT_DIR"
```

Pass only filenames from that listing to the helper. For example, when a
partial detail report exists but no final summary was written:

```bash
fetch_audit_reports discrepancies.partial.jsonl
```

If both `summary.json` and `discrepancies.partial.jsonl` were listed, request
both in a single call instead. Some failures leave only a summary, or no
report at all; use saved logs when no approved artifacts exist. An incomplete
run never becomes retirement evidence just because a final-named file exists.

If a copy fails **after** directory creation (for example a network failure),
preserve anything already copied and choose a fresh local destination before
retrying the helper, such as `REPORT_DIR="${REPORT_DIR}-copy2"`. Do not change
`AUDIT_RUN_ID` or `REMOTE_REPORT_DIR` for another copy of the same audit. A
preflight failure creates no directory, so selecting the correct available
files can reuse the same unused destination. Do not copy the whole run
directory: an unhandled termination can leave raw SQLite staging there.
Keep detail reports private. The reader sleeps for up to 24h; it is not a
storage-retention mechanism or an automatic resource cleanup.

For a retry, delete the old Job after collecting evidence, edit the manifest
to use a fresh `AUDIT_RUN_ID` (for example `run2`), and update the local
`AUDIT_RUN_ID`, `REMOTE_REPORT_DIR` and `REPORT_DIR` by rerunning the selection
block with that new ID. Recreate the Job suspended before starting. The new
ID selects both the new remote output and a new local destination; never
change just one path or reuse the earlier run's evidence. Remove the reader
before starting a new audit: never have two audit Jobs or the reader and an
active audit using the PVC concurrently. Existing output is not overwritten.

### Clean up at the end

After saving and verifying needed evidence, remove the temporary resources in
this order. These commands do not touch the application's database Secret or
the NATS bucket. Deleting the PVC is destructive: confirm no needed evidence
remains there. Verify the target storage class's reclaim policy: `Delete`
requests deletion of the backing volume, while `Retain` requires separate
storage-owner cleanup. Confirm completion so costs/data are not left behind.

```bash
kubectl --context "$KUBE_CONTEXT" -n v1-sync-helper delete pod \
  audit-v1-mappings-results --ignore-not-found --wait=true
kubectl --context "$KUBE_CONTEXT" -n v1-sync-helper delete job \
  audit-v1-mappings --ignore-not-found --cascade=foreground --wait=true
kubectl --context "$KUBE_CONTEXT" -n v1-sync-helper delete pvc \
  audit-v1-mappings --ignore-not-found --wait=true
kubectl --context "$KUBE_CONTEXT" -n v1-sync-helper delete configmap \
  audit-v1-mappings-script-92b19af audit-v1-mappings-db-ca \
  --ignore-not-found
```

If created exclusively for this run, remove the audit-only credential Secret
through its owning secret-management workflow. Do not delete a shared Secret
or an externally managed Secret that would simply be recreated. Retire a
temporary database account separately through its owner once no longer needed.
Finally, delete local sensitive reports when their approved retention ends.

For a standalone audit Secret created directly for this run (and only then):

```bash
kubectl --context "$KUBE_CONTEXT" -n v1-sync-helper delete secret \
  audit-v1-mappings-readonly --ignore-not-found
```

## Tests

```bash
uv run --no-project \
  --with 'nats-py[nkeys]==2.17.0' --with 'psycopg[binary]==3.3.9' \
  python -m unittest discover -s scripts -p 'test_*.py'
```

Tests use synthetic data and fake clients, plus a real NATS-client credentials
handshake against a loopback test endpoint that verifies nonce signing. No
dev/production access or real credentials are used.

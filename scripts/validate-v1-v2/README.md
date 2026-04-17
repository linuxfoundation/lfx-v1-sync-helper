# validate-v1-v2

Validates that ITX DynamoDB records have been correctly synced into LFX V2 OpenSearch.
For each item scanned in DynamoDB it fetches the corresponding document from the `resources` index and does a full field-by-field diff against the `v1_data` sub-document stored there by the sync pipeline.

## Prerequisites

- Go 1.25+
- AWS credentials accessible via the standard SDK chain (`~/.aws/credentials`, environment variables, or instance profile)
- OpenSearch reachable — use a port-forward if running against a cluster:

  ```sh
  kubectl port-forward svc/opensearch-cluster-master 9200:9200 -n lfx
  ```

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `AWS_PROFILE` | _(unset)_ | AWS named profile to use (requires `AWS_SDK_LOAD_CONFIG=1`) |
| `OPENSEARCH_URL` | `http://localhost:9200` | OpenSearch endpoint (no auth required) |

The AWS client uses the standard SDK credential chain — `AWS_PROFILE` + `AWS_SDK_LOAD_CONFIG=1` is the expected usage. The profile's configured region and any assume-role settings are picked up automatically. The OpenSearch index is always `resources`.

## Flags

| Flag | Type | Required | Description |
|---|---|---|---|
| `--table` | string | one of table/service | Comma-separated DynamoDB table name(s) to validate |
| `--service` | string | one of table/service | Service group: `meetings`, `mailing-lists`, `surveys`, `voting` |
| `--projects-file` | string | **yes** | Path to project mapping file (`v2uid=v1sfid` per line) |
| `--project` | string | no | Filter records to a single project — pass the **v2 UID**; the script translates it to the v1 SFID using `--projects-file` |
| `--limit` | int | no | Cap the number of records scanned per table (0 = no cap, default) |
| `--output` | string | no | Output format: `text` (default) or `json` |
| `--verbose` / `-v` | bool | no | Print per-record diffs inline rather than only in the summary |

At least one of `--table` or `--service` is required. `--projects-file` is always required.
When both `--table` and `--service` are given, only tables that belong to the specified service AND match the given table names are processed.

## Project Mapping File

The projects file maps v2 UIDs to v1 Salesforce IDs, one per line:

```
# v2uid=v1sfid
5e3g2535-d674-570f-07g4-fg185b5c6af0=a092M00001O6R6XRST
```

Pre-built files for each environment live in this directory:

| File | Environment |
|---|---|
| `projects-dev.txt` | dev |
| `projects-prod.txt` | prod |

When `--project` is not set, all projects listed in the file are included in the scan. When `--project` is set, only records belonging to that project's v1 SFID are scanned.

## Examples

```sh
# Validate all meetings tables in prod
AWS_PROFILE=itx-prod AWS_SDK_LOAD_CONFIG=1 \
  go run ./scripts/validate-v1-v2 --service meetings \
  --projects-file scripts/validate-v1-v2/projects-prod.txt

# Validate a single table, capped at 5 records
go run ./scripts/validate-v1-v2 --table itx-poll --limit 5 \
  --projects-file scripts/validate-v1-v2/projects-dev.txt

# Validate surveys for one project with verbose diffs (pass the v2 UID)
go run ./scripts/validate-v1-v2 --service surveys \
  --project 5e3g2535-d674-570f-07g4-fg185b5c6af0 \
  --projects-file scripts/validate-v1-v2/projects-dev.txt -v

# Validate mailing lists for one project
go run ./scripts/validate-v1-v2 --service mailing-lists \
  --project 5e3g2535-d674-570f-07g4-fg185b5c6af0 \
  --projects-file scripts/validate-v1-v2/projects-dev.txt

# Full JSON report for voting
go run ./scripts/validate-v1-v2 --service voting --output json \
  --projects-file scripts/validate-v1-v2/projects-dev.txt > report.json
```

## Output

### Text (default)

Progress lines are printed while scanning:

```
  SCAN  itx-poll (object_type=vote)
        scanned=42 missing=0 not_latest=0 mismatch=3 clean=39
  SKIP  itx-zoom-meetings-attachments — attachments not indexed in V2

────────────────────────── SUMMARY ──────────────────────────
  Total scanned   : 42
  Missing in OS   : 0
  Not latest      : 0
  Field mismatch  : 3
  Clean           : 39
```

With `--verbose` or `-v`, per-record diffs are printed inline:

```
    [vote:some-id-123]
      + dynamo only  updated_at = 2024-01-15T10:00:00Z
      ~ changed      status: dynamo=active  os=pending
```

### JSON (`--output json`)

The full report is emitted as a single JSON object:

```json
{
  "tables": [
    {
      "table": "itx-poll",
      "scanned": 42,
      "missing_in_os": 0,
      "not_latest": 0,
      "field_mismatch": 3,
      "clean": 39,
      "sample_diffs": [...]
    }
  ],
  "total": {
    "scanned": 42,
    "missing_in_os": 0,
    "not_latest": 0,
    "field_mismatch": 3,
    "clean": 39
  }
}
```

Each entry in `sample_diffs` has:

```json
{
  "object_id": "vote:some-id-123",
  "only_in_dynamo": { "updated_at": "..." },
  "only_in_os": {},
  "value_diffs": {
    "status": { "dynamodb": "active", "opensearch": "pending" }
  }
}
```

Non-verbose text mode prints at most 10 sample diffs per table; JSON always includes all collected diffs (capped at 10 per table unless `--verbose` is set).

## Service → Table Mapping

| Service | Table | object_type | Notes |
|---|---|---|---|
| meetings | `itx-zoom-meetings-v2` | `v1_meeting` | |
| meetings | `itx-zoom-past-meetings` | `v1_past_meeting` | |
| meetings | `itx-zoom-meetings-registrants-v2` | `v1_meeting_registrant` | |
| meetings | `itx-zoom-meetings-invite-responses-v2` | `v1_meeting_rsvp` | Composite key: `meeting_id:registrant_id` |
| meetings | `itx-zoom-past-meetings-attendees` | `v1_past_meeting_participant` | Composite key: `meeting_id:user_id` |
| meetings | `itx-zoom-past-meetings-recordings` | `v1_past_meeting_recording` | |
| meetings | `itx-zoom-past-meetings-summaries` | `v1_past_meeting_summary` | |
| meetings | `itx-zoom-meetings-attachments` | — | **Skipped** — not indexed in V2 |
| meetings | `itx-zoom-meetings-mappings` | — | **Skipped** — not indexed in V2 |
| meetings | `itx-zoom-past-meetings-invitees` | — | **Skipped** — not indexed in V2 |
| mailing-lists | `itx-groupsio-v2-service` | `groupsio_service` | |
| mailing-lists | `itx-groupsio-v2-subgroup` | `groupsio_mailing_list` | |
| mailing-lists | `itx-groupsio-v2-member` | `groupsio_member` | |
| mailing-lists | `itx-groupsio-v2-artifact` | — | **Skipped** — not indexed in V2 |
| voting | `itx-poll` | `vote` | UUID-keyed records only; non-UUID PKs skipped as dummy data |
| voting | `itx-poll-vote` | `vote_response` | Requires `poll_id`; non-UUID PKs skipped as dummy data |
| surveys | `itx-surveys` | `survey` | Filtered via committee-project mapping table |
| surveys | `itx-survey-responses` | `survey_response` | `auth0|` prefix stripped from usernames before comparison |
| surveys | `surveymonkey-surveys` | `survey_template` | No project filter supported |

## Ignored Fields

Some fields are excluded from the diff because they are added by the V2 indexer or are known legacy fields not present in the indexed data:

| Table | Direction | Field(s) | Reason |
|---|---|---|---|
| `itx-poll` | V2 only | `vote_uid`, `project_uid`, `committee_uid` | Added by V2 indexer |
| `itx-poll-vote` | V2 only | `vote_uid`, `project_uid`, `committee_uid`, `uid` | Added by V2 indexer |
| `itx-poll-vote` | DynamoDB only | `comment_responses`, `committee_ids`, `poll_comment_prompts`, `committee_names`, `committee_voting_statuses`, `committee_types`, `early_end_time` | Not indexed in V2 |
| `itx-surveys` | V2 only | `uid`, `committees[*].committee_uid`, `committees[*].project_uid` | Added by V2 indexer |
| `itx-survey-responses` | V2 only | `uid`, `survey_uid`, `committee_uid`, `project.project_uid` | Added by V2 indexer |
| `itx-survey-responses` | DynamoDB only | `survey_monkey_question_answers[*].response_question_id` | Legacy field never indexed in V2 |

## Caveats

- **`--projects-file` is required** even when `--project` is not set — it determines which projects are in scope for the scan.
- **`--project` takes a v2 UID**, not a v1 Salesforce ID. The script looks up the corresponding v1 SFID in the projects file and uses that to filter DynamoDB (which stores v1 IDs).
- **`surveymonkey-surveys`** has no `project_uid` attribute — `--project` filtering is silently ignored for that table.
- **Scans are full-table** unless `--project` is set; be mindful of RCU cost on large tables.
- **Composite-key tables** derive the OpenSearch `_id` by joining PK attributes with `:`. If a mapping is wrong the record will appear as "missing in OS" rather than erroring.
- **Only `latest:true`** documents in OpenSearch are considered present; older versions count as `not_latest`.
- **Numeric types** are normalized to `float64` before comparison, so `42` (DynamoDB int) equals `42.0` (OpenSearch float).
- **`auth0|` username prefix** — for `itx-survey-responses`, the `auth0|` provider prefix is stripped from both sides before comparison, so `auth0|jdoe` and `jdoe` are treated as equal.
- **Wildcard ignore patterns** — ignore field entries support `[*]` to match any array index, e.g. `committees[*].committee_uid` matches `committees[0].committee_uid`, `committees[1].committee_uid`, etc.

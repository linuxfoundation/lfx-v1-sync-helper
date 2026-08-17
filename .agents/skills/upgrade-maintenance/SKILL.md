---
name: upgrade-maintenance
description: Upgrade Go dependencies (especially the lfx-v2-committee-service and lfx-v2-project-service Goa SDKs), and review the v1<->v2 mapping logic for silently-dropped enum values and fields. Use when bumping SDK versions, after upstream service contract changes (new categories, new fields), or for periodic maintenance of the lfx-v1-sync-helper codebase.
license: MIT
---

# Upgrade Maintenance

Perform an upgrade maintenance pass on the lfx-v1-sync-helper codebase. This
covers three sequential phases: dependency upgrades, a mapping-logic review
(the highest-risk area of this repo), and verification/release.

**Why the mapping review matters**: this service translates data between the
v1 platform (via NATS KV ingest and REST) and v2 Goa services (via SDK clients
and NATS events). The translation code uses explicit allowlists, `switch`
statements, and struct literals — the compiler does NOT enforce
exhaustiveness, so a new upstream enum value or payload field compiles cleanly
while being silently coerced to a fallback (e.g. new committee categories
collapsing to `"Other"`) or zeroed. Every SDK bump must therefore be paired
with the Phase 2 review.

## Phase 1 — Upgrade dependencies

### Step 1.1 — Upgrade the Go toolchain version

Compare the local toolchain version against the `go` directive in `go.mod` and
update if the local toolchain is newer:

```bash
LOCAL=$(go version | awk '{print $3}')         # e.g. go1.26.0
GOMOD=$(grep '^go ' go.mod | awk '{print $2}') # e.g. 1.26.0

echo "Local toolchain: $LOCAL"
echo "go.mod go directive: $GOMOD"
```

If the local toolchain is newer:

```bash
go mod tidy -go=${LOCAL#go}
```

Also check other files that could pin the Go version:

- `docker/Dockerfile.v1-sync-helper` / `docker/Dockerfile.dynamodb-stream-consumer`
  — these build from `cgr.dev/chainguard/go:latest`, not a pinned version, so
  no update is needed here.
- `.github/workflows/*.yml` and `*.yaml` — `publish-main.yaml`,
  `publish-branch.yaml`, and `publish-release.yaml` all use
  `actions/setup-go` with `go-version-file: go.mod`, so these already follow
  `go.mod` automatically and need no manual edit here. Confirm this is still
  true (`grep -n 'setup-go\|go-version' .github/workflows/*.y*ml`); treat any
  hardcoded `go-version: 'X.Y'` found during this check as a bug to fix
  (switch it to `go-version-file: go.mod`), not a version to bump.
- `Makefile` — any hardcoded Go version variable (none currently).

**MegaLinter constraint**: MegaLinter ships its own bundled Go toolchain and
several linters (e.g. `golangci-lint`) run against that bundled version, not
`go.mod`'s. If `go.mod`'s `go` directive is newer than MegaLinter's bundled
Go, those checks fail. Pinning to the exact same patch version as MegaLinter
is not reliable either, since that patch can itself be flagged as vulnerable
by dependency/vulnerability scanners. The safest policy: keep `go.mod` **one
minor version behind** MegaLinter's bundled Go (e.g. if MegaLinter bundles
1.25.x, pin `go.mod` to `1.24.x`), and freely bump the *patch* within that
minor version to pick up security fixes. Check MegaLinter's bundled Go
version in its release notes / Dockerfile before deciding whether a toolchain
bump in this step is actually safe to take.

### Step 1.2 — Upgrade Go module dependencies

Either a full upgrade:

```bash
make update-deps   # Runs go get -u ./... && go mod tidy.
```

Or a targeted SDK bump (preferred when reacting to a specific upstream
release):

```bash
go get github.com/linuxfoundation/lfx-v2-committee-service@vX.Y.Z
go get github.com/linuxfoundation/lfx-v2-project-service@vX.Y.Z
go mod tidy
```

The impactful upgrades are the **`github.com/linuxfoundation/lfx-v2-*`
Goa SDK modules** — discover the current set with:

```bash
grep 'linuxfoundation/lfx-v2-' go.mod
```

As of this writing:

- `lfx-v2-committee-service` — Goa client used by the committee sync flows
  (`client_committees.go`). Its design enum for committee `Category` is the
  v2 source of truth.
- `lfx-v2-project-service` — Goa client used by the project sync flows
  (`client_projects.go`) and the project settings API.

Regeneration of these SDKs happens in their respective owning repos (run
`make apigen` there, then cut a GitHub Release with a `v*` tag); this repo
only consumes the pinned module version. If the enum/field you need is not yet
in any released tag of the owning repo, request or cut a release there first
— do not vendor or hand-copy generated code into this repo.

### Step 1.3 — Verify the build compiles

```bash
make build
```

Common compile errors after a Goa SDK bump:

- **`NewClient` has too few arguments** — the upstream service added new RPCs.
  Find where the client is constructed (grep for `NewClient` in
  `cmd/lfx-v1-sync-helper/`) and pass the new positional arguments; use `nil`
  for encoder arguments on endpoints this service does not call.
- **Struct field no longer exists** — find all usages with `grep` and update.
  If the code was a workaround for a known upstream bug (comments referencing
  a Jira ticket), the workaround can likely be deleted now.

Compile errors are the *easy* case. The dangerous changes are the ones that
compile cleanly — that is what Phase 2 catches.

## Phase 2 — Mapping-logic review (REQUIRED after any lfx-v2-* SDK bump)

This repo maps data in **both directions**. Both directions must be reviewed
for every upstream contract change.

### Step 2.1 — Discover what changed upstream

Diff the generated code between the old and new SDK versions (old version
from `git diff go.mod`). The **complete diff is the required review
artifact** — read it in full, since ordinary generated field declarations
usually contain none of `Enum`, `Category`, `OneOf`, or a quoted capitalized
value, and missing a field change here is exactly the failure mode this skill
exists to prevent:

```bash
MODCACHE=$(go env GOMODCACHE)
SVC=github.com/linuxfoundation/lfx-v2-committee-service
OLD=vX.Y.Z    # from git diff go.mod
NEW=vX.Y.Z+n

diff -r "${MODCACHE}/${SVC}@${OLD}/gen" "${MODCACHE}/${SVC}@${NEW}/gen" > /tmp/sdk-diff.txt
wc -l /tmp/sdk-diff.txt   # review every line of this file
```

Use a grep filter only as an optional secondary view to jump to likely enum
changes first — never as a substitute for reading the full diff:

```bash
grep -E '^[<>].*(Enum|Category|OneOf|"[A-Z])' /tmp/sdk-diff.txt | sort -u
```

Repeat for `lfx-v2-project-service`. Note every **added enum value** and every
**added/renamed/removed payload or result field**.

### Step 2.2 — Review the v1 -> v2 direction (KV ingest -> Goa SDK)

The v1 KV bucket watchers hydrate v1 records and write them to v2 via the Goa
SDKs. Translation happens in per-object handler files.

Committee categories (a known drift-prone area — see the `mapTypeToCategory`
allowlist below):

- `cmd/lfx-v1-sync-helper/handlers_committees.go` —
  `allowedCommitteeCategories` map and `mapTypeToCategory()`. Any v1
  `type__c` value not in the allowlist is **coerced to `"Other"`** with a
  warning log. For every enum value added upstream, add it here (exact string
  match, Title Case).
- Watch for split/combined values: v1 uses the combined
  `"Technical Oversight Committee/Technical Advisory Committee"` while v2
  splits them; `mapTypeToCategory()` special-cases this. New values needing
  similar splitting logic must be handled explicitly.

Then sweep for the same pattern on other objects:

```bash
# Find all allowlists and fallback-to-default mappings.
grep -rn 'allowed[A-Z]\|default:\s*$\|return "Other"\|fallback' cmd/lfx-v1-sync-helper/handlers_*.go
```

### Step 2.3 — Review the v2 -> v1 direction (NATS events -> v1 REST)

The indexer-event subscribers translate v2 resources back to v1 REST
create/PATCH payloads.

- `cmd/lfx-v1-sync-helper/ingest_indexer.go` — `mapV2CategoryToV1()` is a
  `switch` with an explicit pass-through case list and a **`default:` that
  returns `"Other"`**. Every new v2 enum value must be added to the case list
  (or given an explicit v1 translation, as with the TOC/TAC recombination).
- `cmd/lfx-v1-sync-helper/lfx_v1_client.go` — v1 REST payload structs. New
  upstream fields are silently dropped unless mapped here and in the payload
  builders in `ingest_indexer.go`.

```bash
# Find v2->v1 switch mappings and their defaults.
grep -n 'func mapV2' cmd/lfx-v1-sync-helper/*.go
```

### Step 2.4 — Cross-check text representations against the v1 API

The v1 project service (`project-management` repo, deployed by git tag —
NOT ArgoCD) validates enums via its swagger spec. A value accepted by v2 but
missing from v1's spec will make the v2->v1 PATCH **fail validation** rather
than silently coerce. For every new enum value, verify the **exact string**
(case, spacing, punctuation) is present in ALL of:

1. This repo's allowlist + `mapV2CategoryToV1()` (Steps 2.2–2.3).
2. `project-management` `swagger/pmm.yaml` — every Category enum block
   (create, create-platform, update-platform, response schema, and
   query-param filters).
3. `lfx-v2-committee-service` `cmd/committee-api/design/type.go` (the SDK you
   just bumped — confirm the pinned version includes it).
4. PCC frontend `apps/v2-frontend/src/app/shared/utils/constants.ts`
   (`COMMITTEE_TYPES`).
5. `lfx-self-serve`, if the value is user-facing there.

If the v1 spec is missing the value, the `project-management` change must be
merged, tagged, and **deployed before** this repo's fix ships, otherwise
v2->v1 syncs for that value will hard-fail.

### Step 2.5 — Review payload builders for silently-zeroed fields

Struct literals do not require exhaustive initialization. For each upstream
payload struct used in create/update flows (committee create/update payloads
in `handlers_committees.go`, v1 REST payloads in `ingest_indexer.go` /
`lfx_v1_client.go`, comparison/copy logic in `client_committees.go`), compare
the fields in the literal against the current field list of the destination
type in `$(go env GOMODCACHE)`. For every field present on the destination
type but absent from the literal: forward it, or add a comment explaining why
it is intentionally omitted.

Also check the **change-detection/diff logic** (e.g. field comparisons in
`client_committees.go`): a new field that is mapped in the payload but not in
the diff check means updates to only that field are skipped as "unchanged".

## Phase 3 — Verify and release

### Step 3.1 — Quality checks and tests

```bash
make check   # fmt, lint, vet.
make test    # go test -race ./...
```

Update any tests that assert on mapping tables (search `_test.go` files for
the enum values you touched).

### Step 3.2 — Open the PR

Open a PR against this repo with the mapping/SDK changes. Include in the
description:

1. Which SDKs were bumped and what upstream contract changes required fixes.
2. Which mapping tables/allowlists were updated, with the exact enum strings.
3. The cross-repo text-alignment verdict (Step 2.4 table).
4. Any deployment ordering constraint discovered in Step 2.4 (e.g. a
   dependency on another repo's release going out first).

### Step 3.3 — Dev verification (after this PR merges)

This step can only happen once the Step 3.2 PR has **merged to `main`** — dev
deploys automatically from there: argocd-image-updater digest-tracks the
`:development` image tag (see `lfx-v2-argocd`
`apps/dev/lfx-v2-applications.yaml`). Once the new image is running in dev,
verify by exercising both directions, e.g. for a new committee category:

1. Set the category on a committee in v1 — confirm the v2 committee record
   keeps the new value (not `Other`) after KV ingest.
2. Edit the committee in v2 — confirm the v1 record keeps the value after the
   REST PATCH (and that the PATCH does not 4xx on enum validation).
3. Check logs for the fallback warning ("mapped to Other" style messages) —
   there should be none for the new value.

### Step 3.4 — Prod release (separate PR, in lfx-v2-argocd)

Prod has **no image updater** — nothing ships until pins are bumped:

1. Cut a chart/image release of this repo (GitHub Release with a `v*` tag;
   do not push tags manually — use `gh release create`).
2. Open a **separate PR in `lfx-v2-argocd`** bumping the OCI chart
   `targetRevision` pin in `apps/prod/lfx-v2-applications.yaml`
   (lfx-v1-sync-helper entry), plus any image tag override in
   `values/prod/lfx-v1-sync-helper.yaml` if present. Describe in that PR's
   summary what version is being promoted and reference the Step 3.2 PR /
   release notes.
3. If the change depends on another repo's enum addition (Step 2.4), confirm
   that repo's release was deployed **first**.

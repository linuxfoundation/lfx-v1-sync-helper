<!-- Copyright The Linux Foundation and each contributor to LFX. -->
<!-- SPDX-License-Identifier: MIT -->

# V1 mapping HTTP contract

The optional `v1-mapping-api` app exposes read-only V2 → V1 lookups. It calls
the existing `lfx.lookup_v1_mapping` NATS request/reply endpoint, so it uses the
sync helper's active MappingStore (KV, dual, or Postgres). It has no database,
KV, Auth0 management, or sync-writer credentials and creates no separate store.

## Request

`GET /v1-sync-helper/mappings/{resource_type}/{uid}`

- `resource_type`: exactly `project` or `committee`.
- `uid`: lowercase, canonical, hyphenated V2 UUID.
- External callers send their platform OAuth bearer token through the API
  gateway. Heimdall authenticates it and checks membership in the dedicated
  access team before issuing a short-lived internal JWT for this API.
- No arbitrary mapping keys, subjects, forward lookups, users, committee
  members, or batch requests are supported.

For `project`, the only RPC payload is `project.uid.{uid}`; for `committee`,
it is `committee.uid.{uid}`. The subject is not caller-configurable.

## Success (200)

Project:

```json
{
  "resource_type": "project",
  "uid": "00000000-0000-4000-8000-000000000001",
  "project_sfid": "a09000000000001AAA"
}
```

Committee:

```json
{
  "resource_type": "committee",
  "uid": "00000000-0000-4000-8000-000000000001",
  "project_sfid": "a09000000000001AAA",
  "committee_sfid": "00000000-0000-4000-8000-000000000002"
}
```

`committee_sfid` is the legacy committee UUID, despite the SFID name. The API
splits the existing `projectSFID:committeeSFID` RPC reply into named fields;
consumers must not parse compound IDs. Project responses omit `committee_sfid`.
Project SFIDs must contain 15 or 18 alphanumeric characters. Malformed or
legacy bare committee replies fail closed; this API never guesses a project ID.
Responses use `Cache-Control: no-store`.

## Errors

API errors use Goa's `ErrorResult` JSON (`name`, `id`, `message`, `temporary`,
`timeout`, `fault`); gateway-generated authentication/authorization errors may
use Heimdall's response format instead.

| HTTP | Meaning |
| --- | --- |
| 400 | Unsupported resource type or malformed UUID |
| 401 | Missing/invalid bearer token; invalid internal JWT signature, issuer, audience, expiry, or principal |
| 403 | Heimdall denies membership in the configured access team |
| 404 | RPC returned an empty payload (no mapping) |
| 410 | RPC returned exactly `error: mapping_tombstoned` |
| 502 | RPC returned a malformed mapping |
| 503 | NATS unavailable/no responders, or mapping-store error reply |
| 504 | Mapping RPC timed out; `timeout` and `temporary` are both `true` |

Raw store errors are never returned over HTTP. A missing or tombstoned mapping
does not prove a resource never existed or is still deleted. Never use a failed
lookup to authorize creation, restore a mapping, or grant access. The existing
NATS contract and consumers are unchanged.

## Authentication and authorization boundary

The chart always installs `oidc → openfga_check → create_jwt` for GET lookup
requests. The check is `member` on `team:<mappingApi.accessTeamId>` using the
platform's authenticated principal mapping. There is no anonymous authenticator,
scope-only grant, global-admin fallback, or switch to disable authorization.
The existing OpenFGA `team#member: [user]` model needs no schema change.

This team-based policy intentionally supersedes the issue's original
single-client-only policy: **any authenticated principal explicitly added to
this team can call the endpoint**, including a human user or an M2M principal.
For a PCC-only rollout, add only the designated PCC M2M principal, represented
by the platform as `user:<client_id>@clients`, to this dedicated team. Do not
add wildcard, global-team, or unrelated user memberships.

The app validates PS256 signatures using cached Heimdall JWKS, the configured
issuer and API-specific audience, a required unexpired `exp`, `nbf`/`iat` when
present, and a nonempty/non-anonymous `principal`. External OAuth tokens cannot
be passed directly to the app. Membership enforcement lives in Heimdall; its
successful check is attested by the API-audience JWT. Do not create any other
Heimdall rule issuing this audience without the same team check. Internal JWTs
remain valid until expiration; team revocation applies to new gateway requests.
Keys are refreshed by the JWKS cache (minimum refresh interval five minutes).
No bearer tokens, principals, RPC values, or payloads are logged by this app.

## Deployment handoff (dev, staging, production)

The service-local chart defaults to `mappingApi.enabled: false`. For **each**
environment, provision the dedicated team's explicit member tuple(s) in that
environment's OpenFGA store, then configure the following in the owning
`lfx-v2-argocd` environment values and promote the new chart/image normally:

```yaml
mappingApi:
  enabled: true
  accessTeamId: v1-mapping-callers
  audience: lfx-v1-mapping-api
  issuer: heimdall
  jwksUrl: http://lfx-platform-heimdall.lfx.svc.cluster.local:4457/.well-known/jwks
  heimdallUrl: http://lfx-platform-heimdall.lfx.svc.cluster.local:4456
  natsUrl: nats://lfx-platform-nats.lfx.svc.cluster.local:4222
  gateway:
    name: lfx-platform-gateway
    namespace: lfx
    hostname: lfx-api.example.test # replace with this environment's API hostname
```

Enabling without a valid bare team ID or gateway hostname fails Helm rendering.
The API image tag follows the standard service-chart convention:
`mappingApi.image.tag` overrides the packaged chart's `appVersion`. For local
source-chart installs, which have no `appVersion`, set
`--set mappingApi.image.tag=<published-image-tag>` explicitly; the sync helper's
`app.image.tag` is independent.
No real team memberships, environment values, or deployments are provisioned by
this change. The gateway must permit attachment from the sync-helper namespace,
and Traefik must be able to reach Heimdall. The chart installs a dedicated
ForwardAuth middleware in the HTTPRoute's namespace, copying the authorized
`Authorization` response header; do not omit ForwardAuth.
The ClusterIP service exposes only port 8080. Health endpoints (`/livez`,
`/readyz`) are available for probes but are not exposed by the HTTPRoute.

Rollout verification for each environment:

1. A token for the designated team member resolves both project and committee
   mappings through the external gateway.
2. No token, a normal user outside the team, and another M2M client outside
   the team are denied before any lookup. Unrelated scopes do not grant access.
3. If human access is deliberately granted, that explicit team member succeeds.
4. Invalid IDs return 400, an absent mapping returns 404, and retained tombstones
   return 410. Check NATS failure responses in a nonproduction environment.

## Development

`make apigen` regenerates `gen/` from `design/mapping.go`, including Go clients,
server transports, and OpenAPI 2/3 JSON/YAML specifications under `gen/http/`.
Generated files must not be edited manually. `make build` builds the sync helper
and the mapping API; the PR/main/release workflows publish a separate
`ghcr.io/linuxfoundation/lfx-v1-sync-helper/v1-mapping-api` image using ko.

To run locally, set `NATS_URL`, `JWKS_URL`, `JWT_ISSUER`, `JWT_AUDIENCE`, and
optionally `HTTP_ADDRESS` (default `:8080`), then run `go run ./cmd/v1-mapping-api`.
The process fails startup if initial JWKS loading or NATS connection fails.
Readiness tracks NATS connectivity, not whether the mapping RPC currently has
responders. After a successful initial connection, the app reconnects indefinitely
through NATS outages; readiness fails while disconnected and recovers when NATS
returns without requiring a pod restart. No debug payload logger or public
OpenAPI route is enabled.

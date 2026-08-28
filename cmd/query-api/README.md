# query-api

Go, read-only GraphQL analytics service. Part of the Go API epic
(CHAOS-4352). Wave 0 (CHAOS-4366) built the proof infrastructure below;
Wave 1 (CHAOS-4367) ported the first real resolver, `featureFlags`, on top
of it — see
[`docs/contribute/architecture/go-api-wave-0-proof-infrastructure.md`](../../docs/contribute/architecture/go-api-wave-0-proof-infrastructure.md)
and the plan doc,
[`.github/docs-legacy/plans/go-api-epic.md`](../../.github/docs-legacy/plans/go-api-epic.md).

## Wave 1: featureFlags is live behind the switch

`internal/featureflags` ports
`dev_health_ops.api.graphql.resolvers.feature_flags.resolve_feature_flags`
verbatim (same WHERE clauses, argMax latest-row selection, ORDER BY,
LIMIT clamp, and missing-table degraded path). `main.go` mounts `/query`
when `CLICKHOUSE_URI`, `GO_API_REGISTRY_POSTGRES_URI`,
`GO_API_ENVELOPE_JWKS_PATH`, `GO_API_ENVELOPE_ISSUER`,
`GO_API_ENVELOPE_AUDIENCE`, and `GO_API_SCHEMA_DIGEST` are all set —
otherwise it stays Wave-0-empty (only `/healthz`/`/readyz`). `/query`
requests are gated by `routeswitch.Mux` + `PostgresSwitch` (reachable only
when `go_api_routing_state.mode` is `canary`/`primary` for the
featureFlags operation) and authenticated by `principal.Verifier`
(effective-principal envelope, Bearer token). GraphQL eligibility is
registered-documents-only (`query_route.go`'s
`registeredFeatureFlagsDocument`) — see that file's doc comment for the
known gap (a hand-registered single document, not yet sourced from Wave
0's real web-operations inventory).

Stage-2 local dual-run proof (real Python + real Go server, same
producer-seeded scratch state, compared via the CHAOS-4381 comparator):
`tests/api/graphql/test_go_api_dual_run_feature_flags.py`.

## Wave 0 scope: intentionally empty

**No resolver is implemented.** Every GraphQL field gqlgen generated in
`internal/graph/schema.resolvers.go` panics with `"not implemented"`. This
is deliberate, not incomplete — plan §6: "deploy an empty Go query-api and
prove a route becomes reachable when, and only when, its individual switch
is enabled." What exists in this Wave:

- `/healthz`, `/readyz` — process liveness/readiness only, no dependency
  checks yet (nothing to check: no store is wired in).
- `internal/routeswitch` — the per-operation reachability gate, proven by
  its own table-driven test (`switch_test.go`) rather than by a "the route
  is registered" claim (the CHAOS-3033 "cited constructor is not proof of
  capability" lesson, applied here). `PostgresSwitch`
  (`postgres_switch.go`) is the `go_api_registry`-backed implementation of
  the same `Switch` interface, proven against a real Postgres testcontainer
  (`postgres_switch_integration_test.go`, `go test -tags integration`).
- `internal/principal` — the Go VERIFIER half of the effective-principal
  envelope (CHAOS-4377): verifies the Python edge's EdDSA-signed envelope
  via `dev-health-go`'s `authverify.Ed25519JWKSVerifier`, checks
  issuer/audience/expiry and alg (rejecting alg-confusion), and enforces
  the claim-schema-version contract. Not wired into a request path yet —
  Wave 0 proves the mechanism, not a live auth flow.
- The gqlgen-generated executable schema (`internal/graph/`), constructed
  in `main.go` to prove it builds and links against the canonical SDL, but
  **not mounted on any route**.

## Schema-first from the canonical SDL pin

`gqlgen.yml` points directly at
[`contracts/graphql/v1/schema.graphql`](../../contracts/graphql/v1/README.md)
— the CI-checked export of the Strawberry schema — never a copy. To
regenerate after a schema change:

```bash
cd cmd/query-api
go run github.com/99designs/gqlgen generate --config gqlgen.yml
```

Review the diff; it is the same drift-review contract as web's codegen
against the same SDL pin.

## Build / test

```bash
go build ./cmd/query-api/...
go test ./cmd/query-api/...
```

Uses the repo's single root Go module (`github.com/full-chaos/dev-health-ops`)
— no separate `go.mod` here.

## Local compose

Off by default. See
[`deploy/go-api/compose-query-api.yml`](../../deploy/go-api/compose-query-api.yml)
for the overlay and how to bring it up explicitly. Not part of the assumed
local stack — bring it up only for deliberate verification.

## dev-health-go dependency

`go.mod` requires `github.com/full-chaos/dev-health-go v0.1.1` (tagged by
CHAOS-4377, no local `replace`). The repo is **public** (chris ruling
2026-08-28), so `go build`/`go test`/CI fetch it straight from the public
module proxy/sumdb like any other dependency — no `GOPRIVATE`, no git
credentials, nothing workflow-specific. (It was briefly private, requiring
a temporary `GOPRIVATE=github.com/full-chaos/*` workaround; that scaffolding
has been removed from `.github/workflows/go.yml` along with this note's
earlier "CI cannot build this" caveat.)

## What's NOT here yet (later waves / other lanes)

- Any resolver besides `featureFlags` — every other field still panics
  with `"not implemented"`.
- The registered-document registry sourced from Wave 0's real
  web-operations inventory (deliverable 2) — Wave 1 hand-registers its own
  single canary document instead (`query_route.go`).
- Deployed executed proof, shadow, and canary (plan §5 stages 3-5) — this
  wave delivers only local dual-run proof (stage 2); a deploy is routed
  through the epic orchestrator, not this lane.
- `featureFlagEvents` — explicitly out of scope for this canary (plan §6).

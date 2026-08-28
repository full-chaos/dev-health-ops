# query-api

Go, read-only GraphQL analytics service. Part of the Go API epic
(CHAOS-4352), Wave 0 (CHAOS-4366) — see
[`docs/contribute/architecture/go-api-wave-0-proof-infrastructure.md`](../../docs/contribute/architecture/go-api-wave-0-proof-infrastructure.md)
and the plan doc,
[`.github/docs-legacy/plans/go-api-epic.md`](../../.github/docs-legacy/plans/go-api-epic.md).

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

- Any real resolver, or the ClickHouse readers it would need (the shared
  dev-health-go extraction landed its store/contract layer under
  CHAOS-4377 — plan §6 — so query-api's first real route is built on the
  extracted readers, not hand-rolled queries needing a second port later).
- `principal.Verifier` and `routeswitch.PostgresSwitch` wired into an
  actual request path (`/query`, mounted behind `routeswitch.Mux`) — Wave 0
  proves both mechanisms independently; a later wave mounts them together
  for a real (still Python-served, Go-shadowed/canaried) operation.

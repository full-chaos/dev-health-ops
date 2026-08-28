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
  capability" lesson, applied here).
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

## What's NOT here yet (later waves / other lanes)

- The Go JWKS verifier for the effective-principal envelope
  (`src/dev_health_ops/api/graphql/principal_envelope.py` issues it; the
  verifier needs dev-health-go's `authverify` mechanisms, CHAOS-4377).
- Any real resolver, or the ClickHouse/Postgres readers it would need
  (the shared dev-health-go extraction is explicitly required to land
  first — plan §6 — so query-api's first real route is built on the
  extracted readers, not hand-rolled queries needing a second port later).
- The operation registry lookup wired into `routeswitch.Switch` (Wave 0
  ships `StaticSwitch`/`DynamicSwitch`, in-memory only; a
  `go_api_registry`-backed implementation of the same `Switch` interface is
  a follow-up, not a redesign).

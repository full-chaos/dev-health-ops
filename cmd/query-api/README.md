# query-api

Go, read-only GraphQL analytics service. Part of the Go API epic
(CHAOS-4352). Wave 0 (CHAOS-4366) built the proof infrastructure below;
Wave 1 (CHAOS-4367) ported the first real resolver, `featureFlags`;
Wave 2 (CHAOS-4368) ported the second, `reviewEdges`; Wave 3 (CHAOS-4369)
ports `cognitiveLoad` alongside `complexityTimeseries` and `hotspots`
(this lane, same wave) — see
[`docs/contribute/architecture/go-api-wave-0-proof-infrastructure.md`](../../docs/contribute/architecture/go-api-wave-0-proof-infrastructure.md)
and the plan doc,
[`.github/docs-legacy/plans/go-api-epic.md`](../../.github/docs-legacy/plans/go-api-epic.md).

## Wave 1 & 2: featureFlags and reviewEdges are live behind the switch

`internal/featureflags` ports
`dev_health_ops.api.graphql.resolvers.feature_flags.resolve_feature_flags`
verbatim (same WHERE clauses, argMax latest-row selection, ORDER BY,
LIMIT clamp, and missing-table degraded path).

`internal/reviewedges` ports
`dev_health_ops.api.graphql.resolvers.review_edges.resolve_review_edges`
verbatim (same argMax-per-key dedup over the append-only
`review_edges_daily` table, same `ORDER BY reviews_count DESC, repo_id,
reviewer, author, day` deterministic tie-break added by CHAOS-4368 Part A
(#1980) as a stage-2 prerequisite, same optional `repo_ids` filter
resolved through the org-scoped `repos` catalog, same 1..2000 limit
clamp) — with one deliberate divergence from `featureflags`: Python's
`resolve_review_edges` has **no** missing-table degraded path (there is no
`degradedReason` field on `ReviewEdgesResult` at all), so this port does
not invent one; a ClickHouse error propagates as a real GraphQL error on
both sides. Authorization also differs from `featureFlags`: Python's
resolver does not error on an `orgId` argument mismatch — it silently
prefers the envelope's authorized org over whatever the client sent. This
port reproduces that "authorized org always wins" behavior exactly (see
`schema.resolvers.go`'s `ReviewEdges` doc comment) rather than reusing
`featureFlags`'s stricter equality check.

`main.go` mounts `/query` when `CLICKHOUSE_URI`,
`GO_API_REGISTRY_POSTGRES_URI`, `GO_API_ENVELOPE_JWKS_PATH`,
`GO_API_ENVELOPE_ISSUER`, `GO_API_ENVELOPE_AUDIENCE`, and
`GO_API_SCHEMA_DIGEST` are all set — otherwise it stays Wave-0-empty (only
`/healthz`/`/readyz`). `/query` requests are gated by `routeswitch.Mux` +
`PostgresSwitch` (reachable only when `go_api_routing_state.mode` is
`canary`/`primary` for the SPECIFIC operation being dispatched —
featureFlags, reviewEdges, cognitiveLoad, complexityTimeseries, and
hotspots each have their own row and are gated fully independently) and
authenticated by `principal.Verifier`
(effective-principal envelope, Bearer token). GraphQL eligibility is
registered-documents-only (`query_route.go`'s
`registeredFeatureFlagsDocument` / `registeredReviewEdgesDocument` /
`registeredCognitiveLoadDocument` / `registeredComplexityTimeseriesDocument` /
`registeredHotspotsDocument`, indexed by operation name via
`newQueryHandler`'s `digestByOperation` map) — see those constants' doc
comments for the known gap (hand-registered single documents, not yet
sourced from Wave 0's real web-operations inventory) and for the Wave-1
codex-round-3 lesson every document is sourced from the real web client
file to avoid repeating (a wrong operation name on both the test and
route sides would still "match" locally while 404-ing every real client
request).

Stage-2 local dual-run proof (real Python + real Go server, same
producer-seeded scratch state, compared via the CHAOS-4381 comparator):
`tests/api/graphql/test_go_api_dual_run_feature_flags.py`,
`tests/api/graphql/test_go_api_dual_run_review_edges.py`,
`tests/api/graphql/test_go_api_dual_run_cognitive_load.py`, and
`tests/api/graphql/test_go_api_dual_run_complexity_timeseries.py`.

## Wave 3: cognitiveLoad (CHAOS-4369, extended to a third path by CHAOS-4462)

`internal/cognitiveload` ports
`dev_health_ops.api.graphql.resolvers.cognitive_load.resolve_cognitive_load`
verbatim, including the THREE distinct read paths the Python resolver
picks between on `teamId`/`repoId`:

1. **Single-team** (`teamId` set, `repoId` unset): reads
   `team_cognitive_load_daily` directly — already team-scoped and
   OWNERSHIP-resolved at write time (CHAOS-4365 item 2). One dedup query,
   no merge. The ratio fields are genuinely `Nullable(Float64)` here
   (unmeasured vs. a measured `0.0`); ported via one bundled
   `argMax(tuple(...), computed_at)` rather than five independent
   per-column `argMax` calls, so a day whose latest row is genuinely NULL
   never surfaces a stale non-null value from an older row.
2. **Team+repo combined** (both set, CHAOS-4406/CHAOS-4462): neither
   `user_metrics_daily` nor `team_metrics_daily`'s own `team_id` column can
   be trusted (CHAOS-4396 taint). `resolveOwnedRepoID` confirms via
   `team_repo_ownership` (falling back to `teams.repo_patterns` only when
   native ownership resolves no row at all for the candidate repo) that the
   requested repo is CURRENTLY, CANONICALLY owned by the requested team —
   if not owned (or the repo does not exist), returns an explicit empty
   result rather than either the wrong team's data or a confusing error;
   if owned, both queries filter by `repo_id` ALONE.
3. **Org-wide** (`teamId` unset): the original two-query merge over
   `user_metrics_daily`/`team_metrics_daily`, each deduplicated via
   `argMax(..., computed_at)` before aggregating, merged over the UNION of
   days. `repoId`, when set without `teamId`, narrows only
   `user_metrics_daily`; `team_metrics_daily` (`fetchTeamMetrics`) takes no
   `repoId` argument at all.

**History:** the Wave 3 port (PR #1993) initially shipped only paths 1 and
3 — the Wave 3 task briefing's claim that CHAOS-4406 (commit `8519cd2a8`,
adding the ownership-gated third path) was "already in the feature base"
was verified false at the time
(`git merge-base --is-ancestor 8519cd2a8 origin/feature/chaos-4352-go-api`
returned non-zero; the commit existed only on `origin/main`), and was
tracked as follow-up **CHAOS-4462**. The CHAOS-4352 rebase lane then
rebased the feature branch onto `origin/main` (which carries 8519cd2a8),
so `resolvers/cognitive_load.py` at the feature tip is now byte-identical
to `origin/main`'s 3-path version, and this PR ports the third path to
close CHAOS-4462. `resolveOwnedRepoID`'s pattern-fallback resolver
(`repoPatternResolver`) is a small package-private port of
`RepoPatternTeamResolver`/`build_repo_pattern_resolver`
(`providers/teams.py`) — not a shared import, matching this package's
existing convention (see `QueryClient`'s doc comment) of small per-package
helpers over a shared library; a separate, independently-verified copy of
the same logic already exists worker-side
(`internal/jobs/metrics/daily/wellbeing_native_clickhouse.go`).

Like `reviewEdges` (unlike `featureFlags`), `resolve_cognitive_load` has
no missing-table degraded path — no `degradedReason` field on
`CognitiveLoadResult` at all — so a ClickHouse error propagates as a real
GraphQL error on both sides, and this port does not invent one.
Authorization mirrors `reviewEdges` exactly: the envelope's authorized org
always wins over a mismatched `orgId` argument (`schema.resolvers.go`'s
`CognitiveLoad` doc comment).

## Wave 3: complexityTimeseries is live behind the switch

`internal/complexitytimeseries` ports
`dev_health_ops.api.graphql.resolvers.complexity.resolve_complexity_timeseries`
verbatim: same two-table split by `scope` (`REPO` reads
`repo_complexity_daily`, `FILE` reads `file_complexity_snapshots`), same
`argMax(<col>, computed_expr)` latest-compute-pass selection where
`computed_expr` is plain `computed_at` for `DAY` granularity or a
`(day|as_of_day, computed_at)` tuple for `WEEK` granularity, same
repo-scope default top-N-by-latest-complexity subquery when `repoIds` is
empty, same two-stage limit clamp (`1..MAX_ROWS`, then further bounded by
`MAX_TIMESERIES_POINTS / bucketCount`), and the same best-effort
repo-label join (falls back to the repo id string when the `repos`
catalog row is missing). Like `reviewEdges`, it has **no** missing-table
degraded path — a ClickHouse error propagates as a real GraphQL error on
both sides — and reproduces the same "authorized org always wins" GraphQL
`orgId`-argument behavior.

`sinceUtc`/`untilUtc` are the GraphQL `DateTime` scalar, still bound to
the Wave-0 placeholder `graphql.Time` (see the "DateTime and JSON
scalars" section below) — this is safe for `complexityTimeseries` and
`hotspots` specifically because both operations use `DateTime` as INPUT
only (never returned in a response): `graphql.Time`'s `UnmarshalTime`
parses with `time.RFC3339Nano`, a strict superset of the
`"YYYY-MM-DDT00:00:00Z"` shape web's `complexityWindowFromFilter` sends,
so unmarshaling is correct even though the KNOWN INCORRECT `MarshalTime`
formatting bug (`+00:00` vs `Z`) is never exercised by either
operation's output. A later wave that returns a `DateTime`-typed field
must still fix the scalar properly, not extend this exemption.

## The `Date` GraphQL scalar (fixed in Wave 2)

`internal/graphqldate` is a real custom marshaler for the SDL's `scalar
Date`, replacing the Wave-0 placeholder mapping to `graphql.Time` — that
mapping was documented KNOWN INCORRECT from Wave 0 onward (`graphql.Time`'s
RFC3339Nano parser rejects a bare `"2026-08-27"` and its marshaler emits a
full timestamp, not a date) and had to be fixed before `reviewEdges`
(whose `sinceDate`/`untilDate`/`day` fields are all `Date`-typed) could be
ported correctly. `graphqldate.Date` serializes/parses exactly
`"YYYY-MM-DD"`, matching Strawberry's built-in `Date` scalar
(`date.isoformat()`/`date.fromisoformat()`) — see that package's doc
comment for how this was confirmed against the real Python scalar
registry, not assumed.

Regenerating gqlgen after this scalar-mapping change mechanically touched
every OTHER `Date`-typed field across the whole schema (`generated.go`'s
marshal calls, `models_gen.go`'s struct field types) — expected, not
hand-edited; those fields belong to still-unimplemented resolver stubs
this PR does not otherwise touch. `DateTime` and `JSON` are UNCHANGED and
remain the same KNOWN INCORRECT Wave-0 placeholders described below;
`reviewEdges` uses neither, so fixing them is left to whichever later wave
ports a resolver that does.

## Wave 3: hotspots is live behind the switch

`internal/hotspots` ports
`dev_health_ops.api.graphql.resolvers.complexity.resolve_hotspots`
verbatim: same `argMax(<col>, computed_at)` latest-compute-pass selection
over the append-only `file_hotspot_daily` table, same optional `repoIds`
filter resolved through the org-scoped `repos` catalog (bounded to
`MAX_ROWS`=1000 -- NOT the row limit -- before it becomes a bind
parameter, exactly mirroring Python's `list(repo_ids)[:MAX_ROWS]`), same
`ORDER BY risk_score DESC NULLS LAST` at the database level, same
1..500 limit clamp with a default of 50, same best-effort repo-label
join, and the same deterministic `evidenceUrl` deeplink built from
`file_path` (a hand-rolled `quotePython` reproduces
`urllib.parse.quote()`'s default `safe="/"` behavior byte-for-byte --
neither `url.PathEscape` nor `url.QueryEscape` matches it: PathEscape
escapes `/` and QueryEscape encodes space as `+` instead of `%20`).

Deliberate divergence from `complexityTimeseries`'s date handling,
confirmed by reading `resolve_hotspots` line by line: `sinceUtc`/
`untilUtc` are converted to a date WITHOUT a `.astimezone(timezone.utc)`
normalization first (unlike `resolve_complexity_timeseries`) -- this port
reproduces that asymmetry exactly rather than "fixing" it, since the goal
is Python parity, not what Python arguably should have done.

Like `reviewEdges` and `complexityTimeseries`, `hotspots` has no
missing-table degraded path -- a ClickHouse error propagates as a real
GraphQL error on both sides.

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

- Any resolver besides `featureFlags`, `reviewEdges`, `cognitiveLoad`,
  `complexityTimeseries`, and `hotspots` — every other field still
  panics with `"not implemented"`.
- The registered-document registry sourced from Wave 0's real
  web-operations inventory (deliverable 2) — every wave so far
  hand-registers its own canary document(s) instead (`query_route.go`).
- Deployed executed proof, shadow, and canary (plan §5 stages 3-5) — every
  wave delivers only local dual-run proof (stage 2); a deploy is routed
  through the epic orchestrator, not this lane.
- `featureFlagEvents` — explicitly out of scope for the Wave 1 canary
  (plan §6).
- The `DateTime` and `JSON` GraphQL scalars are still the same Wave-0
  placeholder mappings, still KNOWN INCORRECT for the reasons `gqlgen.yml`
  documents — Wave 2 fixed only `Date` (`reviewEdges`'s/`cognitiveLoad`'s
  own scalar need); neither `DateTime` nor `JSON` is used by `cognitiveLoad`.

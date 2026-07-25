# Provider/dataset capability matrix — v1

`matrix.json` is the frozen provider × dataset contract required by CUT-08 of
the Go worker cutover (`docs/plans/go-worker-cutover-implementation-plan.md`
§9, `docs/architecture/go-worker-cutover-trd.md` §10).

It exists because the matrix previously lived only as hand-maintained code in
two languages, with no test asserting the sets were identical.

## What it freezes

All **59** configured pairs across the six providers in TRD §10.1:

| Provider | Pairs |
|---|---|
| github | 17 |
| gitlab | 19 |
| jira | 6 |
| launchdarkly | 1 |
| linear | 5 |
| pagerduty | 11 |

Per pair: the Python source anchor, cost class, watermark behavior, legacy
targets, processor flags, the fixed Go executor kind, shadow eligibility, the
collapsed route dataset, the route destination manifest, route readiness, and
the provider's credential modes.

## What it deliberately does not freeze

`RouteEnabled` — the per-deployment environment switch. The contract records
*readiness*, which no environment may widen. A pair with `route_ready: false`
cannot be routed by any configuration.

## Who verifies it

Both languages regenerate their own side and fail on divergence:

- Go — `internal/providersync/capability_matrix_test.go`
  (`TestProviderMatrixMatchesCheckedInContract`), which rebuilds the artifact
  from the dataset capability registry and the canonical
  `CompleteRouteSwitches.Descriptor`.
- Python — `tests/workers/test_provider_matrix_contract.py`, which rebuilds the
  pair set and per-pair metadata from `src/dev_health_ops/sync/datasets.py` and
  additionally binds producer route eligibility
  (`ProviderUnitRouteSwitches.is_canary_scope`) to `route_ready`.

Regenerate after an intentional registry change:

```
PROVIDER_MATRIX_UPDATE=1 go test ./internal/providersync \
  -run TestProviderMatrixMatchesCheckedInContract -count=1
```

## Field semantics that are easy to get wrong

- `go_executor` is a statement about **compiled code**, not routing. A pair may
  be `native_go` while `route_ready` is `false` because live non-empty parity
  evidence has not been captured. Capability metadata is not execution
  evidence (TRD §10.1).
- `python_compatibility` is reserved for the bounded API-side compatibility
  endpoint described in TRD §10.2 (bounded identifiers and Go's claim context
  only; no credentials, URLs, module names, or provider payloads; Go retains
  lease, status, completion, and watermark ownership). **No pair uses it
  today** — that endpoint does not exist yet, and a Go test fails if any pair
  claims it.
- `native_shadow` marks pairs whose native fetch output may be compared against
  the Python-owned sink for parity evidence. It never implies routing.
- `route_dataset` differs from `dataset` for the linear/jira work-item aliases,
  which collapse onto one complete `work-items` unit while keeping alias
  identity for audit and watermark compatibility.
- `route_destinations` is empty for pairs with no known sink manifest yet.
  Recording a guessed manifest would be worse than recording none.

## Activation status for `(github, repo-metadata)`

CHAOS-3123 flipped the pair to `native_go` / `route_ready: true` on
fixture-level field parity evidence against the production Python collector.
Routing still requires the separate `GithubRepoMetadata` switch
(`WORKER_GITHUB_REPO_METADATA_ENABLED`), which every existing deployment
leaves off by default — `route_ready: true` alone moves no live traffic; see
`CompleteRouteSwitches.Descriptor`'s `github`/`repo-metadata` case.

What this activation waived and satisfied:

1. `TestGitHubRepositoryLiveParityHarness` remains an unimplemented stub and
   is *not* a precondition: canary staging and live-traffic parity are waived
   for this program (no production users yet). Fixture-level field parity is
   the accepted bar. Live parity against a real credentialed repository is
   still valuable operational evidence to capture before the switch is ever
   turned on in an environment with real traffic.
2. The binary that constructs `GitHubRepositoryRouteHandler` **must** also set
   `EffectCommitter.Readback` to `GitHubRepositoryClickHouseEffects`. The
   `repos` effect is `EffectReadbackRequired`, so a committer without a
   readback fails closed with `ErrEffectRecoveryAmbiguous` rather than
   reinserting. `cmd/dev-health-worker/provider_sync.go`'s `BuildExecutor`
   now selects `Handler`/`Sink`/`Readback` by `session.Claim.Provider` and
   `session.Claim.Dataset` (CHAOS-3123) — a single hardcoded
   `LaunchDarklyRouteHandler`/`LaunchDarklyClickHouseEffects` pair would fail
   closed on every claimed github unit rather than serve it.

## Effect timestamp stabilization (applies to every complete route)

`BuildEffectBatch` digests the serialized rows, so any wall-clock value inside
a row is part of the effect identity. `CompleteRouteExecutor` therefore loads
the persisted effect ledger on **every** attempt for a unit occurrence — not
only on expired-lease recovery — and reuses its `created_at` as the
normalization instant.

This matters because `ReleaseForRetry` returns a unit to `dispatching`, so the
next claim is *not* `Recovered`. Stabilizing only on recovery left ordinary
River retries regenerating timestamps, changing the digest, and being rejected
by `PrepareEffects` with `ErrEffectLedgerConflict` before any readback could
run — wedging the unit until it exhausted. Any new complete-route handler that
stamps time into a row inherits this guarantee; handlers must take the
`normalizedAt` passed to `Collect` rather than reading the clock themselves.

Readbacks over a `ReplacingMergeTree` destination must compare against the
**winning** version (`argMax` over the version column), never every physical
version: unmerged history from earlier occurrences is normal and would
otherwise read as a conflict.
3. Raw `repos` readers must be audited. `repos` is
   `ReplacingMergeTree(last_synced)` ordered by `(org_id, id)`; readers that
   join it without `FINAL` or `argMax` can double-count between an insert and
   the next merge. `src/dev_health_ops/api/queries/heatmap.py` is a known
   pre-existing example and is not a regression introduced by this contract.

## Known Go/Python divergences (fail-closed by design)

`repositoryIdentity` mirrors Python's `get_repo_uuid_from_repo` for the ASCII
names GitHub issues, and refuses rather than guesses in two cases:

- Python honours a process-global `REPO_UUID` override
  (`models/git.py::get_repo_uuid_from_repo`). Go never sources identity from
  process-global state, so it returns `ErrRepositoryIdentityAmbiguous` when
  `REPO_UUID` is set.
- Python's `str.lower()` applies full Unicode case mapping (`U+0130` lowers to
  `i` plus a combining dot); Go's `strings.ToLower` applies simple per-rune
  mapping. Non-ASCII repository identifiers are therefore rejected. GitHub
  restricts owner and repository names to `[A-Za-z0-9._-]`.

`normalizedProviderInstance` likewise rejects Unicode host labels that Python's
`str.isalnum()` would accept. In every case Go fails the unit instead of
persisting a value it cannot prove matches Python.

## Known contract-versus-TRD gaps

TRD §10.1 states the PagerDuty contract preserves a `region` parameter. No
region field exists in any PagerDuty auth or config class in this repository,
so `credential_modes` does not record one. This is a documented TRD/code gap,
not an omission.

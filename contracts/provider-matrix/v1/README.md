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

## Known contract-versus-TRD gaps

TRD §10.1 states the PagerDuty contract preserves a `region` parameter. No
region field exists in any PagerDuty auth or config class in this repository,
so `credential_modes` does not record one. This is a documented TRD/code gap,
not an omission.

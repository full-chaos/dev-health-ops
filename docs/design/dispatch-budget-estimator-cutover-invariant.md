# invariant.md -- CHAOS-6036 (design; each code PR in the stack gets its own)

## Invariant, relative to main 7ece7cc0
For every unit the gate evaluates, in `go` mode the estimator returns exactly the list of (bucket key, estimated units, confidence, route family, notes) that Python's `/dispatch-budget-estimate` returns for that unit at that moment, so every admit, defer and consumption decision equals main's; the only differences from main are that the network failure classes do not exist and a whole-chunk database read error fails the pass instead of failing open. In `python` and `shadow` modes behaviour is byte-identical to main; shadow adds counters and logs only.

## Decision table
Axes generated from code, not memory:
- Consumers (rg `DispatchBudgetEstimate` in internal/syncdispatchruntime): E = `enforceRun` (budget_enforce.go), O = `observeRun` (budget_observe_run.go), C = `activeBudgetConsumption` (budget_consumption.go).
- Events: transport/bridge errors from `bridge.do` (bridge.go:88-118: request build, transport, non-2xx 4xx vs 5xx) and `DispatchBudgetEstimate` (decode incl. 1 MiB response cap); Python per-unit exceptions (`batch_estimate_provider_budget_for_units`: unit/integration/source row missing, `resolve_run_auth` failure, estimator exception); unknown provider; worker start.
- Time: same pass; next pass (active units are re-estimated by C every pass); credential rotated between passes; run-stamped vs NULL-stamped run (fallback to the mutable integration credential); the two reads in shadow taken at different instants.
- Plane below: Postgres reads by the Domain role, Fernet key present in the dispatch process, Python API availability.

| # | Event | E today | O today | C today | `go` mode after | Reason |
|---|---|---|---|---|---|---|
| 1 | Transport error / timeout / 5xx / bridge down | chunk admitted unchecked (open), counter `bridge_unavailable` | chunk logged, no estimates | chunk logged, baseline lower | class does not exist | no network |
| 2 | Response over 1 MiB or undecodable | same as 1 (decode error is not a contract error) | same | same | does not exist | no wire; a 500-unit chunk can approach the cap (NOT measured) |
| 3 | 4xx bad token | whole pass ERRORS (closed) | chunk open | chunk open | does not exist | no auth hop |
| 4 | 409 stale run reference | pass errors (closed) | chunk open | chunk open | does not exist; the gate already holds the run row in its transaction | staleness was a race guard for the HTTP hop |
| 5 | 409 unit not in (run, org) | pass errors (closed) | chunk open | chunk open | cannot occur: candidate units come from the gate's own query; C groups by (org, run) exactly as today | tenant fence becomes a SQL predicate |
| 6 | Python per-unit bootstrap/estimator exception | unit gets `()`: no constraint | same | same | SAME: unit gets `()`, same warn message name, plus counter `budget_estimate_unit_failure_total{provider,reason}` | parity; fail-open kept, made visible |
| 7 | Unknown provider | `()` | `()` | `()` | `()` | parity |
| 8 | Whole-chunk Postgres read error | (was a 5xx) chunk open | chunk open | chunk open | pass ERRORS for E; O and C degrade the chunk | deliberate change: the gate runs in one transaction, a failed read poisons it; overturnable default |
| 9 | Go estimator panic | n/a | n/a | n/a | `go` mode: pass errors (recovered, counted); `shadow`: swallowed, counted, Python answer used | shadow must never alter a decision |
| 10 | Worker start without bridge config | refuses to start | same | same | still required while any provider is `python`/`shadow`; not required when all are `go` | staged removal |
| 11 | Credential rotated between passes | active unit re-estimated under the new fingerprint: bucket key changes, old bucket's consumption drops | n/a | same | SAME (Go re-reads the credential each pass) | parity of a Python behaviour, not a fix |
| 12 | NULL-stamped (legacy) run | credential = integration's current credential | same | same | SAME: Go replicates `resolve_run_auth` order (run-stamped first, then integration) | parity |
| 13 | Shadow: Python and Go read at different instants | n/a | n/a | n/a | a diff must persist over two consecutive passes before it counts against the adoption bar | avoids false diffs from a mid-pass edit |
| 14 | Unlisted pagerduty dataset key | 2 units, family `pagerduty_<key>` | same | same | SAME (any-key default reproduced) | Python estimator is a total function for pagerduty |
| 15 | Alias dataset key (pr-reviews, pr-comments, tests, work-item-*) | estimated | same | same | SAME (estimator keyed by dataset_key, not by registry Plannable) | the gate sees every key units carry |

## Enumeration result (generated, this session)
- Python estimator, executed over every `DatasetKey` value plus the 11 pagerduty names: github 17, gitlab 19, jira 6, linear 5, launchdarkly 1 = 48 named pairs; pagerduty returns an estimate for ALL 29 keys tried (total function). 
- Go plan on main: 13 planned + 24 unplanned registry routes (PR 2697); zero cover buckets, notes, or unlisted keys.
- The PR stack proves the invariant per provider by live-Python oracle over (provider x dataset key x span x flags x options) for the estimate struct, then by production shadow; a test pins "dataset keys Python estimates == dataset keys Go estimates" from a generated list, so a new key on either side goes red.

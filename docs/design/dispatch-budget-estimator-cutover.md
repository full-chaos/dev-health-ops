# CHAOS-6036 design: Go budget estimator behind the live dispatch gate

Status: DESIGN ONLY. No code. Base: origin/main 7ece7cc0 (includes #2697 request-plan proof, #2703 request-usage table). Author: lane gwc-api-sankeycov. Invariant and decision table: `invariant.md` (same dir).
Goal (owner): move to Go, do not straddle. Scope: replace the Python `/dispatch-budget-estimate` call that the dispatch budget gate makes.
Verified = read in code this session. NOT verified = marked as such.

## 0. Summary

- The gate (`enforceRun`, `observeRun`, `activeBudgetConsumption`) already talks to ONE interface: `budgetEstimator.DispatchBudgetEstimate(ctx, orgID, runID, unitIDs)` (budget_consumption.go:44). Only `*HTTPBridge` implements it.
- Cutover = a second implementation of that interface in Go, a wrapper that runs both and compares, then a per-provider config switch. The gate's estimate calls do not change; one small gate change is needed for the failure policy (PR D0): classify a native fatal error.
- Hard parts: (1) bucket identity parity (host + credential fingerprint must be byte-identical, or budgets split across two buckets); (2) 30 dataset keys Go does not estimate at all; (3) Go's own honest plan (PR 2697) deliberately differs from Python on 3 datasets, so "parity" and "correct" conflict. See 3.5 and section 6.

## 1. What Python's endpoint computes

Call chain (all read-only, Python):
- HTTP: `POST /api/internal/worker-sync/dispatch-budget-estimate`, `dispatch_budget_estimate_reference`, src/dev_health_ops/api/internal/worker_sync.py:180. Auth: bearer (`_authorize`). Then `_current_sync_run_reference` (:91): stale run reference = HTTP 409.
- `_dispatch_budget_estimate` (:139): opens a Postgres session, `_units_belong_to_run` (:106; unit ids must all belong to (run, org), else 409), then `batch_estimate_provider_budget_for_units`.
- `batch_estimate_provider_budget_for_units`, src/dev_health_ops/sync/budget_guard.py:182: per unit, `SyncTaskBootstrap.load(session, unit_id)` then `estimate_provider_budget(ctx)`. ANY exception per unit is logged and that unit gets `()` (no estimate = no constraint). Never raised.
- `estimate_provider_budget`, src/dev_health_ops/sync/budget.py:24: dispatch on `context.provider.lower()` to six estimators; any other provider returns `()`.
- Estimators (class line): github providers/github/budget.py:25, gitlab providers/gitlab/budget.py:25, jira providers/jira/budget.py:27, linear providers/linear/budget.py:24, launchdarkly providers/launchdarkly/budget.py:128, pagerduty providers/pagerduty/budget.py:56.

Inputs, from `SyncTaskBootstrap.load` (src/dev_health_ops/workers/sync_bootstrap.py:331) into `SyncTaskContext` (:229). The estimators read only these:

| Input | Source | Used by |
|---|---|---|
| provider, dataset_key, org_id, integration_id | `sync_run_units` row | all |
| window_start, window_end (`since_at`, `before_at`) | unit row; `window_span_days` = max(1, (end-start).days), budget_types.py:61 | all |
| processor_flags (bool map) | unit row `processor_flags` | github (sync_prs), jira (worklogs, atlassian gql) |
| dataset_options | `integration_datasets.options` | pagerduty (`enrichment_cap`) |
| credential_id, credential mapping | `resolve_run_auth` (sync_bootstrap.py:163-228), full inventory below | every estimator, ONLY for bucket identity |
| source_is_org_wide_placeholder | source metadata | linear |

Credential resolution inventory (each item changes the bucket identity or whether an estimate exists; verified by reading `resolve_run_auth`, `_resolve_integration_auth`, `_credential_mapping`, `_resolve_env_credentials`, `_verify_stamped_fingerprint`):
1. Source of the credential id: run-stamped `sync_runs.credential_id` when `auth_source` is non-NULL; otherwise the mutable `integrations.credential_id` (NULL-stamped legacy runs).
2. No credential id (stamped NULL, or integration NULL): the ENVIRONMENT of the Python process serving the bridge (`_resolve_env_credentials`, `credentials/resolver.py::PROVIDER_ENV_VARS`) becomes the credential mapping. Go must read the same table in the dispatch pod; a different environment in the two pods changes host and fingerprint. PagerDuty instead raises (no estimate).
3. Mapping = stored `IntegrationCredential.config` merged UNDER the decrypted secret fields, decrypted wins (`_credential_mapping`, task_utils.py:110). Decrypted JSON alone gives a different host and fingerprint (config carries `base_url`).
4. Credential row missing (deleted mid-run) raises: no estimate. PagerDuty also requires an active, provider-matching credential row (`_require_active_pagerduty_credential`): otherwise no estimate.
5. Stamped fingerprint check (`_verify_stamped_fingerprint`): if the run carries a stamped fingerprint and the resolved content differs, `SYNC_RUN_AUTH_STRICT` truthy raises (no estimate), else warns and continues. Go needs the stamped fingerprint, Python's `credential_fingerprint` function for it (a different function from the budget fingerprint), and the strictness setting.
6. PagerDuty only: `hydrate_pagerduty_credentials` runs after resolution. In `oauth` mode it reads OAuth storage, validates scopes and bindings and may refresh a token (a WRITE); in `client_credentials` mode it obtains a token over the network (sync_auth.py:245); failure of either raises: no estimate. The budget estimator itself needs only `region` and `subdomain`, never the token.

Credentials are used for exactly two outputs: `host` and `credential_fingerprint` of the bucket key. Units are computed from dataset, span, flags, options only.

Output per unit: list of `BudgetEstimate` = (bucket{provider, org_id, host, credential_fingerprint, dimension}, estimated_units, confidence, route_family, notes). Wire schema: `BudgetEstimatePayload` (worker_sync.py); Go mirror `budgetEstimate` (budget_estimate_bridge.go:54).

Bucket identity per provider (fingerprint = sha256 of `json.dumps(scope, sort_keys=True, default=str, separators=(",",":"))`; NOTE `ensure_ascii` is the Python default):

| Provider | host rule | fingerprint scope | ref |
|---|---|---|---|
| github | hostname of `base_url`/`baseUrl`, default github host | keys app_id, installation_id, base_url, plus `token_sha256`; fallback {credential_id or "env", integration_id} | github/budget.py:270,280 |
| gitlab | hostname of base url, default gitlab.com | keys user_id, username, group_id, project_id, base_url, plus `token_sha256` of token / private_token / access_token (first present); same fallback | gitlab/budget.py:302,330,344 |
| jira | hostname, default atlassian.net | own key list | jira/budget.py:225,240,254 |
| linear | default api.linear.app | organization_id, workspace_id, team_id | linear/budget.py:187,197,211 |
| launchdarkly | default app.launchdarkly.com | project_key, environment | launchdarkly/budget.py:196,206,220 |
| pagerduty | `api.eu.pagerduty.com` if region=="eu" else `api.pagerduty.com` | sha256 of `subdomain` (default "env"), no JSON | pagerduty/budget.py:56-75 |

Dataset coverage in Python, GENERATED by calling `estimate_provider_budget` for every `DatasetKey` value plus the 11 pagerduty names and listing which return a non-empty result: github 17, gitlab 19, jira 6, linear 5, launchdarkly 1 = 48 named pairs (alias keys pr-reviews, pr-comments, tests, work-item-* included). PagerDuty is a TOTAL function: every dataset key returns an estimate (29 of 29 keys tried); the 8 named datasets get 2 units under family `pagerduty_<name>`, the 3 enrichment datasets (incident-alerts, incident-log-entries, incident-notes) get 2 units plus a fan-out term from `enrichment_cap`, and ANY OTHER key gets 2 units under `pagerduty_<key>` (pagerduty/budget.py:96-123). Go must reproduce that default, or a pagerduty unit with an unlisted dataset silently loses its constraint.

Consumers of the estimates, all in Go (estimate PRESENCE also gates cooldown matching: `matchingCooldownExpiry` at budget_enforce.go:213, budget_surplus.go:72, budget_reconfirm.go:83; `reconfirmCooldowns` at native_dispatch_sync_run_service.go:368): `enforceRun` (budget_enforce.go, fail-open per chunk), `observeRun` (budget_observe_run.go:77, telemetry), `activeBudgetConsumption` (budget_consumption.go:160, re-estimates ACTIVE units of every run to compute consumed budget; the estimate for a running unit is recomputed each pass).

## 2. What Go has, and the gap

Go has `ProviderRequestPlan` (internal/providersync/request_plan.go) returning `RequestEstimate{Dimension, Units, Confidence, RouteFamily}`. It has NO bucket identity, NO notes, and is keyed by the registry (RouteReady+Plannable), not by every dataset key a unit can carry.

State after PR 2697 (verified by enumeration):
- 13 (provider,dataset) pairs have a plan: github cicd, deployments, prs, work-items; gitlab cicd, commit-stats, commits, deployments, incidents; jira incidents, work-items; launchdarkly feature-flags; linear work-items. The alias keys (pr-reviews, pr-comments, tests, work-item-labels/projects/history/comments) ARE handled inside the same switch cases but are not Plannable, so the registry enumeration does not list them.
- 24 Plannable routes have NO plan: github blame, commit-stats, commits, files, repo-metadata, security; gitlab blame, feature-flags, files, prs, repo-metadata, security, work-items; pagerduty all 11.
- 3 of the 13 differ from Python ON PURPOSE (github deployments base term, gitlab deployments extra term, github prs REST term): Go reserves MORE, because Python under-reserves against real Collect runs.
- 10 of the 13 have no harness proof yet (`harnessRemainingRoutes`).

Gap per provider against Python's 48 named pairs plus the pagerduty default:

| Provider | Python pairs | Go plan today | Missing for the gate |
|---|---|---|---|
| github | 17 | 4 dataset groups | repo-metadata, commits, commit-stats, files, blame, security (6); bucket identity; notes; the 3 intended divergences must be decided (section 6) |
| gitlab | 19 | 5 | repo-metadata, files, blame, security, prs family, work-items family, feature-flags (7 + aliases); bucket identity |
| jira | 6 | 6 (oracle-checked) | bucket identity, notes; Go-only dev-status term is NOT in Python (keep out of parity) |
| linear | 5 | 5 (oracle-checked) | bucket identity, `org_wide_placeholder` input |
| launchdarkly | 1 | 1 | bucket identity |
| pagerduty | 11 named + default for any key | 0 | everything: 11 datasets, the any-key default, `enrichment_cap` option, region/subdomain identity |

New on main: #2703 stores actual provider request spend per unit execution in ClickHouse `provider_request_usage` (per org, provider, dataset, unit, execution, transport, flush). It does not feed the gate. It gives the shadow phase and ticket H a production actual to set beside the estimate, so under-reservation can be measured from production instead of only from the fixture harness.

Also missing everywhere: the input loader (unit row, integration dataset options, run-stamped credential resolve) inside the gate's transaction, and the fingerprint canonicalization.

Go credential shape (verified): `providerfoundation.Credential` has `Config map[string]string` and redacted secret fields. Python hashes TYPED JSON (app_id may be an integer). `FernetDecryptor.Decrypt` returns the raw JSON bytes (credentials.go:49), so Go can decode with `UseNumber` and re-serialize Python-style. `Config map[string]string` must NOT be used for the fingerprint (types are lost). Verified in code: the sync-dispatch worker family ALREADY builds a `CredentialResolver` over `postgresDatabase.pools.Domain` with a Fernet decryptor (cmd/dev-health-worker/sync_dispatch.go:594, `catalogDecryptor`) for the team-catalog collectors, so the dispatch process has the settings encryption key and a credential read path on the Domain pool. NOT verified: SELECT on `integration_datasets` and `sync_runs` for the Domain role, and that the catalog path is enabled in production (PR B's first task).

## 3. Cutover mechanism

### 3.1 Seam
New type `goBudgetEstimator` implements `budgetEstimator` (same method, same return type `map[string][]budgetEstimate`). `NewNativeDispatchSyncRunService` already takes the interface (native_dispatch_sync_run_service.go:91). No gate code changes.

### 3.2 Estimator mode, per provider
Config `SYNC_BUDGET_ESTIMATOR_MODE` = comma list `provider=mode`, default all `python`. Modes:
- `python`: today. Bridge required.
- `shadow`: `shadowBudgetEstimator` calls the Python bridge (authoritative, returned to the gate) AND `goBudgetEstimator`; compares per unit; logs and counts; the gate sees ONLY Python's answer. Go errors and panics are swallowed and counted, never reach the gate. Isolation (required, not implied): the Go loader NEVER runs on the gate's transaction. It takes its own connection from the Domain pool inside its own short read-only transaction with a statement timeout and its own context deadline, and it is cancelled if the gate's context ends. Reason: a statement error on the gate's transaction aborts it (SQLSTATE 25P02) even if the shadow wrapper swallows the error, so a swallowed Go failure would still fail the gate's next statement. The gate holds one connection while the loader takes a second, so the Domain pool needs at least two connections per dispatcher goroutine; PR B verifies pool size. In `go` mode the loader uses the same separate connection (a read of unit, dataset, credential and run rows the gate does not modify), so the only Go-mode failure that reaches the gate is the classified fatal error below.
- `go`: `goBudgetEstimator` only. Bridge not called for that provider.
A unit's provider picks the mode; one call can mix providers (the wrapper splits the unit list by provider).
Rollback = set the mode back (config, no deploy of code). Rollback is per provider.

### 3.3 Shadow comparison (the production evidence)
Per unit compare the estimate lists as multisets keyed by (bucket key, route_family, dimension): units, confidence, notes. Diff classes: `equal`, `units_differ`, `confidence_differ`, `notes_differ`, `bucket_differs` (host or fingerprint), `missing_in_go`, `extra_in_go`, `go_error`, `python_unavailable` (no comparison possible).
Emit: counter `budget_estimate_shadow_total{provider,dataset,class}` (class `equal` included, so coverage is measurable), one structured log line per NON-equal unit with both lists (no credential material: bucket key fields are already non-secret hashes) at WARN, and one INFO summary line per provider and dataset every 5 minutes with counts per class and comparison duration (histogram `budget_estimate_shadow_duration_seconds`; a shadow that slows dispatch is visible), so that a shadow that stops comparing (no lines, counter flat) is visible at Info. A diff persisting for one pass is logged at INFO; adoption counts only diffs persisting over two consecutive passes. Adoption bar (proposed): for a provider, at least N consecutive dispatch passes (default 500 units per dataset seen, at least 7 days) with zero non-equal classes, over every dataset that has live units; datasets with no live units are covered by the oracle only and are named in the switch PR.

### 3.4 Failure semantics: today and after
Today (verified in bridge.go:89-118, budget_enforce.go:153-170, budget_estimate_bridge.go, sync_dispatch.go:441):

| Situation | Today |
|---|---|
| Worker start without bridge URL/token/timeout | sync family REFUSES to start (`errWorkerDependencyUnavailable`) |
| Transport error, timeout, HTTP 5xx, decode error | that chunk of units is admitted with NO budget check (fail OPEN), one warn per unit, observer counter `bridge_unavailable` |
| HTTP 4xx (bad token, 409 stale run, 409 unit not in run, oversize) | `enforceRun`: whole pass errors (fail CLOSED). `observeRun`/`activeBudgetConsumption`: that chunk degrades (fail open) |
| Python per-unit bootstrap or estimator exception (bad credential, missing row) | that unit gets empty estimates = no constraint (fail open per unit), logged server-side |
| Unknown provider | empty estimates = no constraint |
| Chunk size | 500 units per call |

After, in `go` mode (proposed defaults; product questions in section 7):

| Situation | After |
|---|---|
| Worker start | bridge config NOT required for a provider whose mode is `go`; still required while any provider is `python`/`shadow`; PR G removes it |
| Transport/5xx/timeout | cannot happen for `go` providers (no network). Counter `bridge_unavailable` stays for python/shadow |
| 4xx classes | vanish; the run-stale and unit-in-run checks become plain SQL predicates on the gate's own transaction (the gate already loaded the run and its candidate units) |
| Per-unit bootstrap failure (row missing, credential undecryptable) | SAME as today: empty estimate, no constraint, warn log with the SAME message name, plus a NEW counter `budget_estimate_unit_failure_total{provider,reason}` so the fail-open is now visible |
| Database read error for the whole chunk (loader, own connection) | `enforceRun`: pass ERRORS (fail closed) through a NEW sentinel `ErrEstimateFatal`; `observeRun` and `activeBudgetConsumption` degrade the chunk. Today's analogue (5xx) is fail open. Deliberate change; overturnable default. Requires a gate change (see PR D0): the gate today treats only `ErrBridgeContractRejected` as fatal, so a plain returned error would fail OPEN. |
| Unknown provider | empty estimates (parity) |
| Chunk cap | gone |

### 3.5 Numbers: parity first
The gate estimator returns Python-PARITY numbers at cutover, proven by the live-Python oracle over the full struct (bucket, units, confidence, route_family, notes) for every (provider, dataset, span, flags) cell. Go's honest plan (`ProviderRequestPlan`) stays separate. Adopting the honest numbers for a dataset is a LATER, per-dataset, config-gated step (mode `go-honest` proposed), because it raises reservations and so raises deferrals in production. Reason: "do not straddle" is about the runtime plane; changing what the budget means is a separate decision.


### 3.6 Production facts (read-only, provided by prod-ops, rev 123, one pod ~1 h; NOT verifiable from the repository) and what they change

- Enforcement is ON. `Dispatch` calls `observeRun` then `enforceRun` unconditionally (native_dispatch_sync_run_service.go:351,356); no on/off key. Effective limits: `SYNC_BUDGET_BUCKET_LIMITS` = github rest_core 250, graphql_cost 500, contents_blob 100, secondary_abuse_risk 25; linear graphql_cost 500; everything else `SYNC_BUDGET_DEFAULT_LIMIT` 1,000,000 (launchdarkly, gitlab, jira, pagerduty are effectively unconstrained). Deferral 60 s + 5 s jitter.
- `dry_run` = `observeRun` (budget_observe_run.go). Verified read-only: it only reads candidate units, uses its OWN limit env (`SYNC_BUDGET_DRY_RUN_*`, none set here) and a local running total, writes nothing to Postgres, and its return value is discarded by `Dispatch`. It only LOGS (`dispatch_sync_run.budget_guard_dry_run`, one line per estimate). It cannot harm admission. `budget_guard_allowed` is the REAL gate's log (budget_enforce.go:371), emitted when `enforceRun` admits.
- The gate is real but idle: 194 allowed, 0 deferred, 0 enforce_failed in the window. The real gate CAN defer (`deferUnitForBudget` writes `available_at`). GitHub buckets have small limits (250/500/100/25), so a wrong github estimate can defer real work; a wrong estimate for the other providers cannot, at current limits.
- Consequence for the shadow phase: it proves ESTIMATE parity (it compares estimates, not decisions), which stays valid on an idle gate. It cannot prove DEFERRAL parity, because production never exercises the defer path. Replacement check before each provider switch: a deferral-parity harness runs the real `enforceRun`, `observeRun` and `activeBudgetConsumption` over a seeded Postgres (testcontainer) with deliberately tiny limits AND cooldowns (matching cooldown, reconfirmation, surplus retry and terminalization paths, not only budget limits), once per estimator (Python estimates computed offline by the oracle for the production-observed input tuples, then `goBudgetEstimator`), and asserts identical admitted and deferred unit sets, `nextDeferredAt`, and per-bucket consumption. The cells come from a read-only SQL export of production unit inputs (distinct provider, dataset, span, flags, options from `sync_run_units` and `integration_datasets`), NOT from logs (equal comparisons are not logged as lines); limits are set so every bucket defers at least once.
- Harm bound of the cutover: github providers are the only ones where a bad estimate changes behaviour today, so github is switched LAST and after the deferral-parity harness; jira/linear(500 graphql)/launchdarkly/pagerduty/gitlab first. Linear has a real limit (500), so linear is in the constrained group.
- "provider budget contended" (github blame/files `provider_unit_exhausted`, seen in production) does NOT come from this gate. It comes from `providerfoundation.ErrBudgetContended` (providerfoundation/budget.go:142): `ValkeyBudgetStore.Acquire` is a per-(provider, org, host, cost class) CONCURRENCY semaphore in Valkey (Lua: deny when in-flight count >= key.Limit), taken per request by `HTTPClient.Do` and released on completion. It uses no estimates, no `SYNC_BUDGET_*` key, and no Python. This cutover does not touch it and cannot fix or worsen it.


## 4. Invariant (one sentence, relative to main)

Relative to main at 7ece7cc0: for every unit the gate evaluates, in `go` mode the estimator returns exactly the list of (bucket key, estimated units, confidence, route family, notes) that Python's endpoint returns for that unit at that moment, so every admit, defer and consumption decision is unchanged; the only differences from main are that the network failure classes no longer exist and a whole-chunk database read error fails the pass instead of failing open. In `python` and `shadow` modes the gate's behaviour is byte-identical to main (shadow adds only counters and logs, and cannot change or delay a decision).

The full decision table, its axes (mode, failure event, time, the Postgres/Fernet plane below) and the enumeration result are in `invariant.md`.

## 5. PR stack (smallest first). No PR carries two interacting mechanisms (R311)

Each PR gets its own `invariant.md` before its first round; max 2 counted rounds per PR. Sizes: S <300 lines, M 300-900, L >900 including tests.

| # | PR (ONE mechanism each) | Size | Depends on | Evidence |
|---|---|---|---|---|
| A | Bucket identity (pure functions): host rules x6, fingerprint canonicalization (Python `json.dumps` byte parity incl. `ensure_ascii`, big ints, floats, `default=str`), pagerduty sha | S | none | live-Python oracle, byte-equal fingerprint over a corpus (unicode, ints, floats, nested, missing keys) |
| B | Estimate input loader (I/O only, own connection, read-only, bounded): unit row, dataset options, org-wide placeholder, the six credential-resolution items of section 1 (stamp order, env source table, config-under-secret merge, missing/inactive rows, stamped-fingerprint + strict flag, PagerDuty region/subdomain without hydration). First tasks: verify Domain-role grants and pool size | M | A | Postgres testcontainer: resolved (credential_id, mapping, host, fingerprint) equals Python `resolve_run_auth` on seeded rows for stamped, NULL-stamped, env, config-merged, deleted, inactive, strict-mismatch, and PagerDuty api_token and oauth |
| D0 | Gate failure classification (the ONLY gate change): sentinel `ErrEstimateFatal`; `enforceRun` treats it like `ErrBridgeContractRejected` (fail closed), `observeRun` and `activeBudgetConsumption` degrade the chunk; the bridge never returns it, so behaviour on main's paths is unchanged | S | none | integration tests over the real gate: fatal error fails the pass, plain error fails open (as main), contract rejection unchanged |
| C1 | Estimators jira, linear, launchdarkly (pure numbers + buckets + notes + families) | M | A | oracle over the full estimate struct, dataset x span x flags grid |
| C2 | Estimators github (6 missing datasets, aliases, PARITY numbers incl. the 3 datasets Go plans differently) | L | A | same |
| C3 | Estimators gitlab (7 missing + aliases) | L | A | same |
| C4 | Estimators pagerduty (11 named + any-key default, `enrichment_cap`, region) | M | A | same, plus unlisted dataset keys |
| D1 | `goBudgetEstimator` adapter: loader + estimators behind the `budgetEstimator` interface, per-unit degrade-to-empty, whole-chunk read error returned as `ErrEstimateFatal`, recovered panic returned as `ErrEstimateFatal`; NOT wired anywhere | M | B, C1..C4 (per provider), D0 | fake-loader unit tests for every failure class; oracle end-to-end per provider |
| D2 | Mode router: `SYNC_BUDGET_ESTIMATOR_MODE`, provider split, wired into the service, default `python` (behaviour = main) | S | D1 | tests per mode; default mode byte-identical to main (same calls, same decisions) |
| D3 | Shadow comparator: run Go beside Python on its own connection, compare per unit, counters, WARN diff log, INFO summary line, swallow Go errors and panics | M | D2 | every diff class; a Go SQL error, a Go panic and a Go timeout leave the gate's transaction usable and the returned Python answer unchanged (probe through the real `enforceRun`) |
| E | Worker start check: bridge required only if some provider is not `go` | S | D2 | start tests per mode combination |
| (test) | Deferral-parity harness: real gate, seeded Postgres, tiny limits, both estimators | M | D1 | identical admit/defer sets, `nextDeferredAt`, per-bucket consumption; required before F for linear and github |
| (ops) | Shadow phase in production per provider; adoption report against the bar in 3.3; #2703 actual spend recorded beside the estimates | - | D3 deployed | production counters, written report |
| F1..F6 | Switch one provider to `go` (chart value only). Order: gitlab, jira, launchdarkly, pagerduty, linear, github last | S | ops evidence (+ deferral-parity harness for linear, github) | before/after deferral counts; rollback drill |
| G | Delete: `HTTPBridge`, `budget_estimate_bridge.go`, bridge config keys, chart keys, worker start check, Python endpoint + tests, 500-chunk code | M | F1..F6 plus soak | grep proof of zero references |
| H (later, per dataset) | Adopt honest numbers (`go-honest`) | S each | harness remainder for THAT route (#2697's `harnessRemainingRoutes`), chris ruling | harness S+Q proof, #2703 production actuals, deferral projection |

Interaction check: A, C* are pure and share nothing but the struct; B is the only I/O; D1 joins them but has no wiring; D2 is the only wiring; D3 is the only comparison. No PR changes both a decision path and its evidence.

Harness remainder gates only H (confirmed by team-lead). A-G are a parity port proven by oracle plus production shadow.

Parallelism: A first; then B and C1..C4 (two at a time, per the backlog rule); D1 after the providers it wires.

## 6. Risks

1. Fingerprint byte parity is the top risk, and its INPUT (the credential mapping) is the larger half: config merge, environment source, stamp order (section 1). A wrong fingerprint does not fail a test in production: it silently splits one bucket into two, so limits stop binding. Mitigation: PR A oracle corpus; shadow class `bucket_differs` is a stop signal.
2. Grants and key: the dispatch family already holds a credential resolver on the Domain pool (sync_dispatch.go:594); the remaining unknown is SELECT on `integration_datasets` and `sync_runs`, and whether the catalog path proves the key is present in production. PR B step 1.
3. Cost: Python decrypts per unit per pass; `activeBudgetConsumption` re-estimates ALL active units of every run on every pass. In Go this becomes a per-pass Fernet decrypt per active unit inside a transaction. Mitigation: memoize (org, credential_id) fingerprint per pass; measure in shadow.
4. Parity of the 3 divergent datasets: in `go` mode with parity numbers, the honest fix of PR 2697 is not live. Known, accepted, ticket H.
5. Enforcement is ON with real limits for github and linear only (section 3.6); the gate has not deferred in the observed hour. So deferral parity is unobserved in production and rests on the synthetic harness.
6. Not verified: the Python-only producers named in the inventory comments (workgraph, provider_sync) do not touch this path.

7. Named exception E1 (PagerDuty hydration, modes `oauth` AND `client_credentials`): Python computes no estimate when `hydrate_pagerduty_credentials` fails (missing OAuth app config, scope or binding error, token fetch or refresh failure; sync_auth.py:214, :245). Go cannot reproduce that without reading OAuth storage and possibly fetching or refreshing a token (a write or a network call), which an estimator must not do, so Go returns the estimate anyway. CORRECTED harm bound (round 2 executed evidence): the estimate is not only a budget input. Its presence activates the cooldown check `matchingCooldownExpiry` in `enforceRun` (budget_enforce.go:213), surplus retry (budget_surplus.go:72) and `reconfirmCooldowns` (budget_reconfirm.go:83, called at native_dispatch_sync_run_service.go:368). A PagerDuty unit whose hydration fails is admitted on main (no estimate, no cooldown match) and can be DEFERRED after E1 when a matching cooldown is active, even at the 1,000,000 default limit (executed against the real `enforceRun`: deferred=false without the estimate, deferred=true with it). So E1 is a behaviour change, not a no-op: an unauthenticatable PagerDuty unit is held by an active cooldown instead of dispatched (it would fail at execution anyway, because the provider worker cannot hydrate either). Ruled by the lead on this corrected basis: KEEP. Present behaviour: with a Go estimate present, a PagerDuty unit (oauth or client_credentials) whose auth cannot be established is subject to cooldown matching like any unit: during a matching cooldown it is deferred, then dispatched and fails at execution. Bound relative to main: the auth failure becomes visible at most one cooldown later; no unit is lost and none is admitted that main refuses. Pins if kept: (a) a test that resolves the limit for a pagerduty bucket from the shipped configuration and fails if it is not the default; (b) worker start logs ERROR and counts `budget_estimator_exception_invalid_total` if `SYNC_BUDGET_BUCKET_LIMITS` holds a `pagerduty:` key while pagerduty's mode is `go`; (c) the deferral-parity harness runs the PagerDuty hydration-failure cell with a matching cooldown and records the expected difference; (d) one more harness cell: the deferred unit IS dispatched after the cooldown ends (no permanent hold). Overturn path: replicate the hydration failure by a read-only check (config present, credential row active, OAuth row present and scoped), which still cannot reproduce a live token-fetch failure.

8. Estimate-existence dependencies outside the estimator's own inputs: `SyncTaskBootstrap.load` also reads `sync_watermarks` for incremental units (sync_bootstrap.py:405-413); a failed read suppresses the estimate although the estimator never uses the watermark. Go's loader does not read it. Treated as part of the whole-chunk Postgres read failure class (same database, same role): a broken `sync_watermarks` read on main gives no estimate for that unit; in Go the estimate is produced. Named limit, recorded, no mitigation (a database serving the other reads but not this table is not an operating state); shadow class `go_extra_estimate` on such a unit is investigated, not adopted silently.

## 7. Product questions (chris; each has an overturnable default)

1. What should the budget MEAN at cutover: Python's numbers (known under-reservation, e.g. github/prs REST 2 vs at least 3 real), or Go's honest numbers (more deferrals)? Default: Python parity first; honest numbers later, per dataset, config-gated, after the harness proof.
2. Unit whose credential cannot be decrypted or whose row is missing: no constraint (today) or defer the unit? Default: no constraint, now counted and visible.
3. Provider with no estimator (unknown provider): no constraint (today) or defer? Default: no constraint.
4. Whole-chunk database read error: fail the pass (proposed) or fail open (today's network analogue)? Default: fail the pass.
5. Answered by prod facts: enforcement ON, limits for github and linear only. Remaining question: keep those limits as is through the cutover (default), or retune them separately?

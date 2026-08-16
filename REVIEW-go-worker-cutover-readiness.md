# Go Worker Cutover Readiness — review of PR #1738

Reviewed head `1001ded9` vs base `ebdc8d28` (318 commits, 1,220 files, ~200k insertions). CI green (31/31).
Measured against the Go Worker Runtime Migration project goal: replace the Python Celery workers with Go equivalents.
(The review request referenced "PR 1783", which does not exist; #1738 matches the reviewed branch exactly.)

## Verdict

**Merge-safe as a capability drop, not yet cutover-ready.** Everything ships default-off, migration 0066 is now
genuinely guarded, and resource usage, sharding, and horizontal scalability are real wins. But measured against
"replace the Celery workers", this is today a **transport migration (Celery→River) more than a runtime migration** —
most compute still executes in Python via HTTP bridges — and two verified critical runtime bugs plus four behavioral
regressions vs the running Celery workers must land before any multi-replica or multi-queue-group deployment is trusted.

| Dimension | Status |
|---|---|
| Completeness | Partial — 59/59 pairs compiled & route-ready, 0% of live workload runs in Go by default; compute families still Python |
| Deployment readiness | Helm-only — compose/swarm/raw-k8s overlays have breaking env drift; recent queue-group/CLI rework genuinely improved ease |
| Correctness | 2 verified critical bugs (multi-family boot failure; drain terminalizes jobs) |
| Parity vs Celery | 4 regressions (rate limits, Retry-After, Jira timestamps, PagerDuty watermark) + 6 genuine improvements |
| Scalability & resources | Confirmed — queue groups, fenced leases, PgBouncer topology, cost budgets are sound; lease *failure handling* needs hardening |
| Simplification | ~50k lines removable; ~28k (mutation harness, domaingrants, evidence) gate nothing and can go now |

## 1 · Completeness

- **Provider sync (59 pairs):** all registered native Go (`internal/providersync/execution_registry.go`) — but 0 enabled
  anywhere; every `WORKER_*_ENABLED` switch defaults off, and `route_ready` was granted on fixture parity with
  canary/live parity explicitly waived (`execution_registry.go:344-349`).
- **Compute is not native.** Daily metrics, all 8 remaining families, work-graph and investment POST to the Python API
  (`cmd/dev-health-worker/daily.go:97-169`, `workgraph.go:44-52`); sync-dispatch effects, operational effects, and
  PagerDuty reconciliation also bridge. Celery can eventually stop; the Python worker code cannot.
- **Scheduler is real now** (`goOwnsMarkers: true`, native materializer, concrete producers) but deployed nowhere, and
  the ask-dev retention schedule provably emits nothing (producer v3 vs contract v2 — `internal/scheduler/fixed/inventory.go:203-265`);
  only Celery Beat purges Ask Dev conversations.
- **Defaults:** all 24 `worker_job_routes` rows seed `celery`; 0066 (23 kinds → river) is on a separate alembic branch
  behind `DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1`. Transitional inventory: 82/146 surfaces `celery_only`, 40 `python_compatibility`, 20 `native_go`.
- **Claim vs code:** inventory says the Go coordinator is "the active path" for sync dispatch, but `sync_dispatch_transport_routes`
  is seeded `celery` (0049) and never flipped; `fixed.go:135-137` carries a stale "goOwnsMarkers is false" comment.

## 2 · Deployment readiness & ease

Recent rework (`f015d78b` queue groups, `1001ded9` CLI-flag topology, `bacd4b98` drain windows) genuinely improved ease:
explicit fail-closed `--queues/--queue-concurrency` replaces profile magic, all four renderers state the topology
identically with tests locking them together, and `bacd4b98` fixed a real blocker (heavy's 7,260s drain window vs the old
5m cap). 0066 went from "fires on `docker compose up`" to triple-guarded with a regression test. Rollback layering
(scale-to-zero → audited `workerctl job-routes rollback` → guarded downgrade) is sound.

Gaps — the credential/DSN layer drifted per renderer:

- **ClickHouse port drift (breaks first):** Go speaks native protocol, but `compose.go-workers.yml:14`,
  `stack.go-workers.yml:14`, `deploy/kubernetes/secrets.yaml:22` hand Go workers the `:8123` HTTP URI. First scale-up of
  any ClickHouse-dependent group fails on compose/swarm/raw-k8s. Helm and the local overlay are correct.
- **Missing secrets outside Helm:** sync group requires `SETTINGS_ENCRYPTION_KEY` (fail-closed, `provider_sync.go:705`)
  but compose/swarm never pass it; raw k8s secrets/configmap lack all four required keys. No test binds manifest
  `secret_env` lists to rendered environments.
- **No single cutover runbook** (operator must synthesize four sources); PgBouncer session-pool defaults (22/10) vs
  manifest budget (23/11); swarm/raw-k8s ship no PgBouncer; no production renderer declares the `WORKER_*_ENABLED` switches.

## 3 · Runtime bugs (criticals verified by direct read)

- **C1 — Multi-family queue groups cannot boot.** Each family builder calls `registerRescueCoverage`, registering
  rescue-only workers for every kind it doesn't own on the shared `river.Workers`; any selection spanning two families
  collides on duplicate kinds and exits at startup — including the shipped `heavy` group (metrics, investment, reports,
  workgraph). `cmd/dev-health-worker/rescue_workers.go:9-22`, `internal/jobrescue/registry.go:89-104`,
  `dependencies.go:459-497`. Multi-queue tests use fake builders, so CI never sees it. Fix: register rescue coverage once, post-composition.
- **C2 — Drain/lease-loss cancellation permanently kills jobs.** `classify` maps `context.Canceled` to retryable
  (`errors.go:122-126`), but `runStatus` writes `terminal` (`idempotency_postgres.go:207-210`), so the River retry hits
  `ClaimTerminal` and is auto-cancelled forever; can terminalize even a handler that succeeded just before cancellation
  (`adapter.go:410-421`).
- **Major — lease/heartbeat fragility:** budget-lease renewal gives up on the first transient error
  (`budget_postgres.go:231-249`; with C2, one DB blip terminalizes a 2h job); one failed presence heartbeat kills the
  process → a 10s DB hiccup restarts the whole fleet (`worker_presence.go:146-159`); idempotency lease fixed at 10min
  with no renewal vs 2h job timeouts → duplicate runs concurrently after minute 10 (`idempotency_postgres.go:27,127-145`).
- **Major — scale-out telemetry & defaults:** per-queue saturation divides fleet-wide running by per-process capacity —
  pegged at 100% for N≥2 replicas (`internal/storage/river/telemetry.go:466-473` vs `:241-247`); default 30s shutdown
  timeout yields a negative drain budget (every default-configured worker fails, `dependencies.go:976-985`); DB-open
  failure produces an alive-but-unready zombie (`dependencies.go:433-435`); all construction failures collapse into one
  opaque sentinel (`shell.go:188-197`).

## 4 · Parity regressions vs Celery

- **High — rate-limited work terminally fails.** Python defers 429s up to 10×/2h without consuming failure budget
  (`rate_limit_defer.py`, `sync_units.py:1501-1638`); Go has no rate-limit branch — a 429 burns one of 5 attempts on a
  5s–5m backoff, so a 30–60min GitHub reset terminalizes the unit (`providerunit.go:419-491`).
- **High — Retry-After clamped to 30s; reset headers ignored** (`providerfoundation/http.go:274-280,350-354`); Python
  honors both up to 300s. Also under-penalizes the shared Valkey backoff gate used by Celery workers.
- **High — Jira incidents rejects Jira Cloud timestamps** (`+0000` offsets; strict RFC3339 parse at
  `jira_incidents_route.go:480-486`, while `jira_work_items_rows.go:652-663` has the correcting helper). Fixtures are
  Z-suffixed, so CI can't catch it.
- **Medium — PagerDuty incidents watermark never advances on empty windows** (`pagerduty_incidents_route.go:505-538` vs
  `sync_units.py:1371-1373`); 401/403/404 retried 5× losing their category; unit timeout 55min→15min with chunking only
  for cicd/tests; deterministic pagination-cap failures re-fetch everything on every attempt.
- Bot findings worth fixing: nil-deref-on-error paths in `gitlab_blame_route.go:340`, `github_tests_route.go:312`,
  `github_tests_chunked_route.go:395`.
- Oracles are strong on row normalization (153 live-Python pairs) and planner windows; every finding above lives in seams
  they don't cover (worker state machine, live HTTP headers, retry-policy parity).

## 5 · Confirmed improvements over Python

Effect ledger + readback fencing; fail-closed pagination (Python silently truncated GitLab incidents at 1,000 rows and
advanced the watermark); atomic terminal transactions; cross-process provider cost budgets; queue-group topology with
exact-coverage validation and correct PgBouncer session/transaction routing; watermark math, planner windows, and repo
identity verified as faithful ports.

## 6 · Simplification (ranked)

| What | Payoff | When |
|---|---|---|
| Mutation harness + own tests + 39 plans — invoked by no workflow | ~16k lines | Now |
| `domaingrants` + grantcheck (advisory-only) | ~8k lines | Now |
| Evidence dirs, user-guide-evidence, local_validate meta-tests | ~4.5k lines | Now |
| Dual env+CLI config paths; retired profile plumbing; 5 hand-synced route-switch lists (derive from matrix.json like Python does) | ~700 lines + edit tax | Now |
| Consolidate 3 lease systems into one retry-until-TTL helper (source of C2 + majors) | bug class | With fixes |
| Coexistence packages (syncreconciler, joboutbox, jobroute/syncroute, bridges, transitional inventory, migration-state trifecta) | ~25–30k lines | Post-cutover |
| Live-Python oracle suites + PyYAML quirk emulation (keep goldens) | tens of k | Post-cutover |

Caution: the metrics HTTP bridge (`compatibility_http.go`) looks like scaffolding but is the **only** compute path for
daily/remaining metrics today.

## 7 · Recommended sequence

1. **Merge posture:** acceptable — default-off, guards verified, CI green; merging ends the 318-commit divergence risk.
2. **Before multi-replica/multi-group deployment:** fix C1, C2, lease renewal retry-until-TTL, presence-heartbeat
   tolerance, idempotency lease renewal, per-queue saturation math.
3. **Before enabling provider routes:** rate-limit deferral parity, Retry-After/reset-header honoring, Jira timestamp
   fix, PagerDuty empty-window watermark advance.
4. **Before cutover:** renderer env drift (ClickHouse port, secret keys) + a manifest→rendered-env contract test; one
   cutover-day runbook; native compute ports or an explicit decision that the Python API bridge is permanent; un-stall
   ask-dev retention; run the waived canary/live parity evidence.
5. **Housekeeping now:** drop non-gating harnesses (~28k lines), collapse route-switch copies, fix bot-flagged nil-deref
   paths and the stale `goOwnsMarkers` comment.

---
Compiled 2026-08-16 from five parallel subsystem audits (completeness, runtime correctness, provider parity, deployment,
simplification); both critical runtime findings re-verified by direct code reading at head `1001ded9`.

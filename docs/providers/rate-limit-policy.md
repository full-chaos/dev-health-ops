# Provider rate-limit policy

> **What this documents:** how each ingest provider (GitHub, GitLab, Jira,
> Linear, LaunchDarkly) exposes rate-limit state, how the sync stack reacts to
> it (in-place backoff, worker-level deferral, budget reservation), which
> auth/permission failures are **not** retryable, and the product invariant that
> **credentials are not capacity**. It is the durable record for the rate-limit
> and budget hardening tracked under
> [CHAOS-2742](https://linear.app/fullchaos/issue/CHAOS-2742). Where a behavior
> is not yet shipped it is labeled with the sub-issue that lands it — do not
> read a target contract as current behavior.

## Credentials are not capacity

This is a **product invariant**, not an implementation detail:

> **Changing or rotating a provider credential must never increase sync
> dispatch capacity.**

Credentials are authentication/runtime state. They are never a throughput lever.
The concrete shape that enforces this today:

- **One credential per integration.** An `Integration` carries a single nullable
  `credential_id` (`models/integrations.py`), resolved once per unit in
  `workers/sync_bootstrap.py`. There is no credential pool, no round-robin, and
  no unit-level credential selection. A sync unit uses exactly the integration's
  one active credential (or the env fallback when `credential_id is None`).
- **Runtime cache is credential-scoped for isolation, not capacity.** The
  provider runtime reuse cache keys on
  `RuntimeCacheKey(org_id, integration_id, credential_id, credential_fingerprint,
  provider, db_url)` (`workers/sync_bootstrap.py`). A rotated credential produces
  a new fingerprint and therefore a fresh runtime — this exists so rotation is
  correct and runtimes never leak across orgs/credentials, **not** so more
  credentials buy more concurrency.
- **The budget bucket is keyed by a credential *fingerprint*, so more
  credentials cannot dilute a shared limit into extra allowance.** Budget
  estimates are bucketed by
  `provider:org_id:host:credential_fingerprint:dimension:route_family`
  (`sync/budget_guard.py` `_budget_key`). Operators set limits by the coarser
  prefixes (`provider:dimension`, `provider:host:dimension:route_family`, …; see
  the candidate ladder in `budget_guard.py`), so capacity is governed by
  provider/host/route, never by how many credentials exist.
- **Run-level auth freezing is determinism, not capacity.** Freezing the
  credential identity/version onto a `SyncRun` at plan time (so a mid-run
  credential edit cannot produce a mixed-auth run) is a *determinism* mechanism.
  It selects **one** auth context for the whole run and must never be used to
  pick different credentials per unit for throughput. **Target contract, landing
  in [CHAOS-2755](https://linear.app/fullchaos/issue/CHAOS-2755):** the `SyncRun`
  model does not yet carry a credential stamp column.

**Out of scope, permanently:** credential pools, credential round-robin,
unit-level credential assignment for throughput, and any UI/API language
implying that more credentials create more allowance.

The invariant is enforced by `tests/test_credential_capacity_invariants.py`
(landing with [CHAOS-2756](https://linear.app/fullchaos/issue/CHAOS-2756)),
which asserts the planner supports no multiple-active-credential path for one
integration, that the runtime cache stays credential-scoped, and that any future
unit-level credential selection fails the suite until budget semantics are
revisited.

## How rate limits are handled

Rate limits are treated as **deferred work, not task failures**. There are three
cooperating layers:

1. **In-provider gate + in-place retry.** Each provider client owns its
   fetch/auth/pagination/retry/rate-limit handling (the provider boundary; raw
   fetch never leaves the provider). A shared backoff gate
   (`connectors/utils/rate_limit_queue.py` `RateLimitGate`, and its Redis-backed
   `DistributedRateLimitGate`) serializes waits so many workers don't stampede
   one API. Server `Retry-After` values are honored but **clamped** to
   `RateLimitConfig.max_backoff_seconds` (default **300s**) so a misbehaving
   upstream cannot park the gate indefinitely. In-place retry budgets per client
   (the shared `retry_with_backoff` decorator in `connectors/utils/retry.py`
   defaults to 5 attempts, but the REST connectors pin `max_retries=3`):

   | Client | In-place attempts on a rate limit | Source |
   | --- | --- | --- |
   | GitHub REST connector | 3 (`retry_with_backoff(max_retries=3)`) | `connectors/github.py` |
   | GitHub GraphQL client | 5 (`max_retries=5`) | `connectors/utils/graphql.py` |
   | GitLab REST connector | 3 (`retry_with_backoff(max_retries=3)`) | `connectors/gitlab.py` |
   | Jira client (JQL + enrichment) | 4 (`max_retries_429=3` → `+1` initial) | `providers/jira/client.py` |
   | Jira Atlassian REST compat | 5 (`RESTClient(max_retries=5)`) | `connectors/utils/rest.py` |
   | Linear | 5 (`DEFAULT_MAX_ATTEMPTS`) | `providers/linear/client.py` |
   | LaunchDarkly | 5 (`max_retries=5`) | `connectors/launchdarkly.py` |

2. **`RateLimitException` as the carrier.** When in-place retries are exhausted
   (or a 429/permission-vs-limit decision is made), the **canonical provider
   clients** (`providers/<provider>/client.py`) raise
   `dev_health_ops.exceptions.RateLimitException` carrying an optional
   `retry_after_seconds`, and the worker deferral (`workers/sync_units.py`)
   catches exactly that type. **Caveat:** the frozen legacy connectors
   (`connectors/github.py`, `connectors/gitlab.py`) raise a *separate*
   `connectors.base.RateLimitException` (it subclasses plain `Exception`, not
   `ConnectorException`), so the two exception hierarchies are **not yet
   unified**. Normalizing every provider onto one signal is
   [CHAOS-2753](https://linear.app/fullchaos/issue/CHAOS-2753) — see
   [Known gaps](#known-gaps).

3. **Worker-level deferral (`workers/rate_limit_defer.py`).** Instead of
   consuming the genuine-failure retry budget and stamping the run `FAILED`, a
   `RateLimitException` re-enqueues a fresh task with the server-provided delay.
   Two budgets bound it so a permanently limited provider still eventually
   surfaces as a real failure:

   - a **count** budget: `RATE_LIMIT_MAX_DEFERRALS = 10`, incremented once per
     provider 429;
   - a **wall-clock** budget: `RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS = 2h`, measured
     from the first deferral.

   Long server delays (e.g. a GitHub primary-limit reset up to ~1h) are
   **chunked**: a single Celery countdown is capped at
   `RATE_LIMIT_MAX_COUNTDOWN_SECONDS = 600s` (kept at/below the distributed
   gate's Redis TTL, `RateLimitConfig.max_backoff_seconds * 2 == 600s`) and an
   absolute `not_before` timestamp is carried forward so the task re-defers
   **without calling the provider again** until the window elapses. Chunk
   re-defers do not consume the count budget. Additive jitter (0–5s)
   de-correlates many orgs waking against the same provider at once.

### Budget reservation and route families

Before a unit dispatches, `estimate_provider_budget(context)` (`sync/budget.py`)
routes to the per-provider estimator under `providers/<provider>/budget.py`,
which returns `BudgetEstimate` values. Each estimate carries a `route_family`
(a provider-specific label for a group of endpoints) and a `BudgetDimension`
from the shared vocabulary in `sync/budget_types.py`:

| Dimension | Meaning |
| --- | --- |
| `rest_core` | Standard REST request budget |
| `graphql_cost` | GraphQL query-cost / complexity budget |
| `contents_blob` | Repository tree/blob/content expansion (high variance) |
| `search` | Search/JQL request budget (separately rate-limited) |
| `secondary_abuse_risk` | Timeline/social/expansion calls that can trip secondary/abuse limits |

The dispatch budget guard reserves against
`provider:org_id:host:credential_fingerprint:dimension:route_family` buckets and
defers units that would exceed `SYNC_BUDGET_BUCKET_LIMITS`. The per-provider
route families below are the authoritative catalog; the estimators are the
source of truth and
`tests/test_rate_limit_policy_doc.py::test_documented_route_families_match_estimators`
fails if code emits a family this page does not document.

### Actual-vs-estimated calibration (CHAOS-2759)

Every successful `run_sync_unit` result carries a `budget_comparison` under
`result['observations']`, joining the unit's run-time budget audit
(`estimate_provider_budget`, same call `budget_estimate` is built from) to
CHAOS-2754's normalized `provider_usage` actuals, one row per
`(route_family, dimension)` with drained actuals this run:

```json
{
  "route_family": "git",
  "dimension": "rest_core",
  "estimated_units": 2,
  "actual_requests": 5,
  "ratio": 2.5,
  "underestimated": true,
  "underestimation_assessable": true,
  "underestimation_assessable_reason": null,
  "unbudgeted_actual": false,
  "incomplete": false,
  "bucket": {"provider": "github", "org_id": "...", "host": "api.github.com",
             "credential_fingerprint": "...", "dimension": "rest_core"},
  "budget_key": "github:...:api.github.com:...:rest_core:git"
}
```

- **Raw numbers only.** `estimated_units` are abstract reservation units
  (see `SYNC_BUDGET_BUCKET_LIMITS` above), never converted against
  `actual_requests`; the two are reported side by side, not blended into one
  number.
- **A route_family/dimension with an estimate but no drained actuals this
  run produces no row** (code datasets, an unwired LaunchDarkly family, …) —
  never a fabricated 100% over-estimation.
- **`unbudgeted_actual`** is the reverse case: actual traffic on a
  route_family/dimension with **no matching estimate at all**, including the
  shared recorder's `unclassified` fallback for an operation that couldn't be
  resolved to any budget family. This is surfaced, not dropped — it is the
  highest-value calibration signal (real provider calls against zero admitted
  budget) — with `estimated_units: 0` and `ratio: null`. The row's `bucket` is
  a shell borrowed from a sibling estimate in the same unit (same
  provider/org/host/credential — a unit has one ctx, so those fields are
  constant across every estimate it produces) with only `dimension`
  overridden to match the actual observation.
- **`underestimated`** is `actual_requests > estimated_units`, but only
  trustworthy when **`underestimation_assessable`** is `true`. A raw request
  count is only comparable to `estimated_units` when the dimension
  denominates in something request-count-like — `rest_core` ("Standard REST
  request budget") and `search` ("Search/JQL request budget"), per the
  dimension table above — or when there is no estimate at all
  (`unbudgeted_actual`; a zero baseline is never a unit-conversion problem).
  For `graphql_cost` (query-cost/complexity points), `contents_blob`
  (high-variance blob/tree expansion), and `secondary_abuse_risk` (a flat
  risk-flag reservation, not a request count), comparing a request count
  against a nonzero estimate would invent a conversion the estimator never
  made: `underestimation_assessable` is `false`,
  `underestimation_assessable_reason` explains why, `ratio` is `null`, and
  `underestimated` stays `false` — **no warning is logged** for that row.
- **`incomplete`** is `true` for every row on a unit whose CHAOS-2754
  recorder hit its 50-key overflow cap. Dropped operations aren't attributed
  to a specific family (the recorder never learns which family they'd have
  joined), so an `incomplete` row's `ratio <= 1` must **not** be read as a
  confirmed over-estimation. A visible `underestimated: true` stays valid even
  when `incomplete`: a capped (undercounted) actual that already exceeds the
  estimate only understates the true overage.
- **Underestimation is surfaced, never auto-tuned.** Each row with
  `underestimated: true` (whether a genuine underestimation or an
  `unbudgeted_actual` row) logs `run_sync_unit.budget_underestimated` with
  the same structured field vocabulary BudgetGuard's own admission logs use
  (`bucket`, `budget_key`, `estimated_units`, `route_family` — see
  `_observe_estimate` above) plus a `reason` of `"underestimated"` or
  `"unbudgeted_actual"`, so an operator can correlate a calibration warning
  with the run's actual admission decision and tell the two cases apart. The
  comparison never changes an estimator's output or `SYNC_BUDGET_*`
  consumption — it is a pure, read-only join
  (`tests/test_budget_calibration.py::test_estimates_never_mutated_by_comparison`).
- **Drift caveat.** This compares against the **run-time** budget audit —
  recomputed just before the unit's dataset fetch — not the estimate
  BudgetGuard admitted at dispatch time. Env-flag-dependent estimators (e.g.
  Jira's `JIRA_FETCH_WORKLOGS` / `ATLASSIAN_GQL_ENABLED` gating) can in
  principle disagree between admission and execution; `observations`'s
  sibling `budget_comparison_computed_at` records when the run-time audit
  ran so that drift is inspectable. Persisting the admit-time estimate onto
  the unit for a drift-free comparison is a deliberately **deferred**
  follow-up (open decision) — it would add an extra UPDATE per admitted unit
  per dispatch pass, and isn't worth it until calibration data shows material
  drift.
- **Out of scope.** The CLI and backfill runner discard job/task returns
  (`metrics/job_work_items.py`, `backfill/runner.py`), so this comparison only
  surfaces through the unitized sync path (unit result / structured logs /
  the admin API's `result` passthrough) — not CLI or backfill output. A unit
  whose estimator produces **no estimate at all** (`budget_audit` empty) is
  also out of scope — there's no bucket context to attribute even an
  unbudgeted row to.

## Observation store

Shipped in [CHAOS-2758](https://linear.app/fullchaos/issue/CHAOS-2758). Every
`RateLimitException` that defers a sync unit (the worker-level deferral path
above) now attempts one durable, normalized row into the
`provider_rate_limit_observations` Postgres table, in the **same
session** as the unit's `RETRYING` stamp
(`workers/sync_units.py`, the `except RateLimitException` branch of
`run_sync_unit`), attempted only after the CAS confirming that stamp landed
succeeds — so the write never fires for a deferral that didn't actually
happen. The write itself is isolated in its own `SAVEPOINT`
(`session.begin_nested()`), not folded into the outer transaction outright:
the observation store is diagnostic, not load-bearing, so a DB-level failure
persisting it (a rolling deploy where migration `0031` isn't applied to every
node yet, schema drift, or any other insert-time error) rolls back only the
observation attempt and is logged — it must never turn a recoverable
rate-limit deferral into a lost/failed unit.

**Schema:** `id`, `org_id` (indexed), `provider`, `host`, `integration_id`,
`sync_run_id`, `sync_run_unit_id`, `route_family`, `route_family_attribution`,
`dimension`, `retry_after_seconds`, `reset_at`, `reason`, `request_id`,
`observed_at`, plus a composite index `(provider, integration_id,
route_family, observed_at)` for the cooldown lookup a future consumer needs
(migration `0031`, `models/rate_limit_observations.py`). Only normalized
fields are persisted — **never raw provider headers or exception text**
(leak/bloat risk); header capture stays best-effort and provider-local
exactly as documented per-provider above, and `reason` is one of a fixed,
allow-listed category vocabulary (below), never free text.

**Why Postgres, not ClickHouse.** This answers the open question the earlier
CHAOS-2742 budget plan left unresolved ("no new persistence table unless
log/result metadata proves insufficient — durable store TBD"). Every store the
dispatch path already consults transactionally is Postgres:
`SyncDispatchOutbox` (`models/integrations.py`), the per-unit rate-limit
deferral columns on `sync_run_units` (migration `0022`),
`SyncComputeCheckpoint` (migration `0025`), and `BudgetGuard`'s own
reservation runs inside a Postgres advisory-lock transaction
(`sync/budget_guard.py`). The [cooldown-gating consumer](#known-gaps)
(CHAOS-2760) needs to read recent observations from inside that same
advisory-lock transaction — only Postgres supports that join here; ClickHouse
is a separate analytics cluster with no transactional relationship to the
dispatch guard. ClickHouse mirroring is deferred and would only be revisited
if this table becomes an analytics source in its own right, not a dispatch
input.

**Enrichment (worker boundary, not the client).** Per the
[`RateLimitSignal` contract](#how-rate-limits-are-handled), provider clients
leave `integration_id`/`route_family` unset. The worker boundary enriches
both before persisting: `integration_id` comes from the unit row already
loaded for the deferral (`SyncRunUnit.integration_id` — never re-resolved from
mutable `Integration` state), and `route_family` is picked from the
`BudgetEstimate` list **already computed for this unit's dispatch** (never
re-estimated — estimators require credential decryption).

**Route-family attribution is confidence-gated, never guessed.** A unit's
estimate can carry multiple `(route_family, dimension)` pairs, and dimension
alone frequently does **not** disambiguate them: Linear's `work-items`
estimator emits `teams`/`issues`/`cycles`/`comments`/`attachments`/`history`
**all** under `graphql_cost`, and Jira's comment-bearing datasets can emit
both `jira_issue_enrichment` and `jira_comments` under `rest_core`. The
observation writer (`_route_family_and_attribution` in `workers/sync_units.py`)
narrows the unit's estimates by the signal's `dimension` (which call kind
actually hit the limit) and only commits to a `route_family` when the
surviving candidates name exactly **one distinct** family — this also covers
the common case of a single-estimate unit, and estimates that share one
family across dimensions (e.g. GitHub's `commit_stats`). Whenever that check
can't produce a unique family — no budget audit, no dimension match, or more
than one distinct family — `route_family` is `NULL` and
`route_family_attribution` is set to `ambiguous_dimension`; `dimension`
itself is still populated. **The CHAOS-2760 cooldown-gating consumer must
fall back to provider+integration+dimension gating whenever
`route_family_attribution` is set**, rather than trusting a guessed family.

**`reason` is a normalized category, never raw exception text.** Legacy
no-signal raise sites build their message from the provider's raw response
body, which can embed header/body-shaped diagnostic content; persisting that
verbatim for the retention window would be a leak risk. `reason` is always
one of a fixed vocabulary — `primary`, `secondary`, `permission` (reserved for
a future classification site), `complexity`, or `unknown` (no signal, or a
value outside this vocabulary) — never `str(exception)`.

**Retention.** A beat-scheduled task (`prune_rate_limit_observations`,
`workers/sync_reconciler.py`, `workers/config.py`) deletes rows older than
`SYNC_RATE_LIMIT_OBSERVATION_RETENTION_DAYS` (default **14**, env-overridable)
once daily. This is an observation log, not an audit trail — expired rows are
deleted outright, no archival path.

**Cooldown gating reads this table back before dispatch** — see the next
section.

## Cooldown gating

Shipped in [CHAOS-2760](https://linear.app/fullchaos/issue/CHAOS-2760). A 429
observed by one unit today only defers *that* unit
(`workers/sync_units.py:731-836`, the `RateLimitException` handler above) —
siblings of the same provider/integration/route-family would otherwise
dispatch anyway and rediscover the same limit in-worker, each burning a
worker slot and a provider round-trip the observation store already knows is
futile. `BudgetGuard.enforce_run` (`sync/budget_guard.py`) closes that gap:
before admitting this dispatch pass's candidates against budget, it consults
`provider_rate_limit_observations` for an ACTIVE cooldown and defers (or, on
deferral-budget exhaustion, terminally fails) any candidate that matches.

**Match key: `(org_id, provider, integration_id, route_family)` — org-scoped,
and deliberately excluding `credential_fingerprint`/`host`.** This is the
credentials-are-not-capacity invariant applied to gating: rotating an
integration's credential between the observation write and the next dispatch
pass must never let a sibling unit slip past a cooldown that is still active
(`tests/test_budget_guard_cooldown.py::test_credential_rotation_does_not_bypass_cooldown`).
`host` is diagnostic-only on the observation row (multi-host GitHub
Enterprise, etc.) and is never part of the match. A cooldown observed under
one `org_id` never gates another org's units, even if `(provider,
integration_id, route_family)` coincide
(`test_cooldown_never_crosses_org_boundary`).

**Ambiguous-attribution fallback.** Per the [observation store's confidence
gate](#observation-store), a row with `route_family=NULL` and
`route_family_attribution='ambiguous_dimension'` carries a populated
`dimension` instead. The gate never treats that NULL family as matching
every candidate (over-defer) or none (silent under-defer): it falls back to
matching on `(org_id, provider, integration_id, dimension)`, gating every
candidate estimate whose dimension matches, regardless of which specific
family within that dimension actually hit the limit
(`test_ambiguous_attribution_falls_back_to_dimension_gating`).

**Cooldown window.** `coalesce(reset_at, observed_at + retry_after_seconds)`,
falling back to a fixed conservative window
(`RATE_LIMIT_DEFAULT_COUNTDOWN_SECONDS`, 60s) when an observation carried
neither — never treated as "already expired" (under-defer) nor "cooldown
forever" (stuck run). `available_at` for a gated unit is derived from
`plan_rate_limit_deferral`'s own `not_before` (plus the same jitter,
`SYNC_BUDGET_DEFERRAL_JITTER_SECONDS`, the budget-defer path already uses),
**not** the raw cooldown expiry — `not_before` already clamps to the
remaining `RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS` wall-clock budget, so a
far-future `reset_at` cannot park a unit past the point the shared
rate-limit-deferral budget says to terminalize instead
(`test_cooldown_available_at_respects_wall_clock_clamp`,
`test_cooldown_wall_clock_budget_exhausted_terminalizes_rather_than_sleeping_past_clamp`).
The jitter itself is added AFTER `not_before`, so it is clamped a second
time against the wall-clock deadline
(`first_seen_at + RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS`) — jitter stacked on an
already-clamped `not_before` must not itself push `available_at` past the
deadline (review finding, round 2). A unit whose estimates span multiple
route families is deferred if **any** one is cooling down (mirrors the
existing would-defer-any-estimate budget semantics) and, when more than one
matches, waits for the last one to clear.

**One indexed query per dispatch pass, never per unit — plus one cheap
re-check immediately before the claim, which fully defers/terminalizes any
match it catches.** `enforce_run` issues a single
`provider_rate_limit_observations` query, scoped to the dispatch pass's
candidate `(org_id, provider, integration_id)` tuples and a bounded recency
window (`SYNC_RATE_LIMIT_COOLDOWN_LOOKBACK_SECONDS`, default
`RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS` plus the max configured jitter plus a
300s skew margin — deliberately NOT equal to the wall-clock budget alone;
see the termination note below), using the `ws-d`
`(provider, integration_id, route_family, observed_at)` index
(`test_single_observation_query_per_dispatch_pass`). Because `enforce_run`
itself does further DB work after that read (budget admission,
active-consumption re-estimation), a sibling unit's 429 can commit a
brand-new observation in that window — one the snapshot never saw — and
without a second look `_claim_units` would dispatch straight into it. Review
finding, closed: `dispatch_sync_run` calls `BudgetGuard.reconfirm_cooldowns`
— the SAME cheap query and matching logic, reusing the estimates
`enforce_run` already computed (no re-estimation, no credential decryption)
— as the LAST read before the atomic claim. A match here is NOT a bare
exclusion: it goes through the exact same `_apply_cooldown_deferral` /
`_terminalize_rate_limit_exhausted` write path `enforce_run`'s own cooldown
loop uses — full `RETRYING` + `available_at` + `rate_limit_deferrals`
bookkeeping, or termination on budget exhaustion — folding the result into
the claim's excluded-id set AND `next_deferred_at` for the redispatch
re-arm. A bare PLANNED exclusion (the original CHAOS-2760 review-round-1
shape) left the unit with zero deferral-budget bookkeeping and livelocked
the run redispatching on a bare ~60s countdown forever, re-triggering the
same exclusion indefinitely without ever counting toward termination
(review finding, round 2) —
(`test_concurrent_observation_between_enforce_run_and_claim_still_defers_sibling`,
`test_late_reconfirm_match_short_reset_window_defers_with_full_bookkeeping`,
`test_late_reconfirm_match_long_reset_window_clamps_to_wall_clock_deadline`,
`test_reconfirm_cooldowns_terminalizes_exhausted_match_directly`). This is
not full serializability — a commit landing in the residual microsecond gap
between that re-check and the claim's own `UPDATE` could still slip through
— but it collapses the exposure window from "however long budget admission
takes" down to back-to-back statements, consistent with how the rest of the
dispatch path tolerates narrow races via CAS predicates rather than
`SERIALIZABLE` transactions.

**Termination does not depend on re-reading the observation.** A unit
deferred by this gate gets `available_at` clamped to the wall-clock budget,
so it becomes due again at roughly the SAME age its causing observation's
`observed_at` has reached — a lookback window equal to
`RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS` would age the row out of visibility at
almost exactly the instant termination should fire instead, making the
observation invisible right when it matters most (review finding, round 2).
Two-part fix: (a) the lookback window carries slack beyond the bare
wall-clock budget (jitter max + a generous skew margin — see above), so a
row that is merely a little older than the budget stays visible
(`test_cooldown_lookback_window_has_slack_beyond_wall_clock_budget`); (b) as
a belt-and-suspenders backstop that does not depend on that window at all,
every candidate with rate-limit-deferral history is ALSO checked directly
against its own persisted `rate_limit_deferrals`/`rate_limit_first_seen_at`
(`_rate_limit_deferral_exhausted`) — if the shared budget is already spent,
it terminalizes from that state alone, with or without a currently-visible
cooldown observation
(`test_cooldown_observation_aged_past_lookback_terminalizes_from_unit_state`).

**Fail-open on a broken read, including a single malformed row.** Any error
querying the observation store (migration not yet applied on a rolling
node, transient DB error, etc.) is logged and treated as "no active
cooldown" — a diagnostic store must never block dispatch
(`test_cooldown_read_failure_fails_open`). Per-row parsing is fail-open too:
a single malformed row (e.g. a non-finite `retry_after_seconds`, where
`timedelta(seconds=...)` raises `OverflowError`) is skipped and logged
rather than aborting the whole pass and blocking dispatch org-wide
(`test_cooldown_read_survives_malformed_observation_row`). The writer
(`workers/sync_units.py` `_build_rate_limit_observation`) also sanitizes
`retry_after_seconds` before it is ever persisted — rejecting non-finite/
negative values and clamping to `RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS` — so a
corrupt value should never reach the store in the first place; the reader's
guard is defense in depth, not the only line of defense
(`test_observation_sanitizes_non_finite_retry_after_seconds`,
`test_observation_clamps_excessive_retry_after_seconds` in
`tests/test_rate_limit_observations.py`).

**Deferral mechanics reuse the existing budget-guard vocabulary, not a new
unit status.** A gated unit is stamped `RETRYING` with the computed
`available_at`, exactly like `_defer_unit_for_budget`, but with a
**distinct** `result.error_category`: `rate_limit_cooldown_deferred` — so
operators can tell a shared-cooldown hit apart from `budget_deferred`
(budget admission) and `rate_limit` (the in-worker per-unit 429 path).
`BudgetGuardResult.next_deferred_at` folds in the earliest cooldown-deferred
`available_at` exactly like a budget deferral, so the existing
`_schedule_redispatch` re-arm (`workers/sync_units.py`) fires and the run
does not strand until the periodic reconciler scan
(`test_next_deferred_at_rearms_redispatch`).

**Cooldown deferrals count against the existing per-unit rate-limit-deferral
budget.** `plan_rate_limit_deferral`
(`workers/rate_limit_defer.py`, `RATE_LIMIT_MAX_DEFERRALS=10` /
`RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS=2h`) is reused for the
`rate_limit_deferrals`/`rate_limit_first_seen_at` bookkeeping — the SAME
columns and budget the in-worker 429 path already uses. This was a deliberate
CHAOS-2742 recon decision: run-liveness beats optimism, so a chronically
rate-limited provider **terminalizes** (stamped `FAILED`,
`error_category=rate_limit_cooldown_exhausted`) once the shared budget is
spent, rather than holding the run open indefinitely on gate hits that never
even reach the provider
(`test_cooldown_deferral_consumes_rate_limit_budget_and_terminalizes`).

**Herd-on-expiry is naturally paced.** `RETRYING` units never consume
concurrency capacity, and due-`RETRYING` claims are still bounded by
`SYNC_UNIT_CONCURRENCY_PER_BUCKET` per dispatch pass exactly like any other
candidate — an expired cooldown does not cause every sibling to dispatch at
once beyond what DispatchGuard's concurrency cap already allows
(`test_cooldown_expiry_drains_bounded_by_concurrency_cap`).

## Per-provider policy

### GitHub

- **Quota dimensions.** REST core hourly quota, GraphQL point/cost budget, and
  **secondary (abuse) limits** for burst/expansion-heavy traffic. Content/blob
  expansion is tracked separately (`contents_blob`) because tree/blob reads are
  high variance.
- **Headers.** `X-RateLimit-Limit` / `X-RateLimit-Remaining` /
  `X-RateLimit-Reset`, and `Retry-After` on secondary/abuse limits. Only a safe
  diagnostic subset of headers is ever logged — never the token.
- **Retry semantics.** Primary-limit resets can be up to ~1h; those are chunked
  through the worker `not_before` path rather than blocking a worker. Secondary
  limits back off per `Retry-After` (or a 60s default when the body signals
  secondary/abuse with no header) and retry in place.
- **403 triage — non-retryable vs. rate limit.** A 403 is classified before
  deciding retryability (`connectors/github.py` `_classify_github_403`,
  `connectors/utils/graphql.py`, and `providers/github/client.py`
  `_raise_github_exception`):
  - **(a) primary rate limit** — `x-ratelimit-remaining: 0` → `RateLimitException`
    (wait for reset).
  - **(b) secondary/abuse limit** — `Retry-After` present, or the body carries
    GitHub's documented `rate limit` / `abuse` / `secondary` wording →
    `RateLimitException` (back off + retry).
  - **(c) permission / SSO / other 403** — no rate-limit markers → non-retryable
    `AuthenticationException`. This short-circuits the otherwise-wasteful retry
    spin on an unfixable permission/SAML-SSO error.
- **Known gaps.** `pr_social` / `work_item_prs` secondary-limit pressure is
  *estimated*, not measured from real header actuals; see
  [Known gaps](#known-gaps).

#### Route families
<!-- route-families:github -->

| Route family | Dimension(s) | Covers | Confidence |
| --- | --- | --- | --- |
| `repo` | `rest_core` | Repository metadata | high |
| `git` | `rest_core` | Commit listing | medium |
| `commit_stats` | `rest_core`, `contents_blob` | Per-commit file/stat expansion | low |
| `files` | `rest_core`, `contents_blob` | Repository tree/blob reads | low |
| `blame` | `rest_core`, `contents_blob` | Blame expansion (file-count dependent) | low |
| `prs` | `rest_core` | Pull requests / reviews / comments core | medium |
| `pr_social` | `graphql_cost`, `secondary_abuse_risk` | PR timeline/social expansion | medium |
| `cicd` | `rest_core`, `contents_blob` | CI/CD workflow runs + artifact expansion | low |
| `tests` | `rest_core`, `contents_blob` | Test report ingestion | low |
| `deployments` | `rest_core`, `contents_blob` | Deployment ingestion | low |
| `security` | `rest_core` | Security alerts | low |
| `work_items` | `rest_core` | Issues / labels / projects / history / comments | medium |
| `work_item_prs` | `graphql_cost`, `secondary_abuse_risk` | Linked-PR expansion on work items (when `sync_prs`) | medium |

### GitLab

- **Quota dimensions.** Request-count rate limits on the REST API (and, for
  self-managed instances, per-instance/application limits). All GitLab families
  currently reserve against `rest_core`.
- **Headers.** `RateLimit-Limit` / `RateLimit-Remaining` / `RateLimit-Reset`
  and `Retry-After`.
- **Retry semantics.** `429` (and a `403` that carries rate-limit headers) →
  `RateLimitException`, honoring `Retry-After` or deriving from
  `RateLimit-Reset` (`providers/gitlab/client.py`
  `_maybe_raise_gitlab_rate_limit`).
- **403 convention — non-retryable (CHAOS-2383).** For GitLab, **429 is the rate
  limit; a plain 403 is permission / feature-disabled and is non-retryable.**
  When a 403 carries no rate-limit headers it means the feature (e.g. Feature
  Flags) is disabled for the project or the token lacks Developer+ scope; it is
  re-raised as a non-retryable `AuthenticationException` so the retry decorator
  does not spin on an unfixable error (`connectors/gitlab.py`, mirroring
  GitHub's permission-403 handling).
- **Known gaps.** GitLab feature-flag budgeting shares the `project` REST-core
  family; dedicated GitLab feature-flag budget families are a follow-up.

#### Route families
<!-- route-families:gitlab -->

| Route family | Dimension(s) | Covers | Confidence |
| --- | --- | --- | --- |
| `project` | `rest_core` | Project metadata, commits, files/blame, security, feature-flags | high–low |
| `merge_requests` | `rest_core` | Merge request iterators (pagination-heavy) | medium |
| `notes` | `rest_core` | MR/issue note + discussion expansion | low |
| `pipelines` | `rest_core` | CI/CD pipeline + job expansion | low |
| `issues` | `rest_core` | Issue iterator + per-issue events | medium |
| `milestones` | `rest_core` | Project/group milestone iterators | medium |
| `epics` | `rest_core` | Group epic expansion (premium APIs) | low |

### Jira

- **Quota dimensions.** Atlassian Cloud enforces per-tenant request-cost limits;
  work-item **listing** goes through the separately-limited REST `search/jql`
  endpoint (`search` dimension), while enrichment (changelog/comments/sprints)
  uses `rest_core`. AGG GraphQL enrichment, when enabled, uses `graphql_cost`.
- **Headers.** `Retry-After` on `429` (authoritative delay). `X-RateLimit-Reset`
  is also sent, but as an **ISO 8601 timestamp** (e.g. `2025-10-08T15:00:00Z`),
  not epoch seconds/milliseconds like GitHub/GitLab/Linear — verified against
  Atlassian's Cloud rate-limiting docs
  (`developer.atlassian.com/cloud/jira/platform/rate-limiting/`) for CHAOS-2758
  (previously unverified; `RateLimitSignal.reset_at_from_iso8601` parses it).
- **Retry semantics.** On `429`, Jira retries **in place** honoring `Retry-After`
  (`providers/jira/client.py` `_request_json`, `max_retries_429=3` → 4 attempts),
  then raises `RateLimitException` when exhausted so the worker deferral path
  takes over.
- **Non-retryable auth cases.** Jira has **no dedicated 403 rate-limit vs.
  permission classification** — a 403 surfaces through the shared HTTP error path
  (`raise_for_status`). This is a known asymmetry vs. GitHub/GitLab; unified 403
  handling is part of the [target contract](#target-contracts-chaos-2742).
- **Gating.** Worklog and AGG-GraphQL route families only appear when
  `JIRA_FETCH_WORKLOGS` / `ATLASSIAN_GQL_ENABLED` are set (`providers/jira/budget.py`).

#### Route families
<!-- route-families:jira -->

| Route family | Dimension(s) | Covers | Confidence |
| --- | --- | --- | --- |
| `jira_metadata` | `rest_core` | Project/label metadata | high |
| `jira_jql` | `search` | Work-item listing via REST `search/jql` pagination | medium |
| `jira_issue_enrichment` | `rest_core` | Per-issue changelog / comment / sprint enrichment | medium |
| `jira_comments` | `rest_core` | Comment pagination (activity-dependent) | low |
| `jira_worklogs` | `rest_core` | Per-issue worklog expansion (`JIRA_FETCH_WORKLOGS`) | low |
| `jira_gql_enrichment` | `graphql_cost` | AGG GraphQL enrichment (`ATLASSIAN_GQL_ENABLED`) | medium |

### Linear

- **Quota dimensions.** Request-count budget **and** a distinct GraphQL
  **complexity** budget (Linear's ~10,000-complexity limit). All Linear families
  reserve against `graphql_cost`.
- **Headers.** `X-RateLimit-Requests-Limit` / `X-RateLimit-Requests-Remaining` /
  `X-RateLimit-Requests-Reset` (parsed in `providers/linear/client.py`
  `_update_rate_limit`). The client **proactively throttles**: when remaining
  ≤ 5 it sleeps until the reset before issuing the next request.
- **Retry semantics.** HTTP `429` → back off (server `Retry-After` when present,
  else gate backoff) and retry in place up to `DEFAULT_MAX_ATTEMPTS = 5`, then
  raise `LinearRateLimitError` (a `RateLimitException`).
- **Non-retryable — complexity rejection.** Linear returns complexity-limit
  rejections as **HTTP 400 GraphQL errors**, surfaced as
  `LinearComplexityLimitError`. This is **not retryable**: the query itself must
  be restructured (smaller nested page sizes), so it never enters the 429
  backoff/deferral loop.
- **Known gaps.** Real request/complexity **actuals** are not yet recorded per
  route family; estimates are static. See [Known gaps](#known-gaps).

#### Route families
<!-- route-families:linear -->

| Route family | Dimension(s) | Covers | Confidence |
| --- | --- | --- | --- |
| `teams` | `graphql_cost` | Team listing | medium |
| `issues` | `graphql_cost` | Issue pages (nested labels/project/comments/attachments/history) | low |
| `cycles` | `graphql_cost` | Cycle expansion | low |
| `comments` | `graphql_cost` | Comment pages | low |
| `attachments` | `graphql_cost` | Attachment edges | low |
| `history` | `graphql_cost` | Issue history edges | low |
| `team_members` | `graphql_cost` | Team member edges (large teams paginate) | medium |
| `projects` | `graphql_cost` | Project listing | medium |

### LaunchDarkly

- **Quota dimensions.** Global and **route-level** REST limits. The audit-log
  endpoint hard-caps pages at 20 entries. Code-reference reads carry a
  `secondary_abuse_risk` reservation in addition to `rest_core`.
- **Headers.** `X-RateLimit-Route-Remaining` (per-route remaining budget) and
  `Retry-After` on `429` (`connectors/launchdarkly.py`).
- **Retry semantics.** `429` and `5xx` are retried with exponential backoff
  (honoring `Retry-After`) up to `max_retries = 5`. On exhaustion the terminal
  type differs: a `429` raises `RateLimitException` (→ worker deferral) while a
  `5xx` raises `APIException` (a normal failure, `connectors/launchdarkly.py`
  `_raise_for_status`). A low `X-RateLimit-Route-Remaining` (< 5) is currently
  only **logged as a warning**, not fed into the deferral/cooldown machinery —
  see [Known gaps](#known-gaps).
- **Non-retryable auth cases.** `401` → `AuthenticationException`; `403` →
  `APIException` (forbidden). LaunchDarkly does **not** distinguish a
  permission-403 from a rate-limit case the way GitHub/GitLab do.
- **Frozen path caveat.** Flag and audit-log fetches still live in the **frozen
  legacy connector** (`connectors/launchdarkly.py`); only the code-reference
  client, code-ref helpers, and the budget estimator are canonical under
  `providers/launchdarkly/`. See
  [LaunchDarkly sync budgeting](../architecture/launchdarkly-sync-budgeting.md).
- **Documented vs. emitted families.** The estimator currently emits `flags`,
  `audit_log`, and `code_refs`. `projects`, `segments`, and `members` are
  **modeled route families** (`LAUNCHDARKLY_BUDGET_ROUTE_FAMILIES` in
  `providers/launchdarkly/budget.py`) that the current `feature-flags` estimator
  does not yet reserve; they are documented here for completeness.

#### Route families
<!-- route-families:launchdarkly -->

| Route family | Dimension(s) | Covers | Confidence |
| --- | --- | --- | --- |
| `flags` | `rest_core` | `GET /api/v2/flags/{projectKey}` | medium |
| `audit_log` | `rest_core` | `GET`/`POST /api/v2/auditlog` (20-entry page cap) | low |
| `code_refs` | `rest_core`, `secondary_abuse_risk` | `GET /api/v2/code-refs/repositories` | medium–low |
| `projects` | `rest_core` | `GET /api/v2/projects` + environments (modeled, not yet emitted) | medium |
| `segments` | `rest_core` | `GET /api/v2/segments/{projectKey}/{environmentKey}` (modeled) | low |
| `members` | `rest_core` | `GET /api/v2/members` (modeled) | low |

## Known gaps

These are the instrumentation/coverage gaps the [CHAOS-2742](https://linear.app/fullchaos/issue/CHAOS-2742)
epic exists to close. They are documented as **current reality**, not defects to
paper over:

- **Calibration only covers instrumented route families.** [CHAOS-2759](https://linear.app/fullchaos/issue/CHAOS-2759)
  attaches a `budget_comparison` (see
  [Actual-vs-estimated calibration](#actual-vs-estimated-calibration-chaos-2759)
  above) wherever CHAOS-2754's recorder actually drained actuals for a
  route_family, but the recorder only records where a client calls it.
  GitHub `pr_social`/`work_item_prs` secondary pressure, Linear
  request/complexity actuals, and LaunchDarkly route-remaining are still
  **uninstrumented**, so those families never produce a comparison row.
  There is also no cross-run aggregation/dashboard yet — calibration today is
  visible per-unit (result + structured log), not rolled up over time.
- **Signal handling is not yet provider-neutral.** There are two unrelated
  `RateLimitException` classes — `dev_health_ops.exceptions.RateLimitException`
  (canonical providers, caught by the worker deferral) and
  `connectors.base.RateLimitException` (frozen legacy connectors) — and neither
  carries a normalized `route_family` / `dimension` / `reason` / `request_id`.
  (Target: [CHAOS-2753](https://linear.app/fullchaos/issue/CHAOS-2753).)
- **Frozen `connectors/` path.** LaunchDarkly flag/audit-log fetches and several
  GitHub/GitLab paths remain under the frozen `connectors/` tree. No new code may
  be added there (see [`AGENTS.md`](../../AGENTS.md)); rate-limit/actuals
  instrumentation for those datasets lands as the fetch moves to
  `providers/<provider>/`.

## Target contracts (CHAOS-2742)

The following are **not shipped**; each lands in its own sub-issue PR, which will
append its section here in the same changeset (per `AGENTS.md`
"behavior docs updated with code"). Do not treat these as current behavior.

- **`RateLimitSignal` normalization — [CHAOS-2753](https://linear.app/fullchaos/issue/CHAOS-2753).**
  A provider-neutral
  `RateLimitSignal(provider, host, integration_id, route_family, dimension,
  retry_after_seconds, reset_at, reason, request_id)` that every provider emits
  and the deferral machinery consumes, preserving GitHub's
  primary/secondary/permission distinction and extending comparable handling to
  Linear, Jira, GitLab, and LaunchDarkly.
- **Shared actuals recorder — [CHAOS-2754](https://linear.app/fullchaos/issue/CHAOS-2754).**
  A shared usage recorder with route-family keying and GitLab/Jira/Linear drains.
- **Run-auth freeze — [CHAOS-2755](https://linear.app/fullchaos/issue/CHAOS-2755).**
  Stamp `credential_id`/version onto `SyncRun` at plan time; bootstrap uses the
  run-stamped auth context, not mutable integration state. Determinism only —
  **never** unit-level credential selection for capacity.
## References

- Epic: [CHAOS-2742](https://linear.app/fullchaos/issue/CHAOS-2742) — Harden sync
  budget and provider rate-limit handling without credential capacity.
- [Sync Unit Model](../architecture/sync-unit-model.md) — how a run decomposes
  into units and why reference data belongs on a different axis.
- [LaunchDarkly sync budgeting](../architecture/launchdarkly-sync-budgeting.md) — LD estimator
  contract and operator budget defaults.
- [Durable Dispatch Outbox](../architecture/dispatch-outbox.md),
  [Connector Inventory](../ops/connector-inventory.md),
  [Workers](../ops/workers.md).
- Code: `providers/<provider>/client.py` (fetch + rate-limit),
  `providers/<provider>/budget.py` (estimators),
  `sync/budget_guard.py` (bucket keys + reservation),
  `workers/rate_limit_defer.py` (deferral budgets),
  `connectors/utils/rate_limit_queue.py` (shared gate).

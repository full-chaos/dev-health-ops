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
- **Exactly one place a credential attaches to sync work.**
  `SyncConfiguration` (the admin API's sync-config row) used to carry its own
  `credential_id` column — a second, *unfrozen* copy of the same selection,
  writable independently of `Integration.credential_id` and never read by
  planning, budgeting, or auth resolution. It was removed
  ([CHAOS-2762](https://linear.app/fullchaos/issue/CHAOS-2762)); the admin API
  still accepts/returns `credential_id` on sync-config payloads, but it is now
  resolved live from the linked `Integration.credential_id` on every read (see
  `api/admin/routers/sync.py`), so it can never drift from the surface auth
  resolution actually uses. **`SyncConfigResponse.credential_id` is a
  compatibility read-through alias, not a design target** — it exists only
  because the web admin UI still reads a sync config's credential off the
  sync-config response. It is a candidate for removal once web reads
  credentials at the `Integration` level directly instead.
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
  pick different credentials per unit for throughput. **Shipped**
  ([CHAOS-2755](https://linear.app/fullchaos/issue/CHAOS-2755), migration
  `0030`): `sync_runs` carries `credential_id` / `credential_fingerprint` /
  `auth_source`, stamped once at plan time by `sync/planner.py`'s
  `_resolve_credential_stamp` and never re-resolved mid-run —
  `workers/sync_bootstrap.py` reads the frozen stamp, falling back to the
  mutable `Integration.credential_id` path only for legacy/in-flight-at-deploy
  runs whose stamp is `NULL`. Enforced by `tests/test_sync_run_auth_freeze.py`
  and `tests/test_sync_planner.py`.

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
| GitHub code client (canonical: repo metadata/listing, git/commit_stats/security/deployments REST, files/blame GraphQL) | 5 (`InstrumentedRESTCore` default) | `providers/github/code_client.py` + `providers/github/graphql.py` |
| GitLab legacy connector shell | n/a (code-dataset fetch methods retired) | `connectors/gitlab.py` |
| GitLab code client (canonical: security/pipelines/deployments/tests/commits/files/blame/merge_requests/notes) | 5 (`InstrumentedRESTCore` default) | `providers/gitlab/code_client.py` |
| GitLab feature-flags (canonical) | 5 (`max_retries=5`) | `providers/gitlab/feature_flags.py` |
   | Jira client (JQL + enrichment) | 4 (`max_retries_429=3` → `+1` initial) | `providers/jira/client.py` |
   | Jira Atlassian REST compat | 5 (`RESTClient(max_retries=5)`) | `connectors/utils/rest.py` |
   | Linear | 5 (`DEFAULT_MAX_ATTEMPTS`) | `providers/linear/client.py` |
   | LaunchDarkly (flags/audit_log) | 5 (`max_retries=5`) | `providers/launchdarkly/client.py` |
   | LaunchDarkly (code_refs) | 5 (`max_retries=5`) | `providers/launchdarkly/code_refs.py` |

2. **`RateLimitException` as the carrier.** When in-place retries are exhausted
   (or a 429/permission-vs-limit decision is made), the **canonical provider
   clients** (`providers/<provider>/client.py`) raise
   `dev_health_ops.exceptions.RateLimitException` carrying a normalized
   `RateLimitSignal` (`route_family`, `dimension`,
   `reason`, `retry_after_seconds`, `reset_at`, `request_id`), and the worker
   deferral (`workers/sync_units.py`) catches exactly that type. The frozen
   legacy connectors (`connectors/github.py`, `connectors/gitlab.py`) raise
   `connectors.base.RateLimitException`, which **subclasses the canonical root**
   (shipped as [CHAOS-2753](https://linear.app/fullchaos/issue/CHAOS-2753),
   ops#1111) — so a rate limit raised deep in a legacy connector reaches the
   same deferral branch and carries the same signal, one unified hierarchy.

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
  run produces no row** (remaining GitHub/GitLab code datasets still on the frozen
  connector path, a `contents_blob`/`secondary_abuse_risk` dimension sharing
  its single REST call with an already-recorded `rest_core` dimension, …) —
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

## Legacy error-text columns (`sync_run_units.error` and siblings)

Shipped in [CHAOS-2766](https://linear.app/fullchaos/issue/CHAOS-2766). Live
verification of CHAOS-2758 above (evidence comment on CHAOS-2742) found that
the *observation store*'s `reason` allow-list did its job, but a
provider exception whose message embeds an `Authorization` header (e.g.
`403 rate limited -- Authorization: Bearer ghp_FAKE...`) still landed
**verbatim** in the pre-existing, free-form `sync_run_units.error` column —
these legacy columns predate CHAOS-2742 and were never brought under the same
discipline.

**Redaction, not an allow-list — deliberately.** `reason` above is a closed,
normalized enum with no diagnostic-text mandate, so allow-listing a fixed
vocabulary is correct there. `sync_run_units.error` and its siblings
(`sync_runs.error`, `sync_run_reference_discoveries.error`,
`sync_dispatch_outbox.last_error`) are free-form, operator-facing
diagnostics — the entire point of persisting them is to help debug a failed
sync without re-running it. Collapsing them to a category string would defeat
that purpose. Instead, `dev_health_ops.sync.error_sanitize.sanitize_error_text`
(one shared helper, imported at every persistence site — no per-site regexes)
strips/masks only the specific credential-shaped substrings that must never
reach the database:

- `Authorization` / `Proxy-Authorization` headers, any scheme
- Bearer tokens, with or without a leading header name
- HTTP Basic auth base64 blobs
- Provider PAT/bot-token prefixes: `ghp_`/`gho_`/`ghu_`/`ghs_`/`ghr_`,
  `github_pat_`, `glpat-`, `xoxb-`/`xoxp-`/`xoxa-`/`xoxr-`
- `token=`/`private_token=`/`api_key=`/`access_token=`/`secret=`-style
  key/value pairs
- A credential embedded in a URL's userinfo component
  (`scheme://user:pass@host`, e.g. `redis://:password@host:6379/0`,
  `amqp://user:pass@host`) — added for a codex review finding on this PR: a
  Celery/broker enqueue-failure exception can embed the configured
  broker/result-backend connection string, and that shape evades every
  pattern above

Everything else in the message — status codes, provider names, retry
context — passes through unchanged. A redacted match is replaced with the
`REDACTION_MARKER` sentinel (`"[REDACTED]"`), and when the input is an
exception object (the common case) the output is prefixed with the exception
**class name** (`"RateLimitException: 403 rate limited -- [REDACTED]"`), so
even a bare `raise SomeError()` with no message still persists something
diagnostically useful. Redaction always runs *before* the length cap
(`DEFAULT_MAX_ERROR_TEXT_LENGTH`, 4000 chars — `sync_dispatch_outbox.last_error`
keeps its pre-existing, tighter 2000-char cap) so truncation can never split a
credential in half and leave a partial value exposed.

**Enforced by a source-level guard, not review discipline.** There is no
type-system way to require "this exception must be sanitized before it
reaches a DB column." `tests/test_error_sanitize_guard.py` AST-walks the
worker/sync modules that own these columns and fails if any raw
`str(exc)`-shaped call or bare `f"...{exc}"` interpolation exists outside a
`logger.*(...)` call (diagnostic logs are a separate risk surface, out of
this ticket's scope) — mirroring the doc-drift-guard precedent in
`tests/test_rate_limit_policy_doc.py` (CHAOS-2757). Two internal
classification helpers (`_classify_error` in `workers/sync_units.py`,
`_is_retryable_discovery_error` in `workers/reference_discovery.py`) are
exempted: they lowercase `str(exc)` only to pattern-match it against a fixed,
curated vocabulary and return a category — the raw text itself is discarded,
never persisted, the same shape as the `reason` allow-list above.

**Out of scope.** Diagnostic *logs* (`logger.warning(..., extra={"error":
str(exc)})`) are not touched — they are a different risk surface (a log
aggregator, not this Postgres table) and were not part of the live-verified
case. `MetricCheckpoint.error` (`metrics/checkpoints.py`) and
`InvestmentBatchJob.error` (`work_graph/investment/batch_store.py`) are also
out of scope: they belong to the LLM-categorization and metrics-checkpoint
subsystems, not the sync-run/unit execution ledger this epic hardens, and
their exception provenance (classification/LLM-batch errors, not raw
provider-client HTTP exceptions) differs from the live-verified case.

**`JobRun.error` / `BackfillJob.error_message` (manual-trigger enqueue
path).** Added for a codex review finding on this PR: `sync/execution_trigger.py`
`mark_job_run_failed` and `api/admin/routers/sync.py` `_mark_backfill_job_failed`
sanitize **at the sink**, not their callers — a Celery/broker enqueue-failure
exception raised from `dispatch_sync_run.apply_async(...)` in the manual-trigger
and backfill admin endpoints can embed the broker/result-backend URL
(credentials included), and both columns surface through admin job-history
responses. Both functions now accept `BaseException | str` and call
`sanitize_error_text` internally, so a future caller cannot bypass it by
passing an already-formatted string; the corresponding `HTTPException`
`detail` on the same enqueue-failure path is sanitized the same way. The
CHAOS-2766 guard test (`tests/test_error_sanitize_guard.py`) covers this file
too, scoped to only the functions that touch these columns — the router file
has many unrelated `except ... str(exc)` sites for ordinary HTTP validation
errors that are out of this guard's scope.

**Copied-column propagation (`sync_observers_for_terminal_sync_run`,
`finalize_sync_run`'s `stamp_sync_run_canonical_config` call).** Added for a
codex review finding on this PR, round 2: two sites in `workers/sync_units.py`
read `SyncRun.error` and assign it VERBATIM into another durable column via a
plain variable (`error = ... (run.error or "...")`, `run_error = ... (run.error
or "...")`) rather than a `str(exc)`/`f"...{exc}"` expression -- a shape the
AST guard, which only recognizes exception-stringification forms, cannot see.
`sync_observers_for_terminal_sync_run` copies it into both
`BackfillJob.error_message` and `JobRun.error`; `finalize_sync_run` copies it
into `SyncConfiguration.last_sync_error` via `stamp_sync_run_canonical_config`.
Both now wrap the copied value in `sanitize_error_text` too (idempotent and
cheap on already-sanitized text) -- this matters even with every write site
above already covered, because `sync_observers_for_terminal_sync_run` also
runs from the reconciler's stale-observer repair path
(`reconcile_sync_dispatch`), which can touch a `SyncRun.error` value written
by an OLDER code path or before this ticket shipped. Since AST-detecting
arbitrary variable-to-column flows (as opposed to a fixed `str(exc)` shape)
is not a tractable static check, this is covered by a targeted regression
test instead
(`test_finalize_sync_run_sanitizes_copied_run_error_into_observer_columns` in
`tests/test_sync_units.py`) rather than an AST guard extension.

**Pre-existing rows.** Any `sync_run_units.error`/`sync_runs.error`/etc. row
written before this column was brought under `sanitize_error_text` keeps its
raw text at rest until the row is next overwritten by a sanitizing write path
(or copied through one of the sinks above, which now re-sanitizes on the way
through). [CHAOS-2780](https://linear.app/fullchaos/issue/CHAOS-2780) closes
that gap: `dev-hops maintenance scrub-error-text` applies `sanitize_error_text`
to every already-persisted row across all ten in-scope columns (the eight
listed above plus `integration_credentials.last_test_error`, whose write path
CHAOS-2780 also sanitizes at the source, and
`sync_configurations.last_sync_stats`'s `'error'` JSON key). Dry-run by
default; `--apply` mutates, `--org` optionally scopes to one organization,
and re-running after a full apply reports zero changes (the scrub, like
`sanitize_error_text` itself, is idempotent). See
`src/dev_health_ops/maintenance/scrub_error_text.py` for the column registry
and compare-and-swap update semantics.

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

**`rate_limit_deferrals`/`rate_limit_first_seen_at` are cleared at episode
boundaries — not left to go stale.** Checking a unit's own persisted
deferral state directly (the belt-and-suspenders backstop above) only works
if that state actually reflects the CURRENT episode. Review finding, round
3: these columns were never cleared once a unit left a rate-limit episode —
a successful claim of a due `RETRYING` unit doesn't touch them, and neither
did any non-rate-limit retry path (expired lease, soft timeout, generic
worker-lost retry). Sequence that broke: a unit takes one rate-limit
deferral, is later claimed fine (the provider recovers), then loses its
worker (lease expiry) or soft-times-out for a totally unrelated reason; once
the stale `first_seen_at` from the OLD episode is more than
`RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS` old, `_rate_limit_deferral_exhausted`
would fire against it and wrongly terminalize healthy, unrelated work — a
silent data gap. Fixed at the root: every transition to `SUCCESS` and every
`RETRYING` stamp for a reason OTHER than a rate limit now explicitly clears
both columns to `0`/`NULL`; only a rate-limit-caused deferral (the in-worker
429 path, or this gate's own cooldown deferral) keeps/accumulates them,
because that is the episode continuing. Defense in depth on top:
`_rate_limit_deferral_exhausted` ALSO requires the unit's own
most-recently-recorded `result.error_category` to be rate-limit-related
(`rate_limit` or `rate_limit_cooldown_deferred`) before it can fire at all —
a stale row that somehow survives a missed clear site still cannot
terminalize healthy work, because its last recorded cause would show
something else
(`test_stale_rate_limit_columns_without_rate_limit_error_category_do_not_terminalize`).
Gating alone would not have been sufficient as the root fix, though: a
genuinely NEW rate-limit episode starting while a stale `first_seen_at`
persists would still terminalize prematurely against the old clock, which
is exactly why the clear-at-episode-boundary fix is the primary mechanism
and the `error_category` check is the backstop, not the other way around.

Every `SyncRunUnit` transition to `RETRYING` or a terminal state across
`workers/sync_units.py`, `sync/budget_guard.py`, and
`workers/sync_reconciler.py` was swept for an explicit keep-or-clear
decision:

| Site | Transition | Cause | Decision |
| --- | --- | --- | --- |
| `run_sync_unit` success stamp (`sync_units.py`) | `RUNNING`→`SUCCESS` | episode resolved | **clear** |
| In-worker `RateLimitException` deferral (`sync_units.py`) | `RUNNING`→`RETRYING` | rate limit | **keep/accumulate** |
| Soft-timeout retry (`_stamp_sync_unit_soft_timeout`, `sync_units.py`) | `RUNNING`→`RETRYING` | unrelated | **clear** |
| Soft-timeout exhausted (`sync_units.py`) | `RUNNING`→`FAILED` | unrelated, terminal | leave (informational) |
| Generic failure (`_stamp_sync_unit_failed`, `sync_units.py`) | `RUNNING`→`FAILED` | unrelated/terminal | leave (informational) |
| Total-cap stale-dispatch fail (`sync_units.py`) | `DISPATCHING`→`FAILED` | unrelated, terminal | leave (informational) |
| `PLANNED` claim (`_claim_units`, `sync_units.py`) | `PLANNED`→`DISPATCHING` | n/a (always fresh) | no change needed |
| Due-`RETRYING` claim (`_claim_units`, `sync_units.py`) | `RETRYING`→`DISPATCHING` | unknown (batch claim, any prior reason) | **no change** — must NOT clear here: this fires before the attempt's outcome is known, so clearing would prematurely reset an ONGOING rate-limit episode's counter before the redispatch even runs; the correct clear point is the transition AFTER this attempt resolves (`SUCCESS`, or a specific non-rate-limit retry stamp) |
| Lease acquire (`DISPATCHING`→`RUNNING`, `sync_units.py`) | transient | n/a | no change needed (outcome still unknown) |
| Expired-lease retry (`sync_reconciler.py`) | `RUNNING`→`RETRYING` | unrelated | **clear** |
| Expired-lease exhausted (`sync_reconciler.py`) | `RUNNING`→`FAILED` | unrelated, terminal | leave (informational) |
| Budget-guard deferral (`_defer_unit_for_budget`, `budget_guard.py`) | any→`RETRYING` | unrelated (budget capacity, not a provider rate limit) | **clear** |
| Cooldown-gate deferral (`_apply_cooldown_deferral`, `budget_guard.py`) | any→`RETRYING` | rate limit | **keep/accumulate** |
| Wall-clock/cooldown exhaustion terminalize (`_terminalize_rate_limit_exhausted`, `budget_guard.py`) | any→`FAILED` | rate limit, terminal | leave (informational — the count that caused termination) |

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

## Instrumented transport core (CHAOS-2773 CS1)

[CHAOS-2773](https://linear.app/fullchaos/issue/CHAOS-2773) migrates the
frozen GitHub/GitLab code-dataset fetch paths (`git`, `commit_stats`,
`files`, `blame`, `cicd`, `tests`, `deployments`, `security` and their
GitLab equivalents — see the "Frozen `connectors/` path" entry under
[Known gaps](#known-gaps)) off `connectors/` onto canonical, instrumented
`providers/github/` / `providers/gitlab/` clients. **CS1 lands the shared
foundation every per-family changeset (CS3+) builds on; it does NOT itself
migrate any dataset** — the frozen connector path is unchanged and remains
the live fetch path for every code-dataset family until its own changeset
cuts over.

**`providers/_http.py::InstrumentedRESTCore`** is the one httpx transport
primitive every canonical code client composes (never inherits) — mirroring
`providers/launchdarkly/client.py` (the sanctioned template) and the
"will be folded into the shared core once it lands" precedent already noted
on `providers/gitlab/feature_flags.py`:

- `request(method, path, *, operation, params, headers)` records ONE
  `UsageRecorder.record` per PHYSICAL HTTP round trip, including retried
  429/5xx attempts — never one record per logical call. The recorded
  diagnostic-header set is **provider-configurable**
  (`diagnostic_header_names` constructor param): code clients pass
  `GITHUB_DIAGNOSTIC_HEADER_NAMES` / `GITLAB_DIAGNOSTIC_HEADER_NAMES`, which
  are pinned by parity tests
  (`tests/providers/test_http_core.py::TestDiagnosticHeaderParity`) to match
  the existing work-client recorders' header sets EXACTLY (request IDs,
  `x-ratelimit-used`/`-resource`, `x-accepted-github-permissions`,
  `x-request-id`/`x-runtime` — never the token), so observations from the
  core are shape-compatible with today's `latest_headers` diagnostics.
- Retry/backoff and terminal-429 delay resolution go through the EXISTING
  shared `providers/_ratelimit.py` helpers — `Retry-After` first
  (delta-seconds or HTTP-date), **falling back to the provider's
  epoch-seconds reset header** (`reset_header_name`:
  `X-RateLimit-Reset`/`RateLimit-Reset`) via
  `resolve_retry_after_seconds(...)`, the provider-parameterized
  generalization of #1142's `gitlab_resolve_retry_after_seconds` (which now
  delegates to it — still exactly one implementation). This matters at the
  worker boundary: `workers/sync_units.py` plans a deferral's `not_before`
  from `exc.retry_after_seconds`, not `signal.reset_at`, so a reset-only
  429 must carry the derived delay rather than `None` (which would wake the
  unit on the 60s default instead of the provider's real reset window). On
  retry exhaustion of a 429 the core raises the canonical
  `dev_health_ops.exceptions.RateLimitException` carrying a CHAOS-2753
  `RateLimitSignal` built via `RateLimitSignal.reset_at_from_epoch_seconds`
  (GitHub/GitLab both report epoch SECONDS, unlike LaunchDarkly's epoch
  milliseconds) — `integration_id`/`route_family` are left `None`
  (worker-boundary enrichment, unchanged).
- **Explicit redirect policy + unauthenticated follow-up hop.** The
  underlying httpx client never follows redirects, so the core refuses to
  let a 3xx masquerade as success: by default any 3xx is a terminal
  `APIException` naming the redirect target (the physical attempt is still
  recorded — no unaccounted hops). The two-hop pattern for GitHub's
  artifact-zip download (`connectors/github.py::download_artifact_zip`;
  ports in CS5): (1) a per-call `raw_redirect=True` opt-in returns the 3xx
  response itself (recorded, never retried); (2)
  `request_unauthenticated(location, operation=...)` follows the absolute
  `Location` through a SECOND, headerless client owned by the core — httpx
  sends a client's default headers even on absolute-URL requests and
  rejects `Authorization: None`, so a separate no-default-headers client is
  the only clean way to keep the provider token off the pre-signed
  blob/CDN URL. The hop is still recorded through the same `UsageRecorder`,
  and the response comes back unclassified (the pre-signed host is a
  different error domain — the artifact contract's 404/410 →
  convenience-empty decision belongs to the CS5 code client, not the
  transport). Pinned end-to-end by
  `tests/providers/test_http_core.py::TestUnauthenticatedFollow` (second
  hop carries no `Authorization`, both hops recorded one apiece).
- `paginate_link_header` (GitHub's RFC 5988 `Link` header) and
  `paginate_page_param` (GitLab's `page`/`per_page` + `X-Next-Page`) both
  carry hard page caps as a defensive backstop against a misbehaving/looping
  cursor.
- Base-URL joining reuses the existing `GitHubAuth.base_url` /
  `GitLabAuth.base_url` string fields (`github_rest_base_url` /
  `gitlab_rest_base_url`) — GHE (`https://ghe.example.com/api/v3`, joined
  as-is) and self-hosted GitLab (`GITLAB_URL` + `/api/v4`) both preserved.
- Credential resolution stays entirely ABOVE this module — it never touches
  auth, only the headers a caller already resolved.
- **Composition, not inheritance.** The core's own status-code
  classification is intentionally generic (429/5xx retry; 401/403/404/other
  4xx map to the obvious exception). GitHub's 403 triage and GitLab's
  header-qualified-403 semantics are NOT baked in here — a future code
  client wires them in via the `is_retryable_status` / `classify_error`
  extension points, reusing the shared classifiers below.

**Shared 403/429 classifiers** (extracted so a REST work client and its
future httpx code-client twin never carry a second copy of the same
triage):

- `providers/github/ratelimit.py::classify_github_403` — primary
  (`x-ratelimit-remaining: 0`) vs. secondary/abuse (`Retry-After` header or
  documented body wording) vs. permission/SSO (not a rate limit).
  `providers/github/client.py::GitHubWorkClient._raise_github_exception` now
  delegates to it for the 403 branch; behavior is pinned by porting the
  relevant cases from `tests/test_github_403_observability.py` into
  `tests/providers/test_github_ratelimit_classifier.py`.
- `providers/gitlab/ratelimit.py::classify_gitlab_status` — 429-primary /
  header-qualified-403-secondary, built directly on the
  `providers/_ratelimit.py` primitives (`gitlab_403_is_rate_limited` /
  `gitlab_resolve_retry_after_seconds`, #1142).
  `providers/gitlab/client.py::_maybe_raise_gitlab_rate_limit` now delegates
  to it, so exactly one predicate/delay implementation exists for GitLab
  (the #1142 feature-flags client already called the `_ratelimit.py`
  primitives directly; this closes the remaining gap in the work client).

**Resolver explicit-prefix short-circuit**
(`providers/usage.py::OperationResolver.resolve`). The substring marker scan
alone cannot carry a family-prefixed operation-label scheme: families are
scanned in order and the FIRST marker hit wins, and GitLab's `project`
family already registers broad markers (`"/projects/:id"`, `"/projects/"`)
that would swallow, e.g., a `pipelines:GET /projects/:id/pipelines` label
before `pipelines` is ever consulted. Fixed structurally: before the marker
scan runs, an operation label of the form `"<registered-family>:..."`
resolves DIRECTLY to that family. Unprefixed labels — every existing
work-client label today — take the marker-scan path completely UNCHANGED,
pinned by an exhaustive label→family resolver test
(`tests/providers/test_usage_resolver_prefix.py`) that (a) asserts every
current GitHub/GitLab work-client operation label resolves EXACTLY as it did
before this change, (b) asserts representative prefixed labels per
registered family resolve directly to that family, and (c) asserts no
existing work-client label accidentally starts with a registered
`"<family>:"` prefix (the false-trigger regression guard). Code clients
built on this core (CS3+) are expected to author both the label and its
family prefix themselves, so resolution for their traffic is deterministic
by construction rather than by marker tuning.

### Usage drain wiring + first re-bucketing (CHAOS-2773 CS2)

CS1 landed the shared transport/resolver plumbing but migrated no traffic.
[CHAOS-2803](https://linear.app/fullchaos/issue/CHAOS-2803) (CS2) is the
proof-of-pipe that the plumbing actually reaches a `budget_comparison` row:

- **`usage_sink` contract.** `processors/dataset_adapters.py::_run_github_dataset`
  / `_run_gitlab_dataset` own a `usage_sink: list[dict]` list, pass it into
  `process_github_repo` / `process_gitlab_project`, and the processor drains
  every instrumented client it constructs into it in a `finally:` block — on
  BOTH the success and failure path. On success the returned payload gains
  `observations: {provider_usage: <sink>}`, the exact shape
  `workers/sync_units.py::_attach_budget_comparison` joins as-is. On a
  mid-sync raise, whatever was drained before the exception is attached to it
  via `providers/usage.py::attach_partial_observations` — a provider-neutral
  alias for the mechanism `metrics/job_work_items.py` already used for the
  work-items sync path (`attach_work_item_partial_observations`, which now
  delegates to it); both write the SAME exception attribute, so
  `workers/sync_units.py::_merge_partial_observations_into_result` reads
  either origin unchanged. Within the review batch itself, failure semantics
  are split: a `RateLimitException` PROPAGATES (after the finally-drain) so
  the unit defers to RETRYING with the partial actuals preserved — swallowing
  it would stamp SUCCESS with reviews silently missing and bypass the
  deferral branch entirely (codex HIGH on the CS2 review); any other
  review-fetch error keeps the pre-existing degrade-and-log behavior, since
  reviews are optional enrichment on top of the PR rows. Legacy entry points
  that call a processor without an adapter-owned sink (the CLI
  `processors/sync.py` batch path, webhooks) get `usage_sink=None`; the
  processor still drains any client it builds (so its recorder never leaks
  across calls) but only logs the observations at debug level — never
  persisted.
- **First intentional re-bucketing: `prs` / `pr_social`.** GitHub's PR
  review-batch enrichment (`processors/github.py::_enrich_prs_with_reviews_batch`)
  constructs a local `GitHubWorkClient` to batch-fetch reviews over GraphQL,
  but never drained it and emitted an unprefixed operation label — so its
  traffic silently fell through the resolver's transport default onto
  `work_item_prs` (documented as a gap below) and was discarded outright (no
  drain at all). CS2 fixes both: the client is now drained via the
  `usage_sink` contract above, and its GraphQL calls are labeled with the
  `pr_social:` prefix (the CS1 short-circuit convention), so they resolve
  DIRECTLY to the `pr_social` route family the estimator already reserves for
  the `prs`/`pr-reviews`/`pr-comments` dataset family. The unprefixed call
  shape (`operation_family=None`) is preserved exactly for the work-items
  PR-as-work-item path (`providers/github/provider.py`), which keeps
  resolving to `work_item_prs` unshifted — pinned by
  `tests/providers/test_usage_resolver_prefix.py`'s new
  `pr_social:`-prefixed cases alongside its existing unshifted-attribution
  proof, and by call-count tests in
  `tests/providers/test_github_pr_social_batch.py` (N physical GraphQL
  requests → one aggregated `pr_social`/`graphql_cost` observation with
  `request_count == N`; a mid-pagination failure leaves the prior successful
  request(s) drained). CS8 moves the REST `prs` listing and PR-commit fetches
  onto `GitHubCodeClient`, labels them with the `prs:` prefix, and flips the
  `providers/github/budget.py` `prs` REST-core marker; `pr_social` remains the
  separate GRAPHQL_COST family for review/social enrichment.
- **GitLab.** `process_gitlab_project` accepts the same `usage_sink`
  parameter for a uniform cross-provider adapter contract. Migrated
  GitLabCodeClient-backed families drain into it on both success and failure:
  `security` (CS10), `pipelines`/`deployments` (CS11), `tests` (CS12),
  commits + aggregate commit stats (CS13), and `files`/`blame` (CS14) --
  all under the existing `project` family.
  Frozen connector-only paths still leave the sink untouched until their own
  migration changeset lands.

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
- **Known gaps.** `pr_social` / `work_item_prs` **secondary-limit** pressure
  (the `secondary_abuse_risk` dimension) is still *estimated*, not measured —
  no client observes a distinct secondary-limit signal on a success response,
  so this stays an abstract reservation for both families. `pr_social`'s
  primary (`graphql_cost`) traffic IS now measured as of
  [CHAOS-2803](https://linear.app/fullchaos/issue/CHAOS-2803) (CS2, see
  [Usage drain wiring](#usage-drain-wiring-first-re-bucketing-chaos-2773-cs2)
  above) for the PR review-batch enrichment; REST `prs` listing/PR commits and
  incident-label issue fetches are measured as of CHAOS-2809 (CS8). See
  [Known gaps](#known-gaps).
- **Actuals instrumentation (CHAOS-2807).** Commit listing (`git`) and
  per-commit file stats (`commit_stats`) now fetch through
  `providers/github/code_client.py::GitHubCodeClient` from processor/backfill paths,
  drain into the shared `usage_sink`, and resolve through explicit `git:` /
  `commit_stats:` operation prefixes. The `commit_stats` `contents_blob`
  reservation remains estimate-only because the migrated REST commit-detail call
  reports as `rest_core`.
- **Actuals instrumentation (CHAOS-2808).** File contents (`files`) and blame
  (`blame`) now fetch via GraphQL through the SAME
  `providers/github/code_client.py::GitHubCodeClient`, over provider-owned
  GraphQL support relocated from the frozen connector's GraphQL client onto
  `providers/github/graphql.py`, and drain into the shared `usage_sink`,
  resolving through explicit `files:` / `blame:` operation prefixes. Unlike
  `commit_stats` (where `rest_core` is the live dimension), `files`/`blame`
  traffic is 100% GraphQL, so `contents_blob` carries the marker; their
  `rest_core` reservation stays estimate-only -- it belongs to the
  repository tree listing that discovers candidate paths, which remains on
  the frozen PyGithub connector (out of CS7 scope).
- **Actuals instrumentation (CHAOS-2809).** Pull-request listing and PR commits
  (`prs`) now fetch through `GitHubCodeClient` REST pagers and resolve through
  explicit `prs:` operation prefixes. Incident-label issue fetches also use
  `GitHubCodeClient` and emit an `incidents:` prefix; `incidents` is a
  resolver/actuals-only family so the estimator vocabulary stays frozen while
  real incident issue traffic still appears in calibration output as an
  unbudgeted actual when present.
- **Actuals instrumentation (CHAOS-2810).** Repository metadata (`get_repo`)
  and discovery/listing (`list_repositories`, `list_installation_repositories`)
  now fetch through `GitHubCodeClient`, resolving through an explicit `repo:`
  operation prefix; `process_github_repos_batch`'s repo discovery/metadata
  fan-out and the admin credential repo-list endpoint
  (`api/admin/routers/credentials.py`) both moved off `GitHubConnector`/PyGithub
  onto the same client.

#### Route families
<!-- route-families:github -->

| Route family | Dimension(s) | Covers | Confidence |
| --- | --- | --- | --- |
| `repo` | `rest_core` | Repository metadata + discovery (`GitHubCodeClient`, CHAOS-2773 CS9) | high |
| `git` | `rest_core` | Commit listing (`GitHubCodeClient`, CHAOS-2773 CS6) | medium |
| `commit_stats` | `rest_core`, `contents_blob` | Per-commit file/stat expansion (`GitHubCodeClient` REST core, CHAOS-2773 CS6; contents/blob still estimate-only) | low |
| `files` | `rest_core`, `contents_blob` | Repository tree listing (`rest_core`, frozen/estimate-only) + blob-text reads (`contents_blob`, `GitHubCodeClient` GraphQL, CHAOS-2773 CS7) | low |
| `blame` | `rest_core`, `contents_blob` | Blame expansion (file-count dependent; `contents_blob` via `GitHubCodeClient` GraphQL, CHAOS-2773 CS7) | low |
| `prs` | `rest_core` | Pull-request listing + PR commits (`GitHubCodeClient`, CHAOS-2773 CS8) | medium |
| `incidents` | `rest_core` | Incident-label issue fetches (`GitHubCodeClient`, resolver-only actuals family, CHAOS-2773 CS8) | low |
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
- **Feature-flag fetch migrated off the frozen connector (CHAOS-2785).**
  `GitLabConnector.get_feature_flags` / `get_project_name`
  (`connectors/gitlab.py`) previously carried this same 403/429 convention but
  through the un-instrumented `connectors/utils/rest.py::GitLabRESTClient`, so
  the fetch never produced actuals. The feature-flags sync unit now fetches
  through the canonical `providers/gitlab/feature_flags.py::
  GitLabFeatureFlagsClient` -- byte-for-byte behavior parity (403 stays
  non-retryable, 429 stays a retryable `RateLimitException` with signal;
  pinned by `tests/test_gitlab_feature_flags_client.py`), now wired to the
  shared CHAOS-2754 recorder. `connectors/gitlab.py` is left in place, unused
  by the feature-flags sync path; its other (code-dataset) methods were
  retired as dead code in CS17 / CHAOS-2819 (see below), leaving only the
  credential shell needed by processor bridges.
- **Actuals instrumentation (CHAOS-2785).** The feature-flags fetch
  (`get_feature_flags`, `get_project_name`) records real per-request counts
  through the shared CHAOS-2754 recorder, resolving under the existing
  `project` route family the `GitLabBudgetEstimator` already reserves for
  `DatasetKey.FEATURE_FLAGS` -- no new route family or estimator change was
  needed, since `GITLAB_USAGE_ROUTE_FAMILIES`' `project` marker (`/projects/`)
  already matches these operations.
- **Known gaps.** GitLab feature-flag budgeting shares the `project` REST-core
  family; dedicated GitLab feature-flag budget families (distinguishing them
  from generic project-metadata reads in calibration) remain a follow-up.

#### Route families
<!-- route-families:gitlab -->

| Route family | Dimension(s) | Covers | Confidence |
| --- | --- | --- | --- |
| `project` | `rest_core` | Project metadata, commits, files/blame, feature-flags | high–low |
| `merge_requests` | `rest_core` | Merge request iterators, MR commits, approvals (`GitLabCodeClient`, CHAOS-2816/CS15) | medium |
| `notes` | `rest_core` | MR note + discussion expansion (`GitLabCodeClient`, CHAOS-2816/CS15); issue notes still use work-item paths | low |
| `pipelines` | `rest_core` | CI/CD pipeline listing + job expansion (`GitLabCodeClient.get_pipelines`) | low |
| `deployments` | `rest_core` | Deployments, releases, deployment→MR resolution (`GitLabCodeClient`, CHAOS-2773 CS11) | low |
| `security` | `rest_core` | Vulnerability findings + dependency-scan alerts (`GitLabCodeClient`, CHAOS-2773 CS10) | low |
| `tests` | `rest_core` | Pipeline test_report, pipeline jobs, job artifact download (`GitLabCodeClient`, CHAOS-2773 CS12) | low |
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
  (`raise_for_status`). This is a known asymmetry vs. GitHub/GitLab — see
  [Known gaps](#known-gaps).
- **Gating.** Worklog and AGG-GraphQL route families only appear when
  `JIRA_FETCH_WORKLOGS` / `ATLASSIAN_GQL_ENABLED` are set (`providers/jira/budget.py`).

#### Route families
<!-- route-families:jira -->

| Route family | Dimension(s) | Covers | Confidence |
| --- | --- | --- | --- |
| `jira_metadata` | `rest_core` | Project/label metadata | high |
| `jira_jql` | `search` | Work-item listing via REST `search/jql` pagination | medium |
| `jira_jsm_incident_admission` | `rest_core` | Bounded native JSM incident admission checks for JQL candidates | low |
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
  `Retry-After` on `429` (`providers/launchdarkly/client.py`,
  `providers/launchdarkly/code_refs.py`).
- **Retry semantics.** `429` and `5xx` are retried with exponential backoff
  (honoring `Retry-After`) up to `max_retries = 5`. On exhaustion the terminal
  type differs: a `429` raises `RateLimitException` (→ worker deferral) while a
  `5xx` raises `APIException` (a normal failure, `providers/launchdarkly/
  client.py` / `providers/launchdarkly/code_refs.py` `_raise_for_status`). A
  low `X-RateLimit-Route-Remaining` (< 5) is currently only **logged as a
  warning**, not fed into the deferral/cooldown machinery — see
  [Known gaps](#known-gaps).
- **Non-retryable auth cases.** `401` → `AuthenticationException`; `403` →
  `APIException` (forbidden). LaunchDarkly does **not** distinguish a
  permission-403 from a rate-limit case the way GitHub/GitLab do.
- **Canonical provider migration complete (CHAOS-2761).** Flag and audit-log
  fetches moved off the frozen legacy connector
  (`connectors/launchdarkly.py`, left in place unused by the sync path — it
  still backs the admin credentials "test connection" endpoint, which mirrors
  the same raw/legacy-client pattern already used there for Jira/Linear
  connectivity checks and carries no actuals-instrumentation gap) into
  `providers/launchdarkly/client.py::LaunchDarklyClient`, mirroring the
  request/retry semantics byte-for-byte (parity pinned by
  `tests/test_rate_limit_signal.py::test_launchdarkly_403_is_authentication_error`).
  Combined with the pre-existing canonical `providers/launchdarkly/code_refs.py`,
  all of `providers/launchdarkly/` is now canonical. See
  [LaunchDarkly sync budgeting](../architecture/launchdarkly-sync-budgeting.md).
- **Actuals instrumentation (CHAOS-2761).** All 3 currently-emitted route
  families (`flags`, `audit_log`, `code_refs`) now record real per-request
  counts through the shared CHAOS-2754 recorder
  (`LAUNCHDARKLY_USAGE_RESOLVER` in `providers/launchdarkly/budget.py`), so
  LaunchDarkly units produce a `budget_comparison` row like GitHub/GitLab/
  Jira/Linear work-item units already did. `code_refs`' `secondary_abuse_risk`
  reservation shares its single REST call with the `rest_core` reservation, so
  only `rest_core` ever gets a live actual (same one-call/two-dimension shape
  as GitHub's `commit_stats`/`files`/`blame` `contents_blob` entries).
- **Documented vs. emitted families.** The estimator currently emits `flags`,
  `audit_log`, and `code_refs`. `projects`, `segments`, and `members` are
  **modeled route families** (`LAUNCHDARKLY_BUDGET_ROUTE_FAMILIES` in
  `providers/launchdarkly/budget.py`) that the current `feature-flags` estimator
  does not yet reserve, and no client fetches yet; they are documented here for
  completeness.

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

These are the remaining instrumentation/coverage gaps after the
[CHAOS-2742](https://linear.app/fullchaos/issue/CHAOS-2742) epic (all eight
sub-issues shipped). They are documented as **current reality**, not defects to
paper over:

- **Jira 403s are not classified rate-limit vs. permission.** Unlike
  GitHub/GitLab, Jira has no dedicated 403 classification — a 403 surfaces
  through the shared HTTP error path rather than as a `RateLimitSignal` with a
  `permission` reason (see [Jira](#jira) above). Untracked; file under a
  follow-up if it bites in practice.
- **Calibration only covers instrumented route families.** [CHAOS-2759](https://linear.app/fullchaos/issue/CHAOS-2759)
  attaches a `budget_comparison` (see
  [Actual-vs-estimated calibration](#actual-vs-estimated-calibration-chaos-2759)
  above) wherever CHAOS-2754's recorder actually drained actuals for a
  route_family, but the recorder only records where a client calls it.
  GitHub/GitLab `pr_social`/`work_item_prs` **secondary** (`secondary_abuse_risk`)
  pressure, Linear request/complexity actuals, and LaunchDarkly
  route-remaining are still **uninstrumented**, so those families never
  produce a comparison row for that dimension. GitHub `pr_social`'s primary
  (`graphql_cost`) traffic from the PR review-batch enrichment IS now
  instrumented as of [CHAOS-2803](https://linear.app/fullchaos/issue/CHAOS-2803)
  (CS2) — see
  [Usage drain wiring](#usage-drain-wiring-first-re-bucketing-chaos-2773-cs2)
above. GitHub `git` and `commit_stats` REST-core traffic is instrumented as of
  CHAOS-2807, `files`/`blame` `contents_blob` traffic (GraphQL) as of
  CHAOS-2808, REST `prs` listing / PR commits / incident-label issue
  fetches as of CHAOS-2809, and `repo` metadata/discovery traffic as of
  CHAOS-2810 (CS9); GitLab merge-request and MR-note REST-core traffic
  is instrumented as of CHAOS-2816 (CS15), and GitLab repo metadata, project
  discovery, and batch orchestration as of CHAOS-2817 (CS15b). The
  `files`/`blame` `rest_core` tree listing and remaining GitHub/GitLab
  code-dataset families stay uninstrumented pending their own CHAOS-2773
  changesets (see
  "Frozen `connectors/` path" below). There is also no cross-run
  aggregation/dashboard yet — calibration today is visible per-unit (result +
  structured log), not rolled up over time.
- **Frozen `connectors/` path.** GitHub repo metadata/listing and batch
  orchestration now use `GitHubCodeClient` and emit `repo:` usage actuals, and
  the dead GitHub connector PR-review, PR-commit, file-contents, security-alert,
  security-advisory, and rate-limit helpers were retired in CS16 / CHAOS-2818;
  the PyGithub-based `GitHubConnector` now remains only as the credential/
  PyGithub/artifact holder after the base-ABC data-method cleanup (CHAOS-2865).
  GitLab repo
  metadata, project discovery, and batch orchestration now use `GitLabCodeClient`
  and emit `project:` usage actuals, and the GitLab connector code-dataset fetch
  methods were retired in CS17 / CHAOS-2819; `connectors/gitlab.py` now retains
  only the credential shell needed by processor bridges, while
  `connectors/utils/rest.py` is intentionally retained in the tree for a future
  consumer. No new code may be added under `connectors/` (see
  [the platform contract](../contributing/platform-contract.md)); rate-limit/actuals instrumentation for those
  datasets lands as the fetch moves to `providers/github/` / `providers/gitlab/`
  — tracked as its own canonical-provider-migration effort in
  [CHAOS-2773](https://linear.app/fullchaos/issue/CHAOS-2773), since it is a
  much larger migration (~1400 lines per connector, off PyGithub/python-gitlab
  entirely) than a follow-up-ticket-sized change. LaunchDarkly's equivalent
  gap (flag/audit-log fetches on the frozen connector) was closed in
  [CHAOS-2761](https://linear.app/fullchaos/issue/CHAOS-2761); see
  [LaunchDarkly](#launchdarkly) above. GitLab's feature-flags fetch
  (`get_feature_flags` / `get_project_name`) is likewise closed as of
  [CHAOS-2785](https://linear.app/fullchaos/issue/CHAOS-2785) — see
  [GitLab](#gitlab) above. GitHub's `git`/`commit_stats`
  ([CHAOS-2807](https://linear.app/fullchaos/issue/CHAOS-2807), CS6) and
  `files`/`blame` ([CHAOS-2808](https://linear.app/fullchaos/issue/CHAOS-2808),
  CS7 -- the file-contents/blame GraphQL fetch relocated onto
  `providers/github/graphql.py`) moved to
  `providers/github/code_client.py::GitHubCodeClient`; GitHub `prs` REST
  listing, PR commits, and incident-label issue fetches
  ([CHAOS-2809](https://linear.app/fullchaos/issue/CHAOS-2809), CS8), and
  GitHub repo metadata + batch discovery/orchestration
  ([CHAOS-2810](https://linear.app/fullchaos/issue/CHAOS-2810), CS9) also moved
  onto `GitHubCodeClient`. GitLab's `security`
  ([CHAOS-2811](https://linear.app/fullchaos/issue/CHAOS-2811), CS10),
  `pipelines`+`deployments`
  ([CHAOS-2812](https://linear.app/fullchaos/issue/CHAOS-2812), CS11), and
  `tests` (+CI adapter usage draining;
  [CHAOS-2813](https://linear.app/fullchaos/issue/CHAOS-2813), CS12),
  commits + aggregate commit stats
  ([CHAOS-2814](https://linear.app/fullchaos/issue/CHAOS-2814), CS13), and
  `files`/`blame` (`get_file_contents` batched GraphQL blob fetch +
  `get_file_blame` REST, both under the `project` family;
  [CHAOS-2815](https://linear.app/fullchaos/issue/CHAOS-2815), CS14), and
  `merge_requests`/`notes` (`iter_merge_requests`, MR commits, MR approvals,
  MR notes;
  [CHAOS-2816](https://linear.app/fullchaos/issue/CHAOS-2816), CS15)
  code-dataset families plus repo metadata/project discovery/batch orchestration
  ([CHAOS-2817](https://linear.app/fullchaos/issue/CHAOS-2817), CS15b) are
  migrated onto the canonical, instrumented
  `providers/gitlab/code_client.py::GitLabCodeClient`; the legacy connector
  methods for those families were removed in CS17 / CHAOS-2819.


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

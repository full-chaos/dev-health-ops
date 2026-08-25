# Transitional workload inventory (CUT-01)

**Epic:** CHAOS-3033 (Go worker cutover) &middot; **Sub-issue:** CHAOS-3073 CUT-01
**Contract:** `contracts/jobs/v1/transitional-inventory.json`
**CI gate:** `ci/check_transitional_inventory.py`
**Tests:** `tests/workers/test_transitional_inventory_contract.py`

## Why this exists

The [Go worker cutover TRD](go-worker-cutover-trd.md) (§6, §8.3) and the
[implementation plan](../plans/go-worker-cutover-implementation-plan.md) (CUT-01)
require a single checked artifact that maps **every** legacy Celery/Beat/API/
registry/stream surface to a target owner or an explicit removal decision, so
that:

- nothing can be "quietly" left behind when Celery is finally removed;
- CI -- not a human re-reading the plan -- is what proves the inventory is
  still complete and non-contradictory as the codebase changes.

This inventory is intentionally **transitional**: per TRD §8.3 it is deleted
alongside the final Celery cleanup once CI proves no legacy surface remains.

## What is in the contract

`contracts/jobs/v1/transitional-inventory.json` has 154 rows: the 147 surfaces
discovered in the Wave-0 audit, plus 5 added during CUT-01 round-2 hardening
(a Codex adversarial review found a live dispatch the original scanner
missed), 1 added during round-3 hardening (an ordinary two-hop API
trigger the call-graph fallback missed), and 1 added during round-4
hardening (a separately deployed second FastAPI app forwarding to a
handler defined in another file) -- see "Round-2/3/4/5 hardening"
below. Classes: Celery task decorators,
Beat schedule entries (19 unconditional + 1 conditional), literal dispatch
calls (`.delay(`, `.apply_async(`, `send_task(`, `.signature(`, a bare
celery-canvas invocation, or a bound-method/`functools.partial` alias), the
`getattr(x, "delay"|"apply_async"|"send_task")` indirection form (12 of 34
dispatch call sites use this grep-evading form), API dispatch endpoints (REST
and GraphQL resolvers), job registry kinds (`contracts/jobs/v1/registry.json`),
stream consumer surfaces, sync-dispatch transport routes
(`contracts/sync-dispatch/v1/transport-routes.json`), and celery-canvas
imports (a fail-closed guard, not a specific call site).

Each row has:

| Field | Meaning |
|---|---|
| `id` | `<class>:<file>:<line>`, stable identity for the row |
| `surface` | the task/entry/endpoint/kind name from the audit |
| `class` | one of the 10 discovered surface kinds (see below) |
| `source` | `{file, line}` anchor into the current tree |
| `dispatch_mechanism` | how this surface is invoked |
| `owner_role` | `primary` or `contributor` -- see "Ownership model" below |
| `target_owner` | `{type, value}` -- `native_kind` \| `native_process` \| `explicit_removal`, plus the owning kind/process name |
| `target_kind_id` | the specific native target this row's ownership claim is about (only set on `primary` rows) |
| `current_implementation_state` | `native_go` \| `python_compatibility` \| `celery_only` \| `dormant_go` |
| `verification_status` | `verified`, or `registry_only_unverified` when a registry kind's Go handler is wired but no producer has been found anywhere in the codebase that would ever create a job of that kind |
| `compatibility_dependency` | what Python/Celery machinery this row still depends on, if any |
| `deletion_evidence_requirement` | what must be true before this row can be deleted from the inventory |
| `acceptance_test_id` | an existing, executable test file that exercises this row (or the CI contract test itself, for rows whose only current acceptance test is "this surface is correctly inventoried and owned") |
| `notes` | audit findings and any orchestrator resolution recorded for this row |
| `route_mount_prefix` | `api_trigger_endpoint` rows only (default `""`): a prefix stripped from `surface`'s recorded path before comparing it, exactly, to the decorator/registration's local path literal during content-drift re-verification (added round-5; see "Round-5 hardening" below) |

### Ownership model: `primary` vs `contributor`

A naive "every row must have a unique target owner" rule breaks the moment one
process legitimately serves many callers (e.g. 4 different API endpoints all
routing sync dispatch through "native Go coordinators"). Instead:

- **`primary`** rows are the exclusive canonical claimant of a `target_kind_id`
  -- a Beat entry, a registry kind, a stream, a transport route, or a
  standalone Celery task that no Beat entry/registry kind/transport route
  already claims (e.g. `health_check`, `sync_team_drift`,
  `process_pagerduty_webhook_event`, `flush_external_ingest_recompute`,
  `run_daily_metrics`, `dispatch_external_ingest_recompute_bridge`).
- **`contributor`** rows are dispatch/trigger call sites or implementation
  bodies that feed an existing `primary` row. They may freely share a
  `target_owner.value` with other contributors.

**CI rule:** at most one row may be `owner_role: primary` for any given
`target_kind_id`. This is what "ownership must be exclusive" means in
practice -- it does not mean `target_owner.value` itself must be globally
unique.

## The six previously-open surfaces (resolved for CUT-01)

The Wave-0 audit flagged 6 surfaces as open (`trd_covered: false` or
ambiguous). Orchestrator resolutions, baked into the contract:

1. **`flush_external_ingest_recompute`** -- target owner: native external
   recompute controller in the stream-external runner (CUT-11); `celery_only`
   today (debounced `.apply_async(countdown=...)` self-dispatch).
2. **`run_dora_metrics`** -- owner: `metrics.remaining.dora` under the Go heavy
   worker (CUT-12). Confirmed **chain-triggered, not scheduled**:
   `internal/syncdispatchruntime/native_post_sync.go` calls
   `remaining.StartRunTx(ctx, tx, "dora", *plan, "")` when `plan.DORA` is true,
   as part of the native post-sync fanout (CHAOS-2596/CHAOS-3051); there is no
   Beat entry or standalone cron for it.
3. **`health_check`** -- `explicit_removal` (replaced by Go health/readiness/
   metrics endpoints, CUT-04). Verified during CUT-01: a repo-wide search for
   `.delay(`/`.apply_async(`/`send_task(`, the `getattr` indirection form,
   Beat entries, and CLI/admin invocations finds **zero** dispatch sites --
   only the task decorator and two `__all__` re-export sites exist.
4. **`metrics.remaining.team_metrics` + `metrics.remaining.extra_metrics`** --
   investigated in depth (see `cmd/dev-health-worker/daily.go`'s
   `addRemainingWorker` registration and `internal/syncdispatchruntime/
   native_post_sync.go`'s `Fanout()`). The Go handlers **are** constructed and
   wired, identically to the other 6 `metrics.remaining.*` families, and the
   Python compute functions (`_run_team_metrics`/`_run_extra_metrics` in
   `worker_metrics.py`) are real. But **no Celery task, Beat entry, admin/API
   trigger, or Go producer anywhere in the codebase ever creates a
   `remaining_metric_runs` row for either family** -- `native_post_sync.go`
   only ever starts `"complexity"`, `"membership_backfill"`, or `"dora"` runs.
   Marked `current_implementation_state: dormant_go`,
   `verification_status: registry_only_unverified`. **Do not count these as
   covered** until a producer is added or the kind is confirmed intentionally
   dormant.
5. **PagerDuty webhook path** -- target owner: PagerDuty Go stream handler
   (CUT-04). Noted structural asymmetry vs. the other 3 streams: there is no
   Python poll/Beat consumer for this stream at all -- it is a direct
   write-then-`.delay` from the webhook handler, and the Go package
   (`internal/jobs/pagerduty`) exists but is unreachable from any `cmd/`
   binary (`dormant_go`).
6. **`investment.*` kinds** -- real current executor is the work-graph
   compatibility bridge `cmd/dev-health-worker/workgraph.go` (confirmed: it
   constructs `workgraph.NewHTTPCompatibilityExecutor` against
   `/internal/worker/workgraph/v1/execute`), **not** `internal/jobs/investment`
   as `registry.json`'s `handler_owner` metadata claims. That metadata drift is
   a known gap (4.7) whose fix belongs to CUT-13 -- this inventory records the
   real owner without editing `registry.json`.

## The CI gate

`ci/check_transitional_inventory.py` independently re-discovers every surface
class from source (it does not trust the inventory file's own bookkeeping) and
fails when:

1. a discovered surface has no corresponding inventory row (**unowned
   surface**);
2. an inventory row has no `target_owner.value`;
3. two rows are both `owner_role: primary` for the same `target_kind_id`
   (**duplicate exclusive ownership**);
4. a row's `target_kind_id` names something that was never actually
   discovered in source (**closed-vocabulary check** -- a row can't dodge #3
   by renaming its `target_kind_id` to an unregistered variant, e.g.
   `kind:metrics.remaining.capacity-v2`);
5. a row's `source.file`/`source.line` anchor no longer exists on disk, is
   past the end of the (possibly shrunk) file, or its current line content no
   longer matches the pattern expected for that row's class (**staleness /
   content drift** -- e.g. the dispatch call at that exact line was replaced
   by unrelated code).

Discovery specifically re-derives, from scratch:

- Celery task decorators, including the `@app.task(`/`@shared_task` aliased
  forms (`src/dev_health_ops/workers/`);
- Beat schedule entries, both the `beat_schedule = {...}` dict literal and the
  conditional, indented `beat_schedule["..."] = {...}` rollout seam;
- literal dispatch calls (`.delay(`, `.apply_async(`, `send_task(`,
  `.signature(`, a bare celery-canvas invocation `chord(...)()` /
  `chain(...)()` / `group(...)()`, or a bound-method/`functools.partial`
  dispatch alias), skipping docstring prose, trailing comments, and
  multi-line-call continuations;
- the `getattr(x, "delay"|"apply_async"|"send_task")` indirection form;
- celery-canvas imports (`from celery import chain, chord, group` or
  `from celery.canvas import ...`) as a fail-closed guard: any new use of a
  canvas primitive requires its own row even where the exact invocation shape
  downstream can't be statically enumerated;
- API trigger endpoints: REST endpoints by finding the enclosing function's
  `@router.<method>(...)`/`@router.api_route(...)` decorator (correctly
  handling both single- and multi-line decorator argument lists via a
  bracket-depth-tracking scan, a router/app alias such as `r = router` or
  any name bound to `FastAPI()`/`APIRouter()`, and a dispatch call sitting
  in a shared undecorated helper via a transitive same-file call-graph
  BFS of any depth up to a 12-hop backstop); GraphQL resolvers (which have
  no per-function route decorator at all) by anchoring to the call site
  itself; `router.add_api_route(...)`/`.add_route(...)` registrations
  across every api/ module; and one targeted cross-file shape -- a module
  with its own `FastAPI()` instance whose route handler forwards to an
  imported name that is itself already a proven dispatch-relevant endpoint
  elsewhere (e.g. a separately deployed edge app calling a handler defined
  in the main API);
- job registry kinds (`contracts/jobs/v1/registry.json`);
- sync-dispatch transport routes (`contracts/sync-dispatch/v1/transport-routes.json`);
- stream surfaces (`CONSUMER_GROUP` constants in `**/streams.py` modules, plus
  the PagerDuty write-then-dispatch stream as a documented special case since
  it has no consumer-side counterpart).

Run it directly:

```bash
python3 ci/check_transitional_inventory.py --root .
```

It is also exercised as a pytest test
(`tests/workers/test_transitional_inventory_contract.py::test_real_tree_passes_the_gate`),
so it runs automatically in the existing unit test tier
(`ci/run_tests.sh unit`) without any separate workflow wiring. That test module
also asserts exact per-class parity between discovery and the inventory (not
just "every discovered surface has a row" -- discovered-count must equal
inventory-count per class), and proves the gate catches every violation kind
using synthetic fixture trees under `tmp_path` (never a real unowned surface
committed to the repo): one positive fixture per discovery class (literal
dispatch, getattr indirection, canvas import, bare canvas invocation, bound-
method alias, `functools.partial` alias, REST endpoint with a single-line and
a multi-line decorator, the shared-helper fan-in shape, a GraphQL resolver,
conditional Beat entry, registry kind, transport route, stream surface, and
an aliased task decorator), plus an unowned Celery task, an unowned Beat
entry, a duplicate exclusive-ownership claim, a row with no target owner, an
unknown `target_kind_id`, and two shapes of stale/content-drifted anchor.

### Round-2 hardening (Codex adversarial review)

A first Codex review of the CUT-01 branch returned **BLOCK** with real gaps,
since fixed:

- **A live dispatch the scanner missed (HIGH):** `metrics_partitioned.py:91`
  fans out via a bare `chord([...], cb)()` invocation -- no `.apply_async()`
  suffix -- which the original literal-dispatch regex didn't recognize. Fixed
  by adding a bare-canvas-invocation pattern, a new `celery_canvas_import`
  class that fail-closes on any `chain`/`chord`/`group` import (defense in
  depth against invocation shapes that can't be statically enumerated, e.g.
  `partial(task.apply_async, ...)` or `enqueue = task.apply_async` bound-alias
  forms, both now also detected), and 5 new inventory rows.
- **Two discovery classes were silently dead (HIGH):** the API-trigger walker
  found zero REST endpoints (it stopped at the endpoint's own `def` line
  before reaching its decorator, and broke entirely on multi-line decorator
  argument lists), and the conditional-Beat regex required column-zero while
  the real entry is indented under an `if`. Runtime reconciliation was
  silently 138/147, not 147/147. Fixed by a bracket-depth-tracking decorator
  scan and a de-anchored conditional-Beat regex; the test suite now asserts
  exact discovered-vs-inventory parity per class as a standing regression
  guard.
- **Staleness didn't check content (MED):** replacing the code at an anchored
  line with something unrelated (while keeping the file the same length)
  used to pass. Fixed by re-matching each row's class-specific pattern
  against its anchor line's *current* content.
- **`target_kind_id` wasn't a closed vocabulary (MED):** a row could dodge
  duplicate-primary detection by renaming its `target_kind_id` to an
  unregistered variant. Fixed by validating every `target_kind_id` against
  the same source-derived discovery already used for the unowned-surface
  check.
- **Weak acceptance-test coverage (MED):** the billing rows pointed at
  unrelated tests (e.g. the API call-site row referenced
  `tests/test_sync_units.py`). Fixed those three rows to point at the real
  `tests/api/test_worker_operational_bridge.py` billing test, added a
  regression test for it, and added a check that any `acceptance_test_id`
  naming a specific test (`path.py::test_name`) actually defines that test.

### Round-3 hardening (Codex adversarial review, round 2)

A second Codex review returned **BLOCK** again with two more HIGH findings and
four MED findings, all fixed. This is the final hardening round; the
[Known limitations](#known-limitations) section below documents what
remains a deliberate, judged trade-off rather than an oversight.

- **Qualified/aliased celery-canvas forms still evaded discovery (HIGH):**
  `import celery.canvas as canvas`, `from celery import canvas`,
  `importlib.import_module("celery.canvas")` (static-string form), a
  parenthesized multi-line `from celery import (...)`, and a qualified bare
  invocation (`canvas.chord(...)()`) all produced no surface; an aliased bare
  invocation (`c = chord; c(...)()`) added no new row in an already-inventoried
  module. Fixed by `_scan_celery_canvas`, a per-file binding scanner that
  tracks both "direct" canvas names (through `as`-aliasing and simple
  `c = chord`-style local aliases) and "module" aliases (through
  `import celery.canvas as X`, `from celery import canvas`, or
  `importlib.import_module`), and matches qualified/aliased bare invocations
  in both their multi-line-opening and complete-single-line shapes.
- **API registration by router alias or `add_api_route`/`add_route` evaded
  discovery (HIGH):** `r = router; @r.post(...)` and
  `router.add_api_route(path, helper)` (registering an already-dispatching
  helper with no decorator at all) produced no api_trigger_endpoint surface.
  Fixed by `_router_aliases` (the same simple-bare-alias tracking as canvas
  names, seeded with the repo convention `router`) feeding an alias-aware
  decorator pattern, plus a dedicated scan for `add_api_route`/`add_route`
  calls across every api/ module -- not only ones that already hold another
  discovered dispatch call site, since the registration itself is the missed
  surface.
- **Closed vocabulary silently skipped unknown prefixes, and self-validated
  against any discovered task (MED, the most serious of the four):** renaming
  a `target_kind_id` to `bogus:anything` returned zero errors
  (`vocabulary.get(prefix)` returning `None` was treated as "no constraint"),
  and renaming it to an unrelated *discovered-but-unclaimed* task name (40
  such names exist) also passed, since the old `task:` vocabulary was "every
  discovered Celery task." Fixed by making an unrecognized prefix itself an
  error, and by replacing the `task:` vocabulary with
  `TRD_MAPPED_TASK_TARGETS`, a closed, curated allowlist of exactly the six
  standalone tasks the TRD gap analysis calls out (matching
  `STANDALONE_PRIMARY` in the inventory generator) -- an arbitrary other
  discovered task name is no longer automatically a valid claim.
- **Content drift was class-shape-only for eight of ten classes (MED):**
  swapping the dispatched task at a `call_site_literal` row's anchor line for
  a *different* task's `.apply_async(...)`, or changing `send_task("old")` to
  `send_task(other_var)`, still passed as long as the line still had a
  `.apply_async(`/`.delay(`/`send_task(` shape -- only registry/transport rows
  compared the discovered name against the row. Fixed by
  `_expected_dispatch_token` (best-effort extraction of the specific task
  name a row's `surface` records) plus per-class re-verification: `celery_task`
  re-derives the decorated function's name and compares it to `surface`;
  `beat_entry`/`beat_entry_conditional` compare the schedule key;
  `call_site_literal`/`call_site_getattr_indirection` search a forward window
  (multi-line statements put the identifying token a few lines below the
  anchor) for the extracted token; `stream_surface` compares the captured
  `CONSUMER_GROUP` value to the row's own `target_kind_id`.
- **Per-class parity compared only `Counter(class)` totals in one direction
  (MED):** a missed surface and an unrelated scanner phantom in the same
  class could net to zero, and the runtime gate itself only checked
  discovered→row, never the reverse. Fixed in both places: the test now
  compares the full `(class, file, line)` key sets in both directions, and
  `check()` itself gained a **PHANTOM ROW** check (a row whose anchor
  independent discovery does not find) alongside the existing
  **UNOWNED SURFACE** check (a discovered surface with no row) -- true
  bidirectional identity, not count-matching.
- **Three of the five round-2 rows had incomplete structured owners:** the
  metrics-chord import and invocation rows named only
  `metrics.daily_partition`, dropping the chord callback's
  `metrics.daily_finalize`; the work-graph canvas-import row named only
  `investment.chunk`, dropping `investment.finalize` (the same chord's
  callback) and the separately-owned `workgraph.build` +
  `metrics.remaining.membership_backfill` chain it also backs (TRD §6.1,
  `go-worker-cutover-trd.md:156` and `:170`). Fixed by updating all three
  rows' `target_owner.value` to name every kind the row's dispatch actually
  reaches.

## Update procedure

When you add, remove, or re-home a legacy surface:

1. Run `python3 ci/check_transitional_inventory.py --root .` -- it will tell
   you exactly which surface is unowned (class, file, line).
2. Add or update the corresponding row in
   `contracts/jobs/v1/transitional-inventory.json`, filling in every field
   above. If the surface is a genuinely new native target (not already fed by
   an existing `primary` row), set `owner_role: primary` and choose a
   `target_kind_id` that isn't already claimed by another primary row -- the
   gate will fail if it is.
3. If the change is a **removal** (Celery task/Beat entry deleted because its
   native replacement is proven), delete the row rather than leaving it
   dangling with a stale anchor.
4. Re-run the gate and `pytest tests/workers/test_transitional_inventory_contract.py`.

**Removing a `registry_kind` row is a separate, additional acknowledgement.**
Deleting a row here tells the CUT-01 census the surface is no longer
untracked; it says nothing to `cmd/worker-contractcheck`'s `compare` step
(`internal/jobcontract/compatibility.go`), which independently treats any
registered kind that disappears from `contracts/jobs/v1/registry.json`
between merged commits as a breaking regression, on purpose -- most removals
of a live kind ARE a regression. To retire a kind that genuinely has zero
producer anywhere (CHAOS-4243 established that "registered but unbound" is
itself the broken state, not an acceptable final one), add a matching entry
to registry.json's own `retired_kinds` array in the same commit that deletes
the kind: `{kind, retired_on, reason, ticket, replacement}`, where
`replacement` names the file:line of whatever inline compute already covers
the kind's tables (there always is one, or the kind was doing real work and
should not be retired). `CompareTrees` treats a removal as acknowledged only
when the kind appears there; `TestRetiredKindsAreFullyRemoved`
(`internal/scheduler/fixed/producers_test.go`) is table-driven off that same
array and fails if the kind still exists in the registry, the deployment
manifest, `internal/jobs/metrics/remaining/families.json`, or the fixed
schedule -- so one ledger entry is both the compatibility-gate waiver and the
full-removal proof for every future retirement, not just this one.

When the whole inventory is finally decommissioned (TRD §8.3: "removed with
the final Celery cleanup after CI proves no legacy surface remains"), delete
this document, the contract file, the CI script, and its test module in the
same PR that removes the last Celery surface.

### Round-4 hardening (Codex adversarial review, round 3)

A third Codex review returned **BLOCK** with 3 HIGH + 1 MED. Unlike prior
rounds, these weren't new evasion shapes -- they were **internal
inconsistencies** between discovery and re-verification, and one genuinely
*ordinary* (not exotic) pattern this codebase already uses. All fixed:

- **A real, ordinary two-hop API trigger was under-inventoried (HIGH):** the
  Stripe webhook route (`billing/router.py:319`) reaches its Celery dispatch
  (`billing/router.py:133`) through two helper calls
  (`stripe_webhook -> _process_subscription_event -> _enqueue_billing_notification`),
  and the one-hop call-graph fallback only followed one. Two hops is not an
  exotic shape here -- it's how this exact webhook handler is written. Fixed
  by making the same-file call-graph fallback a proper fixed-point BFS
  (`_transitive_router_anchor`, bounded at 12 hops purely as a runaway
  backstop) instead of a single lookup, and added the missing
  `api_trigger_endpoint` row for the Stripe webhook.
- **The specific-name content-drift check matched anywhere in a 6-line
  window, including comments (HIGH):** `other_task.apply_async()` followed
  by a comment or log line mentioning the *old* task name still passed.
  Fixed by `_statement_window`: the token/path check now only searches the
  bracket-depth-bounded logical statement starting at the anchor, with each
  line's trailing `#` comment (and any whole one-line docstring) stripped
  before matching -- a comment can no longer keep a swapped dispatch alive.
- **Discovery and content-drift re-verification had literally different
  pattern tables (HIGH):** a qualified/aliased canvas call or a router-alias
  decorator that discovery correctly found could still fail its OWN
  content-drift check, because the re-verifier only knew the base,
  unaliased regex -- so adding the row the gate demanded produced
  `STALE ANCHOR` instead of `OK`. Fixed by extracting single shared
  functions (`_call_site_bare_invocation_patterns`, and re-deriving
  `_router_aliases`/`_router_decorator_re_for`/`_add_route_re_for` at
  content-check time exactly as discovery does) used by BOTH passes, so a
  form discovery accepts can never be rejected by re-verification. Locked in
  by an end-to-end test per form: synthesize the surface, add the row the
  gate demands, assert the gate then passes.
- **Two "documented limitations" were actually routine edits, not exotic
  (MED):** replacing `from celery import chain, chord` with
  `from celery import group`, or editing a REST route's path string, both
  used to pass content-drift. Fixed: `celery_canvas_import` re-verification
  now compares the actual imported name set to the row's recorded names
  (`_expected_import_names`); `api_trigger_endpoint` REST rows now compare
  the decorator/registration's literal path argument to the row's recorded
  path via a suffix relationship (`_actual_route_path` /
  `_expected_rest_path`) -- suffix, not exact equality, because an
  `APIRouter`'s local path argument is frequently only the tail of the row's
  recorded full effective path (the router's own `prefix=` mount, declared
  elsewhere, supplies the rest).

### Round-5 hardening (Codex adversarial review, round 4)

A fourth Codex review returned **BLOCK** with 2 HIGH + 2 MED, scoped to
exactly four findings per the team lead's direction (this is the final
round; anything found beyond these four is triaged as follow-up rather than
another automation round):

- **A deployed second FastAPI app forwarding cross-file was invisible
  (HIGH):** `billing_edge.py` is a SEPARATELY DEPLOYED app (its own
  `app = FastAPI(...)` instance, deployed per
  `deploy/helm/dev-health/templates/billing-edge-deployment.yaml`) whose
  `/api/v1/billing/webhooks/stripe` route is a thin proxy calling the
  imported `stripe_webhook` from `billing/router.py` -- a real production
  shape, not exotic, but outside the same-file call-graph BFS's reach.
  Fixed with `discover_cross_file_forwarding_endpoints`: a *targeted*
  extension (not a general cross-file call-graph resolver) scoped to
  modules with their own `FastAPI()` instance, whose route handler calls a
  same-file-imported name that is ITSELF already a proven dispatch-relevant
  endpoint elsewhere (`_endpoint_function_names`). Added the missing
  `billing_edge.py` row.
- **The round-4 suffix-based path comparison false-passed unrelated
  same-tail paths, prefix changes, and HTTP method swaps (HIGH):** a row
  for `POST /sync` accepted `@router.post("/other/sync")` just because both
  end in `/sync`. Replaced the symmetric-suffix heuristic with exact
  comparison: each row now optionally records its own `route_mount_prefix`
  (default `""`, meaning the row's path already equals the decorator's
  exact local literal); content-drift strips that prefix from the row's
  recorded path and requires an EXACT match to the decorator/registration's
  literal argument, plus an exact HTTP method match
  (`_expected_rest_identity`). Only two real rows need a non-empty
  `route_mount_prefix` (pagerduty.py and the webhooks row below); every
  other REST row's surface path already equals its decorator's literal
  exactly.
- **The compressed multi-route webhook row skipped path identity entirely
  (MED):** `POST /webhooks/github|gitlab|jira` had no single literal to
  check, so changing the real `/github` decorator to `/sync` still passed.
  Fixed by re-anchoring that row to the one real route at its exact anchor
  line (`/github`, with `route_mount_prefix: "/webhooks"`) and documenting
  in its notes that the sibling `gitlab`/`jira` routes share the same
  dispatch mechanism rather than needing separate rows.
- **Dispatch-identity token matching was substring-based (MED):**
  `send_billing_notification_v2.delay()` satisfied the
  `send_billing_notification.delay` row just because the old name is a
  substring of the new one. Fixed with a word-boundary regex
  (`\bTOKEN\b`) instead of a plain `in` check.

## Known limitations

`ci/check_transitional_inventory.py` is a regex/line-based static scanner, not
a Python interpreter or a full AST-with-type-inference tool. Four full
rounds of adversarial review (see "Round-2/3/4/5 hardening" above) closed
every concrete evasion found against *this* codebase's actual patterns,
including same-file multi-hop API registration and one specific cross-file
shape (a second deployed FastAPI app forwarding to an imported handler),
both established as ordinary (not exotic) patterns here and now fully
handled. Per the team lead's direction, round-5 is the last automation
round: findings beyond these are triaged as follow-up in review rather than
chased with more scanner machinery. The following forms remain theoretically
possible and are **deliberately not chased further** -- the goal is that an
ordinary code change can't slip through unnoticed, not that the gate is
evasion-complete against a hypothetically adversarial contributor.
Reviewers: treat any of these shapes appearing in a PR as a reason to ask
for an explicit inventory row by hand.

- **Non-literal dynamic imports.** `importlib.import_module(some_variable)`
  (the module name comes from a variable, config value, or computed
  expression rather than a string literal) is undecidable statically and is
  not detected, for celery-canvas imports or anything else.
- **Aliasing beyond a simple bare `name = other_name` assignment.** A router
  or canvas-primitive alias introduced through a function return value, a
  container/dict lookup (`routers["v1"]`), a class attribute, or a decorator
  wrapper is not tracked by `_router_aliases`/`_scan_celery_canvas`'s bare-name
  alias propagation (capped at 4 fixed-point iterations, which comfortably
  covers realistic short alias chains but not an arbitrarily long or
  indirect one).
- **General cross-file API registration.** Same-file multi-hop call chains
  of any depth (up to the 12-hop backstop) ARE resolved, and one specific
  cross-file shape (a module with its own `FastAPI()` instance forwarding to
  an imported, already-proven dispatch-relevant handler) IS resolved as of
  round-5 (`discover_cross_file_forwarding_endpoints`). A general cross-file
  call-graph resolver is not implemented: a handler imported and called
  through a longer or less direct cross-file chain, or an `add_api_route(...)`
  registration whose handler comes from another module without being a
  thin one-line forward, is not resolved.
- **Dynamically constructed decorators.** `@getattr(router, method_name)(...)`
  or building the HTTP verb from a variable is not recognized -- only a
  literal `.get`/`.post`/`.put`/`.patch`/`.delete`/`.api_route` (or
  `add_api_route`/`add_route`) method name is. `@router.api_route(...,
  methods=[...])` and `add_api_route`/`add_route`'s `methods=[...]` are
  registration forms whose *method* isn't independently re-verified (it's a
  runtime list, not one literal) -- only that a route registration is still
  present at that anchor.
- **Bound-alias/partial forms combined with getattr or double indirection**
  (e.g. `partial(getattr(task, "apply_async"), ...)`) are not recognized by
  either the alias or the getattr-indirection pattern individually.

## What CUT-01 does **not** cover

Per the program-wide acceptance rules (implementation plan §5), an inventory
row's `acceptance_test_id` proves the surface is *correctly inventoried and
owned* -- it does not by itself prove production parity, durable idempotency,
or crash-window safety for that surface. Those remain the responsibility of
the CUT lane that owns the row's `target_owner` (CUT-03 through CUT-13, per
the implementation plan). Rows without dedicated behavioral tests reference
this contract's own CI test as their current acceptance test; deep behavioral
tests are added when the corresponding cutover lane lands.

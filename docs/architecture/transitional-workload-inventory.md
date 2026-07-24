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

`contracts/jobs/v1/transitional-inventory.json` has 152 rows: the 147 surfaces
discovered in the Wave-0 audit, plus 5 added during CUT-01 round-2 hardening
(a Codex adversarial review found a live dispatch the original scanner
missed -- see "Round-2 hardening" below). Classes: Celery task decorators,
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
  `@router.<method>(...)` decorator (correctly handling both single- and
  multi-line decorator argument lists via a bracket-depth-tracking scan, and a
  dispatch call sitting in a shared undecorated helper via one hop of
  same-file call-graph lookup), and GraphQL resolvers (which have no
  per-function route decorator at all) by anchoring to the call site itself;
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

When the whole inventory is finally decommissioned (TRD §8.3: "removed with
the final Celery cleanup after CI proves no legacy surface remains"), delete
this document, the contract file, the CI script, and its test module in the
same PR that removes the last Celery surface.

## What CUT-01 does **not** cover

Per the program-wide acceptance rules (implementation plan §5), an inventory
row's `acceptance_test_id` proves the surface is *correctly inventoried and
owned* -- it does not by itself prove production parity, durable idempotency,
or crash-window safety for that surface. Those remain the responsibility of
the CUT lane that owns the row's `target_owner` (CUT-03 through CUT-13, per
the implementation plan). Rows without dedicated behavioral tests reference
this contract's own CI test as their current acceptance test; deep behavioral
tests are added when the corresponding cutover lane lands.

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

`contracts/jobs/v1/transitional-inventory.json` has 147 rows, one per surface
discovered in the Wave-0 audit: Celery task decorators, Beat schedule entries
(19 unconditional + 1 conditional), literal dispatch calls (`.delay(`,
`.apply_async(`, `send_task(`, `.signature(`), the `getattr(x, "delay"|
"apply_async"|"send_task")` indirection form (12 of 34 dispatch call sites use
this grep-evading form), API dispatch endpoints, job registry kinds
(`contracts/jobs/v1/registry.json`), stream consumer surfaces, and
sync-dispatch transport routes (`contracts/sync-dispatch/v1/transport-routes.json`).

Each row has:

| Field | Meaning |
|---|---|
| `id` | `<class>:<file>:<line>`, stable identity for the row |
| `surface` | the task/entry/endpoint/kind name from the audit |
| `class` | one of the 9 discovered surface kinds (see below) |
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
4. a row's `source.file`/`source.line` anchor no longer exists on disk or is
   past the end of the (possibly shrunk) file (**staleness**).

Discovery specifically re-derives, from scratch:

- Celery task decorators (`@celery_app.task(` in `src/dev_health_ops/workers/`);
- Beat schedule entries, both the `beat_schedule = {...}` dict literal and the
  conditional `beat_schedule["..."] = {...}` rollout seam;
- literal dispatch calls (`.delay(`, `.apply_async(`, `send_task(`,
  `.signature(`), skipping docstring prose and multi-line-call continuations;
- the `getattr(x, "delay"|"apply_async"|"send_task")` indirection form;
- API trigger endpoints, by walking back from a dispatch call site under
  `src/dev_health_ops/api/` to its nearest `@router.<method>(...)` decorator;
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
(`ci/run_tests.sh unit`) without any separate workflow wiring. The same test
module proves the gate actually catches violations, using synthetic fixture
trees under `tmp_path` (never a real unowned surface committed to the repo):
an unowned Celery task, an unowned Beat entry, a duplicate exclusive-ownership
claim, a row with no target owner, and two shapes of stale anchor.

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

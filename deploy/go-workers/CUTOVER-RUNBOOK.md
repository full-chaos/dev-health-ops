# Celery-to-River cutover day

One document, start to finish, for the operator running the cutover. It exists
because the procedure was previously spread across four sources that each held
one part of it: the coexistence runbook in [`README.md`](./README.md), migration
`0066`'s docstring, the route mechanics in `docs/reference/cli/index.md`, and
`docs/operate/run/workers-and-jobs.md`. Those remain the reference material;
this is the order of operations.

**What this document covers:** moving the 23 checked-in worker job kinds from
Celery to River, verifying them, and rolling back.

**What it deliberately does not cover:**

- **Provider sync routes.** Every `WORKER_*_ENABLED` switch stays off. All 59
  provider/dataset pairs in `contracts/provider-matrix/v1/matrix.json` are
  `route_ready`, but enabling them is a separate, per-route decision with its
  own parity evidence. Cutover day does not flip one.
- **`sync.provider_unit`.** Migration `0066` deliberately leaves it on the
  `river_canary` route seeded by `0061`. Promoting it is its own reviewed
  change.
- **The four sync-dispatch kinds** (`dispatch_sync_run`, `finalize_sync_run`,
  `post_sync`, `reference_discovery`). Those live in
  `sync_dispatch_transport_routes`, which `0049` seeds `transport='celery'` and
  which no migration flips. They move only through
  `dev-health-workerctl routes apply`, separately from this procedure.
- **The scheduler.** Beat ownership is not transferred here.

---

## Preconditions

Do not start until every one of these is true. Each is a stop, not a warning.

1. **The coexistence canary has been run and held.** Follow
   [`README.md` § CHAOS-3052 deployment runbook](./README.md) to completion.
   Cutover day begins from a topology that has already been green.
2. **Every Go group that owns a retargeted queue is scaled up and ready.**
   This is the ordering requirement in `0066`'s own docstring: a kind routed to
   River stages envelopes in `worker_job_outbox` for the reconciler to insert.
   With no consumer for the owning queue, work accumulates unexecuted and
   nothing fails loudly.

   ```bash
   dev-health-workerctl workers queues status
   ```

   Confirm for each group: `queues`, `desired_replicas`, non-zero expiring
   `live_replicas`, `drain_state` clear, and connection-budget headroom.
3. **The reconciler is running and ready.** It is what relays outbox rows into
   River. A cutover with workers but no reconciler stages work nowhere.
4. **The application schema is current, with `0066` still pending.**

   ```bash
   python -m dev_health_ops.cli migrate postgres upgrade
   python -m dev_health_ops.cli migrate postgres status --check
   ```

   Without the opt-in below, this advances the `application_schema` branch and
   leaves the sibling `0066` River activation pending. **Never run
   `alembic upgrade head`** — the graph intentionally has multiple heads, and a
   direct run against a database with no Go workers would silently stop
   background processing for every retargeted kind.
5. **Baseline metrics captured**, so post-cutover numbers mean something:
   `worker_jobs_available`, `worker_job_oldest_age_seconds`,
   `worker_execution_saturation_ratio`, `worker_database_pool_saturation_ratio`.
6. **A rollback decision-maker is on the call** and the decision points below
   have been read in advance, not during an incident.

---

## Phase 1 — Drain Celery

`0066` cannot see a Celery message already in the broker. Draining is what
makes the cutover a handoff rather than an overlap.

Order matters:

1. **Stop Celery producers first** — the API and anything that enqueues.
   Draining consumers while producers keep enqueuing does not converge.
2. **Let the Celery consumers finish in-flight work.** Watch queue depth to
   zero. Do not kill workers mid-task; a partially-completed task is exactly
   the state neither runtime can reason about.
3. **Stop the Celery consumers and Beat.**
4. **Confirm the broker is empty** for every queue owning a retargeted kind.

> Do **not** use `workers queues drain` for this. That command pauses named
> queues for a whole Go group — it is for Go-side rollouts, not for Celery
> shutdown.

---

## Phase 2 — Run the cutover migration

The authorization is a one-shot, scoped to this run. There is **no** separate
interactive confirmation once it is present: with the variable set, any
migration run that reaches `0066` applies it.

```bash
# 1. SET — scoped to this command, not exported to a shell you keep using.
DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1 \
  python -m dev_health_ops.cli migrate postgres upgrade

# 2. VERIFY — the river_cutover branch head is now applied.
python -m dev_health_ops.cli migrate postgres status --check
```

Without the variable, `0066` raises rather than applying — that refusal is the
guard working, not a failure to diagnose.

**3. UNSET.** Remove the authorization from the environment, the deployment
manifest, the CI job, and any shell still open. Leaving it set turns every
future migration run into an unattended cutover. If your `migrate` service
mounts a live checkout of this repository, this is not hypothetical — see
[`README.md` § Live landmine](./README.md) and CHAOS-3143.

Confirm the variable is gone before continuing:

```bash
env | grep DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER   # must print nothing
```

---

## Phase 3 — Per-kind verification

`0066` retargeted 23 kinds at the time this runbook was written. Two of them,
`metrics.remaining.extra_metrics`/`metrics.remaining.team_metrics`, were
registered handlers with zero producer and were removed entirely by
CHAOS-4243 (not retargeted -- deleted from the registry, the worker, and this
list). The verification loop below reflects the current 21-kind registry;
`job-routes apply` is a verifying no-op for every one of them, which makes it
a safe read-back:

```bash
for kind in \
  investment.chunk investment.dispatch investment.finalize investment.materialize \
  metrics.daily_dispatch metrics.daily_finalize metrics.daily_partition \
  metrics.remaining.capacity metrics.remaining.complexity metrics.remaining.dora \
  metrics.remaining.membership_backfill \
  metrics.remaining.recommendations metrics.remaining.release_impact \
  operational.billing_notification operational.webhook_delivery \
  report.execute_on_demand report.execute_scheduled \
  sync.team_autoimport system.heartbeat system.retention_cleanup workgraph.build
do
  dev-health-workerctl job-routes status "$kind"
done
```

The authoritative list is `_KINDS` in
`src/dev_health_ops/alembic/versions/0066_activate_river_worker_job_routes.py`;
if it has changed, that file wins over the copy above.

For each kind confirm: `transport=river`, not paused, and the generation
incremented. Then confirm work is actually moving, not merely routed:

- `worker_job_outbox` is draining, not growing.
- `worker_jobs_available` falls and `worker_job_oldest_age_seconds` does not
  climb monotonically.
- Each retargeted queue shows `active_jobs` and completed work in
  `workers queues status`.

A kind routed to River with a growing outbox and no active jobs means the
consumer for its queue is missing — that is the failure mode Phase 1's
precondition 2 exists to prevent, and it is a rollback trigger.

---

## Rollback

### Decision points

Roll back if any of these hold after the cutover:

- A retargeted kind shows a growing `worker_job_outbox` with no corresponding
  River execution.
- `worker_job_oldest_age_seconds` climbs past its alert threshold for a queue
  and does not recover.
- A Go group cannot hold readiness, or a group's replicas crash-loop.
- Any retargeted kind produces terminal failures at a rate the pre-cutover
  baseline did not show.

### Per-kind rollback — the audited path, and the default

Prefer this. It proves quiescence per kind: `jobroute.Controller.Rollback`
refuses to move a route while `worker_job_outbox` holds pending or claimed rows
for the kind, or `worker_job_runs` shows it running. That refusal is the
protection against two runtimes owning the same work.

```bash
dev-health-workerctl job-routes rollback \
  --reason cutover_rollback \
  --correlation-id <change-id> \
  <kind>
```

If it refuses, the kind is still executing. Let it finish; do not force it.

### Wholesale downgrade — stop Go first

`0066`'s `downgrade()` is plain SQL and **does not prove quiescence**. Running
it while Go workers are live can leave two runtimes owning the same kind.

Order is not optional:

1. **Stop the Go workers and the reconciler.** All of them, for every
   retargeted queue.
2. Confirm no `worker_job_runs` rows are running and no `worker_job_outbox`
   rows are pending or claimed.
3. Only then run the downgrade of the `river_cutover` branch.
4. Restart the Celery consumers, Beat, and the producers.

The inverse of Phase 1: Go stops before routes move back, exactly as Celery
stopped before routes moved forward.

---

## After cutover

- **Compose:** keep the Celery worker, Beat, and Valkey DB 0 definitions in
  place. The `compose.go-workers-only.yml` overlay scales them to zero rather
  than deleting them, and Go-only remains a release gate, not a switch — see
  [`README.md` § Go-only is a release gate](./README.md).
- **Helm/Kubernetes (CHAOS-4195):** there is nothing to keep in place — the
  Celery `worker`/`beat` templates and manifests, and the
  `go-workers-only.yaml`/`values-go-workers-only.yaml` scale-to-zero
  overlays, are deleted. Helm/Kubernetes render only the Go topology; the
  release-gate discipline still governs when you scale a group above zero.
- `DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER` stays unset.
- For failures after this point, use
  `docs/operate/runbooks/worker-or-queue-failure.md`, which carries the
  post-cutover rollback scenario.

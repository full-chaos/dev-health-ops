# Sync dispatch transport routes v1

`transport-routes.json` is the language-neutral, validation-only contract for
the four existing sync-dispatch outbox wakeups. Its exact Draft 2020-12 shape
is defined by `transport-routes.schema.json`. It does not activate a production
transport, claim work, or publish a message.

All four kinds are checked in at `route: river` with `rollback_route: none`:
no Celery producer serves any of these kinds in any deployment, so there is
no supported rollback target left below `river`. Editing this artifact alone
still cannot activate or roll back a transport; that remains the audited
durable route-row controller's job (`internal/syncroute`).

All four wakeups are `at_least_once`. `post_sync` uses the same live-claim,
publish-or-insert, and terminal-mark transaction boundary as the other kinds.
On a publish or insert failure the claim is released with bounded backoff. The
post-sync consumers are generation-safe: readers select the newest compute
generation per logical key, so a re-drive cannot inflate their result.

## Narrow bridge calls are a dependency, not a fifth route

Two of the four native Go coordinator kinds also make a synchronous HTTP call
back into `src/dev_health_ops/api/internal/worker_sync.py` as part of their
OWN work, not as a routing decision this contract governs:

* `reference_discovery` calls `/reference-discovery-populate` (identifiers
  in, the populator's summary dict out) for the one step -- credential
  resolution and the team/sprint import -- that stays Python-side
  (CHAOS-4175, CHAOS-4198).
* `dispatch_sync_run` calls `/dispatch-budget-estimate` (identifiers in, the
  closed `BudgetEstimate` schema out) for the one step -- credential
  resolution and the six per-provider budget estimators -- that stays
  Python-side for the identical reason (CHAOS-4175, CHAOS-4198).

Neither call is `transport-routes.json`-governed: they are not claimed,
published, or retried through the outbox/transport-route machinery this
file describes, they carry no `route_generation`, and pausing or rolling
back a kind's transport route here has no effect on them. They are a plain
dependency of the Go worker's own business logic on Python-side machinery
that has not been ported yet (see CHAOS-4198's residual-bridge inventory),
gone once that porting lands -- not a parallel routing surface to reason
about alongside the four coordinator kinds above.

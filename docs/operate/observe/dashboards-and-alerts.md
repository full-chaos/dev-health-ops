---
page_id: op-alerts
summary: Build actionable dashboards and alerts from service objectives and recovery ownership.
content_type: task-guide
owner: platform-operations
source_of_truth:
  - alerts/rules.yml
applicability: current
lifecycle: active
---

# Dashboards and alerts

A dashboard should connect user impact with service, queue, provider, store, and data-progress signals.

An alert should include:

- affected service and environment;
- impact or violated objective;
- current value and threshold window;
- organization or provider scope when safe;
- runbook and owner;
- suppression or grouping behavior;
- evidence required to close.

Alert on sustained impact or recovery risk, not every transient retry. Test routing and runbook links regularly.

## Recovery seams need a PAIR of signals, not one

A recovery seam that reports what it *selected* separately from what it
*acted on* cannot be watched by either half alone, and `alerts/rules.yml`
carries three rules that exist because of one incident where it was.

Sync run `115e6246` held 17 provider units in `dispatching` for thirteen hours.
The unreclaimable sweep selected all 17 every second and terminalized none —
its default mode is `shadow`, no deploy shape had ever set
`SYNC_UNRECLAIMABLE_SWEEP`, and every tick logged `candidates=17
terminalized=0` at WARN with nothing reading it.

- `SyncDispatchUnreclaimableCandidatesNotTerminalizing` reads
  `sync_dispatch_unreclaimable_candidates` **against**
  `increase(sync_dispatch_unreclaimable_terminalized_total)`. The gauge alone is
  not a fault: a healthy sweep also shows candidates on the pass that finds
  work. What separates recovering from watching is whether the counter moves.
- `SyncDispatchUnreclaimableSweepFailing` is separate on purpose. On a failed
  pass the candidate gauge reads zero — identical to a healthy idle system — so
  the first rule is structurally unable to fire while the sweep is broken.
- `ProviderUnitStrandRearmsClimbing` watches
  `worker_outbox_reconciler_provider_unit_strands_rearmed_total`, the recovery
  side of the same strand.

**The delivery-attempt evidence is in logs, not in a metric.**
`worker_job_outbox.attempt_count` is a column and the reconciler exports
per-pass deltas, so there is no delivery-attempt gauge; run `115e6246` reached
624 delivery attempts on one row with nothing to alert on. Three log lines carry
the identifiers instead, and each rule's description names the one it needs:

| Line | Emitted by | Carries |
| --- | --- | --- |
| `syncreconciler.unreclaimable_sweep_mode_resolved` | reconciler startup | the resolved sweep `mode`, and `source` = `configured` or `default` |
| `provider unit strand rearmed after a terminal river delivery` | outbox reconciler, one per rearm | outbox id, dedupe key, dead River job id |
| `dispatch_sync_run.publish_hit_terminal_delivery` | dispatcher, one per unit | sync run id, unit id, provider, dataset |

Writing a rule against a metric nobody emits is worse than having no rule: it
never fires and it *looks* covered. the Go tests in `internal/syncreconciler/unreclaimable_sweep_test.go`
pins every metric name these rules read against the Go `WritePrometheus` bodies
that actually render them.

## Auth service rules

`alerts/rules.yml` carries an `auth_service` group over the auth service's operator `/metrics`: target down, scrape absent, a sustained rate-limit rejection rate, and a 5xx ratio above 5% that only fires with real traffic. Rate-limit store errors are alerted in the `auth_service_rate_limit` group (a warning at any error, and a critical tier above 20 in ten minutes kept beside it). Every rule of the `auth_service` group and the critical tier select `job="auth-service"`, and the only label they group by is the `limit` id.

The file is the text of record: no Prometheus loads it in production. The production rules are created in SigNoz from this text. A test pins every `dev_health_api_` metric name a rule reads against the series the service really exposes, so a rule on a name nobody emits fails the build instead of looking covered. The scrape series `up` (and `absent(up)`) are scrape metadata, not exposed by the service: the test checks their rule shape, and they depend on the collector's job name `auth-service`. The test also pins each rule's threshold shape and `for:` duration, that no rule result carries the scrape instance, and that the 5xx rule keeps its traffic floor.

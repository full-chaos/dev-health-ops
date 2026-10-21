# Runtime

Intro.

<!-- BEGIN GENERATED QUEUE MAP -->
| Go process (binary) | Go queue | Job kind(s) | Timeout(s) | Max attempts | Historical Celery queue(s) | Historical Celery consumer(s) | Plane / route status |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `heavy` (`bin-heavy`) | `investment` | `investment.materialize` | 7200 | 3 | — | — | Go-native -- no Celery predecessor<br>`investment.materialize`: state=`celery_removed` ⚠, route=`river`, rollback_route=`none` (migration-state.json) |
| `heavy` (`bin-heavy`) | `metrics` | `metrics.a`<br>`metrics.b` | 600-900 | 5 | `metrics`, `backfill` | `worker-heavy` | Celery dormant since 2026-08-19 (CHAOS-4026); Go/River live<br>backfill's family (metrics.remaining.membership_backfill) rides this queue, not a dedicated Go 'backfill' queue.<br>`metrics.a`: state=`go_default`, route=`river`, rollback_route=`none` (migration-state.json)<br>`metrics.b`: state=`go_default`, route=`river`, rollback_route=`none` (migration-state.json) |
| `heavy` (`bin-heavy`) | `reports` | `reports.run` | 300 | 3 | `reports` | `worker` | Celery dormant since 2026-08-19 (CHAOS-4026); Go/River live<br>`reports.run`: state=`go_default`, route=`river`, rollback_route=`none` (migration-state.json) |
| `heavy` (`bin-heavy`) | `workgraph` | `workgraph.x` | 120 | 2 | `default` | `worker` | Celery dormant since 2026-08-19 (CHAOS-4026); Go/River live<br>work_graph_tasks.py routed through the shared 'default' catch-all, not a dedicated queue.<br>`workgraph.x`: state=`go_default`, route=`river`, rollback_route=`none` (migration-state.json) |
| `ops` (`bin-ops`) | `coverage` | `coverage.c` | 60 | 1 | — | — | Go-native -- no Celery predecessor<br>`coverage.c`: state=`go_default`, route=`river`, rollback_route=`none` (migration-state.json) |
| `ops` (`bin-ops`) | `heartbeat` | `heartbeat.h` | 30 | 1 | `default` | `worker` | Celery dormant since 2026-08-19 (CHAOS-4026); Go/River live<br>system_ops.phone_home_heartbeat routed through 'default', not a dedicated queue.<br>`heartbeat.h`: state=`go_default`, route=`river`, rollback_route=`none` (migration-state.json) |
| `ops` (`bin-ops`) | `retention` | — | — | — | — | `beat` | Go-native consolidated sweep; historical retention work was several discrete Beat-scheduled tasks (retired under CHAOS-4026, e.g. ask-dev-retention-sweep) |
| `ops` (`bin-ops`) | `webhooks` | `webhooks.w` | 30 | 4 | `webhooks` | `worker` | Celery dormant since 2026-08-19 (CHAOS-4026); Go/River live<br>`webhooks.w`: state=`go_default`, route=`river`, rollback_route=`none` (migration-state.json) |
| `reconciler` (`bin-rec`) | `—` | — | — | — | — | — | Go-native -- no Celery predecessor<br>Control loop, not a River queue -- no -Q for this process. |
| `scheduler` (`bin-sched`) | `—` | — | — | — | `scheduler` | `beat` | Celery Beat retired 2026-08-21 (CHAOS-4026); Go scheduler is sole production owner<br>Control loop, not a River queue -- no -Q for this process. |
| `stream-external` (`bin-se`) | `—` | — | — | — | `external-ingest` | `worker-external-ingest` | Celery dormant since 2026-08-19 (CHAOS-4026); Go stream runner live<br>Valkey stream consumer, not a River queue -- no -Q for this process. |
| `stream-ingest` (`bin-si`) | `—` | — | — | — | `ingest` | `worker-ingest` | Celery dormant since 2026-08-19 (CHAOS-4026); Go stream runner live<br>Valkey stream consumer, not a River queue -- no -Q for this process. |
| `stream-pagerduty` (`bin-sp2`) | `—` | — | — | — | — | — | Go-native -- no Celery predecessor<br>Valkey stream consumer, not a River queue -- no -Q for this process. |
| `sync` (`bin-sync`) | `sync` | `sync.s` | 3600 | 3 | `sync` | `worker` | Celery dormant since 2026-08-19 (CHAOS-4026); Go/River live. Historically also the shared fallback queue for all providers with PROVIDER_SYNC_QUEUES_ENABLED off.<br>`sync.s`: state=`go_default`, route=`river`, rollback_route=`none` (migration-state.json) |
| `sync-provider` (`bin-sp`) | `sync_provider` | `sync.provider_unit` | 3600 | 3 | `sync.github`, `sync.gitlab`, `sync.linear`, `sync.jira`, `sync.launchdarkly`, `sync.github.light`, `sync.github.medium`, `sync.github.heavy`, `sync.gitlab.light`, `sync.gitlab.medium`, `sync.gitlab.heavy`, `sync.jira.medium`, `sync.linear.medium` | `worker`, `worker-heavy` | Celery fleet-wide dormant since 2026-08-19 (CHAOS-4026); Go/River live. This queue's one kind (`sync.provider_unit`) was still `canary` per-kind for a time after the fleet-wide Celery retirement -- its own migration has since completed too (see below). Enablement is -Q topology plus the user's own sync config: CHAOS-4054 deleted the provider/dataset WORKER_*_ENABLED switch plane outright, so a shipped route is always executable and nothing hides it behind an environment flag.<br>The per-provider cost-class split (light/medium/heavy) has no Go equivalent yet -- CHAOS-4027, parked. All cost classes collapse onto the single sync_provider queue in Go.<br>`sync.provider_unit`: state=`canary` ⚠, route=`river_canary`, rollback_route=`celery` (migration-state.json) |

Celery queues carrying no work reachable through a Go queue at all (telemetry, not routed work):

| Celery queue | Why it has no Go queue |
| --- | --- |
| `monitoring` | queue-depth telemetry; superseded by native worker_jobs_available / worker_job_oldest_age_seconds / worker_execution_saturation_ratio metrics, not a queue |
<!-- END GENERATED QUEUE MAP -->

Tail text.

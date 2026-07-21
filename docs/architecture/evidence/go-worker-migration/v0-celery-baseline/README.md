# v0 Celery baseline

This artifact defines the reproducible baseline that River task families must
meet or improve. It intentionally contains no claimed production numbers yet.
The machine-readable [`capture.json`](capture.json) therefore remains
`not_recorded`, and that state blocks every production canary.

A separate [`local-resource-snapshot.json`](local-resource-snapshot.json)
records the 2026-07-20 local one-shot resource observation. It proves the
capture shape only; it is not a latency, load, SLO, or production sizing
baseline. In that capture, `worker-wi` is the concurrency-2 work-item/provider
medium worker, not the external-ingest worker.

## Evidence boundary

Local Compose output is useful for checking commands and metric availability,
but it is not representative of production traffic, worker sizing, PostgreSQL
load, provider latency, or rollout behavior. Production capture must use a
representative UTC window that includes normal scheduled and interactive load
and at least one worker deployment. Record the exact source revision, window,
query backend, label substitutions, and missing series in `capture.json`.

Never commit raw query responses or logs. Reduce them to aggregate, redacted
values in `capture.json`; retain raw evidence only in the approved restricted
operations system.

## PromQL capture

[`queries.promql`](queries.promql) contains the canonical queries for task
throughput/outcomes, successful-task duration, worker resource use, restarts,
and PostgreSQL load. Use the Prometheus range-query API so the time window is
explicit and repeatable:

```bash
export BASELINE_PROMETHEUS_URL="https://prometheus.example.invalid"
export BASELINE_START="2026-07-01T00:00:00Z"
export BASELINE_END="2026-07-08T00:00:00Z"
export BASELINE_STEP="60s"

curl --fail --silent --show-error --get \
  "${BASELINE_PROMETHEUS_URL%/}/api/v1/query_range" \
  --data-urlencode 'query=sum by (task_name, state) (rate(devhealth_celery_tasks_total[15m]))' \
  --data-urlencode "start=${BASELINE_START}" \
  --data-urlencode "end=${BASELINE_END}" \
  --data-urlencode "step=${BASELINE_STEP}"
```

If the backend requires authentication, use the approved secret-bearing client
configuration; do not paste an authorization header, cookie, or signed URL into
this repository. Repeat the range request for each expression in
`queries.promql`. Record absent series as an observability gap.

The 2026-07-20 local check of the API `/metrics` endpoint exposed HELP/TYPE
metadata for `devhealth_celery_*` but no samples. The custom counters and
histograms are process-local to worker processes, and workers expose no scrape
endpoint. Therefore an empty PromQL result is an observability gap, not zero
traffic. A real production capture must use available OTLP/trace evidence or
add an export/scrape path before relying on those queries.

The current worker instrumentation also does **not** expose queue depth,
oldest-message age, enqueue-to-start latency, stream pending age, or deploy
recovery as Prometheus series. Do not fabricate PromQL for those signals. The
commands and structured-log captures below are the v0 source of truth until a
sanitized exporter exists.

## Local command rehearsal

These commands verify the capture procedure against the local Compose stack.
Their results must be labeled `local` and must not populate production values:

```bash
docker compose ps
docker compose exec -T worker celery \
  -A dev_health_ops.workers.celery_app inspect ping
docker compose exec -T worker celery \
  -A dev_health_ops.workers.celery_app inspect active
docker compose exec -T worker celery \
  -A dev_health_ops.workers.celery_app inspect reserved
docker compose stats --no-stream \
  worker worker-ingest worker-wi worker-heavy beat
curl --fail --silent http://localhost:8010/metrics \
  | rg '^(# (HELP|TYPE) )?devhealth_celery_'
docker compose logs --since 24h \
  worker worker-ingest worker-wi worker-heavy beat \
  | rg 'queue_depth|queue_backlog|external_ingest_stream_health'
```

The scheduled `monitor_queue_depths` task logs every non-empty declared queue's
depth and, when the `enqueued_at` header is present, oldest-message age. Keep the
raw log outside git and record only per-profile percentiles/maxima plus the
window and log query used.

For stream lag, enumerate Valkey database 1 stream keys without committing the
output, then query each expected consumer group with `XINFO GROUPS` and
`XPENDING`. Hash or remove tenant-bearing stream-key components before recording
aggregates:

```bash
docker compose exec -T valkey valkey-cli -n 1 \
  --scan --type stream --pattern 'ingest:*'
docker compose exec -T valkey valkey-cli -n 1 \
  --scan --type stream --pattern 'product-telemetry:*'
docker compose exec -T valkey valkey-cli -n 1 \
  --scan --type stream --pattern 'external-ingest:*'
```

## Production-only capture

Production evidence must additionally record:

- enqueue-to-start and oldest-queue-age percentiles by workload profile;
- task success, retry, failure, and discard rates;
- worker CPU and memory by profile;
- PostgreSQL connections, transaction rate, lock/I/O pressure, and saturation;
- process-crash recovery and one deployment's drain/recovery duration;
- sync lease-expiry rate;
- stream pending count, lag, and oldest pending age.

Use the production log backend for `queue_depth`, `queue_backlog`, stream-health,
and rollout events. Record the exact redacted query and UTC window in the
restricted evidence system, then write only aggregates and its evidence
reference into `capture.json`.

If `devhealth_celery_*` remains process-local in production, capture task
outcomes/durations from the approved OTLP/trace backend or add instrumentation
that exports them. The production evidence must name that source; API
HELP/TYPE metadata without samples does not satisfy the gate.

## Gate

- Missing production baseline does not by itself block Phase 1 foundation work.
- It does block shadow-to-canary promotion and every production canary.
- Empty/missing series require either an instrumentation fix or an explicitly
  approved alternate measurement before the canary gate can open.

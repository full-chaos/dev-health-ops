# v0 Celery baseline

This directory contains the reproducible Celery baseline that replacement
worker stacks must meet or improve. The running local Compose project
`dev-health` is the designated production-equivalent runtime for this phase:
it runs the real Celery workers against the shared representative dataset, so
the capture does not require a separate hosted production environment.

[`capture.json`](capture.json) is generated evidence, not a hand-authored SLO.
Its measurements describe what the designated runtime actually did, including
backlogs and failures. Reviewers choose parity thresholds separately; an
unhealthy observed value must not silently become an acceptable target.

## Capture

From the `dev-health-ops` repository root, run:

```bash
.venv/bin/python scripts/worker/capture_celery_baseline.py \
  --project dev-health \
  --duration-seconds 300 \
  --interval-seconds 30 \
  --history-seconds 86400
```

The recorder is read-only with respect to the running stack. It does not stop,
restart, rebuild, scale, drain, enqueue, acknowledge, delete, or purge
anything. It:

- streams retained worker logs through an in-memory reducer and keeps only
  task-family counts and successful-duration summaries;
- reads Celery queue depth and the stamped oldest-message age through Kombu;
- reads Valkey stream/group depth, pending count, lag, and oldest pending age;
- samples Docker CPU/memory and process counts;
- reads PostgreSQL cumulative statistics, locks, and aggregate sync-lease
  counters; and
- asks Celery inspect only for aggregate worker/active/reserved/scheduled
  counts.

The output is written atomically only after schema and redaction validation.
The recorder never writes raw logs, task IDs, task arguments/results, stream
keys, tenant IDs, credentials, DSNs, container environments, or absolute host
paths. [`capture.schema.json`](capture.schema.json) and
`tests/worker_baseline/test_capture_celery_baseline.py` lock that contract.

## Comparing concurrent stacks

The project name and output path are parameters, so an alternate Celery
Compose project with the same service topology can be sampled against the same
dataset without replacing this baseline:

```bash
.venv/bin/python scripts/worker/capture_celery_baseline.py \
  --project alternate-worker-project \
  --output /tmp/alternate-worker-capture.json
```

Keep alternate captures outside git unless they are deliberately promoted to
a versioned evidence artifact. A Go stack uses its own runtime probe and emits
the matching normalized measurement paths; it does not run this Celery-specific
recorder. Compare matching measurement paths and capture windows; do not
compare a five-minute resource sample with a one-shot value or an unrelated
historical log window.

The read-only observer comparison now freezes one UTC cutoff and one limit
before either runtime reads. It imports one exported PostgreSQL snapshot in
Python and reads that same snapshot through the Go exporter transaction, then
compares only the redacted parity fields: cutoff, limit, predicate and digest
versions, digest, truncated state, sampled count, and per-kind aggregate
counts. Raw candidate IDs, payloads, tenant data, claim tokens, and source URLs
are never compared or stored. The first result is recorded in the
[v2 sync-dispatch parity evidence](../v2-sync-dispatch-parity/README.md).

## Evidence boundary

The production-equivalent designation applies to this shared dataset and
runtime session. The recorder still records the bound source revision, image
IDs, service start times, restart counters, history window, live-sampling
window, and whether the source worktree was dirty. A bind-mounted source
revision is the Git HEAD observed at capture time; it cannot independently
prove which modules were already imported by a process that started earlier.

Raw logs are intentionally not retained. This means explicit Celery terminal
markers can produce success/retry/failure/discard aggregates, but failure
reasons and payloads cannot be reconstructed from the checked-in evidence.

The historical [`local-resource-snapshot.json`](local-resource-snapshot.json)
is the earlier one-shot rehearsal. It is superseded for comparisons by the
multi-sample resource fields in `capture.json` and remains only as provenance.

## Explicit observability gaps

The recorder does not invent signals that the current workers do not emit:

- enqueue-to-start latency is unavailable because the enqueue timestamp is not
  present in completion logs or Celery inspect output;
- process-crash recovery is unavailable until a naturally occurring or
  separately authorized controlled crash is observed;
- deployment drain duration is unavailable because no drain-start marker is
  emitted (worker start-to-ready can still be measured when retained logs
  contain the ready marker);
- discard rate is a lower bound from explicit revoked/rejected/ignored log
  markers; and
- `devhealth_celery_*` metrics remain process-local and are not exported by
  worker scrape endpoints.

Each gap is machine-readable in `capture.json`. A missing signal is never
encoded as zero.

[`queries.promql`](queries.promql) remains the canonical query set for a
deployment that exports the same measurements through Prometheus. It is an
alternate source, not a requirement for the designated Compose baseline.

## Gate

- Phase 1 foundation is complete. Baseline capture and comparisons continue
  as independent promotion evidence.
- `capture.json` is authoritative evidence for the designated
  production-equivalent session.
- Production canary approval remains false until required observability gaps
  are resolved or explicitly accepted and parity thresholds are reviewed.

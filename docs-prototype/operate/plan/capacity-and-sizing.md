---
page_id: op-sizing
summary: Size API, worker, queue, and data-store capacity from measured workload and retry behavior.
content_type: reference
owner: platform-operations
source_of_truth:
  - docs/architecture/worker-scaling-readiness.md
  - current worker and synchronization implementation
applicability: current
lifecycle: active
---

# Capacity and sizing

Base initial capacity on:

- connected organizations and repositories;
- historical backfill size and incremental cadence;
- provider request and cost budgets;
- worker concurrency, queue depth, job duration, retry, and lease behavior;
- API request rate and query cost;
- data-store ingest, retention, compaction, and query load;
- model or external-service rate and spend limits.

Scale from observed queue age, saturation, latency, error rate, and data-store health. Increasing concurrency without provider, queue, or storage capacity can make recovery slower.

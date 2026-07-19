---
page_id: op-metrics
summary: Monitor throughput, latency, errors, saturation, queue age, provider budgets, storage, synchronization, and model usage.
content_type: reference
owner: platform-operations
source_of_truth:
  - docs/architecture/platform-sync-observability.md
  - docs/architecture/sync-usage-actuals.md
  - docs/llm/investment-llm-telemetry.md
  - docs/llm/spend-observability.md
applicability: current
lifecycle: active
---

# Metrics and traces

Monitor at minimum:

- API request rate, latency, errors, and query timeouts;
- worker throughput, duration, retry, terminal failure, and saturation;
- queue depth and oldest age;
- provider request, cost, deferral, rate-limit, and authentication signals;
- data-store connection, write, query, compaction, and storage health;
- synchronization planned, dispatched, running, stale, completed, and failed units;
- model latency, error, circuit-breaker, token or usage, and spend where applicable.

Use tenant-safe labels. Avoid high-cardinality payload values and secrets.

---
page_id: op-rb-perf
summary: Recover saturation, provider throttling, or performance degradation without creating retry amplification.
content_type: runbook
owner: platform-operations
applicability: current
lifecycle: active
---

# Performance, rate limit, or saturation

1. Identify the constrained service, queue, provider bucket, store, or external service.
2. Check latency, error, saturation, queue age, deferrals, retry rate, and provider response headers.
3. Reduce or defer optional workload before increasing concurrency.
4. Confirm cache, query, batch, and connection behavior.
5. Recover a bounded workload and watch the limiting signal.
6. Restore normal budgets gradually.

Do not bypass provider budgets or rate limits in a way that hides delayed coverage or creates an outage loop.

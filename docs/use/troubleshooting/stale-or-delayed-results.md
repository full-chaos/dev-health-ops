---
page_id: use-stale
summary: Determine whether a result is old, still processing, or no longer advancing.
content_type: troubleshooting
owner: product-analytics
applicability: current
lifecycle: active
---

# Stale or delayed results

1. Record the last-computed, source, run, or synchronization time shown by the product.
2. Compare it with the selected time window and expected source cadence.
3. Confirm whether a report, ingestion, backfill, or computation is still active.
4. Avoid repeated manual retries while status is advancing.
5. Escalate to [Check synchronization status and freshness](../../admin/sync-and-coverage/status-and-freshness.md) when the user-level state cannot explain the delay.

A stale value exists but is old. A delayed value is not complete yet. Neither should be represented as current or as zero.

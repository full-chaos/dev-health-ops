---
page_id: op-rb-ingestion
summary: Safely diagnose a platform-level ingestion failure after user and administrator checks are complete.
content_type: runbook
owner: platform-operations
source_of_truth:
  - current worker, queue, ingestion, and observability implementation
applicability: current
lifecycle: active
---

# Recover from ingestion failure

## Trigger

Use this runbook when a configured source and scope are valid but ingestion or downstream processing is failed, repeatedly retrying, or no longer advancing.

## Immediate safety

- Do not rotate, print, or copy credentials into the incident record unless the credential is confirmed compromised and the approved rotation procedure applies.
- Do not run destructive reprocessing, deletion, or database repair commands from documentation alone.
- Preserve job identifiers, timestamps, source identifiers, sanitized errors, and the last known successful checkpoint.

## Diagnose

1. Confirm the failure is platform-level rather than a user filter or workspace-configuration problem.
2. Identify the affected source, organization or workspace, queue or worker class, and time range.
3. Check health, logs, retries, rate limits, authentication failures, and downstream storage availability.
4. Determine whether processing is stopped, delayed, partially progressing, or repeatedly replaying the same unit.
5. Use only the supported operational control for retry, replay, or backfill.
6. Verify new records advance beyond the retained checkpoint and that the product freshness state recovers.

## Escalate

Escalate when the safe supported control does not restore progress, when repeated retries risk duplication or provider pressure, or when storage, migration, tenant-isolation, or credential safety is uncertain.

## Close

Record the cause, affected interval, recovery action, verification evidence, residual gap, and whether a backfill or customer communication remains required.

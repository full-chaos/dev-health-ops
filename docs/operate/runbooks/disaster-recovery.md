---
page_id: op-rb-dr
summary: Restore a replacement environment from approved recovery points and verify end-to-end service and data progress.
content_type: runbook
owner: platform-operations
applicability: current
lifecycle: active
---

# Disaster recovery

1. Declare the incident, owner, recovery objectives, and communication path.
2. Freeze destructive changes and preserve evidence.
3. Select the recovery point and compatible application revision.
4. Restore stores, configuration references, secret access, network, API, workers, and schedules in the approved order.
5. Verify tenant isolation, schema, health, queue state, provider connectivity, data progress, and representative product results.
6. Reconcile the outage interval and run required bounded backfills.
7. Cut traffic only after the acceptance checklist passes.
8. Retain the failed environment until investigation and data-retention decisions permit removal.

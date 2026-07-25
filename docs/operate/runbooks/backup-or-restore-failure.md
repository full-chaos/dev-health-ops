---
page_id: op-rb-backup
summary: Recover when a backup is incomplete, unreadable, unverified, or a restore does not produce a healthy platform.
content_type: runbook
owner: platform-operations
applicability: current
lifecycle: active
---

# Backup or restore failure

1. Preserve the failed artifact, logs, manifest, encryption context, and recovery target.
2. Determine whether the failure is capture, transfer, retention, key access, integrity, compatibility, or restore verification.
3. Do not overwrite the last known good recovery copy.
4. Retry into an isolated target using the approved procedure.
5. Verify schema, data integrity, API, workers, source coverage, and representative queries.
6. Record the remaining recovery-point gap.

Escalate immediately when no verified recovery copy remains or encryption keys are unavailable.

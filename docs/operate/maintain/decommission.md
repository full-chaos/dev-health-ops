---
page_id: op-decommission
summary: Remove or replace an environment while preserving required evidence and revoking access.
content_type: task-guide
owner: platform-operations
source_of_truth:
  - docs/ops/customer-offboarding.md
applicability: current
lifecycle: active
---

# Decommission or replace an environment

1. Confirm the approved retention, export, deletion, and communication requirements.
2. Stop new synchronization and scheduled work.
3. Capture required final backups and audit evidence.
4. Revoke provider installations, tokens, model credentials, signing keys, and service accounts.
5. Remove DNS, callbacks, access policies, and deployment resources in the approved order.
6. Verify data and backups are retained or deleted as required.
7. Record completion, residual dependencies, and owner.

Do not remove the only recovery copy before the retention decision is verified.

---
page_id: op-hardening
summary: Harden secrets, identities, network paths, stores, images, dependencies, logs, and backups.
content_type: task-guide
owner: security
source_of_truth:
  - docs/security/credential-encryption-rotation.md
  - current deployment and credential implementation
applicability: current
lifecycle: active
---

# Production hardening

- Use immutable reviewed images and supported dependency versions.
- Store and encrypt secrets with least-privilege service identities.
- Restrict data stores, queues, metrics, and administrative routes.
- Enforce TLS and narrow trusted-proxy configuration.
- Separate application, migration, backup, and operator permissions.
- Redact logs and protect backups with tested access and retention.
- Monitor credential use, authentication failures, and unexpected egress.
- Patch and rotate on an owned schedule.

Test recovery and rotation; a control that cannot be operated safely during an incident is incomplete.

---
page_id: op-prereq
summary: Confirm compute, container, data-store, queue, network, provider, and recovery prerequisites.
content_type: task-guide
owner: platform-operations
applicability: current
lifecycle: active
---

# Installation prerequisites

Before installation, document and verify:

- the supported deployment artifact and image source;
- container runtime or orchestration access;
- primary data store and migration permissions;
- Redis or the supported queue/rate-limit store for non-development environments;
- outbound access to required provider APIs and external services;
- ingress, DNS, TLS, proxy, and trusted-proxy design;
- secret-manager and credential ownership;
- backup, restore, rollback, and log-retention locations;
- expected organizations, repositories, volume, and synchronization cadence.

Do not copy example credentials or sample hostnames into production unchanged.

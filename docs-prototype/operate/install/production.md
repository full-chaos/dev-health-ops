---
page_id: op-production
summary: Deploy an immutable reviewed revision using a supported repository deployment artifact.
content_type: task-guide
owner: platform-operations
source_of_truth:
  - docs/ops/deployment-guide.md
  - deploy/kubernetes/
  - deploy/docker-compose/
  - deploy/docker-swarm/
applicability: current
lifecycle: active
---

# Install a production environment

1. Select the supported deployment artifact for the environment.
2. Pin the reviewed application image or revision.
3. Configure external secrets rather than embedding credentials in manifests.
4. Configure data stores, queues, ingress, TLS, trusted proxies, workers, and schedules.
5. Apply migrations through the supported release procedure.
6. Deploy API and worker services.
7. Verify health before enabling provider synchronization or user traffic.
8. Retain the prior artifact and rollback procedure.

Examples in legacy deployment material are source evidence; review their versions, defaults, and security assumptions before use.

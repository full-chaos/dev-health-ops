---
page_id: op-local
summary: Evaluate the platform locally with repository Compose artifacts and non-production data.
content_type: tutorial
owner: platform-operations
source_of_truth:
  - compose.yml
  - deploy/docker-compose/
applicability: current
lifecycle: active
---

# Evaluate locally

1. Check out a reviewed repository revision.
2. Use the repository Compose artifact and example configuration as a starting point.
3. Supply non-production credentials and isolated data stores.
4. Start the API, workers, queue, and required stores.
5. Verify `/health`, logs, worker readiness, and one bounded source synchronization.
6. Remove local credentials and data when evaluation ends.

A local evaluation is not a production hardening, backup, availability, or scale test.

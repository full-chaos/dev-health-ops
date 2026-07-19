---
page_id: op-healthchecks
summary: Use liveness, readiness, dependency, worker, and data-progress checks for distinct decisions.
content_type: reference
owner: platform-operations
applicability: current
lifecycle: active
---

# Health checks

The deployment material exposes `/health` for API health. Do not use one endpoint as proof that the whole platform is ready.

Check separately:

- API liveness and readiness;
- data-store and queue connectivity;
- migration compatibility;
- worker and scheduler readiness;
- provider authentication;
- oldest queue age and job progress;
- latest successful synchronization and product freshness.

Route health endpoints so they are available to the orchestrator and operators without exposing unnecessary internal detail publicly.

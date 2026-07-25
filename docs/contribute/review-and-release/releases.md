---
page_id: con-release
summary: Build an immutable release, apply migrations in the approved order, verify health and data progress, and retain rollback evidence.
content_type: task-guide
owner: engineering
applicability: current
lifecycle: active
---

# Release and rollback expectations

1. Select the reviewed immutable revision and artifact.
2. Record schema, configuration, API, provider, feature, and documentation compatibility changes.
3. Back up required data and preserve the prior artifact.
4. Apply migrations through one controlled release path.
5. Deploy and verify API, workers, queues, stores, source progress, and representative product tasks.
6. Monitor the stabilization window.
7. Roll back only when schema and data compatibility permit it.

Release notes must distinguish new, changed, deprecated, removed, fixed, and known-limited behavior.

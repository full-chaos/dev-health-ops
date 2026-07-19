---
page_id: op-controls
summary: Use rate, queue, budget, feature, and retry controls without creating hidden data gaps.
content_type: task-guide
owner: platform-operations
source_of_truth:
  - docs/architecture/launchdarkly-sync-budgeting.md
  - current synchronization budget and queue controls
applicability: current
lifecycle: active
---

# Safe operational controls

For each control, record the purpose, unit, default, safe range, owner, observability signal, and rollback.

- Provider budget controls should defer work visibly rather than silently drop it.
- Queue routing must match deployed consumers.
- Concurrency changes must respect provider and store limits.
- Retry changes must preserve idempotency and terminal-failure behavior.
- Feature flags must have an owner, expiry or review trigger, and observable effect.

Change one control at a time during recovery unless an approved incident plan says otherwise.

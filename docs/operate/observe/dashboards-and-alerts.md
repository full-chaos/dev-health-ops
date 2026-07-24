---
page_id: op-alerts
summary: Build actionable dashboards and alerts from service objectives and recovery ownership.
content_type: task-guide
owner: platform-operations
source_of_truth:
  - docs/alerting.md
  - docs/slos.md
applicability: current
lifecycle: active
---

# Dashboards and alerts

A dashboard should connect user impact with service, queue, provider, store, and data-progress signals.

An alert should include:

- affected service and environment;
- impact or violated objective;
- current value and threshold window;
- organization or provider scope when safe;
- runbook and owner;
- suppression or grouping behavior;
- evidence required to close.

Alert on sustained impact or recovery risk, not every transient retry. Test routing and runbook links regularly.

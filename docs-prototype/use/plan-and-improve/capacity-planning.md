---
page_id: use-capacity
summary: Use the current Completion Forecast without presenting a probabilistic window as a promise.
content_type: workflow-guide
owner: product-analytics
source_of_truth:
  - current /plan/capacity product surface
  - current Monte Carlo throughput forecast implementation
applicability: current
lifecycle: active
---

# Use Completion Forecast

The current Plan destination exposes **Completion Forecast** at `/plan/capacity`.

1. Confirm the backlog or work set, team or repository scope, and historical period.
2. Read the forecast range and probability labels shown in the current view.
3. Check whether throughput history is representative of the planned work.
4. Run scenarios only when the changed assumption is explicit.
5. Communicate a range and confidence, not a promised date.

A forecast can become unreliable when scope, work size, staffing, workflow, or source coverage changes materially.

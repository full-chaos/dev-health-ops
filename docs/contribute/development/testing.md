---
page_id: con-tests
summary: Select unit, integration, contract, live-like, browser, and migration tests based on the risk being changed.
content_type: task-guide
owner: engineering
source_of_truth:
  - current tests and CI workflows
applicability: current
lifecycle: active
---

# Testing layers and local validation

- Unit tests protect deterministic business logic and parsing.
- Integration tests protect store, provider, queue, API, and migration boundaries.
- Contract tests protect public schema, authorization, tenant isolation, idempotency, and compatibility.
- Live-like tests protect behavior that fixtures cannot reproduce, such as real provider permissions or deployment routing.
- Browser tests protect a small set of essential user tasks and accessibility interactions.

Test the failure path and denied path, not only the happy path. Avoid tests that freeze exact prose, decorative composition, or screenshot hashes.

---
page_id: ref-provider-limits
summary: Provider scope, rate, pagination, backfill, record-family, and authentication constraints.
content_type: reference
owner: platform-api
source_of_truth:
  - current provider clients, sync planners, budgets, and connector inventory
applicability: current
lifecycle: active
---

# Provider limits

For each provider and connection type, record:

- supported host or deployment type;
- authentication method and minimum scopes;
- organizations, groups, projects, repositories, and record families supported;
- pagination and historical-access constraints;
- request, GraphQL-cost, abuse, search, or route-family budgets;
- incremental watermark and bounded-backfill behavior;
- webhook or polling support;
- deletion and reconciliation behavior;
- current limitations and known incompatibilities.

Provider limits change. Generate values from the current client and configuration where possible and identify the review date.

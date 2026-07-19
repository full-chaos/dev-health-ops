---
page_id: ref-work-graph
summary: Canonical Work Graph node, relationship, identity, source, and attribution concepts.
content_type: reference
owner: platform-api
source_of_truth:
  - current Work Graph IDs, builder, GraphQL schema, and team-attribution contracts
applicability: current
lifecycle: active
---

# Work Graph model

Work Graph connects supported work, source, and organizational records through stable tenant-scoped identities.

Concepts include:

- organization-scoped node identity;
- repositories, commits, pull requests, reviews, work items, teams, and identities;
- provider-native IDs retained for reconciliation;
- typed relationships with explicit source and direction;
- primary team attribution selected by the current precedence model;
- evidence and coverage states for missing or unresolved relationships.

Graph proximity is not causation or ownership. A missing edge can represent unavailable source data, unsupported mapping, or incomplete processing.

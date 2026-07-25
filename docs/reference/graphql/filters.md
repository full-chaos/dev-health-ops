---
page_id: ref-graphql-filters
summary: Organization, date-range, dimension, measure, scope, interval, top-N, and Sankey filter rules.
content_type: api-reference
owner: platform-api
source_of_truth:
  - current GraphQL input types and SQL compiler
applicability: current
lifecycle: active
---

# Filters and scope

GraphQL analytics requests are bounded by:

- authorized organization context;
- inclusive/exclusive date semantics defined by the current input;
- supported dimensions and measures from `catalog`;
- optional repository, team, provider, work-category, or other allowlisted filters;
- interval and bucket limits;
- top-N and Sankey node/edge limits;
- query cost, depth, alias, and subrequest controls.

Use stable IDs where the schema requires them. A display label is not a substitute for the canonical filter value. Preserve the exact request with any exported result.

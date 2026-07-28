---
page_id: ref-graphql-schema
summary: Generated GraphQL schema entry points and supported analytics result families.
content_type: generated-reference
owner: platform-api
source_of_truth:
  - src/dev_health_ops/api/graphql/schema.py
  - generated GraphQL schema artifact
applicability: current
lifecycle: active
---

# Schema and fields

Generate the exact field and enum reference from the current GraphQL schema. The principal analytics entry points include:

- `catalog` for dimensions, measures, allowed values, and limits;
- `analytics` for timeseries, breakdown, and Sankey requests and results;
- `devScopeSearch(orgId, input)` for a maximum of 25 authorized,
  deterministically ordered Ask Dev V1 scope candidates;
- `devMetricCatalog(orgId, input)` for the exact eight code-owned Ask Dev V1
  definitions supported by an optional scope shape;
- `devMetric(orgId, input)` for one bounded, authorized metric query and its
  immediately preceding equal-duration comparison;
- approved product-specific queries exposed by the current schema.

`DevScopeSearchInput.kinds` accepts only repository, project, WorkUnit, issue,
and pull-request candidates. The authenticated organization itself is already
known and is not searched. Team is a filter/dimension handled during scope
resolution, while incidents, deployments, commits, reviews, CI/test runs, AI
workflow runs, and files are supporting evidence and are absent from the search
enum.

The result includes canonical IDs, safe labels, an optional canonical repository
ID, `queryVersion`, and `catalogWatermark`. The resolver requires an
authenticated user with metrics-read permission and an enabled canonical
`ask_dev` decision. It uses the same application service as `resolve_scope.v1`;
GraphQL is not a separate authorization, entitlement, or entity-resolution
implementation.

`DevMetricQueryInput` accepts one typed metric ID, one resolved direct-scope
shape, an optional team filter, supported dimensions, and bounded comparison or
series options. Windows are half-open. Unsupported IDs, dimensions, direct
scopes, unresolved repository relationships, comparison windows, ranges longer
than 366 days, and series limits above 366 points fail closed.

`DevMetricResult` keeps measured zero distinct from `NO_MATCH`,
`INSUFFICIENT_EVIDENCE`, `PARTIAL`, `STALE`, `UNCONFIGURED`, and `UNAVAILABLE`.
It also returns coverage, the source watermark, definition/query/source
versions, and evidence references. GraphQL calls the shared `list_metrics.v1`
and `query_metric.v1` application service; it is not an independent metric
implementation.

An implementation resolver or frontend query is not an additional schema. The generated schema owns field names, arguments, required values, enums, and nullability.

Do not copy the entire schema into narrative guides. Link generated definitions from the task that uses them.

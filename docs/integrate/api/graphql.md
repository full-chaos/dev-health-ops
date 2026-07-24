---
page_id: int-graphql
summary: Query allowlisted analytics primitives through the read-only GraphQL API.
content_type: task-guide
owner: platform-api
source_of_truth:
  - src/dev_health_ops/api/graphql/schema.py
  - src/dev_health_ops/api/graphql/resolvers/analytics.py
  - docs/api/graphql-overview.md
applicability: current
lifecycle: active
---

# Query the GraphQL API

The analytics GraphQL endpoint is `POST /graphql`. Queries compile allowlisted primitives into parameterized storage queries; arbitrary SQL is not supported.

1. Authenticate through the supported deployment boundary.
2. Provide the organization context required by the schema and authorization layer.
3. Query `catalog` to discover supported dimensions, measures, values, and limits.
4. Submit bounded analytics requests for timeseries, breakdown, or Sankey results.
5. Handle GraphQL errors and nullable fields explicitly.
6. Preserve the scope, date range, interval, top-N, and limit inputs with downstream output.

Use [GraphQL reference](../../reference/graphql/index.md) for exact schema and filters.

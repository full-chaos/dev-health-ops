---
page_id: ref-graphql
summary: Exact GraphQL schema, filters, scope, cost, nullability, and error behavior.
content_type: landing
owner: platform-api
source_of_truth:
  - src/dev_health_ops/api/graphql/schema.py
  - src/dev_health_ops/api/graphql/resolvers/
  - src/dev_health_ops/api/graphql/sql/
applicability: current
lifecycle: active
---

# GraphQL reference

The analytics API is read-only and available at `POST /graphql`. It compiles allowlisted analytics primitives to parameterized queries. Arbitrary SQL is not accepted.

- [Schema and fields](schema.md)
- [Filters and scope](filters.md)
- [Nullability and errors](nullability-and-errors.md)

Use `catalog` to discover supported dimensions, measures, values, and cost limits before constructing an analytics request.

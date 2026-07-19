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
- approved product-specific queries exposed by the current schema.

An implementation resolver or frontend query is not an additional schema. The generated schema owns field names, arguments, required values, enums, and nullability.

Do not copy the entire schema into narrative guides. Link generated definitions from the task that uses them.

---
page_id: ref-graphql-errors
summary: Distinguish transport, GraphQL, authorization, validation, nullable-field, and empty-result behavior.
content_type: api-reference
owner: platform-api
applicability: current
lifecycle: active
---

# Nullability and errors

A successful HTTP response can contain GraphQL errors and partial data. Clients must inspect both.

- Transport/authentication failure: request did not reach a valid GraphQL execution.
- Validation or cost error: query or inputs are unsupported or exceed limits.
- Authorization error: organization or field access is denied.
- Resolver error: a supported field failed during execution.
- Nullable field: the contract permits no value; it is not automatically zero.
- Empty list: the query succeeded with no matching items.

Do not replace null with zero unless the field's exact contract says zero is the defined default.

---
page_id: ref-api
summary: Exact supported HTTP and API boundaries, authentication, error, pagination, and rate-limit behavior.
content_type: landing
owner: platform-api
source_of_truth:
  - current API routers, schema, and authorization implementation
applicability: current
lifecycle: active
---

# API reference

The supported public boundaries currently include:

- analytics GraphQL at `POST /graphql`;
- Customer Push ingestion at `/api/v1/external-ingest/*`;
- Customer Push administration at `/api/v1/admin/customer-push/*`;
- provider webhook receivers at `/api/v1/webhooks/*`;
- deployment-specific product and administration routes documented by their owning task guides.

- [Authentication and authorization](authentication.md)
- [Errors, pagination, and rate limits](errors-pagination-rate-limits.md)

A route appearing in source does not by itself make it a supported customer API. This reference includes only boundaries with an approved reader task and contract.

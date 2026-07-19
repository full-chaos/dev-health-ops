# Phase 8 integration evidence

## Canonical sources

- Customer Push: current `/api/v1/external-ingest/*` and `/api/v1/admin/customer-push/*` routes, server-generated schema, worker, status, and bounded recompute implementation.
- GraphQL: current schema, analytics resolvers, allowlisted SQL compiler, cost validation, and persisted-query implementation.
- Webhooks: current GitHub, GitLab, and Jira handlers and provider-specific signature validation.
- Provider extension: current provider, normalization, sync-unit, budgeting, queue, and sink contracts.

## Important separations

- Customer Push is not provider webhook ingestion.
- An `fcpush_` token is not a general GraphQL or administrator credential.
- Analytics GraphQL is read-only and does not accept arbitrary SQL.
- Exact schemas, record kinds, enum values, fields, limits, and defaults belong in generated Reference pages.

# GraphQL security posture

The `/graphql` endpoint is locked down by default. Production-like environments
disable the browser IDE, disable schema introspection, apply validation limits,
and reject oversized request bodies before Strawberry parses the query.

Environment controls:

- `ENVIRONMENT` (fallbacks: `APP_ENV`, `ENV`) defaults to `production`.
  `development`, `dev`, and `local` are treated as local development.
- `GRAPHQL_IDE_ENABLED=true` explicitly exposes GraphiQL. If unset, GraphiQL is
  enabled only in local development and disabled everywhere else.
- `GRAPHQL_INTROSPECTION_ENABLED=true` explicitly allows `__schema` and other
  introspection fields. If unset, introspection is disabled outside development.
- `GRAPHQL_SECURITY_ENABLED=false` disables the production hardening rules for
  local diagnostics. If unset, hardening is enabled outside development.
- `GRAPHQL_MAX_QUERY_BYTES` sets the HTTP body limit for `/graphql` requests.
  The safe default is `16384` bytes (16 KB).

Default validation limits are intentionally conservative: maximum selection
depth is 12 and maximum aliases per operation is 15. Resolver-level cost limits
remain separate and continue to protect analytics workloads after validation.

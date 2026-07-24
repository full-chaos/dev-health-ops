# GraphQL security posture

The `/graphql` endpoint is locked down by default. Production environments disable the browser IDE, disable schema introspection, apply validation limits, and reject oversized request bodies before Strawberry parses the query.

## 1. Introspection and GraphiQL IDE controls

Production environments restrict access to GraphQL schema information and the interactive interface.

* **Introspection**: Schema discovery via introspection fields like `__schema` is disabled outside development by default.
  * **Environment variable**: `GRAPHQL_INTROSPECTION_ENABLED=true` explicitly allows introspection. If unset, introspection is disabled in production.
  * **Default posture**: Disabled outside local development.
  * **Implementation**: Enforced by the `is_graphql_introspection_enabled` helper (`src/dev_health_ops/api/graphql/security.py:88`) and the custom rule `NoSchemaIntrospectionCustomRule` from `graphql.validation.rules.custom.no_schema_introspection` (registered on `src/dev_health_ops/api/graphql/security.py:162`).
* **GraphiQL IDE**: The interactive in-browser developer tool is hidden in production.
  * **Environment variable**: `GRAPHQL_IDE_ENABLED=true` explicitly exposes GraphiQL.
  * **Default posture**: Disabled outside local development.
  * **Implementation**: Controlled via `is_graphql_ide_enabled` (`src/dev_health_ops/api/graphql/security.py:79`) and passed to `GraphQLRouter` (`src/dev_health_ops/api/graphql/app.py:134`).

Local development environments are identified by checking the `ENVIRONMENT`, `APP_ENV`, or `ENV` environment variables against the set `{"development", "dev", "local"}` (`src/dev_health_ops/api/graphql/security.py:67`). If these variables are unset, the system defaults to production.

## 2. Request body size limit

Unreasonable payloads are rejected at the ASGI layer before any query parsing occurs.

* **Hard body limit**: Request payloads are limited to a maximum of 16 KB by default.
  * **Environment variable**: `GRAPHQL_MAX_QUERY_BYTES` sets the HTTP request body size limit in bytes.
  * **Default value**: `16384` bytes (16 KB), defined as `DEFAULT_GRAPHQL_MAX_QUERY_BYTES` in `src/dev_health_ops/api/graphql/security.py:26`.
  * **Implementation**: Enforced by the custom `GraphQLQuerySizeLimitMiddleware` class (`src/dev_health_ops/api/graphql/security.py:179`). This middleware is registered in `src/dev_health_ops/api/_middleware.py:60`.
  * **Failure mode**: Payloads exceeding the byte limit are immediately terminated. The server returns HTTP status `413 Payload Too Large` with the JSON response:
    ```json
    {
      "detail": {
        "message": "GraphQL request body exceeds size limit",
        "limit_bytes": 16384
      }
    }
    ```
  * **Probe rejection**: If a `GET` request is sent to `/graphql` and GraphiQL is disabled, the size limit middleware intercepts the request and responds with a standard HTTP `404 Not Found` to reject scans (`src/dev_health_ops/api/graphql/security.py:225`).

## 3. Query validation limits

To prevent query complexity attacks and resource exhaustion, the API registers two AST validation rules during schema compilation.

* **Query depth validation**:
  * **Limit**: Maximum query selection depth of 12.
  * **Implementation**: Evaluated by the `MaxDepthLimit` AST validation rule (`src/dev_health_ops/api/graphql/security.py:97`).
  * **Failure mode**: Standard GraphQL validation error with message: `GraphQL query depth exceeds limit of 12`.
* **Field alias validation**:
  * **Limit**: Maximum of 15 aliases per operation.
  * **Implementation**: Checked by the `MaxAliasLimit` AST validation rule (`src/dev_health_ops/api/graphql/security.py:127`).
  * **Failure mode**: Standard GraphQL validation error with message: `GraphQL alias count exceeds limit of 15`.

Hardening rules can be disabled for local diagnostics by setting `GRAPHQL_SECURITY_ENABLED=false` (`src/dev_health_ops/api/graphql/security.py:73`). If unset, hardening remains active in production. These validation rules are registered on the Strawberry schema instance (`src/dev_health_ops/api/graphql/schema.py:299`).

## 4. Resolver-level cost limits

Valid queries with high analytical costs are evaluated by cost budgets prior to ClickHouse query compilation. This layer guards against database abuse.

* **Limit definitions**: Defined in `src/dev_health_ops/api/graphql/cost.py:12` through the `CostLimits` class.
  * **Maximum date range**: `3650` days (10 years)
  * **Maximum timeseries buckets**: `100` buckets
  * **Maximum breakdown items (top_n)**: `100` items
  * **Maximum Sankey nodes**: `100` nodes
  * **Maximum Sankey edges**: `500` edges
  * **Maximum sub-requests in a single batch**: `10` sub-requests
  * **Query timeout**: `30` seconds
* **Validation functions**:
  * `validate_date_range(start_date, end_date)` checks the date range length.
  * `validate_top_n(top_n)` limits top_n query requests.
  * `validate_sankey_limits(max_nodes, max_edges)` checks Sankey node and edge caps.
  * `validate_sub_request_count(timeseries_count, breakdowns_count, has_sankey, has_flow_matrix)` restricts batch sizes.
  * `validate_buckets(start_date, end_date, interval)` estimates bucket counts before execution.
* **Failure mode**: On limit violation, resolvers raise a `CostLimitExceededError`. This surfaces as a standard GraphQL error block:
  ```json
  {
    "errors": [
      {
        "message": "Date range of 4000 days exceeds limit of 3650",
        "extensions": {
          "code": "COST_LIMIT_EXCEEDED",
          "limit_name": "max_days",
          "limit_value": 3650,
          "requested_value": 4000
        }
      }
    ]
  }
  ```

## 5. Field-level authorization and scoping

Scoping constraints and permissions are enforced at both execution and resolver boundaries.

* **Authentication requirements**:
  * Users must present a valid JWT in the `Authorization` header.
  * **Override**: `GRAPHQL_AUTH_REQUIRED=false` can bypass auth check in local dev environments (`src/dev_health_ops/api/graphql/app.py:46`).
  * If unauthenticated, the request context setup raises a FastAPI `HTTPException` with status `401 Unauthorized` (`src/dev_health_ops/api/graphql/app.py:78`).
* **Org-ID boundary scoping**:
  * Consistently scopes database queries to prevent cross-tenant data leaks.
  * Enforced at the execution layer by the `OrgIdAuthExtension` class (`src/dev_health_ops/api/graphql/extensions.py:19`).
  * If the variables map supplies a requested `org_id`, the extension validates it against the authenticated user's JWT `org_id` context before resolving any fields. A mismatch raises an `AuthorizationError` (`src/dev_health_ops/api/graphql/extensions.py:63`).
* **Resolver-level permissions**:
  * Resolvers can be annotated with the `@require_permission(*permissions)` decorator (`src/dev_health_ops/api/graphql/authz.py:15`).
  * This extracts user context and verifies role privileges via `has_any_permission` or `has_all_permissions` in the permissions service.
  * A permissions violation throws an `AuthorizationError`.

## 6. Rate limiting

The API uses `slowapi` for general endpoint protection.

* **GraphQL middleware coverage**:
  * The GraphQL endpoint `/graphql` is mounted as a standard router on the FastAPI application (`src/dev_health_ops/api/main.py:189`).
  * The `SlowAPIMiddleware` wraps the FastAPI app to support rate limits (`src/dev_health_ops/api/_middleware.py:73`).
  * Because `/graphql` does not carry explicit `@limiter.limit` decorators, it currently bypasses rate limit constraints. This avoids introducing query-level friction on complex dashboard loads.
  * Rate-limit configuration and Redis storage setup reside in `src/dev_health_ops/api/middleware/rate_limit.py`.

## 7. Performance and N+1 prevention

Complex resolvers utilize batching loaders to eliminate N+1 query patterns.

* **Context DataLoaders**:
  * Scoped DataLoaders are constructed per request (`src/dev_health_ops/api/graphql/context.py:98`).
  * **Core loaders**:
    * `team_loader` and `team_by_name_loader` batch team retrieval.
    * `repo_loader` and `repo_by_name_loader` batch repository lookup.
    * `loaders` (`DataLoaders.create(client)`) batches analytics queries, grouping timeseries and breakdown queries by dimension, measure, and interval.
  * High-frequency resolvers, including `analytics` and `work_graph`, load through these interfaces to keep database roundtrips minimal.

## 8. Audit logging and tracing

To support audits, GraphQL operations emit telemetry.

* **Access logging**:
  * Every GraphQL request flows through the FastAPI `CorrelationIdMiddleware` (`src/dev_health_ops/api/_middleware.py:76`).
  * This middleware injects a unique trace identifier (`X-Request-ID`) into logging contexts.
  * The context factory emits debug info upon context initialization (`src/dev_health_ops/api/graphql/app.py:57`).
* **Distributed tracing**:
  * **OpenTelemetry**: Integrated via `instrument_fastapi_app(app)` in `src/dev_health_ops/api/_observability.py:32`.
  * OpenTelemetry tracks individual resolver execution spans and database execution times.
* **Error tracking**:
  * **Sentry**: Captures unexpected execution errors via `init_sentry()` (`src/dev_health_ops/api/main.py:22`).

## 9. How to audit this in code

Review these canonical source files to verify the security posture details:

* `src/dev_health_ops/api/graphql/security.py`: Validation rules, depth and alias validators, request size limits, and environment checkers.
* `src/dev_health_ops/api/graphql/app.py`: Request context setup, GraphiQL mounting configuration, and authorization controls.
* `src/dev_health_ops/api/graphql/cost.py`: Cost budget ranges, batch constraints, and query validations.
* `src/dev_health_ops/api/graphql/authz.py`: Resolver permission decorators and error rules.
* `src/dev_health_ops/api/graphql/extensions.py`: Org scoping validation hook.
* `src/dev_health_ops/api/graphql/context.py`: Batching loader registrations.
* `src/dev_health_ops/api/_middleware.py`: Middleware stack order and custom HTTP size middleware mounting.

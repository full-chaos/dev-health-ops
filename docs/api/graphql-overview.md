# GraphQL Analytics API

Primary server code:
- `src/dev_health_ops/api/graphql/app.py`
- `src/dev_health_ops/api/graphql/schema.py`
- `src/dev_health_ops/api/graphql/resolvers/analytics.py`
- `src/dev_health_ops/api/graphql/sql/compiler.py`

## What it provides
- **Breakdowns**: grouped aggregations (for treemaps, tables)
- **Timeseries**: bucketed metrics (for area/line charts)
- **Sankey**: node/edge flows (for investment flows)

## Key design points
- Queries compile to SQL via `src/dev_health_ops/api/graphql/sql/*` and execute against the analytics store.
- Cost limits and validation are enforced in `src/dev_health_ops/api/graphql/cost.py`.
- Caching and persisted queries are supported via `src/dev_health_ops/api/graphql/persisted.py` and `src/dev_health_ops/api/graphql/persisted_queries.json`.

## Persisted queries
See: `docs/50-api/04-persisted-queries.md`.

## Web client
See: `docs/50-api/06-web-graphql-client.md`.

# GraphQL Analytics API

Primary server code:
- `api/graphql/app.py`
- `api/graphql/schema.py`
- `api/graphql/resolvers/analytics.py`
- `api/graphql/sql/compiler.py`

## What it provides
- **Breakdowns**: grouped aggregations (for treemaps, tables)
- **Timeseries**: bucketed metrics (for area/line charts)
- **Sankey**: node/edge flows (for investment flows)

## Key design points
- Queries compile to SQL via `api/graphql/sql/*` and execute against the analytics store.
- Cost limits and validation are enforced in `api/graphql/cost.py`.
- Caching and persisted queries are supported via `api/graphql/persisted.py` and `api/graphql/persisted_queries.json`.

## Persisted queries
See: `docs/50-api/04-persisted-queries.md`.

## Web client
See: `docs/50-api/06-web-graphql-client.md`.

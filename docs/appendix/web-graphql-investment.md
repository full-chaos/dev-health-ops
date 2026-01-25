# GraphQL Analytics for Investment View

This document describes how to use the GraphQL analytics endpoint for the Investment view, which is now available behind a feature flag.

## Feature Flag

Set the environment variable to enable GraphQL transport for Investment view data:

```bash
# In .env.local or environment
NEXT_PUBLIC_USE_GRAPHQL_ANALYTICS=true
```

For compiled builds (`next start`), the runtime flag is written to
`public/runtime-config.js` at startup (see `scripts/write-runtime-config.mjs`).
That file mirrors all `NEXT_PUBLIC_*` values under `publicEnv` (for example
`NEXT_PUBLIC_DOCS_URL` and `NEXT_PUBLIC_DEV_HEALTH_TEST_MODE`). You can also
set `USE_GRAPHQL_ANALYTICS=true` for a runtime-only flag.

When enabled, the following Investment view API calls switch from REST to GraphQL:

- `getInvestment()` - theme/subcategory distributions
- `getInvestmentFlow()` - Sankey flow (team → category → repo)
- `getInvestmentRepoTeamFlow()` - Sankey flow (repo → team)

When disabled (default), all calls continue to use the existing REST endpoints.

## Running GraphQL Locally

### Backend (dev-health-ops)

The GraphQL endpoint is already mounted at `/graphql`:

```bash
cd dev-health-ops
# Start the API server
python cli.py api --port 8000 --db "$DATABASE_URI"

# GraphiQL is available at http://localhost:8000/graphql
```

### Frontend (dev-health-web)

```bash
cd dev-health-web
# Enable GraphQL analytics
export NEXT_PUBLIC_USE_GRAPHQL_ANALYTICS=true
npm run dev
```

For a compiled build:

```bash
npm run build
NEXT_PUBLIC_USE_GRAPHQL_ANALYTICS=true npm start
```

`npm start` runs `scripts/write-runtime-config.mjs` to refresh `public/runtime-config.js`.

## Investment Query Examples

### 1. Investment Breakdown (Theme/Subcategory Distribution)

```graphql
query InvestmentBreakdown($orgId: String!, $batch: AnalyticsRequestInput!) {
  analytics(orgId: $orgId, batch: $batch) {
    breakdowns {
      dimension
      measure
      items {
        key
        value
      }
    }
  }
}
```

Variables:

```json
{
  "orgId": "my-org",
  "batch": {
    "breakdowns": [
      {
        "dimension": "THEME",
        "measure": "CHURN_LOC",
        "dateRange": { "startDate": "2025-01-01", "endDate": "2025-01-31" },
        "topN": 50
      },
      {
        "dimension": "SUBCATEGORY",
        "measure": "CHURN_LOC",
        "dateRange": { "startDate": "2025-01-01", "endDate": "2025-01-31" },
        "topN": 100
      }
    ]
  }
}
```

### 2. Investment Sankey Flow

```graphql
query InvestmentSankey($orgId: String!, $batch: AnalyticsRequestInput!) {
  analytics(orgId: $orgId, batch: $batch) {
    sankey {
      nodes { id label dimension value }
      edges { source target value }
    }
  }
}
```

Variables:

```json
{
  "orgId": "my-org",
  "batch": {
    "sankey": {
      "path": ["TEAM", "THEME", "REPO"],
      "measure": "CHURN_LOC",
      "dateRange": { "startDate": "2025-01-01", "endDate": "2025-01-31" },
      "maxNodes": 50,
      "maxEdges": 200
    }
  }
}
```

## Persisted Query IDs

For reduced query string churn, use these persisted query IDs:

| ID | Description |
|----|-------------|
| `investment-breakdown` | Theme and subcategory breakdowns |
| `investment-sankey` | Sankey flow data |

Example:

```bash
curl -X POST /graphql \
  -H "X-Persisted-Query-Id: investment-breakdown" \
  -H "Content-Type: application/json" \
  -d '{"variables": {"orgId": "my-org", "batch": {...}}}'
```

## Response Shape Mapping

The GraphQL fetchers adapt responses to match the existing REST shapes:

### Breakdown → InvestmentResponse

```typescript
// GraphQL response
{ analytics: { breakdowns: [{ dimension: "theme", items: [...] }] } }

// Adapted to REST shape
{ theme_distribution: {...}, subcategory_distribution: {...} }
```

### Sankey → SankeyResponse

```typescript
// GraphQL response
{ analytics: { sankey: { nodes: [...], edges: [...] } } }

// Adapted to REST shape
{ mode: "investment", nodes: [...], links: [...] }
```

## Testing

With the flag enabled, the Investment page should render identically to REST mode:

- Same charts (treemap, sunburst, Sankey)
- Same tooltip wording
- Same filter behavior
- Same Sankey structure and values (within expected rounding)

With the flag disabled, everything works exactly as before using REST endpoints.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Frontend (dev-health-web)               │
├─────────────────────────────────────────────────────────────┤
│  src/lib/api.ts                                              │
│    ↓ checks runtime GraphQL flag                             │
│    ├─ false → REST endpoints (/api/v1/investment/*)          │
│    └─ true  → GraphQL fetchers                               │
│                                                              │
│  scripts/write-runtime-config.mjs                            │
│    ↓ writes public/runtime-config.js at start                │
│                                                              │
│  src/lib/graphql/                                            │
│    ├─ client.ts          # GraphQL request wrapper           │
│    ├─ types.ts           # TypeScript types                  │
│    ├─ queries.ts         # Query strings                     │
│    └─ investmentFetchers.ts  # Adapters to REST shapes       │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                      Backend (dev-health-ops)                │
├─────────────────────────────────────────────────────────────┤
│  /graphql (POST)                                             │
│    ├─ catalog(orgId, dimension?) → dimension values          │
│    └─ analytics(orgId, batch) → timeseries, breakdowns, sankey│
│                                                              │
│  api/graphql/                                                │
│    ├─ resolvers/analytics.py   # Query execution             │
│    ├─ sql/compiler.py          # SQL generation              │
│    └─ persisted_queries.json   # Cached queries              │
└─────────────────────────────────────────────────────────────┘
```

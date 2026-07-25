# GraphQL Migration Guide

This guide covers migrating from the REST API to the GraphQL API for dev-health-ops.

## Overview

The GraphQL API provides a modern, type-safe alternative to the REST API with several advantages:

- **Batch queries** - Request multiple data types in a single request
- **Real-time subscriptions** - Get notified of data changes via WebSocket
- **DataLoaders** - Automatic batching and caching of database queries
- **Strong typing** - Full schema validation and IDE autocompletion
- **Flexible responses** - Request only the fields you need

## Endpoints

| Type | Endpoint | Protocol |
|------|----------|----------|
| Queries/Mutations | `/graphql` | HTTP POST |
| Subscriptions | `/graphql` | WebSocket (graphql-ws) |
| GraphiQL IDE | `/graphql` | HTTP GET |

## REST to GraphQL Mapping

### Home Metrics

**REST:**
```bash
POST /api/v1/home
```

**GraphQL:**
```graphql
query HomeMetrics($orgId: String!) {
  home(orgId: $orgId) {
    freshness {
      lastIngestedAt
      coverage {
        reposCoveredPct
        prsLinkedToIssuesPct
      }
    }
    deltas {
      metric
      label
      value
      unit
      deltaPct
    }
  }
}
```

### Analytics (Breakdowns, Timeseries, Sankey)

**REST:**
```bash
POST /api/v1/investment
POST /api/v1/sankey
```

**GraphQL:**
```graphql
query InvestmentAnalytics($orgId: String!, $batch: AnalyticsRequestInput!) {
  analytics(orgId: $orgId, batch: $batch) {
    breakdowns {
      dimension
      measure
      items {
        key
        value
      }
    }
    sankey {
      nodes { id label dimension value }
      edges { source target value }
      coverage { teamCoverage repoCoverage }
    }
  }
}
```

### Catalog (Dimensions, Measures)

**REST:**
```bash
GET /api/v1/filters/options
```

**GraphQL:**
```graphql
query Catalog($orgId: String!, $dimension: DimensionInput) {
  catalog(orgId: $orgId, dimension: $dimension) {
    dimensions { name description }
    measures { name description }
    limits { maxDays maxBuckets maxTopN }
    values { value count }  # If dimension specified
  }
}
```

## Using Persisted Queries

For production use, prefer persisted queries to reduce payload size and enable server-side caching:

```bash
curl -X POST /graphql \
  -H "Content-Type: application/json" \
  -H "X-Org-Id: your-org" \
  -H "X-Persisted-Query-Id: investment-breakdown" \
  -d '{"variables": {"orgId": "your-org", "batch": {...}}}'
```

Available persisted query IDs:
- `catalog-dimensions`
- `team-values`
- `repo-values`
- `investment-breakdown`
- `investment-sankey`
- `investment-full`
- `timeseries-weekly`
- `team-analytics-bundle`
- `repo-analytics-bundle`

## Real-time Subscriptions

Subscribe to real-time updates via WebSocket:

```graphql
subscription MetricsUpdates($orgId: String!) {
  metricsUpdated(orgId: $orgId) {
    orgId
    day
    updatedAt
    message
  }
}

subscription TaskProgress($taskId: String!) {
  taskStatus(taskId: $taskId) {
    taskId
    status
    progress
    message
  }
}
```

## Error Handling

GraphQL errors are returned in the `errors` array:

```json
{
  "data": null,
  "errors": [
    {
      "message": "Cost limit exceeded: max_buckets is 100",
      "locations": [{"line": 2, "column": 3}],
      "path": ["analytics"],
      "extensions": {
        "code": "COST_LIMIT_EXCEEDED"
      }
    }
  ]
}
```

## Cost Limits

The GraphQL API enforces the same cost limits as REST:

| Limit | Value |
|-------|-------|
| Max date range | 3650 days |
| Max buckets per timeseries | 100 |
| Max top-N items | 100 |
| Max sankey nodes | 100 |
| Max sankey edges | 500 |
| Max sub-requests per batch | 10 |
| Query timeout | 30 seconds |

## Migration Checklist

1. ✅ Update frontend to use GraphQL client (urql recommended)
2. ✅ Replace REST calls with GraphQL queries
3. ✅ Add WebSocket support for subscriptions
4. ✅ Enable Zod validation for response types
5. ✅ Test with GraphiQL IDE at `/graphql`
6. ✅ Monitor for deprecation warnings on REST endpoints

# GraphQL Analytics API

The GraphQL analytics API provides a read-only query interface for dev-health-ops analytics data. All queries compile allowlisted primitives into parameterized SQL—no arbitrary queries are permitted.

## Endpoint

```
POST /graphql
```

## Authentication

All queries require `org_id` as a query parameter. This scopes all data access to the specified organization.

## Schema Overview

### Query.catalog

Fetch available dimensions, measures, and cost limits.

```graphql
query Catalog($orgId: String!, $dimension: DimensionInput) {
  catalog(orgId: $orgId, dimension: $dimension) {
    dimensions { name description }
    measures { name description }
    limits {
      maxDays
      maxBuckets
      maxTopN
      maxSankeyNodes
      maxSankeyEdges
      maxSubRequests
    }
    values { value count }  # Only when dimension is specified
  }
}
```

### Query.analytics

Execute batch analytics queries including timeseries, breakdowns, and Sankey flows.

```graphql
query Analytics($orgId: String!, $batch: AnalyticsRequestInput!) {
  analytics(orgId: $orgId, batch: $batch) {
    timeseries {
      dimension
      dimensionValue
      measure
      buckets { date value }
    }
    breakdowns {
      dimension
      measure
      items { key value }
    }
    sankey {
      nodes { id label dimension value }
      edges { source target value }
    }
  }
}
```

---

## Example Queries

### 1. Catalog Values for TEAM and REPO

```graphql
# Fetch available teams
query TeamValues {
  catalog(orgId: "my-org", dimension: TEAM) {
    values { value count }
  }
}

# Fetch available repos
query RepoValues {
  catalog(orgId: "my-org", dimension: REPO) {
    values { value count }
  }
}
```

### 2. Analytics with Timeseries, Breakdown, and Sankey

```graphql
query FullAnalytics {
  analytics(
    orgId: "my-org"
    batch: {
      timeseries: [{
        dimension: THEME
        measure: COUNT
        interval: WEEK
        dateRange: {
          startDate: "2025-01-01"
          endDate: "2025-01-31"
        }
      }]
      breakdowns: [{
        dimension: REPO
        measure: CHURN_LOC
        dateRange: {
          startDate: "2025-01-01"
          endDate: "2025-01-31"
        }
        topN: 10
      }]
      sankey: {
        path: [WORK_TYPE, REPO, TEAM]
        measure: COUNT
        dateRange: {
          startDate: "2025-01-01"
          endDate: "2025-01-31"
        }
        maxNodes: 50
        maxEdges: 200
      }
    }
  ) {
    timeseries {
      dimension
      dimensionValue
      buckets { date value }
    }
    breakdowns {
      dimension
      items { key value }
    }
    sankey {
      nodes { id label value }
      edges { source target value }
    }
  }
}
```

#### Curl example (Sankey coverage)

```bash
curl -s -X POST "http://localhost:8000/graphql?org_id=default" \
  -H "Content-Type: application/json" \
  -H "X-Org-Id: default" \
  -d '{
    "query": "query CoverageSankey($orgId: String!, $batch: AnalyticsRequestInput!) { analytics(orgId: $orgId, batch: $batch) { sankey { coverage { teamCoverage repoCoverage } nodes { id label dimension value } edges { source target value } } } }",
    "variables": {
      "orgId": "default",
      "batch": {
        "useInvestment": true,
        "timeseries": [],
        "breakdowns": [],
        "sankey": {
          "path": ["WORK_TYPE", "REPO", "TEAM"],
          "measure": "COUNT",
          "dateRange": { "startDate": "2026-01-01", "endDate": "2026-01-31" }
        }
      }
    }
  }'
```

#### Curl example (Investment breakdowns via dev-health-web)

```bash
curl -s -X POST "http://localhost:3000/graphql?org_id=default" \
  -H "Content-Type: application/json" \
  -H "X-Org-Id: default" \
  -d '{
    "query": "query InvestmentBreakdown($orgId: String!, $batch: AnalyticsRequestInput!) { analytics(orgId: $orgId, batch: $batch) { breakdowns { dimension measure items { key value } } } }",
    "variables": {
      "orgId": "default",
      "batch": {
        "useInvestment": true,
        "breakdowns": [
          { "dimension": "THEME", "measure": "COUNT", "dateRange": { "startDate": "2026-01-06", "endDate": "2026-01-20" }, "topN": 50 },
          { "dimension": "SUBCATEGORY", "measure": "COUNT", "dateRange": { "startDate": "2026-01-06", "endDate": "2026-01-20" }, "topN": 100 }
        ],
        "filters": { "scope": { "level": "ORG", "ids": [] } }
      }
    }
  }'
```

---

## Available Dimensions

| Dimension | Description |
|-----------|-------------|
| `TEAM` | Team identifier |
| `REPO` | Repository identifier |
| `AUTHOR` | Author/contributor identifier |
| `WORK_TYPE` | Type of work item (issue, PR, etc.) |
| `THEME` | Investment theme category |
| `SUBCATEGORY` | Investment subcategory |

## Available Measures

| Measure | Description |
|---------|-------------|
| `COUNT` | Count of work units |
| `CHURN_LOC` | Lines of code changed |
| `CYCLE_TIME_HOURS` | Average cycle time in hours |
| `THROUGHPUT` | Distinct work units completed |

## Bucket Intervals

For timeseries queries: `DAY`, `WEEK`, `MONTH`

---

## Cost Limits

All requests are subject to cost limits to protect database performance:

| Limit | Default | Description |
|-------|---------|-------------|
| `maxDays` | 365 | Maximum date range in days |
| `maxBuckets` | 100 | Maximum timeseries buckets |
| `maxTopN` | 50 | Maximum breakdown items |
| `maxSankeyNodes` | 100 | Maximum Sankey nodes |
| `maxSankeyEdges` | 500 | Maximum Sankey edges |
| `maxSubRequests` | 10 | Maximum queries in a batch |

Requests exceeding limits return a `COST_LIMIT_EXCEEDED` error:

```json
{
  "errors": [{
    "message": "Date range of 400 days exceeds limit of 365",
    "extensions": {
      "code": "COST_LIMIT_EXCEEDED",
      "limit_name": "max_days",
      "limit_value": 365,
      "requested_value": 400
    }
  }]
}
```

---

## Error Codes

| Code | Description |
|------|-------------|
| `COST_LIMIT_EXCEEDED` | Request exceeds a cost limit |
| `VALIDATION_ERROR` | Invalid dimension, measure, or input |
| `AUTHORIZATION_ERROR` | Missing or invalid org_id |
| `PERSISTED_QUERY_ERROR` | Unknown query ID or version mismatch |
| `QUERY_TIMEOUT` | Query exceeded timeout limit |

---

## Persisted Queries

For performance, you can use persisted query IDs instead of sending the full query text:

```bash
curl -X POST /graphql \
  -H "X-Persisted-Query-Id: catalog-dimensions" \
  -H "Content-Type: application/json" \
  -d '{"variables": {"orgId": "my-org"}}'
```

Available persisted queries:

- `catalog-dimensions` – Fetch catalog with dimensions and measures
- `team-values` – Fetch distinct team values
- `repo-values` – Fetch distinct repo values

---

## Security Notes

1. **No arbitrary SQL**: All queries are compiled from allowlisted primitives
2. **org_id enforcement**: Required on all queries, enforced in all SQL WHERE clauses
3. **Parameterized SQL**: All values are parameterized, preventing SQL injection
4. **Query timeouts**: Default 30-second timeout on all database queries

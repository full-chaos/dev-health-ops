---
page_id: int-graphql
summary: Query allowlisted analytics primitives through the read-only GraphQL API.
content_type: task-guide
owner: platform-api
source_of_truth:
  - contracts/graphql/v1/schema.graphql
  - internal/queryapi/analytics/resolve.go
  - docs/api/graphql-overview.md
applicability: current
lifecycle: active
---

# Query the GraphQL API

The analytics GraphQL endpoint is `POST /graphql`. Queries compile allowlisted primitives into parameterized storage queries; arbitrary SQL is not supported.

1. Authenticate through the supported deployment boundary.
2. Provide the organization context required by the schema and authorization layer.
3. Query `catalog` to discover supported dimensions, measures, values, and limits.
4. Submit bounded analytics requests for timeseries, breakdown, or Sankey results.
5. Handle GraphQL errors and nullable fields explicitly.
6. Preserve the scope, date range, interval, top-N, and limit inputs with downstream output.
7. Show a name field, not an id. A name field is `null` when no name is stored; show "Unresolved" then, never the id.

Use [GraphQL reference](../../reference/graphql/index.md) for exact schema and filters.

## Name fields

Some rows carry an id next to a name field. The name comes from the stored name of that entity, inside your organization. It is `null` when no name is stored. The API never fills a name field with the id.

| Type | Name fields |
| --- | --- |
| `AIAttributionEvidenceRow`, `AIGovernanceViolationRow` | `repoName`, `teamName`, `subjectTitle` (the pull request title, for a `pull_request` subject) |
| `AIGovernanceViolationRow` | `ruleName` (the display name of a known policy rule) |
| `AliasSuggestion` | `suggestedCanonicalName` (the display name of the suggested identity, else its email) |
| `TestOpsRiskQuadrantPoint` | `name` (the repository full name) |
| `ImproveOpportunity` | `entityDisplayName` (the repository full name or the team name) |

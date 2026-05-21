# AI Workflow Analytics — GraphQL Contracts

This document covers the GraphQL surface for AI Workflow Intelligence
(CHAOS-1582).  It is the read-side projection of the metrics, work
graph, and governance signals delivered by CHAOS-1579 through
CHAOS-1587.

> **Authority:** the GraphQL schema (`api/graphql/schema.py`) is the
> source of truth.  This page explains intent and usage; the SDL
> defines the actual types.

## Mission

Expose every AI-relevant signal that already lives in ClickHouse —
attribution, impact, review load, risk, governance, and work-graph
evidence — through stable, typed GraphQL contracts that the web client
can consume without bespoke joins or recomputation.

## Design rules

- **Read-only.** Resolvers never categorise, never compute metrics,
  never write.  All persistence happens in the metrics and governance
  jobs (CHAOS-1581, CHAOS-1587).
- **Empty / partial / populated states are first-class.** Every result
  carries `dataAvailable: Boolean!` so the UI can render the correct
  state without ad-hoc null probing.
- **Provenance accompanies every metric.** `computedAt` and bucketed
  evidence references travel with the response so the UI can show
  freshness and drill into the underlying artifacts.
- **Drilldown IDs map to Work Graph evidence.** The IDs returned in
  every contract resolve through `aiWorkflowDrilldown` or
  `workGraphEdges` — no synthetic identifiers.
- **Stable contract before completeness.** `aiOpportunities` ships an
  empty stable contract today; the detector (CHAOS-1586) populates it
  later without a breaking change.

## Buckets

The metrics rollup persists every PR (and every reviewed artifact) into
exactly one `attributionBucket`.  These mirror
`dev_health_ops.metrics.ai_impact.AttributionBucket` (a `StrEnum`) and
the GraphQL `AIAttributionBucketInput` enum:

| Bucket          | Meaning                                                    |
| --------------- | ---------------------------------------------------------- |
| `ai_assisted`   | Human authored with explicit AI assistance.                |
| `agent_created` | Autonomous agent produced this artifact.                   |
| `ai_review`     | AI performed the review.                                   |
| `human`         | Human-only baseline (no detected AI involvement).          |
| `unknown`       | Attribution unresolved — never guessed.                    |

## Query catalog

All seven queries scope to `orgId` (enforced by `OrgIdAuthExtension`).

### `aiImpactSummary`

Per-bucket totals + a daily timeseries for the requested window, plus
decomposed Operating Leverage components.  Use this as the
single-fetch contract for the AI Impact dashboard.

```graphql
query AIImpact($orgId: String!, $range: AIDateRangeInput!) {
  aiImpactSummary(orgId: $orgId, dateRange: $range) {
    totalPrs
    aiAssistedPrs
    agentCreatedPrs
    humanPrs
    unknownPrs
    aiAssistedPrRatio
    byBucket {
      bucket
      prsTotal
      cycleTimeAvgHours
      aiReviewAmplification
      leverage {
        prsComponent
        cycleTimeComponent
        reviewComponent
        reworkComponent
        testComponent
        incidentComponent
      }
    }
    dataAvailable
    computedAt
  }
}
```

### `aiComparison`

Side-by-side aggregation of the AI-attributed buckets against the
`human` baseline plus per-metric deltas.

### `aiReviewLoad`

Review-load breakdown per bucket: `reviewsPerPr`,
`changesRequestedPerPr`, and the persisted `reviewAmplification` from
CHAOS-1581.

### `aiRiskBreakdown`

Per-bucket rework / revert / test-gap / incident rates.  Computed from
persisted counts, no weighting.

### `aiOpportunities`

Stable contract — `recommendations: []`, `detectorReady: false` —
until CHAOS-1586 lands the detector.  Clients can render an empty
state today and gain population without a schema change.

Recommendations include metric `evidenceRefs`. When those refs point to PR
artifacts, the response also includes `workGraphDrilldowns` so clients can
open `aiWorkflowDrilldown(rootType: PR, rootId: ...)` and inspect the
underlying Work Graph evidence instead of treating a recommendation as a
standalone dashboard card.

### `aiGovernanceSummary`

Coverage rollups (`declarationCoverage`, `humanReviewCoverage`,
`securityScanCoverage`, `inPolicyCoverage`) plus recent violations
(rule id + severity from the canonical AI policy registry).

### `aiWorkflowDrilldown`

Partial Work Graph rooted at an issue, PR, or work_unit.  Returns
typed nodes and edges with `confidence`, `source`, and short evidence
references so the UI can render an explanation without re-querying.

## Filtering

`AIScopeInput` is optional on every query that accepts it:

```graphql
input AIScopeInput {
  repoId: String
  teamId: String
  workType: String
  buckets: [AIAttributionBucketInput!]
}
```

`AIDateRangeInput` is required where present and is inclusive on both
ends.  Reversed ranges raise a validation error.

## Provenance

Every aggregated row in `aiImpactSummary` is sourced from
`ai_impact_metrics_daily` rows that carry their own `computedAt`
timestamp.  The top-level `computedAt` is the maximum of the
underlying row timestamps.  Coverage and violation rows carry
`observedAt` straight from `ai_governance_*` tables.

## What this does **not** ship

- **No** rankings of individuals.
- **No** prompt or session content.
- **No** UX-time recomputation of categories or rates.
- **No** new persistence paths — every resolver reads from existing
  ClickHouse tables only.

## Frontend integration

The web client regenerates types from the exported SDL.  After
schema changes:

```bash
# in dev-health-ops
python -m dev_health_ops.api.graphql.export_schema --out /tmp/schema.graphql
cp /tmp/schema.graphql ../dev-health-web/src/lib/graphql/schema.graphql

# in dev-health-web
npm run codegen
```

`npm run codegen:check` will fail the web CI if the committed types
fall behind the schema.

## Tests

Contract tests live at
`tests/api/graphql/test_ai_resolver.py`.  Each query has explicit
**empty / partial / populated** assertions so future changes preserve
the empty-state guarantees demanded by the dashboards.

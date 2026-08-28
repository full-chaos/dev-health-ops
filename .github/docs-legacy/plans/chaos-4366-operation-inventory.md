# CHAOS-4366 Wave 0 — GraphQL operation inventory

Deliverable 2 of CHAOS-4366. Source: full read of `dev-health/web`
(`codegen.ts`, `src/lib/graphql/`, `src/lib/feature-flags/`,
`src/lib/testops/`, `src/lib/reports/`, and every `.graphql`/tagged-template
document under `src/`).

## Production frequency: not available for this pass

Production operation frequency was to come from SigNoz (`mcp__signoz__*`).
That MCP server is **not connected in this session** (no tool registered,
no connection-failure diagnostic — i.e. not configured, not a transient
outage). Per the ticket's fallback instruction, this inventory covers
**documents only**; frequency and the wave picks below are a
document-shape-based first cut, not traffic-validated. **Before Wave 1
starts, re-run frequency analysis against SigNoz** — plan §6 already
flags this exact risk: "If production inventory shows `featureFlags` gets
no real traffic, use it for local/staging proof only and pick
`reviewEdges` as the first production canary."

## Document mechanics (confirmed)

Two coexisting styles, neither persisted:

1. **Codegen client-preset** (4 ops) — standalone `.graphql` files under
   `src/app/(app)/data-health/**`, picked up by `graphql-codegen`
   (`documentMode: "string"`, preset `"client"`). Generated `graphql()` +
   typed `Document` constants in `src/lib/graphql/__generated__/`.
2. **Raw template-literal strings** (~53 ops, majority) — plain
   `` `query X(...) {...}` `` constants across `queries.ts`,
   `feature-flags/queries.ts`, `testops/queries.ts`, `reports/queries.ts`,
   `productTelemetryFetchers.ts`. Sent via urql's `useQuery`/`useMutation`
   or the shared `graphqlFetch()` helper — full document text, not a typed
   node, every request.

**No persisted-query mechanism exists anywhere in web** — no
`X-Persisted-Query-Id`, no `.graphqlrc`/manifest, no codegen
persisted-queries plugin. This matters directly for the registry: CHAOS-4380
(now Done — registered/persisted documents only, chris's ruling) means the
Wave-0 registry must be seeded from **this document inventory**, keyed by
`document_digest` computed from each of these document strings as they
exist today — web is not already producing persisted-query IDs the
registry could reuse.

**Existing schema-drift gate**: `web/.github/workflows/live-e2e.yml`
diffs a fresh `export_schema` output against `web/src/lib/graphql/schema.graphql`,
but soft-skips (`exit 0` + warning) if the Python export import fails —
see `contracts/graphql/v1/README.md` (this PR's stack, deliverable 1) for
the follow-up filed on that gap.

## Operation table

Wave column is a first-cut recommendation per plan §6 sequencing, not yet
traffic-validated (see above). "Dead" = zero call sites found; excluded from
wave assignment until confirmed live-or-removed.

| Operation | Type | Defined in | Call sites | Wave (draft) |
|---|---|---|---|---|
| FeatureFlagRegistry | query | `feature-flags/queries.ts` | 1 | **1** (first canary candidate — pending SigNoz confirmation) |
| FeatureFlagEvents | query | `feature-flags/queries.ts` | 1 | 4 (explicitly NOT wave 1, plan §6) |
| ReviewEdges | query | `queries.ts` | 1 | **2** (after tie-ordering fix — CHAOS-4381 rule 5; fallback wave-1 canary if FeatureFlagRegistry has no traffic) |
| Hotspots | query | `queries.ts` | 2 | 3 |
| ComplexityTimeseries | query | `queries.ts` | 2 | 3 |
| CognitiveLoad | query | `queries.ts` | 1 | 3 |
| WorkGraphEdges | query | `queries.ts` | 5 | 4 |
| WorkGraphFlow | query | `queries.ts` | 1 | 4 |
| WorkGraphArtifacts | query | `queries.ts` | 1 | 4 |
| AIWorkflowDrilldown | query | `queries.ts` | 3 | 4 |
| CompoundingRisk | query | `queries.ts` | 2 | 4 |
| InvestmentFull | query | `queries.ts` | 2 | 4 |
| InvestmentBreakdown | query | `queries.ts` | 4 | 4 |
| InvestmentSankey | query | `queries.ts` | 0 (dead) | unassigned — confirm live/remove |
| CapacityForecast | query | `queries.ts` | 3 | 4 |
| ThroughputForecast | query | `queries.ts` | 1 | 4 |
| CapacityForecasts | query | `queries.ts` | 0 (dead) | unassigned — confirm live/remove |
| OperatingReview | query | `queries.ts` | 2 | 4 |
| SecurityOverview | query | `queries.ts` | 2 | 4 |
| SecurityAlerts | query | `queries.ts` | 1 | 4 |
| AIImpactSummary | query | `queries.ts` | 2 | 4 |
| AIComparison | query | `queries.ts` | 1 | 4 |
| AIOpportunities | query | `queries.ts` | 2 | 4 |
| ImproveOpportunities | query | `queries.ts` | 2 | 4 |
| AIReviewLoad | query | `queries.ts` | 2 | 4 |
| AIRiskBreakdown | query | `queries.ts` | 1 | 4 |
| AIGovernanceSummary | query | `queries.ts` | 2 | 4 |
| AIAttributedPrs | query | `queries.ts` | 1 | 4 |
| AIAttributionOverview | query | `queries.ts` | 2 | 4 |
| BusFactor | query | `queries.ts` | 1 | 4 |
| PrDetail | query | `queries.ts` | 1 | 4 |
| CatalogValues | query | `queries.ts` | 1 | 4 |
| FlowMatrix | query | `queries.ts` | 1 | 4 |
| WorkUnitTeamAttributions | query | `queries.ts` | 1 | 4 |
| Experiments | query | `queries.ts` | 1 | 4 |
| FeatureFlagTimeseries | query | `feature-flags/queries.ts` | 1 | 4 |
| ReleaseImpact | query | `feature-flags/queries.ts` | 1 | 4 |
| TestOpsPipeline | query | `testops/queries.ts` | 1 | 4 |
| TestOpsTest | query | `testops/queries.ts` | 1 | 4 |
| TestOpsCoverage | query | `testops/queries.ts` | 1 | 4 |
| TestOpsRisk | query | `testops/queries.ts` | 1 | 4 |
| ProductTelemetryDashboard | query | `productTelemetryFetchers.ts` | 1 | 4 |
| ProductTelemetryPlatformDashboard | query | `productTelemetryFetchers.ts` | 1 | 4 |
| GetConnectorsDataHealth | query | `.../data-health/connectors/connectors.graphql` | 1 | 4 |
| DataHealthIdentity | query | `.../data-health/identity/identity.graphql` | 1 | 4 |
| MetricLineage | query | `.../data-health/lineage.graphql` | 1 | 4 |
| GetMappingCoverageHealth | query | `.../data-health/mapping/mapping.graphql` | 1 | 4 |
| savedReports | query | `reports/queries.ts` | 1 | 5 (Postgres-backed) |
| savedReport | query | `reports/queries.ts` | 1 | 5 (Postgres-backed) |
| reportRuns | query | `reports/queries.ts` | 1 | 5 (Postgres-backed) |
| createSavedReport | mutation | `reports/queries.ts` | 1 | 7 (mutation gate) |
| updateSavedReport | mutation | `reports/queries.ts` | 1 | 7 (mutation gate) |
| cloneSavedReport | mutation | `reports/queries.ts` | 1 | 7 (mutation gate) |
| deleteSavedReport | mutation | `reports/queries.ts` | 1 | 7 (mutation gate) |
| triggerReport | mutation | `reports/queries.ts` | 1 | 7 (mutation gate) |
| MetricsUpdated | subscription | `hooks/useSubscription.ts` | 0 (scaffolded) | out of scope (subscriptions stay Python) |
| TaskStatus | subscription | `hooks/useSubscription.ts` | 0 (scaffolded) | out of scope (subscriptions stay Python) |

**Excluded from this registry entirely**: `IssueCreate` (mutation) in
`src/app/api/feedback/route.ts` targets Linear's public API
(`api.linear.app/graphql`), not this backend — not governed by
`schema_digest`.

Totals: 51 query operations, 5 mutation operations, 2 subscription
operations against the internal schema = 58 documents. 4 have zero
confirmed call sites (2 queries, 2 subscriptions) — flagged above rather
than silently dropped or silently carried forward.

## Follow-ups filed against this inventory

- Re-run with SigNoz production-frequency data before Wave 1 selection is
  finalized (this doc's frequency gap, above).
- Resolve the 4 zero-call-site operations (dead code vs. not-yet-wired) —
  candidate for the CHAOS-4248 dead-code class already tracked for the REST
  surface.
- `.github/workflows/live-e2e.yml` (web repo) soft-skip-on-import-failure
  gap (see `contracts/graphql/v1/README.md`).

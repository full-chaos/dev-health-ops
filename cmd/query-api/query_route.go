// CHAOS-4367 Wave 1: wires the ONE live route this binary now serves --
// featureFlags -- behind routeswitch.Mux + the registry-backed
// PostgresSwitch, gated by a verified effective-principal envelope. See
// main.go's package doc for what Wave 0 left empty; this file is what
// Wave 1 adds on top of it.
//
// CHAOS-4368 Wave 2 adds a SECOND operation, reviewEdges, on the same
// /query route and the same routeswitch.Mux + PostgresSwitch pipeline --
// each operation gets its own registered document + digest + Mux
// registration, gated independently (the go_api_routing_state row for one
// operation has no effect on the other's reachability).
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/99designs/gqlgen/graphql"
	gqlhandler "github.com/99designs/gqlgen/graphql/handler"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vektah/gqlparser/v2/gqlerror"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/authctx"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/digest"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/featureflags"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/principal"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/routeswitch"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/workgraph"
)

// registeredFeatureFlagsDocument is the ONE query document query-api
// recognizes for the featureFlags operation this wave (plan §7 open
// decision 2: "GraphQL eligibility = registered documents only"). A
// request's exact query text must digest-match this canonical document
// before its operation name even reaches the reachability check -- this
// is what closes PostgresSwitch's documented gap #1 ("document identity
// is NOT verified against the live request ... wiring the exact
// registered-document-identity contract end to end is a later wave's
// job, when Mux is actually mounted on a live route") for the one route
// this wave mounts.
//
// Known, deliberate gap (same "name it, don't hide it" convention
// PostgresSwitch's own doc comment uses): this is a hand-registered
// single document, not Wave 0 deliverable 2's actual web-operations
// inventory. A later wave sources the registered-document set from that
// real inventory; Wave 1 does not build that general pipeline.
//
// Document text is byte-for-byte the REAL production query (codex review,
// 2026-08-28, round 3 -- corrected after round 2's wrong conclusion):
// web/src/lib/feature-flags/queries.ts's FEATURE_FLAG_REGISTRY_QUERY names
// its operation "FeatureFlagRegistry", not "FeatureFlags". Round 2 of this
// review raised the same name mismatch citing only the operation inventory
// doc, and was dismissed as a documentation-only ambiguity because Wave 0's
// registry test fixtures use `selected_operation="featureFlags"`internally
// -- that dismissal was wrong for a different reason than the one being
// discussed: `selected_operation` is this code's OWN internal map key
// (Mux.Register / PostgresSwitch.documentDigests), never compared against
// the document text, so it can stay "featureFlags" (matching that existing
// test-fixture precedent) with NO effect on reachability. What actually
// gates reachability is operationForDocument's digest match against this
// constant's literal text -- and that text previously used the WRONG
// operation name, so a real web request's digest would never have matched
// it, 404-ing every real featureFlags query while local tests (which also
// used the wrong name on both sides) stayed green. Copied verbatim from
// the real client source so the digest this route checks is the digest a
// real request actually produces.
//
// CHAOS-4696 correction, 2026-08-31: "copied verbatim from the real
// client source" above was correct but insufficient -- urql never sends
// the web SOURCE text over the wire, and the gap is BIGGER than a
// reflowed argument list. `client.query(...)` (web's graphqlFetch,
// server.ts) hands urql a string; the real exchange chain
// (`[timingExchange, errorExchange, cacheExchange, fetchExchange]`,
// server.ts:61) then does TWO transformations before anything reaches
// the network, not one:
//  1. `cacheExchange` maps every query through `formatDocument`
//     (@urql/core's own `mapTypeNames`), which injects a `__typename`
//     selection into every non-root selection set -- this is NOT
//     optional or cache-implementation-specific, it runs for any client
//     that includes `cacheExchange` before `fetchExchange`, which this
//     repo's server client does.
//  2. `fetchExchange` then calls `stringifyDocument`, which is
//     graphql-js-family `print()` output (@0no-co/graphql.web) and
//     reflows an argument list once it exceeds 80 characters -- this
//     operation's field line is 122 characters in the web source's
//     single-line form.
//
// A digest computed from print() output alone (skipping step 1) still
// disagrees with a real request: verified by capturing an actual request
// off this repo's own unmodified graphqlFetch and finding its digest did
// NOT match a print()-only recomputation, only a
// print(formatDocument(...)) one. Every real featureFlags request 404'd
// and silently fell back to Python, invisibly, because plan §5 defines
// "digest miss" as "stay on Python". The text below is now the WIRE form
// (the web repo's `scripts/graphql-wire-parity.ts` calls @urql/core's
// real `createRequest` + `formatDocument` + `stringifyDocument`, in that
// order -- not a hand-reflowed guess and not a partial reproduction), and
// CI's graphql-wire-parity gate (web repo's `.github/workflows/tests.yml`
// `graphql-wire-parity` job, ops repo's `go.yml` `graphql-wire-parity`
// job) keeps it that way: it fails the moment this text and the web
// repo's pinned urql wire form disagree, for this or any of the other 11
// registered documents. See
// cmd/query-api/testdata/wire_capture/featureflags_captured.graphql and
// its README for a byte-for-byte fixture captured off a real HTTP
// request produced by this repo's actual graphqlFetch code path against
// a real local HTTP listener -- not reconstructed from source and not
// produced by invoking urql's printer alone -- and
// query_route_wire_capture_test.go, which asserts this const's digest
// against that captured fixture's digest independently of this file's
// own doc comment claims.
const registeredFeatureFlagsDocument = `query FeatureFlagRegistry($orgId: String!, $provider: String, $project: String, $includeArchived: Boolean, $limit: Int!) {
  featureFlags(
    orgId: $orgId
    provider: $provider
    project: $project
    includeArchived: $includeArchived
    limit: $limit
  ) {
    flags {
      flagId
      flagKey
      provider
      projectKey
      flagType
      createdAt
      archivedAt
      __typename
    }
    totalCount
    degradedReason
    __typename
  }
}`

// registeredReviewEdgesDocument is CHAOS-4368 Wave 2's registered document
// for the reviewEdges operation -- same "registered documents only"
// contract registeredFeatureFlagsDocument's doc comment explains, applied
// to a second operation. Copied byte-for-byte from the REAL production
// query web actually sends (web/src/lib/graphql/queries.ts's
// REVIEW_EDGES_QUERY, operation name "ReviewEdges", input variable named
// `$input` of type `ReviewEdgesInput!` -- NOT individual scalar args, a
// different shape than featureFlags's query above) -- learning Wave 1's
// codex-round-3 lesson forward: this text is sourced from the client file
// itself, not reconstructed from the SDL or an operation-inventory doc,
// so the digest this route checks is the digest a real web request
// actually produces.
const registeredReviewEdgesDocument = `query ReviewEdges($input: ReviewEdgesInput!) {
  reviewEdges(input: $input) {
    edges {
      reviewer
      author
      reviewsCount
      day
      repoId
      __typename
    }
    totalCount
    __typename
  }
}`

// registeredCognitiveLoadDocument is CHAOS-4369 Wave 3's registered
// document for the cognitiveLoad operation -- same "registered documents
// only" contract, sourced byte-for-byte from the REAL production query
// (web/src/lib/graphql/queries.ts's COGNITIVE_LOAD_QUERY, operation name
// "CognitiveLoad", input variable named `$input` of type
// `CognitiveLoadInput!` -- same single-input-object shape as
// registeredReviewEdgesDocument above, not featureFlags's individual
// scalar args).
const registeredCognitiveLoadDocument = `query CognitiveLoad($input: CognitiveLoadInput!) {
  cognitiveLoad(input: $input) {
    orgId
    teamId
    totalDays
    signals {
      day
      prInterruptionLoad
      contextSpreadCount
      reviewRequestLoad
      afterHoursCommitRatio
      weekendCommitRatio
      __typename
    }
    __typename
  }
}`

// registeredComplexityTimeseriesDocument is CHAOS-4369 Wave 3's registered
// document for the complexityTimeseries operation -- same "registered
// documents only" contract, same "sourced from the real client file, not
// reconstructed" discipline as registeredReviewEdgesDocument above. Copied
// byte-for-byte from web/src/lib/graphql/queries.ts's
// COMPLEXITY_TIMESERIES_QUERY, operation name "ComplexityTimeseries",
// input variable named `$input` of type `ComplexityTimeseriesInput!`.
const registeredComplexityTimeseriesDocument = `query ComplexityTimeseries($input: ComplexityTimeseriesInput!) {
  complexityTimeseries(input: $input) {
    points {
      date
      scopeId
      scopeName
      locTotal
      cyclomaticPerKloc
      cyclomaticTotal
      cyclomaticAvg
      highComplexityFunctions
      veryHighComplexityFunctions
      __typename
    }
    totalScope
    __typename
  }
}`

// registeredHotspotsDocument is CHAOS-4369 Wave 3's registered document
// for the hotspots operation (the second of the two Wave 3 operations,
// after complexityTimeseries) -- same "registered documents only"
// contract, same "sourced from the real client file, not reconstructed"
// discipline as registeredReviewEdgesDocument above. Copied byte-for-byte
// from web/src/lib/graphql/queries.ts's HOTSPOTS_QUERY, operation name
// "Hotspots", input variable named `$input` of type `HotspotsInput!`.
const registeredHotspotsDocument = `query Hotspots($input: HotspotsInput!) {
  hotspots(input: $input) {
    rows {
      filePath
      repoId
      repoName
      churnLoc30d
      churnCommits30d
      cyclomaticTotal
      cyclomaticAvg
      blameConcentration
      riskScore
      evidenceUrl
      __typename
    }
    __typename
  }
}`

// registeredOperatingReviewDocument is CHAOS-4352 Wave 4 Lane B's
// (CHAOS-4505) registered document for the operatingReview operation --
// same "registered documents only" contract, same "sourced from the real
// client file, not reconstructed" discipline as every document above.
// Copied byte-for-byte from web/src/lib/graphql/queries.ts's
// OPERATING_REVIEW_QUERY (queries.ts:394-425), operation name
// "OperatingReview", TWO variables -- `$orgId` of type `String!` AND
// `$input` of type `OperatingReviewInput!` -- unlike every other operation
// registered in this file except featureFlags, matching the schema's
// `operatingReview(orgId: String!, input: OperatingReviewInput!)` (the
// $orgId variable is parsed by this route but never trusted for scoping;
// see operatingreview package's doc comment's Authorization section).
const registeredOperatingReviewDocument = `query OperatingReview($orgId: String!, $input: OperatingReviewInput!) {
  operatingReview(orgId: $orgId, input: $input) {
    orgId
    teamId
    weekStart
    priorWeekStart
    sections {
      key
      title
      changed
      improved
      worsened
      metrics {
        key
        label
        value
        unit
        delta {
          value
          priorValue
          absolute
          percent
          status
          __typename
        }
        __typename
      }
      __typename
    }
    recommendations
    recommendationsEmptyState
    __typename
  }
}`

// registeredWorkGraphEdgesDocument is CHAOS-4352 Wave 4 Lane A's
// (CHAOS-4504) registered document for the workGraphEdges operation --
// same "registered documents only" contract, same "sourced from the real
// client file, not reconstructed" discipline as
// registeredReviewEdgesDocument above. Copied byte-for-byte from
// web/src/lib/graphql/queries.ts:427's WORK_GRAPH_EDGES_QUERY, operation
// name "WorkGraphEdges", `$orgId`/`$filters` scalar+input arguments (NOT
// a single wrapping `$input` object, a different shape than
// reviewEdges/cognitiveLoad/complexityTimeseries/hotspots above --
// matches featureFlags's individual-argument shape instead).
const registeredWorkGraphEdgesDocument = `query WorkGraphEdges($orgId: String!, $filters: WorkGraphEdgeFilterInput) {
  workGraphEdges(orgId: $orgId, filters: $filters) {
    edges {
      edgeId
      sourceType
      sourceId
      sourceDisplayName
      targetType
      targetId
      targetDisplayName
      edgeType
      provenance
      confidence
      evidence
      repoId
      provider
      theme
      subcategory
      __typename
    }
    totalCount
    pageInfo {
      hasNextPage
      hasPreviousPage
      startCursor
      endCursor
      __typename
    }
    degradedReason
    __typename
  }
}`

// registeredWorkGraphFlowDocument is CHAOS-4504's registered document for
// the workGraphFlow operation. Copied byte-for-byte from
// web/src/lib/graphql/queries.ts:462's WORK_GRAPH_FLOW_QUERY, operation
// name "WorkGraphFlow".
const registeredWorkGraphFlowDocument = `query WorkGraphFlow($orgId: String!, $filters: WorkGraphEdgeFilterInput) {
  workGraphFlow(orgId: $orgId, filters: $filters) {
    rows {
      nodeType
      inflow
      outflow
      __typename
    }
    degradedReason
    __typename
  }
}`

// registeredWorkGraphArtifactsDocument is CHAOS-4504's registered document
// for the workGraphArtifacts operation. Copied byte-for-byte from
// web/src/lib/graphql/queries.ts:477's WORK_GRAPH_ARTIFACTS_QUERY,
// operation name "WorkGraphArtifacts".
const registeredWorkGraphArtifactsDocument = `query WorkGraphArtifacts($orgId: String!, $filters: WorkGraphEdgeFilterInput) {
  workGraphArtifacts(orgId: $orgId, filters: $filters) {
    rows {
      nodeType
      nodeId
      displayName
      degree
      evidence
      __typename
    }
    degradedReason
    __typename
  }
}`

// registeredFlowMatrixDocument is CHAOS-4506 Wave 4's registered document
// for the flowMatrix operation, ONE of the three analytics-touching
// documents in web/src/lib/graphql/queries.ts -- the ONLY one this PR
// registers. Copied byte-for-byte from queries.ts:56-74's
// FLOW_MATRIX_QUERY, operation name "FlowMatrix".
//
// The other two analytics documents, INVESTMENT_BREAKDOWN_QUERY
// (queries.ts:9) and INVESTMENT_FULL_QUERY (queries.ts:222), are
// DELIBERATELY NOT registered here -- both are investment-path-only in
// live traffic (every real caller in investmentFetchers.ts/
// hooks/useInvestment.ts sets useInvestment: true; useAnalytics.ts's
// InvestmentBreakdown caller does too), and this PR's
// internal/analytics port explicitly rejects useInvestment=true rather
// than silently answering with non-investment numbers. Registering
// either of those two documents before the investment-path follow-up
// ticket lands would let real traffic reach that rejection in
// production instead of staying on Python -- register them only once
// that follow-up ships.
//
// FlowMatrix is safe to register precisely because it is NOT gated the
// same way: compile_flow_matrix's TEAM/REPO/WORK_TYPE branch never reads
// use_investment at all (Python source, compiler.py:495-518), and the
// real live caller (web/src/lib/graphql/hooks/useChordFlow.ts) sends
// useInvestment=true on every request for exactly those three
// dimensions -- proven safe by
// TestResolve_FlowMatrix_RealClientShape_BatchUseInvestmentTrueDoesNotReject
// in internal/analytics, not assumed from the document text alone.
const registeredFlowMatrixDocument = `query FlowMatrix($orgId: String!, $batch: AnalyticsRequestInput!) {
  analytics(orgId: $orgId, batch: $batch) {
    flowMatrix {
      nodes {
        id
        label
        dimension
        value
        __typename
      }
      edges {
        source
        target
        value
        __typename
      }
      __typename
    }
    __typename
  }
}`

// registeredInvestmentBreakdownDocument is CHAOS-4538's registered
// document for the investment-path `analytics` breakdown query -- same
// "registered documents only" contract, same "sourced from the real
// client file, not reconstructed" discipline as every document above.
// Copied byte-for-byte from web/src/lib/graphql/queries.ts:10-28's
// INVESTMENT_BREAKDOWN_QUERY, operation name "InvestmentBreakdown".
//
// REGISTRATION IS NOT ENABLEMENT (chris's standing ruling, carried
// verbatim from CHAOS-4538's brief): PostgresSwitch.Enabled() is
// fail-closed (routeswitch/postgres_switch.go) -- a missing registry
// row, a lookup error, or an unresolvable digest all return false and
// Python keeps serving every real request. Registering this document
// only makes it POSSIBLE for a future, separately-decided enablement to
// route traffic here; it does not itself route anything. Whether/when
// to enable is the orchestrator's/chris's call, not this port's --
// "register the two documents" is CHAOS-4538's literal scope item 5,
// "enable" is explicitly NOT.
//
// This document's name implies "investment-only" but the schema does
// not enforce that: `useInvestment` is a batch-level VARIABLE the web
// client sets inside `$batch` at call time (schema.graphql's
// AnalyticsRequestInput.useInvestment), never pinned in this document's
// text -- see this package's investment.go doc comments for the
// investment-path SQL this operation can now reach.
const registeredInvestmentBreakdownDocument = `query InvestmentBreakdown($orgId: String!, $batch: AnalyticsRequestInput!) {
  analytics(orgId: $orgId, batch: $batch) {
    breakdowns {
      dimension
      measure
      items {
        key
        value
        __typename
      }
      __typename
    }
    evidenceQualityDistribution
    evidenceQualityStats {
      mean
      stddev
      total
      bandCounts
      __typename
    }
    __typename
  }
}`

// registeredInvestmentFullDocument is CHAOS-4538's registered document
// for the combined investment breakdowns+sankey `analytics` query --
// same contract as registeredInvestmentBreakdownDocument above. Copied
// byte-for-byte from web/src/lib/graphql/queries.ts:223-252's
// INVESTMENT_FULL_QUERY, operation name "InvestmentFull".
//
// Deliberately NOT registered here: INVESTMENT_SANKEY_QUERY
// (queries.ts:32) -- it is dead and is being deleted in parallel under
// CHAOS-4496; a dead document must not be registered as if it carried
// traffic. This document's own `sankey { ... coverage { ... } }`
// selection includes SankeyResult.Coverage, which this port's
// analytics.Resolve does NOT compute (always nil -- resolve.go's
// package doc comment); registering the document is still correct
// (registration gates reachability, not response completeness, and
// nothing is enabled regardless), but a caller enabling this operation
// later must know `coverage` resolves to null on the Go plane until
// that follow-up lands.
const registeredInvestmentFullDocument = `query InvestmentFull($orgId: String!, $batch: AnalyticsRequestInput!) {
  analytics(orgId: $orgId, batch: $batch) {
    breakdowns {
      dimension
      measure
      items {
        key
        value
        __typename
      }
      __typename
    }
    sankey {
      nodes {
        id
        label
        dimension
        value
        __typename
      }
      edges {
        source
        target
        value
        __typename
      }
      coverage {
        teamCoverage
        repoCoverage
        __typename
      }
      unit
      __typename
    }
    __typename
  }
}`

// digestHex is a thin wrapper over the ONE canonical document-digest
// algorithm (CHAOS-4696): sha256(strings.TrimSpace(text)), hex-encoded,
// now shared code in cmd/query-api/internal/digest so
// cmd/query-api/tools/registrydump computes the EXACT SAME digest this
// running process does -- not a second hand-typed copy of a two-line
// function that could silently drift from this one. (The schema-digest
// half of what this comment used to call "no canonical algorithm has
// landed" is a separate follow-up PR's concern -- see this file's
// GO_API_SCHEMA_DIGEST verification for that half; this function is
// document digest only.)
func digestHex(document string) string {
	return digest.Document(document)
}

// queryRouteConfig is env-sourced configuration for the live /query
// route. All fields are required together -- see loadQueryRouteConfig.
type queryRouteConfig struct {
	ClickHouseURI       string
	RegistryPostgresURI string
	EnvelopeJWKSPath    string
	EnvelopeIssuer      string
	EnvelopeAudience    string
	SchemaDigest        string
}

// loadQueryRouteConfig reads the /query route's configuration from the
// environment. ok is false when ANY required variable is unset -- Wave
// 0's default ("nothing mounted, only /healthz and /readyz live") is
// preserved for any caller that does not set these, rather than the
// process failing to start. This mirrors the deployment shape: a plain
// `go build`/`go vet`/CI run, or an operator who has not yet configured
// this service's dependencies, must not be forced to also configure
// ClickHouse/Postgres/JWKS just to build or run the binary.
func loadQueryRouteConfig() (queryRouteConfig, bool) {
	cfg := queryRouteConfig{
		ClickHouseURI:       os.Getenv("CLICKHOUSE_URI"),
		RegistryPostgresURI: os.Getenv("GO_API_REGISTRY_POSTGRES_URI"),
		EnvelopeJWKSPath:    os.Getenv("GO_API_ENVELOPE_JWKS_PATH"),
		EnvelopeIssuer:      os.Getenv("GO_API_ENVELOPE_ISSUER"),
		EnvelopeAudience:    os.Getenv("GO_API_ENVELOPE_AUDIENCE"),
		SchemaDigest:        os.Getenv("GO_API_SCHEMA_DIGEST"),
	}
	if cfg.ClickHouseURI == "" || cfg.RegistryPostgresURI == "" || cfg.EnvelopeJWKSPath == "" ||
		cfg.EnvelopeIssuer == "" || cfg.EnvelopeAudience == "" || cfg.SchemaDigest == "" {
		return cfg, false
	}
	return cfg, true
}

// queryRouteMaxResultRows overrides dev-health-go/clickhouse's per-request
// safety-net default (Options.MaxResultRows=1,000) for THIS route.
// queryRouteMaxBytesToRead has been RETIRED (CHAOS-4651, below); the route
// now sends ClickHouse's own "unrestricted" for that setting instead of
// naming a client-side value at all.
//
// ROOT DEFECT (CHAOS-4647): those two defaults were calibrated by
// CHAOS-3848 for a completely different endpoint -- a 200-row
// pull_requests batch -- and borrowed here, unexamined, for an endpoint
// that legitimately reads whole-org history. That mismatch, not either
// specific number, is what actually broke hotspots and workGraphEdges
// against real org 70d529e0 data (EXECUTED, live-local runner) while
// every unit test, gofmt/vet/build, and prior codex review stayed green
// -- none of those send SQL to a real engine. Both PASS on
// producer-seeded scratch, whose working set sits far below either
// ceiling, and neither failure is malformed SQL or caller error: an
// org-wide hotspots read and a real membership graph are both
// spec-valid per contracts/graphql/v1/schema.graphql.
//
//   - MaxBytesToRead: RETIRED (CHAOS-4651, dev-health-go v0.6.1). This was
//     never a capacity-boundable value -- it protects ClickHouse's OWN
//     read volume, which is the engine's resource, governed by its
//     server profile, not a per-client guess. CHAOS-4653 measured that
//     premise live rather than assuming it: querying system.settings
//     directly on BOTH the local dev-health-clickhouse-1 container and
//     prod's fullchaosdev-clickhouse-1 (via `ssh oci`, read-only) shows
//     max_bytes_to_read/max_result_rows/max_rows_to_read/
//     max_execution_time/max_memory_usage/max_concurrent_queries_for_user
//     all at 0 (unrestricted) with changed=0 on BOTH -- so sending
//     unlimited here restores the status quo those two engines already
//     run under; it does not open a new hole. CHAOS-4652 measured the
//     real cost this was gating: an unscoped hotspots scan (repoIds
//     omitted, spec-valid) reads 6,942,432 rows / ~1002 MiB of
//     file_hotspot_daily against real org 70d529e0 data -- confirmed via
//     system.query_log -- because that table's sort key
//     (repo_id, day, file_path) has no org_id predicate to serve
//     org-scoping, so an unscoped read degenerates to a full scan of the
//     retention window; an 8x-headroom 512 MiB ceiling was tried first
//     and also failed, at 513.63 MiB. That is what "no client-side cap"
//     actually has to tolerate -- Python's equivalent queries impose none
//     and have run this way safely for years. dev-health-go @v0.4.0 could
//     not express literal zero: applyOptions' defaultPositiveUint64
//     substituted its positive fallback whenever MaxBytesToRead was <= 0,
//     so deleting the field did NOT remove the cap, it silently reverted
//     to the ORIGINAL 64 MiB bug. v0.6.1 makes MaxBytesToRead a *uint64:
//     nil now means "unset, use the 64 MiB default" and a non-nil pointer
//     to 0 means literal "unrestricted", sent to the driver unchanged
//     (clickhouse/options.go's resolveCeilingUint64). Those are NOT
//     interchangeable -- deleting the field lands on nil, i.e. the 64 MiB
//     default, i.e. CHAOS-4647 again. newQueryRouteClickHouseClient below
//     therefore passes an explicit pointer to a zero-valued local, never
//     an absent field. See query_route_integration_test.go's
//     tip_config_sends_unrestricted_max_bytes_to_read subtest, which
//     reads system.settings back through this exact constructor and
//     failed RED (observed "67108864", not "0") against a deliberate
//     "just delete the field" version of this function before this fix
//     was written.
//
//   - MaxResultRows (queryRouteMaxResultRows below): still PROVISIONAL,
//     successor CHAOS-4654, unchanged by this PR. This
//     IS capacity-boundable in principle -- it protects THIS PROCESS
//     (query-api's own pooled connections, MaxOpenConns=8 by default,
//     and the in-memory row slice every resolver buffers before the
//     HTTP response is written) -- but a capacity derivation needs a
//     declared container memory budget, and query-api has none: no
//     mem_limit/deploy.resources in deploy/go-api/compose-query-api.yml
//     (its only deploy artifact), no k8s/helm manifest at all (checked
//     2026-08-31 -- this service hasn't reached that deploy layer yet).
//     A capacity number derived from an unknown capacity is a workload
//     guess wearing better clothes, so this is NOT that: it is still a
//     WORKLOAD derivation -- 4*workgraph.MaxEdgesLimit (edges.go's
//     already-enforced ceiling on a single workGraphEdges request's
//     filters.limit; 2 endpoints/edge x 2 membership rows/endpoint --
//     see the long comment trail this constant's git blame carries)
//     plus 100,000 rows of headroom -- proven EXECUTED and CONFIRMED
//     against real org 70d529e0 data (live-local runner, 12/12, three
//     times) but NOT proof against a fan-out dimension nobody has yet
//     found, which is exactly how rounds 1 and 2 of this same review
//     each undercounted the round before. CHAOS-4654 makes "give
//     query-api a declared container memory budget" a precondition of
//     actually DEPLOYING this service, not of merging this PR; once
//     that budget exists, re-derive this value from it (rows this
//     process can safely buffer per request, generously above any
//     legitimate result -- real results here are thousands of rows, a
//     capacity bound should land orders of magnitude above that) and
//     delete the workgraph.MaxEdgesLimit dependency below entirely,
//     since a capacity-derived bound needs no fan-out arithmetic at
//     all. INVALIDATED BY: CHAOS-4654 landing a declared memory budget
//     (re-derive from capacity then) OR a THIRD undiscovered fan-out
//     multiplier tripping this value first (see
//     TestCategoryKindCardinalityHasNoNewValue in membership_test.go --
//     it fails loudly the moment a new category_kind value appears,
//     which is the earliest possible signal that this workload
//     derivation's assumptions changed).
const queryRouteMaxResultRows uint = 4*workgraph.MaxEdgesLimit + 100_000 // = 500,000 -- PROVISIONAL, workload derivation, successor CHAOS-4654

// newQueryRouteClickHouseClient is the ONE place this route constructs its
// ClickHouse client -- pulled out of buildQueryRoute so a test can exercise
// the REAL production wiring (these exact options reaching the real
// driver) instead of a hand-copied literal that could silently drift from
// what buildQueryRoute actually does (codex review round 1, P3).
func newQueryRouteClickHouseClient(dsn string) (*dhclickhouse.Client, error) {
	maxResultRows := queryRouteMaxResultRows
	// Explicit pointer to a zero-valued local, NOT an absent field: nil
	// means "unset, use the 64 MiB default" under dev-health-go v0.6.1
	// (clickhouse/options.go's resolveCeilingUint64), and a deleted field
	// zero-values to nil. A non-nil pointer to 0 is the only way to reach
	// ClickHouse's own "unrestricted" -- see the long comment above.
	maxBytesToRead := uint64(0)
	return dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{
		DSN:            dsn,
		MaxResultRows:  &maxResultRows,
		MaxBytesToRead: &maxBytesToRead,
	})
}

// buildQueryRoute wires the real featureFlags path from env-sourced
// config: the shared dev-health-go ClickHouse client, a real Postgres
// pool, and the effective-principal verifier, then hands them to
// newQueryHandler. The returned cleanup func closes the Postgres pool;
// call it on shutdown. The returned ready func is CHAOS-4512's fix: a
// live liveness check of the SAME two dependencies /query actually reads
// (ClickHouse via chClient.Ping, the registry Postgres pool via
// pgPool.Ping), for main.go's readyzHandler to call on every /readyz
// request -- co-located here, not in main.go, because these are the only
// two places this route's live dependency handles exist. See
// readyzHandler's doc comment in main.go for the full contract this
// closes over.
func buildQueryRoute(cfg queryRouteConfig) (http.HandlerFunc, func(context.Context) error, func(), error) {
	chClient, err := newQueryRouteClickHouseClient(cfg.ClickHouseURI)
	if err != nil {
		return nil, nil, nil, err
	}
	// Eager readiness check, matching cmd/dev-health-worker's own
	// documented contract for this exact env var (deploy/go-workers/
	// README.md, "ClickHouse: the Go worker needs the native port, not
	// the HTTP port"): CLICKHOUSE_URI resolves to a DIFFERENT port for a
	// Go process (native wire protocol, :9000 locally) than for a Python
	// process (HTTP, :8123 locally) despite sharing the same env var
	// name across this repo's deployments -- operator-configured per
	// process, not auto-translated here (codex review, 2026-08-28: this
	// route previously mounted successfully even when CLICKHOUSE_URI was
	// the repo-standard HTTP endpoint, then failed every request with a
	// handshake error instead of failing loudly at startup). Ping now so
	// a misconfigured endpoint refuses to start, the same "measurement
	// that did not happen must FAIL, loudly" discipline dev-health-worker
	// already applies to this identical class of mistake.
	pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := chClient.Ping(pingCtx); err != nil {
		return nil, nil, nil, fmt.Errorf("query-api: ClickHouse readiness check failed (CLICKHOUSE_URI must be the NATIVE protocol port, not the HTTP port -- see deploy/go-workers/README.md): %w", err)
	}

	pgPool, err := pgxpool.New(context.Background(), cfg.RegistryPostgresURI)
	if err != nil {
		return nil, nil, nil, err
	}

	verifier, err := principal.NewVerifier(cfg.EnvelopeJWKSPath, cfg.EnvelopeIssuer, cfg.EnvelopeAudience)
	if err != nil {
		pgPool.Close()
		return nil, nil, nil, err
	}

	handler := newQueryHandler(chClient, pgPool, verifier, cfg.SchemaDigest)
	cleanup := func() { pgPool.Close() }
	ready := readinessCheck(chClient, pgPool)
	return handler, ready, cleanup, nil
}

// readinessCheck returns a func that pings BOTH of /query's live
// dependencies -- ClickHouse and the registry Postgres pool -- and
// returns the first error either reports. This is CHAOS-4512's actual
// fix: buildQueryRoute above already pings ClickHouse ONCE, eagerly, at
// startup (so a ClickHouse that is down at boot refuses to start the
// process at all -- "measurement that did not happen must FAIL, loudly").
// That one-shot check says nothing about a ClickHouse that goes
// unreachable LATER, and says nothing about Postgres at all:
// pgxpool.New above is lazy by design (pgx does not dial until a query
// or Ping is issued), so an unreachable or misconfigured
// GO_API_REGISTRY_POSTGRES_URI has never failed startup -- the process
// comes up, the OLD readyzHandler answered 200 unconditionally, and every
// real request then failed against a pool that has never once connected.
// Calling this on every /readyz request (with a bounded timeout applied
// by the caller) is what closes that gap: readiness now reflects the
// CURRENT reachability of both dependencies, not their state at process
// start.
func readinessCheck(chClient *dhclickhouse.Client, pgPool *pgxpool.Pool) func(context.Context) error {
	return func(ctx context.Context) error {
		if err := chClient.Ping(ctx); err != nil {
			return fmt.Errorf("clickhouse: %w", err)
		}
		if err := pgPool.Ping(ctx); err != nil {
			return fmt.Errorf("registry postgres: %w", err)
		}
		return nil
	}
}

// newQueryHandler wires the routeswitch.Mux + registry-backed
// PostgresSwitch + gqlgen handler pipeline over ALREADY-CONSTRUCTED
// dependencies -- the plan §6 "deploy an empty Go query-api and prove a
// route becomes reachable when, and only when, its individual switch is
// enabled" contract, now with real resolvers behind it (CHAOS-4367 Wave 1
// featureFlags; CHAOS-4368 Wave 2 reviewEdges). Split out from
// buildQueryRoute so a reachability test can wire this exact pipeline
// against a real Postgres testcontainer and a fake ClickHouse client,
// without needing a real ClickHouse or a real CLICKHOUSE_URI to prove the
// SWITCH half of the contract -- see query_route_integration_test.go.
//
// Both operations share the SAME gqlgen handler instance (one schema, one
// executable server -- gqlgen's handler is safe for concurrent reuse
// across requests) but are registered under DISTINCT Mux operation keys,
// each gated by its own go_api_routing_state row: enabling featureFlags
// does not enable reviewEdges and vice versa.
//
// Inherited, pre-existing gap this wave does NOT close (codex review,
// 2026-08-28, re-raising it against this route -- it is PostgresSwitch's
// own documented gap #2, not something introduced here): eligible_orgs
// and rollout_percentage are not enforced. Mode=canary/primary is
// reachable for every authenticated org once dispatched here, because
// Switch.Enabled(operation string) takes no org argument at all -- see
// PostgresSwitch's doc comment in internal/routeswitch/postgres_switch.go
// for why (threading org through Enabled is a later wave's job, the same
// wave that would also verify request document identity, gap #1). Both
// waves are local dual-run proof only (plan §5 stage 2); org-scoped
// canary enforcement is a stage-5 concern this PR does not claim to
// satisfy.
// maxUnwrapChainLogBytes bounds the CHAOS-4647 unwrap-chain log line
// (codex review, merge-gate round, P3 ARGUED): the deepest cause is
// frequently a ClickHouse *proto.Exception, whose Message field is
// server-authored text this process does not control -- some ClickHouse
// error classes echo back query/data fragments, so an unbounded join
// could put an arbitrarily large or awkward line into the process log.
// Truncating caps that exposure; it is a size bound, not a redaction
// guarantee -- this log line already carried backend-authored
// diagnostic text before this change and still does, just with a hard
// ceiling on how much.
const maxUnwrapChainLogBytes = 4096

// truncateForLog bounds s to at most max bytes, appending how much was
// cut so a truncated line is visibly truncated rather than looking
// complete. A pure function so the CHAOS-4647 P3 fix has a direct,
// fast unit test instead of only being provable by capturing real log
// output.
func truncateForLog(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + fmt.Sprintf("... [truncated, %d bytes total]", len(s))
}

func newQueryHandler(chClient featureflags.QueryClient, pgPool *pgxpool.Pool, verifier *principal.Verifier, schemaDigest string) http.HandlerFunc {
	// digestByOperation is this route's registered-document inventory:
	// operation name -> the sha256 digest of that operation's registered
	// document text. CHAOS-4369 Wave 3 generalizes what Wave 1/2 hardcoded
	// as two named locals (featureFlagsDigest, reviewEdgesDigest) into a
	// map keyed by operation name, so a later wave adding a further
	// operation extends this map rather than threading another positional
	// parameter through newQueryHandler/operationForDocument.
	// NOTE (CHAOS-4538): "investmentBreakdown"/"investmentFull" below are
	// this route's OWN internal Mux/PostgresSwitch operation keys, chosen
	// to disambiguate the TWO distinct registered documents that both
	// invoke the SAME GraphQL root field (`analytics`) -- unlike every
	// other row in this map, where the key already matches the GraphQL
	// field name 1:1. operationForDocument resolves by DOCUMENT DIGEST,
	// never by GraphQL operation/field name, so this key only has to be
	// internally consistent across this map, digestByOperation's reverse
	// index, and each go_api_routing_state row PostgresSwitch looks up by
	// this same string -- it is never compared against request text.
	digestByOperation := map[string]string{
		"featureFlags":         digestHex(registeredFeatureFlagsDocument),
		"reviewEdges":          digestHex(registeredReviewEdgesDocument),
		"cognitiveLoad":        digestHex(registeredCognitiveLoadDocument),
		"complexityTimeseries": digestHex(registeredComplexityTimeseriesDocument),
		"hotspots":             digestHex(registeredHotspotsDocument),
		"operatingReview":      digestHex(registeredOperatingReviewDocument),
		"workGraphEdges":       digestHex(registeredWorkGraphEdgesDocument),
		"workGraphFlow":        digestHex(registeredWorkGraphFlowDocument),
		"workGraphArtifacts":   digestHex(registeredWorkGraphArtifactsDocument),
		"flowMatrix":           digestHex(registeredFlowMatrixDocument),
		"investmentBreakdown":  digestHex(registeredInvestmentBreakdownDocument),
		"investmentFull":       digestHex(registeredInvestmentFullDocument),
	}
	sw := routeswitch.NewPostgresSwitch(pgPool, schemaDigest, digestByOperation)
	routeMux := routeswitch.NewMux(sw)

	// operationByDigest is digestByOperation's reverse index, built once
	// here rather than on every request -- operationForDocument does a
	// single map lookup per request, not a linear scan.
	operationByDigest := make(map[string]string, len(digestByOperation))
	for operation, digest := range digestByOperation {
		operationByDigest[digest] = operation
	}

	schema := graph.NewExecutableSchema(graph.Config{Resolvers: &graph.Resolver{ClickHouse: chClient}})
	gqlHandler := gqlhandler.NewDefaultServer(schema)
	// CHAOS-4647 diagnostic: the process log carries nothing per-request,
	// and gqlgen's default presenter surfaces only err.Error() -- which for
	// a dev-health-go *operationError (clickhouse/client.go) is the fixed
	// string "ClickHouse <operation> failed" with the real driver cause
	// reachable only via Unwrap(). Log the full Unwrap() chain server-side
	// on every resolver error so a live-data failure's actual cause is
	// visible without changing the response the client sees.
	//
	// Bounded (codex review, merge-gate round, P3 ARGUED): the deepest
	// cause here is frequently a ClickHouse *proto.Exception, whose
	// Message field is server-authored text this process does not
	// control -- some ClickHouse error classes echo back query/data
	// fragments, so an unbounded join could put an arbitrarily large or
	// awkward line into the process log. Truncating caps that exposure;
	// it is a size bound, not a redaction guarantee -- this log line was
	// already carrying backend-authored diagnostic text before this
	// change and still does, just with a hard ceiling on how much.
	gqlHandler.SetErrorPresenter(func(ctx context.Context, err error) *gqlerror.Error {
		chain := []string{err.Error()}
		for unwrapped := errors.Unwrap(err); unwrapped != nil; unwrapped = errors.Unwrap(unwrapped) {
			chain = append(chain, unwrapped.Error())
		}
		log.Printf("query-api: resolver error unwrap chain: %s", truncateForLog(strings.Join(chain, " <- "), maxUnwrapChainLogBytes))
		return graphql.DefaultErrorPresenter(ctx, err)
	})
	for operation := range digestByOperation {
		routeMux.Register(operation, gqlHandler)
	}

	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		// Same body-size contract the Python edge's
		// GraphQLQuerySizeLimitMiddleware enforces for /graphql
		// (security.py's GRAPHQL_MAX_QUERY_BYTES, default 16 KiB) --
		// codex review, 2026-08-28: reading up to 1 MiB unconditionally
		// let a body between the configured limit and 1 MiB through
		// silently, bypassing that existing request-size contract for
		// this canaried operation. LimitReader+1 lets a body of EXACTLY
		// the limit succeed while still detecting one byte over it,
		// without buffering the oversized remainder.
		limit := graphQLMaxQueryBytes()
		bodyBytes, readErr := io.ReadAll(io.LimitReader(r.Body, int64(limit)+1))
		_ = r.Body.Close()
		if readErr != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if len(bodyBytes) > limit {
			http.Error(w, "GraphQL request body exceeds size limit", http.StatusRequestEntityTooLarge)
			return
		}

		var parsed struct {
			Query string `json:"query"`
		}
		if err := json.Unmarshal(bodyBytes, &parsed); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		operation, ok := operationForDocument(parsed.Query, operationByDigest)
		if !ok {
			// Unregistered document: plan §5's safe default ("unregistered
			// documents ... stay on Python") applied at this router --
			// indistinguishable from an unknown route, exactly like an
			// operation with no Mux registration at all.
			//
			// CHAOS-4696 telemetry: this branch was previously SILENT --
			// exactly the failure mode that hid featureFlags's digest-miss
			// from every real request for as long as this route existed.
			// A digest miss is either a real, un-registered document (the
			// intended safe default) or a registered document whose const
			// has drifted from what a real client sends (the CHAOS-4696
			// defect class); an operator cannot tell those apart without a
			// log line naming the digest that missed. Bounded the same way
			// the CHAOS-4647 unwrap-chain log line is (maxUnwrapChainLogBytes
			// above): the query text is caller-supplied, so truncate before
			// logging it rather than trusting its length.
			log.Printf(
				"query-api: unregistered document digest-miss: digest=%s query=%s",
				digestHex(parsed.Query), truncateForLog(parsed.Query, maxUnwrapChainLogBytes),
			)
			http.NotFound(w, r)
			return
		}

		token, ok := bearerToken(r.Header.Get("Authorization"))
		if !ok {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		claims, err := verifier.Verify(token)
		if err != nil {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		r = r.WithContext(authctx.WithClaims(r.Context(), authctx.Claims{OrgID: claims.OrgID}))
		r.Body = io.NopCloser(bytes.NewReader(bodyBytes))

		routeMux.Dispatch(operation, w, r)
	}
}

// operationForDocument resolves a request's raw query text to a
// registered operation name -- "registered documents only" (plan §7 open
// decision 2), never an AST-shape match. operationByDigest is the reverse
// (digest -> operation) index newQueryHandler builds once from
// digestByOperation, so this is a single O(1) map lookup per request
// regardless of how many operations this route registers, rather than a
// growing chain of per-operation switch cases.
func operationForDocument(query string, operationByDigest map[string]string) (string, bool) {
	operation, ok := operationByDigest[digestHex(query)]
	return operation, ok
}

// defaultGraphQLMaxQueryBytes mirrors security.py's
// DEFAULT_GRAPHQL_MAX_QUERY_BYTES (16 KiB) exactly.
const defaultGraphQLMaxQueryBytes = 16 * 1024

// graphQLMaxQueryBytes mirrors security.py's get_graphql_max_query_bytes:
// same env var name, same fall-back-to-default behavior for an unset or
// unparseable value, same floor of 1 (never zero or negative).
func graphQLMaxQueryBytes() int {
	raw := os.Getenv("GRAPHQL_MAX_QUERY_BYTES")
	if raw == "" {
		return defaultGraphQLMaxQueryBytes
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return defaultGraphQLMaxQueryBytes
	}
	if value < 1 {
		return 1
	}
	return value
}

// bearerToken mirrors services/auth.py's extract_token_from_header
// exactly: split on ANY whitespace (not just a literal "Bearer " prefix),
// require exactly two fields, and compare the scheme case-INSENSITIVELY
// -- codex review, 2026-08-28: the previous case-sensitive prefix check
// rejected the standards-valid `bearer <token>` scheme Python's edge
// already accepts, a real authentication-behavior divergence for the
// same canaried operation.
func bearerToken(header string) (string, bool) {
	parts := strings.Fields(header)
	if len(parts) != 2 {
		return "", false
	}
	scheme, token := parts[0], parts[1]
	if !strings.EqualFold(scheme, "Bearer") {
		return "", false
	}
	return token, true
}

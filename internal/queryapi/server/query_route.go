// CHAOS-8702: the per-row switch these comments call PostgresSwitch is gone. Every registered document is served
// (routeswitch.NewCatalogSwitch) and no routing row decides it; where a comment below describes registration as not
// enabling a document, it describes the history before that change.
//
// CHAOS-4367 Wave 1: wires the ONE live route this binary now serves --
// featureFlags -- behind routeswitch.Mux, gated by a verified effective-principal envelope. See
// main.go's package doc for what Wave 0 left empty; this file is what
// Wave 1 adds on top of it.
//
// CHAOS-4368 Wave 2 adds a SECOND operation, reviewEdges, on the same
// /query route and the same routeswitch.Mux + PostgresSwitch pipeline --
// each operation gets its own registered document + digest + Mux
// registration. query-api serves every registered operation (routeswitch.NewCatalogSwitch): no routing row
// decides it.
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/chclient"
	"io"
	"log"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/99designs/gqlgen/graphql"
	gqlhandler "github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/handler/extension"
	"github.com/99designs/gqlgen/graphql/handler/transport"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"

	schemav1 "github.com/full-chaos/dev-health-ops/contracts/graphql/v1"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/analytics"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/digest"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/featureflags"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
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
// internal/queryapi/server/testdata/wire_capture/featureflags_captured.graphql and
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
      reviewerKey
      authorKey
      reviewerName
      authorName
      reviewsCount
      day
      repoId
      __typename
    }
    totalCount
    __typename
  }
}`

// registeredReviewEdgesV1Document is the text of `reviewEdges` BEFORE the Review Network table asked for the served names and keys instead of the stored reviewer and author strings (CHAOS-8485).
// It stays a legacy text (see legacyDigestsByOperation), so a web build still sending it keeps working while the
// new one rolls out; the operation's ONE current document is registeredReviewEdgesDocument above. Remove it
// with the cleanup ticket once no client sends it (testdata/wire_capture/reviewedges_v1_captured.graphql).
const registeredReviewEdgesV1Document = `query ReviewEdges($input: ReviewEdgesInput!) {
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
    repos {
      repoId
      repoName
      topFilePath
      topRiskScore
      evidenceUrl
      __typename
    }
    __typename
  }
}`

// registeredHotspotsV1Document is the text hotspots accepted BEFORE it asked for `repos` (CHAOS-8488): the text of
// CHAOS-4369 Wave 3. It stays a legacy text (see legacyDigestsByOperation), so a web build still sending it keeps
// working while the new web rolls out; the operation's ONE current document is registeredHotspotsDocument above.
const registeredHotspotsV1Document = `query Hotspots($input: HotspotsInput!) {
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
//
// The `rateState` line of a metric (the state of change failure rate) is
// NOT from the web file: the web does not select it yet, and sends the V3
// text below. The line is here so the field can be asked for at all (a
// field no registered text selects is unreachable); the web's real wire
// text wins over this one if they differ when the web selects the field.
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
        hasData
        scope
        rateState
        delta {
          value
          priorValue
          absolute
          percent
          status
          hasPriorData
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

// registeredOperatingReviewV3Document is the text of `operatingReview` BEFORE a metric carried the state of change
// failure rate (CHAOS-8981, `rateState`): the text of CHAOS-8516, with `scope`. It stays a legacy text (see
// legacyDigestsByOperation) beside V1 and V2, so a web build still sending it keeps working; it is the text every
// web build sends until the web selects `rateState`. Remove it with the cleanup ticket once no client sends it
// (testdata/wire_capture/operatingreview_v3_captured.graphql).
const registeredOperatingReviewV3Document = `query OperatingReview($orgId: String!, $input: OperatingReviewInput!) {
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
        hasData
        scope
        delta {
          value
          priorValue
          absolute
          percent
          status
          hasPriorData
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

// registeredOperatingReviewV2Document is the text of `operatingReview` BEFORE the Operating Review asked for the
// scope of a metric (CHAOS-8516): the text of CHAOS-8115, with hasData and hasPriorData. It stays a legacy text
// (see legacyDigestsByOperation) beside V1, so a web build still sending it keeps working while the new one rolls
// out. Remove it with the cleanup ticket once no client sends it
// (testdata/wire_capture/operatingreview_v2_captured.graphql).
const registeredOperatingReviewV2Document = `query OperatingReview($orgId: String!, $input: OperatingReviewInput!) {
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
        hasData
        delta {
          value
          priorValue
          absolute
          percent
          status
          hasPriorData
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

// registeredOperatingReviewV1Document is the text of `operatingReview` BEFORE the Operating Review asked whether each week of a metric holds data (CHAOS-8115).
// It stays a legacy text (see legacyDigestsByOperation), so a web build still sending it keeps working while the
// new one rolls out; the operation's ONE current document is registeredOperatingReviewDocument above. Remove it
// with the cleanup ticket once no client sends it (testdata/wire_capture/operatingreview_v1_captured.graphql).
const registeredOperatingReviewV1Document = `query OperatingReview($orgId: String!, $input: OperatingReviewInput!) {
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

// registeredHomeV6Document is the Home text from before a signal carried its coverage
// (CHAOS-6545: the share of a compounding-risk score's weight that was present,
// `HomeSignal.coverage`): the V5 text plus CHAOS-9072's `rateCoverage`. It stays a legacy text
// so every web build that does not select `coverage` remains accepted. Its fixture is
// testdata/wire_capture/home_v6_captured.graphql.
const registeredHomeV6Document = `query Home($orgId: String!, $filters: FilterInput, $window: HomeWindowInput) {
  home(orgId: $orgId, filters: $filters, window: $window) {
    freshness {
      lastIngestedAt
      latestSuccessfulSyncAt
      sources {
        provider
        status
        __typename
      }
      coverage {
        reposCoveredPct
        prsLinkedToIssuesPct
        issuesWithCycleStatesPct
        __typename
      }
      __typename
    }
    deltas {
      metric
      label
      value
      unit
      deltaPct
      hasData
      hasPriorData
      spark {
        ts
        value
        __typename
      }
      rateState
      rateCoverage
      __typename
    }
    reworkThemeAllocation {
      theme
      label
      allocation
      allocationPct
      prsMerged
      churnLoc
      __typename
    }
    summary {
      id
      text
      evidenceLink
      __typename
    }
    tiles {
      key
      value {
        title
        subtitle
        link
        __typename
      }
      __typename
    }
    constraint {
      title
      claim
      evidence {
        label
        link
        __typename
      }
      experiments
      __typename
    }
    events {
      ts
      type
      text
      link
      __typename
    }
    healthState {
      status
      headline
      summary
      asOf
      __typename
    }
    signals {
      id
      title
      metric
      currentValue
      priorValue
      delta
      direction
      severity
      confidence
      affectedScope
      evidenceCount
      whyItMatters
      recommendedAction
      evidenceRef
      category
      scopeEntity {
        id
        displayName
        __typename
      }
      attribution {
        items
        sources {
          source
          items
          share
          __typename
        }
        confidence {
          confidence
          items
          share
          __typename
        }
        __typename
      }
      __typename
    }
    limitingFactor {
      claim
      whyItMatters
      recommendedAction
      confidence
      evidenceRef
      __typename
    }
    dataConfidence {
      level
      coveragePct
      connectedSources
      missingSources
      caveats
      __typename
    }
    scopeDataConfidence {
      level
      coveragePct
      lastIngestedAt
      caveats
      __typename
    }
    __typename
  }
}`

// registeredHomeV7Document is the Home text from before a metric and a signal carried
// whether the request's repository filter narrowed them (CHAOS-9093: `MetricDelta.repoFilterApplied`
// and `HomeSignal.repoFilterApplied`): the V6 text plus a signal's `coverage` (CHAOS-6545). It stays
// a legacy text (see legacyDigestsByOperation) so every web build that does not select the field
// remains accepted. Remove it with the cleanup ticket once no client sends it
// (testdata/wire_capture/home_v7_captured.graphql).
const registeredHomeV7Document = `query Home($orgId: String!, $filters: FilterInput, $window: HomeWindowInput) {
  home(orgId: $orgId, filters: $filters, window: $window) {
    freshness {
      lastIngestedAt
      latestSuccessfulSyncAt
      sources {
        provider
        status
        __typename
      }
      coverage {
        reposCoveredPct
        prsLinkedToIssuesPct
        issuesWithCycleStatesPct
        __typename
      }
      __typename
    }
    deltas {
      metric
      label
      value
      unit
      deltaPct
      hasData
      hasPriorData
      spark {
        ts
        value
        __typename
      }
      rateState
      rateCoverage
      __typename
    }
    reworkThemeAllocation {
      theme
      label
      allocation
      allocationPct
      prsMerged
      churnLoc
      __typename
    }
    summary {
      id
      text
      evidenceLink
      __typename
    }
    tiles {
      key
      value {
        title
        subtitle
        link
        __typename
      }
      __typename
    }
    constraint {
      title
      claim
      evidence {
        label
        link
        __typename
      }
      experiments
      __typename
    }
    events {
      ts
      type
      text
      link
      __typename
    }
    healthState {
      status
      headline
      summary
      asOf
      __typename
    }
    signals {
      id
      title
      metric
      currentValue
      priorValue
      delta
      direction
      severity
      confidence
      affectedScope
      evidenceCount
      whyItMatters
      recommendedAction
      evidenceRef
      category
      scopeEntity {
        id
        displayName
        __typename
      }
      coverage
      attribution {
        items
        sources {
          source
          items
          share
          __typename
        }
        confidence {
          confidence
          items
          share
          __typename
        }
        __typename
      }
      __typename
    }
    limitingFactor {
      claim
      whyItMatters
      recommendedAction
      confidence
      evidenceRef
      __typename
    }
    dataConfidence {
      level
      coveragePct
      connectedSources
      missingSources
      caveats
      __typename
    }
    scopeDataConfidence {
      level
      coveragePct
      lastIngestedAt
      caveats
      __typename
    }
    __typename
  }
}`

// registeredHomeDocument is the registered document for the home
// operation: the wire form of the web app's HOME_QUERY (variables orgId,
// filters, window), kept byte-identical in
// testdata/wire_capture/home_captured.graphql (see that directory's
// README for how the wire form is produced from the web repo's own
// pinned urql). Its selection set covers the schema's own
// type declarations (contracts/graphql/v1/schema.graphql: HomeResult,
// Freshness, HomeFreshnessSource, Coverage, MetricDelta, SparkPoint,
// ReworkThemeAllocation, SummarySentence, HomeTileEntry, HomeTile,
// ConstraintCard, ConstraintEvidence, EventItem, HealthState, HomeSignal,
// ScopeEntityRef, SignalAttribution, SignalAttributionSourceCount,
// SignalAttributionConfidenceCount, HomeLimitingFactor, HomeDataConfidence,
// HomeScopeDataConfidence -- every field each type declares, not a hand-picked
// subset), formatted to match
// this file's other entries' urql-print convention (multi-line,
// `__typename` on every object selection). The digest of the captured
// wire fixture must equal this constant's
// (query_route_wire_capture_test.go's
// TestRegisteredHomeDocument_MatchesCapturedWireFixture); the web repo's
// graphql-wire-parity check runs the current HOME_QUERY against the
// digest registered here. query_route_integration_test.go's
// TestHomeRoute_ReachableOnlyWhenSwitchEnabled proves an in-process
// request built from THIS EXACT constant reaches queryResolver.Home
// (routing/auth gating only -- it does not assert response content),
// and registered_home_document_schema_parity_test.go's
// TestRegisteredHomeDocumentSelectsEveryHomeResultField is the content
// side: it fails the moment this constant's selection set falls behind
// what contracts/graphql/v1/schema.graphql's HomeResult (recursively)
// declares -- the defect class where the schema grows but the registered
// document does not (see the comment below).
//
// Why the selection must track the schema: growing HomeResult's SDL and
// homeResultFromResponse's mapping to the full home payload while leaving
// THIS constant selecting only the original 3 fields
// (freshness/deltas/reworkThemeAllocation) would leave every new field the SDL
// and the resolver now support (summary/tiles/constraint/events/
// healthState/signals/limitingFactor/dataConfidence,
// freshness.latestSuccessfulSyncAt, freshness.sources)
// UNREACHABLE through /query: operationForDocument matches by exact
// document digest (see that function below), so any client selecting a
// new field got a 404 digest-miss even though queryResolver.Home mapped
// it correctly. So this selection set is exhaustive per type.
//
// Two lines are NOT a capture: `rateState` in deltas (the state of a rate
// that is a ratio of stored counts) and `rateCoverage` after it (the coverage
// of the pull request rework ratio). The web selects neither yet and sends
// the V4 text below; the lines are here because a field no registered text
// selects is unreachable. The web's real capture wins over this text if they
// differ when the web selects the fields (testdata/wire_capture/README.md).
const registeredHomeDocument = `query Home($orgId: String!, $filters: FilterInput, $window: HomeWindowInput) {
  home(orgId: $orgId, filters: $filters, window: $window) {
    freshness {
      lastIngestedAt
      latestSuccessfulSyncAt
      sources {
        provider
        status
        __typename
      }
      coverage {
        reposCoveredPct
        prsLinkedToIssuesPct
        issuesWithCycleStatesPct
        __typename
      }
      __typename
    }
    deltas {
      metric
      label
      value
      unit
      deltaPct
      hasData
      hasPriorData
      spark {
        ts
        value
        __typename
      }
      rateState
      rateCoverage
      repoFilterApplied
      __typename
    }
    reworkThemeAllocation {
      theme
      label
      allocation
      allocationPct
      prsMerged
      churnLoc
      __typename
    }
    summary {
      id
      text
      evidenceLink
      __typename
    }
    tiles {
      key
      value {
        title
        subtitle
        link
        __typename
      }
      __typename
    }
    constraint {
      title
      claim
      evidence {
        label
        link
        __typename
      }
      experiments
      __typename
    }
    events {
      ts
      type
      text
      link
      __typename
    }
    healthState {
      status
      headline
      summary
      asOf
      __typename
    }
    signals {
      id
      title
      metric
      currentValue
      priorValue
      delta
      direction
      severity
      confidence
      affectedScope
      evidenceCount
      whyItMatters
      recommendedAction
      evidenceRef
      category
      scopeEntity {
        id
        displayName
        __typename
      }
      coverage
      repoFilterApplied
      attribution {
        items
        sources {
          source
          items
          share
          __typename
        }
        confidence {
          confidence
          items
          share
          __typename
        }
        __typename
      }
      __typename
    }
    limitingFactor {
      claim
      whyItMatters
      recommendedAction
      confidence
      evidenceRef
      __typename
    }
    dataConfidence {
      level
      coveragePct
      connectedSources
      missingSources
      caveats
      __typename
    }
    scopeDataConfidence {
      level
      coveragePct
      lastIngestedAt
      caveats
      __typename
    }
    __typename
  }
}`

// registeredHomeV5Document is the Home text from before CHAOS-9072 added
// MetricDelta.rateCoverage: the V4 text plus `rateState`. It was the current
// registered text and no capture, so a client can have been built against it;
// it stays a legacy text so such a client remains accepted. Its fixture is
// testdata/wire_capture/home_v5_captured.graphql.
const registeredHomeV5Document = `query Home($orgId: String!, $filters: FilterInput, $window: HomeWindowInput) {
  home(orgId: $orgId, filters: $filters, window: $window) {
    freshness {
      lastIngestedAt
      latestSuccessfulSyncAt
      sources {
        provider
        status
        __typename
      }
      coverage {
        reposCoveredPct
        prsLinkedToIssuesPct
        issuesWithCycleStatesPct
        __typename
      }
      __typename
    }
    deltas {
      metric
      label
      value
      unit
      deltaPct
      hasData
      hasPriorData
      spark {
        ts
        value
        __typename
      }
      rateState
      __typename
    }
    reworkThemeAllocation {
      theme
      label
      allocation
      allocationPct
      prsMerged
      churnLoc
      __typename
    }
    summary {
      id
      text
      evidenceLink
      __typename
    }
    tiles {
      key
      value {
        title
        subtitle
        link
        __typename
      }
      __typename
    }
    constraint {
      title
      claim
      evidence {
        label
        link
        __typename
      }
      experiments
      __typename
    }
    events {
      ts
      type
      text
      link
      __typename
    }
    healthState {
      status
      headline
      summary
      asOf
      __typename
    }
    signals {
      id
      title
      metric
      currentValue
      priorValue
      delta
      direction
      severity
      confidence
      affectedScope
      evidenceCount
      whyItMatters
      recommendedAction
      evidenceRef
      category
      scopeEntity {
        id
        displayName
        __typename
      }
      attribution {
        items
        sources {
          source
          items
          share
          __typename
        }
        confidence {
          confidence
          items
          share
          __typename
        }
        __typename
      }
      __typename
    }
    limitingFactor {
      claim
      whyItMatters
      recommendedAction
      confidence
      evidenceRef
      __typename
    }
    dataConfidence {
      level
      coveragePct
      connectedSources
      missingSources
      caveats
      __typename
    }
    scopeDataConfidence {
      level
      coveragePct
      lastIngestedAt
      caveats
      __typename
    }
    __typename
  }
}`

// registeredHomeV4Document is the real Home text from before CHAOS-8981
// added MetricDelta.rateState (the state of change failure rate). It stays
// a legacy text so every web build that does not select the state remains
// accepted; it is the text the web sends until it selects `rateState`. Its
// captured wire form is testdata/wire_capture/home_v4_captured.graphql.
const registeredHomeV4Document = `query Home($orgId: String!, $filters: FilterInput, $window: HomeWindowInput) {
  home(orgId: $orgId, filters: $filters, window: $window) {
    freshness {
      lastIngestedAt
      latestSuccessfulSyncAt
      sources {
        provider
        status
        __typename
      }
      coverage {
        reposCoveredPct
        prsLinkedToIssuesPct
        issuesWithCycleStatesPct
        __typename
      }
      __typename
    }
    deltas {
      metric
      label
      value
      unit
      deltaPct
      hasData
      hasPriorData
      spark {
        ts
        value
        __typename
      }
      __typename
    }
    reworkThemeAllocation {
      theme
      label
      allocation
      allocationPct
      prsMerged
      churnLoc
      __typename
    }
    summary {
      id
      text
      evidenceLink
      __typename
    }
    tiles {
      key
      value {
        title
        subtitle
        link
        __typename
      }
      __typename
    }
    constraint {
      title
      claim
      evidence {
        label
        link
        __typename
      }
      experiments
      __typename
    }
    events {
      ts
      type
      text
      link
      __typename
    }
    healthState {
      status
      headline
      summary
      asOf
      __typename
    }
    signals {
      id
      title
      metric
      currentValue
      priorValue
      delta
      direction
      severity
      confidence
      affectedScope
      evidenceCount
      whyItMatters
      recommendedAction
      evidenceRef
      category
      scopeEntity {
        id
        displayName
        __typename
      }
      attribution {
        items
        sources {
          source
          items
          share
          __typename
        }
        confidence {
          confidence
          items
          share
          __typename
        }
        __typename
      }
      __typename
    }
    limitingFactor {
      claim
      whyItMatters
      recommendedAction
      confidence
      evidenceRef
      __typename
    }
    dataConfidence {
      level
      coveragePct
      connectedSources
      missingSources
      caveats
      __typename
    }
    scopeDataConfidence {
      level
      coveragePct
      lastIngestedAt
      caveats
      __typename
    }
    __typename
  }
}`

// registeredHomeV3Document is the real Home text from before CHAOS-8102
// added HomeSignal.attribution. It stays a legacy text so the CHAOS-8107
// web build remains accepted while the attribution query rolls out. Its
// captured wire form is testdata/wire_capture/home_v3_captured.graphql.
const registeredHomeV3Document = `query Home($orgId: String!, $filters: FilterInput, $window: HomeWindowInput) {
  home(orgId: $orgId, filters: $filters, window: $window) {
    freshness {
      lastIngestedAt
      latestSuccessfulSyncAt
      sources {
        provider
        status
        __typename
      }
      coverage {
        reposCoveredPct
        prsLinkedToIssuesPct
        issuesWithCycleStatesPct
        __typename
      }
      __typename
    }
    deltas {
      metric
      label
      value
      unit
      deltaPct
      hasData
      hasPriorData
      spark {
        ts
        value
        __typename
      }
      __typename
    }
    reworkThemeAllocation {
      theme
      label
      allocation
      allocationPct
      prsMerged
      churnLoc
      __typename
    }
    summary {
      id
      text
      evidenceLink
      __typename
    }
    tiles {
      key
      value {
        title
        subtitle
        link
        __typename
      }
      __typename
    }
    constraint {
      title
      claim
      evidence {
        label
        link
        __typename
      }
      experiments
      __typename
    }
    events {
      ts
      type
      text
      link
      __typename
    }
    healthState {
      status
      headline
      summary
      asOf
      __typename
    }
    signals {
      id
      title
      metric
      currentValue
      priorValue
      delta
      direction
      severity
      confidence
      affectedScope
      evidenceCount
      whyItMatters
      recommendedAction
      evidenceRef
      category
      scopeEntity {
        id
        displayName
        __typename
      }
      __typename
    }
    limitingFactor {
      claim
      whyItMatters
      recommendedAction
      confidence
      evidenceRef
      __typename
    }
    dataConfidence {
      level
      coveragePct
      connectedSources
      missingSources
      caveats
      __typename
    }
    scopeDataConfidence {
      level
      coveragePct
      lastIngestedAt
      caveats
      __typename
    }
    __typename
  }
}`

// registeredHomeV2Document is the real Home text from before CHAOS-8107
// added scopeDataConfidence. It stays a legacy text so the CHAOS-8169
// web build remains accepted while the scoped-confidence query rolls out.
// Its captured wire form is testdata/wire_capture/home_v2_captured.graphql.
const registeredHomeV2Document = `query Home($orgId: String!, $filters: FilterInput, $window: HomeWindowInput) {
  home(orgId: $orgId, filters: $filters, window: $window) {
    freshness {
      lastIngestedAt
      latestSuccessfulSyncAt
      sources {
        provider
        status
        __typename
      }
      coverage {
        reposCoveredPct
        prsLinkedToIssuesPct
        issuesWithCycleStatesPct
        __typename
      }
      __typename
    }
    deltas {
      metric
      label
      value
      unit
      deltaPct
      hasData
      hasPriorData
      spark {
        ts
        value
        __typename
      }
      __typename
    }
    reworkThemeAllocation {
      theme
      label
      allocation
      allocationPct
      prsMerged
      churnLoc
      __typename
    }
    summary {
      id
      text
      evidenceLink
      __typename
    }
    tiles {
      key
      value {
        title
        subtitle
        link
        __typename
      }
      __typename
    }
    constraint {
      title
      claim
      evidence {
        label
        link
        __typename
      }
      experiments
      __typename
    }
    events {
      ts
      type
      text
      link
      __typename
    }
    healthState {
      status
      headline
      summary
      asOf
      __typename
    }
    signals {
      id
      title
      metric
      currentValue
      priorValue
      delta
      direction
      severity
      confidence
      affectedScope
      evidenceCount
      whyItMatters
      recommendedAction
      evidenceRef
      category
      scopeEntity {
        id
        displayName
        __typename
      }
      __typename
    }
    limitingFactor {
      claim
      whyItMatters
      recommendedAction
      confidence
      evidenceRef
      __typename
    }
    dataConfidence {
      level
      coveragePct
      connectedSources
      missingSources
      caveats
      __typename
    }
    __typename
  }
}`

// registeredHomeV1Document is the real Home text from before CHAOS-8169
// added the two per-window data-presence selections. It stays a legacy text
// so deployed web builds continue to resolve while the new query rolls out.
// Its captured wire form is testdata/wire_capture/home_v1_captured.graphql.
const registeredHomeV1Document = `query Home($orgId: String!, $filters: FilterInput, $window: HomeWindowInput) {
  home(orgId: $orgId, filters: $filters, window: $window) {
    freshness {
      lastIngestedAt
      latestSuccessfulSyncAt
      sources {
        provider
        status
        __typename
      }
      coverage {
        reposCoveredPct
        prsLinkedToIssuesPct
        issuesWithCycleStatesPct
        __typename
      }
      __typename
    }
    deltas {
      metric
      label
      value
      unit
      deltaPct
      spark {
        ts
        value
        __typename
      }
      __typename
    }
    reworkThemeAllocation {
      theme
      label
      allocation
      allocationPct
      prsMerged
      churnLoc
      __typename
    }
    summary {
      id
      text
      evidenceLink
      __typename
    }
    tiles {
      key
      value {
        title
        subtitle
        link
        __typename
      }
      __typename
    }
    constraint {
      title
      claim
      evidence {
        label
        link
        __typename
      }
      experiments
      __typename
    }
    events {
      ts
      type
      text
      link
      __typename
    }
    healthState {
      status
      headline
      summary
      asOf
      __typename
    }
    signals {
      id
      title
      metric
      currentValue
      priorValue
      delta
      direction
      severity
      confidence
      affectedScope
      evidenceCount
      whyItMatters
      recommendedAction
      evidenceRef
      category
      scopeEntity {
        id
        displayName
        __typename
      }
      __typename
    }
    limitingFactor {
      claim
      whyItMatters
      recommendedAction
      confidence
      evidenceRef
      __typename
    }
    dataConfidence {
      level
      coveragePct
      connectedSources
      missingSources
      caveats
      __typename
    }
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

// registeredReleaseImpactDocument is the registered document for the
// `releaseImpact` operation: the release impact query the feature flag pages
// send, which selects the `workGraphEdges` root field with the filters
// `nodeId`, `sourceType` and `limit`. It is the exact wire-form text a real
// web client sends (testdata/wire_capture/releaseimpact_captured.graphql).
const registeredReleaseImpactDocument = `query ReleaseImpact($orgId: String!, $filters: WorkGraphEdgeFilterInput) {
  workGraphEdges(orgId: $orgId, filters: $filters) {
    edges {
      edgeId
      sourceType
      sourceId
      targetType
      targetId
      edgeType
      provenance
      confidence
      evidence
      repoId
      provider
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
    __typename
  }
}`

// registeredWorkUnitTeamAttributionsDocument is the registered document for
// the `workUnitTeamAttributions` operation: the work unit team attribution
// query the investment view sends, the exact wire-form text a real web
// client sends (testdata/wire_capture/workunitteamattributions_captured.graphql).
const registeredWorkUnitTeamAttributionsDocument = `query WorkUnitTeamAttributions($orgId: String!, $workUnitIds: [String!], $teamId: String) {
  workUnitTeamAttributions(
    orgId: $orgId
    workUnitIds: $workUnitIds
    teamId: $teamId
  ) {
    workUnitId
    teamId
    teamName
    source
    confidence
    isPrimary
    memberCount
    evidence
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
// REGISTRATION SERVES THE DOCUMENT (CHAOS-8702; it was "not enablement" under the per-row switch, chris's ruling in
// CHAOS-4538's brief): the catalog switch serves every registered document and reads no routing row.
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

// registeredInvestmentEvidenceQualityDocument is CHAOS-8104's registered
// document for the Investment Evidence table's served mean-by-group column.
// Its text is the captured wire document that CHAOS-8745's browser client
// sent, not a schema-derived reconstruction. The capture fixture and its
// digest test keep this registration aligned with that client request.
//
// Registration serves this document through the catalog switch. It does not
// add a resolver, a route handler, or an MCP class: the document selects the
// existing analytics root, whose MCP class remains mcp:analytics.
const registeredInvestmentEvidenceQualityDocument = `query InvestmentEvidenceQuality($orgId: String!, $batch: AnalyticsRequestInput!) {
  analytics(orgId: $orgId, batch: $batch) {
    evidenceQualityByGroup {
      key
      label
      mean
      total
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
// selection includes SankeyResult.Coverage. That field was hardcoded
// nil when this document was first registered -- and once the routing
// row was enabled, that reached users as "Not reported" on the
// Allocation coverage tiles. It is now computed
// (internal/analytics/sankeycoverage.go), so every field this document
// selects is populatable and the registered-document field gate needs
// no exception for it.
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

// registeredCapacityForecastDocument is CHAOS-5349's registered document
// for the capacityForecast operation -- the on-demand Monte Carlo, one of
// the three capacity/forecast operations this ticket moves off Python.
//
// Same "registered documents only" contract as every const above: this is
// the urql WIRE FORM, not the source text in
// web/src/lib/graphql/queries.ts. urql's cacheExchange runs formatDocument
// (injecting the __typename selections visible below) and fetchExchange
// runs stringifyDocument before the bytes leave the browser, so a const
// copied from the source file digests to a value no real client ever
// sends -- which is exactly the defect CHAOS-4696 found sitting live on
// featureFlags. Produced by the web repo's OWN wire-parity tooling
// (web/scripts/graphql-wire-parity.ts's wireForm), which is the same
// function its CI check compares this registry against; the featureFlags
// fixture under testdata/wire_capture remains the independent, real-HTTP
// proof that wireForm's output IS what a real fetch() puts on the wire.
// Source const: CAPACITY_FORECAST_QUERY, queries.ts:236.
const registeredCapacityForecastDocument = `query CapacityForecast($orgId: String!, $input: CapacityForecastInput) {
  capacityForecast(orgId: $orgId, input: $input) {
    forecastId
    computedAt
    teamId
    workScopeId
    backlogSize
    targetItems
    targetDate
    p50Date
    p85Date
    p95Date
    p50Days
    p85Days
    p95Days
    p50Items
    p85Items
    p95Items
    throughputMean
    throughputStddev
    historyDays
    insufficientHistory
    highVariance
    completionDistribution {
      runs
      unfinishedRuns
      horizonDays
      days {
        value
        count
        cumulativeShare
        __typename
      }
      items {
        value
        count
        cumulativeShare
        __typename
      }
      __typename
    }
    __typename
  }
}`

// registeredCapacityCompletionDistributionDocument is CHAOS-8598's per-team read document: one team's capacityForecast, selecting only
// completionDistribution{days items}. It is the MCP run_operation read of the Monte Carlo histograms. The text is the wire form (urql formatDocument + stringifyDocument, so __typename is injected) of the web source const the graphql-wire-parity gate pairs it with.
// The whole input is ONE variable ($input: CapacityForecastInput): acr refuses a variable nested in a literal argument. The client supplies
// {teamId}; historyDays and simulations default in the SDL input type (90 and 10000; 10000 is mcpMaxSimulations). The operation key differs from the GraphQL root (capacityForecast) because a digest maps to exactly one operation.
const registeredCapacityCompletionDistributionDocument = `query CapacityCompletionDistribution($orgId: String!, $input: CapacityForecastInput) {
  capacityForecast(orgId: $orgId, input: $input) {
    completionDistribution {
      days {
        value
        count
        __typename
      }
      items {
        value
        count
        __typename
      }
      __typename
    }
    __typename
  }
}`

// registeredCapacityForecastV2Document is the text capacityForecast accepted BEFORE it asked for `runs` and for
// `cumulativeShare` on the bins of completionDistribution (CHAOS-8477): the text CHAOS-7994 registered. It stays a
// legacy text (see legacyDigestsByOperation), beside V1 below, so a web build still sending it keeps working
// while the new web rolls out; the operation's ONE current document is registeredCapacityForecastDocument above.
// Wire form, same provenance: testdata/wire_form/capacityForecast.v2.graphql.
const registeredCapacityForecastV2Document = `query CapacityForecast($orgId: String!, $input: CapacityForecastInput) {
  capacityForecast(orgId: $orgId, input: $input) {
    forecastId
    computedAt
    teamId
    workScopeId
    backlogSize
    targetItems
    targetDate
    p50Date
    p85Date
    p95Date
    p50Days
    p85Days
    p95Days
    p50Items
    p85Items
    p95Items
    throughputMean
    throughputStddev
    historyDays
    insufficientHistory
    highVariance
    completionDistribution {
      days {
        value
        count
        __typename
      }
      items {
        value
        count
        __typename
      }
      __typename
    }
    __typename
  }
}`

// registeredCapacityForecastV1Document is the text capacityForecast accepted BEFORE it asked for
// completionDistribution (CHAOS-7994, CHAOS-8000 dual accept). It stays a legacy text (see
// legacyDigestsByOperation) so a web build still sending it keeps working while the new web rolls out; the
// operation's ONE current document is registeredCapacityForecastDocument above. Wire form, same provenance:
// testdata/wire_form/capacityForecast.v1.graphql.
const registeredCapacityForecastV1Document = `query CapacityForecast($orgId: String!, $input: CapacityForecastInput) {
  capacityForecast(orgId: $orgId, input: $input) {
    forecastId
    computedAt
    teamId
    workScopeId
    backlogSize
    targetItems
    targetDate
    p50Date
    p85Date
    p95Date
    p50Days
    p85Days
    p95Days
    p50Items
    p85Items
    p95Items
    throughputMean
    throughputStddev
    historyDays
    insufficientHistory
    highVariance
    __typename
  }
}`

// registeredCapacityForecastsDocument is CHAOS-5349's registered document
// for the capacityForecasts operation -- the persisted-row connection,
// which reads what the native worker executor already writes.
//
// Same wire-form provenance as registeredCapacityForecastDocument above.
// Source const: CAPACITY_FORECASTS_QUERY, queries.ts:330.
const registeredCapacityForecastsDocument = `query CapacityForecasts($orgId: String!, $filters: CapacityForecastFilterInput) {
  capacityForecasts(orgId: $orgId, filters: $filters) {
    edges {
      node {
        forecastId
        computedAt
        teamId
        workScopeId
        backlogSize
        targetItems
        targetDate
        p50Date
        p85Date
        p95Date
        p50Days
        p85Days
        p95Days
        p50Items
        p85Items
        p95Items
        throughputMean
        throughputStddev
        historyDays
        insufficientHistory
        highVariance
        __typename
      }
      cursor
      __typename
    }
    pageInfo {
      hasNextPage
      hasPreviousPage
      startCursor
      endCursor
      __typename
    }
    totalCount
    __typename
  }
}`

// registeredThroughputForecastDocument is CHAOS-5349's registered document
// for the throughputForecast operation -- the rolling-window model, the
// largest of the three and the only one with no pre-existing Go kernel.
//
// Same wire-form provenance as registeredCapacityForecastDocument above.
// Source const: THROUGHPUT_FORECAST_QUERY, queries.ts:264.
const registeredThroughputForecastDocument = `query ThroughputForecast($orgId: String!, $input: ThroughputForecastInput!) {
  throughputForecast(orgId: $orgId, input: $input) {
    forecastId
    computedAt
    teamId
    workScopeId
    backlogSize
    historyWeeks
    p50Weeks
    p75Weeks
    p90Weeks
    insufficientHistory
    rollingWindows {
      windowWeeks
      meanWeeklyThroughput
      sampleCount
      insufficientHistory
      __typename
    }
    primaryRisk {
      kind
      score
      label
      value
      threshold
      active
      __typename
    }
    wipCongestion {
      kind
      score
      label
      value
      threshold
      active
      __typename
    }
    staleWip {
      p50AgeHours
      p90AgeHours
      __typename
    }
    estimateCoverage {
      ratio
      estimatedCount
      unestimatedCount
      backlogSize
      __typename
    }
    reviewBottleneck {
      kind
      score
      label
      value
      threshold
      active
      __typename
    }
    incidentLoad {
      kind
      score
      label
      value
      threshold
      active
      __typename
    }
    __typename
  }
}`

// registeredFeatureFlagEventsDocument is CHAOS-5523's registered document
// for the featureFlagEvents operation -- the operation featureFlags's own
// Wave 1 canary deliberately deferred (internal/queryapi/server/README.md's former
// "featureFlagEvents -- explicitly out of scope for the Wave 1 canary"
// bullet, removed by this change).
//
// Source const: FEATURE_FLAG_EVENTS_QUERY, web/src/lib/feature-flags/
// queries.ts:19, operation name "FeatureFlagEvents", individual scalar
// arguments (orgId/flagKey/environment/limit) -- same shape as
// featureFlags above, not the single-`$input`-object shape most other
// operations in this file use.
//
// WIRE FORM, not source text -- same CHAOS-4696 discipline
// registeredFeatureFlagsDocument's own doc comment explains at length:
// urql's real exchange chain runs the source query through TWO
// transformations before it leaves the browser (cacheExchange's
// formatDocument, which injects a `__typename` selection into every
// non-root selection set, then fetchExchange's stringifyDocument, which
// reflows long argument lists) before anything hits the network. This
// text is a REAL CAPTURE, not hand-reflowed: produced by running this
// repo's own unmodified graphqlFetch (src/lib/graphql/server.ts) against
// a real local HTTP listener, adapted from
// web/scripts/capture-graphql-wire-fixture.ts (that script itself
// hardcodes FEATURE_FLAG_REGISTRY_QUERY; this operation's capture used a
// same-mechanism variant script run once against an unmodified
// dev-health-web checkout, output verified byte-for-byte against
// internal/queryapi/server/testdata/wire_capture/featureflagevents_captured.graphql
// and its digest cross-checked independently in Python before being
// pasted here -- see that file's README section and
// query_route_wire_capture_test.go's
// TestRegisteredFeatureFlagEventsDocument_MatchesCapturedWireFixture,
// which asserts this const's digest against that fixture on every run,
// independently of this comment's claim).
const registeredFeatureFlagEventsDocument = `query FeatureFlagEvents($orgId: String!, $flagKey: String, $environment: String, $limit: Int!) {
  featureFlagEvents(
    orgId: $orgId
    flagKey: $flagKey
    environment: $environment
    limit: $limit
  ) {
    events {
      flagKey
      eventType
      prevState
      nextState
      actorType
      environment
      eventTs
      __typename
    }
    totalCount
    degradedReason
    __typename
  }
}`

// registeredCompoundingRiskV1Document is the text of `compoundingRisk` BEFORE a point carried its coverage (CHAOS-6545:
// the share of the score's weight that was present). It stays a legacy text (see legacyDigestsByOperation) beside the
// current one, so a web build still sending it keeps working; it is the text every web build sends until the web selects
// `coverage`. Remove it with the cleanup ticket once no client sends it
// (testdata/wire_capture/compoundingrisk_v1_captured.graphql).
const registeredCompoundingRiskV1Document = `query CompoundingRisk($orgId: String!, $filter: CompoundingRiskFilterInput = null) {
  compoundingRisk(orgId: $orgId, filter: $filter) {
    orgId
    breakout
    generatedAt
    rows {
      day
      scope
      scopeId
      scopeLabel
      score
      severity
      computedAt
      components {
        churnNorm
        complexityNorm
        ownershipNorm
        reviewNorm
        reworkChurn
        complexityDelta
        ownershipGini
        singleOwnerRatio
        reviewLatencyP90h
        __typename
      }
      weights {
        churn
        complexity
        ownership
        review
        __typename
      }
      thresholds {
        elevated
        high
        __typename
      }
      __typename
    }
    trend {
      day
      score
      severity
      __typename
    }
    __typename
  }
}`

// registeredCompoundingRiskDocument is the registered document for the
// `compoundingRisk` operation: the text of V1 with one line, `coverage` after
// `score` in each row (CHAOS-6545). The web sends exactly this text once it shows the
// coverage with the score (testdata/wire_capture/compoundingrisk_captured.graphql).
const registeredCompoundingRiskDocument = `query CompoundingRisk($orgId: String!, $filter: CompoundingRiskFilterInput = null) {
  compoundingRisk(orgId: $orgId, filter: $filter) {
    orgId
    breakout
    generatedAt
    rows {
      day
      scope
      scopeId
      scopeLabel
      score
      coverage
      severity
      computedAt
      components {
        churnNorm
        complexityNorm
        ownershipNorm
        reviewNorm
        reworkChurn
        complexityDelta
        ownershipGini
        singleOwnerRatio
        reviewLatencyP90h
        __typename
      }
      weights {
        churn
        complexity
        ownership
        review
        __typename
      }
      thresholds {
        elevated
        high
        __typename
      }
      __typename
    }
    trend {
      day
      score
      severity
      __typename
    }
    __typename
  }
}`

// registeredTestOpsPipelineDocument is the registered document for the `testOpsPipeline`
// operation, the exact wire-form text a real web client sends
// (testdata/wire_capture/testopspipeline_captured.graphql).
const registeredTestOpsPipelineDocument = `query TestOpsPipeline($orgId: String!, $batch: AnalyticsRequestInput!) {
  analytics(orgId: $orgId, batch: $batch) {
    timeseries {
      dimension
      dimensionValue
      measure
      buckets {
        date
        value
        __typename
      }
      __typename
    }
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
    __typename
  }
}`

// registeredTestOpsTestDocument is the registered document for the `testOpsTest`
// operation, the exact wire-form text a real web client sends
// (testdata/wire_capture/testopstest_captured.graphql).
const registeredTestOpsTestDocument = `query TestOpsTest($orgId: String!, $batch: AnalyticsRequestInput!) {
  analytics(orgId: $orgId, batch: $batch) {
    timeseries {
      dimension
      dimensionValue
      measure
      buckets {
        date
        value
        __typename
      }
      __typename
    }
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
    __typename
  }
}`

// registeredTestOpsCoverageDocument is the registered document for the `testOpsCoverage`
// operation, the exact wire-form text a real web client sends
// (testdata/wire_capture/testopscoverage_captured.graphql).
const registeredTestOpsCoverageDocument = `query TestOpsCoverage($orgId: String!, $batch: AnalyticsRequestInput!) {
  analytics(orgId: $orgId, batch: $batch) {
    timeseries {
      dimension
      dimensionValue
      measure
      buckets {
        date
        value
        __typename
      }
      __typename
    }
    breakdowns {
      dimension
      measure
      items {
        key
        value
        label
        __typename
      }
      __typename
    }
    __typename
  }
}`

// registeredFeatureFlagTimeseriesDocument is the registered document for the `featureFlagTimeseries`
// operation, the exact wire-form text a real web client sends
// (testdata/wire_capture/featureflagtimeseries_captured.graphql).
const registeredFeatureFlagTimeseriesDocument = `query FeatureFlagTimeseries($orgId: String!, $batch: AnalyticsRequestInput!) {
  analytics(orgId: $orgId, batch: $batch) {
    timeseries {
      dimension
      dimensionValue
      measure
      buckets {
        date
        value
        __typename
      }
      __typename
    }
    __typename
  }
}`

// registeredCoverageBaselinesDocument is the registered document for the
// `coverageBaselines` operation (CHAOS-8111, Go-only: no Python resolver
// exists), the exact wire-form text the web client sends
// (testdata/wire_capture/coveragebaselines_captured.graphql; the wire form of
// TESTOPS_COVERAGE_BASELINES_QUERY, computed with the web's pinned urql).
const registeredCoverageBaselinesDocument = `query CoverageBaselines($orgId: String!, $endDate: Date!, $repoIds: [String!], $teamIds: [String!]) {
  coverageBaselines(
    orgId: $orgId
    endDate: $endDate
    repoIds: $repoIds
    teamIds: $teamIds
  ) {
    repoId
    repoName
    lineBaselinePct
    lineDays
    branchBaselinePct
    branchDays
    __typename
  }
}`

// registeredCoverageScopeBaselineV1Document is the unscoped text accepted
// before CHAOS-8682. Keep it as a legacy document while older web builds roll
// out.
const registeredCoverageScopeBaselineV1Document = `query CoverageScopeBaseline($orgId: String!, $endDate: Date!) {
  coverageScopeBaseline(orgId: $orgId, endDate: $endDate) {
    lineBaselinePct
    lineDays
    __typename
  }
}`

// registeredCoverageScopeBaselineDocument is the scoped registered document
// for the `coverageScopeBaseline` operation (CHAOS-8682, Go-only: no Python
// resolver exists). It is the exact wire form of the web's
// TESTOPS_COVERAGE_SCOPE_BASELINE_QUERY, produced with the web's pinned urql
// and captured at testdata/wire_capture/coveragescopebaseline_captured.graphql.
const registeredCoverageScopeBaselineDocument = `query CoverageScopeBaseline($orgId: String!, $endDate: Date!, $repoIds: [String!], $teamIds: [String!]) {
  coverageScopeBaseline(
    orgId: $orgId
    endDate: $endDate
    repoIds: $repoIds
    teamIds: $teamIds
  ) {
    lineBaselinePct
    lineDays
    __typename
  }
}`

// registeredSourceHealthDocument is the registered document for the
// `sourceHealth` operation (CHAOS-8906, Go-only: no Python resolver exists):
// the member-level source health of the caller's organization. It carries no
// error text. testdata/wire_capture/sourcehealth_captured.graphql is its wire
// form.
const registeredSourceHealthDocument = `query SourceHealth($orgId: String!) {
  sourceHealth(orgId: $orgId) {
    provider
    scope
    lastSyncAt
    lastFailure {
      occurredAt
      stage
      __typename
    }
    __typename
  }
}`

// registeredTestopsJobFailuresDocument is the registered document for the
// `testopsJobFailures` operation (CHAOS-8513, Go-only: no Python resolver
// exists), the exact wire-form text the web client sends
// (testdata/wire_capture/testopsjobfailures_captured.graphql; the wire form of
// TESTOPS_JOB_FAILURES_QUERY, computed with the web's pinned urql).
const registeredTestopsJobFailuresDocument = `query TestOpsJobFailures($orgId: String!, $input: TestOpsJobFailuresInput!) {
  testopsJobFailures(orgId: $orgId, input: $input) {
    groups {
      workflowName
      jobName
      provider
      runs
      failedRuns
      failureRate
      __typename
    }
    totalCount
    truncated
    __typename
  }
}`

// registeredTestopsRiskDocument is the registered document for the
// `testopsRisk` operation, the exact wire-form text a real web client
// sends (testdata/wire_capture/testopsrisk_captured.graphql).
const registeredTestopsRiskDocument = `query TestOpsRisk($orgId: String!, $input: TestOpsRiskInput!) {
  testopsRisk(orgId: $orgId, input: $input) {
    releaseConfidence
    qualityDragHours
    pipelineStability
    timeseries {
      date
      riskScore
      __typename
    }
    qualityDragBreakdown {
      category
      hours
      __typename
    }
    quadrantData {
      id
      name
      pipelineSuccessRate
      testPassRate
      __typename
    }
    confidenceSpark {
      ts
      value
      __typename
    }
    confidenceDelta
    dragSpark {
      ts
      value
      __typename
    }
    dragDelta
    stabilitySpark {
      ts
      value
      __typename
    }
    stabilityDelta
    __typename
  }
}`

// registeredTestopsRiskV1Document is the text testopsRisk accepted BEFORE it selected the CHAOS-8954 name fields. It stays a
// legacy text (see legacyDigestsByOperation), so a web build still sending it keeps working while the new web rolls
// out; the operation's ONE current document is registeredTestopsRiskDocument above.
const registeredTestopsRiskV1Document = `query TestOpsRisk($orgId: String!, $input: TestOpsRiskInput!) {
  testopsRisk(orgId: $orgId, input: $input) {
    releaseConfidence
    qualityDragHours
    pipelineStability
    timeseries {
      date
      riskScore
      __typename
    }
    qualityDragBreakdown {
      category
      hours
      __typename
    }
    quadrantData {
      id
      pipelineSuccessRate
      testPassRate
      __typename
    }
    confidenceSpark {
      ts
      value
      __typename
    }
    confidenceDelta
    dragSpark {
      ts
      value
      __typename
    }
    dragDelta
    stabilitySpark {
      ts
      value
      __typename
    }
    stabilityDelta
    __typename
  }
}`

// registeredBusFactorDocument is the registered document for the
// `busFactor` operation, the exact wire-form text a real web client sends
// (testdata/wire_capture/busfactor_captured.graphql).
const registeredBusFactorDocument = `query BusFactor($orgId: String!, $scope: BusFactorScopeInput = null) {
  busFactor(orgId: $orgId, scope: $scope) {
    orgId
    scope {
      repoId
      teamId
      __typename
    }
    value
    evidenceSampleCount
    topMaintainers {
      author
      sharePercent
      __typename
    }
    repos {
      repoId
      repoName
      value
      evidenceSampleCount
      topMaintainers {
        author
        sharePercent
        __typename
      }
      __typename
    }
    __typename
  }
}`

// registeredAiImpactSummaryDocument is the registered document for the
// `aiImpactSummary` operation, the exact wire-form text a real web client sends
// (testdata/wire_capture/aiimpactsummary_captured.graphql).
const registeredAiImpactSummaryDocument = `query AIImpactSummary($orgId: String!, $dateRange: AIDateRangeInput!, $scope: AIScopeInput) {
  aiImpactSummary(orgId: $orgId, dateRange: $dateRange, scope: $scope) {
    orgId
    startDate
    endDate
    totalPrs
    aiAssistedPrs
    agentCreatedPrs
    humanPrs
    unknownPrs
    aiAssistedPrRatio
    dataAvailable
    computedAt
    byBucket {
      bucket
      prsTotal
      prsMerged
      aiAssistedPrRatio
      agentCreatedPrCount
      cycleTimeAvgHours
      aiCycleTimeDeltaHours
      aiReviewAmplification
      reworkDragRate
      revertRate
      incidentDragRate
      testGapRate
      leverage {
        prsComponent
        cycleTimeComponent
        reviewComponent
        reworkComponent
        testComponent
        incidentComponent
        __typename
      }
      __typename
    }
    daily {
      bucket
      prsTotal
      prsMerged
      cycleTimeAvgHours
      reviewsPerPr
      changesRequestedPerPr
      reworkPrs
      reworkRate
      revertPrs
      revertRate
      incidentsCount
      incidentRate
      testGapPrs
      testGapRate
      day
      __typename
    }
    repoBreakdown {
      scopeId
      scopeLabel
      aiPrsTotal
      aiAssistedPrRatio
      reworkRateDelta
      __typename
    }
    teamBreakdown {
      scopeId
      scopeLabel
      aiPrsTotal
      aiAssistedPrRatio
      reworkRateDelta
      __typename
    }
    __typename
  }
}`

// registeredAiImpactSummaryV1Document is the text of `aiImpactSummary` BEFORE the Impact page asked for `daily.day`
// (CHAOS-7992, CHAOS-8000 dual accept): a web build still on the old text keeps working while the new one rolls out.
// Listed in legacyDigestsByOperation; remove it with the cleanup ticket once no client sends it
// (testdata/wire_capture/aiimpactsummary_v1_captured.graphql).
const registeredAiImpactSummaryV1Document = `query AIImpactSummary($orgId: String!, $dateRange: AIDateRangeInput!, $scope: AIScopeInput) {
  aiImpactSummary(orgId: $orgId, dateRange: $dateRange, scope: $scope) {
    orgId
    startDate
    endDate
    totalPrs
    aiAssistedPrs
    agentCreatedPrs
    humanPrs
    unknownPrs
    aiAssistedPrRatio
    dataAvailable
    computedAt
    byBucket {
      bucket
      prsTotal
      prsMerged
      aiAssistedPrRatio
      agentCreatedPrCount
      cycleTimeAvgHours
      aiCycleTimeDeltaHours
      aiReviewAmplification
      reworkDragRate
      revertRate
      incidentDragRate
      testGapRate
      leverage {
        prsComponent
        cycleTimeComponent
        reviewComponent
        reworkComponent
        testComponent
        incidentComponent
        __typename
      }
      __typename
    }
    daily {
      bucket
      prsTotal
      prsMerged
      cycleTimeAvgHours
      reviewsPerPr
      changesRequestedPerPr
      reworkPrs
      reworkRate
      revertPrs
      revertRate
      incidentsCount
      incidentRate
      testGapPrs
      testGapRate
      __typename
    }
    repoBreakdown {
      scopeId
      scopeLabel
      aiPrsTotal
      aiAssistedPrRatio
      reworkRateDelta
      __typename
    }
    teamBreakdown {
      scopeId
      scopeLabel
      aiPrsTotal
      aiAssistedPrRatio
      reworkRateDelta
      __typename
    }
    __typename
  }
}`

// registeredAiComparisonDocument is the registered document for the
// `aiComparison` operation, the exact wire-form text a real web client sends
// (testdata/wire_capture/aicomparison_captured.graphql).
const registeredAiComparisonDocument = `query AIComparison($orgId: String!, $dateRange: AIDateRangeInput!, $scope: AIScopeInput) {
  aiComparison(orgId: $orgId, dateRange: $dateRange, scope: $scope) {
    orgId
    startDate
    endDate
    dataAvailable
    aiSide {
      bucket
      prsTotal
      prsMerged
      cycleTimeAvgHours
      reviewsPerPr
      reworkRate
      revertRate
      testGapRate
      incidentRate
      __typename
    }
    baselineSide {
      bucket
      prsTotal
      prsMerged
      cycleTimeAvgHours
      reviewsPerPr
      reworkRate
      revertRate
      testGapRate
      incidentRate
      __typename
    }
    delta {
      cycleTimeDeltaHours
      reviewsPerPrDelta
      reworkRateDelta
      revertRateDelta
      testGapRateDelta
      incidentRateDelta
      __typename
    }
    __typename
  }
}`

// registeredAiReviewLoadDocument is the registered document for the
// `aiReviewLoad` operation, the exact wire-form text a real web client sends
// (testdata/wire_capture/aireviewload_captured.graphql).
const registeredAiReviewLoadDocument = `query AIReviewLoad($orgId: String!, $dateRange: AIDateRangeInput!, $scope: AIScopeInput) {
  aiReviewLoad(orgId: $orgId, dateRange: $dateRange, scope: $scope) {
    orgId
    startDate
    endDate
    dataAvailable
    byBucket {
      bucket
      prsTotal
      reviewsTotal
      reviewsPerPr
      changesRequestedPerPr
      reviewAmplification
      postFirstReviewPushesCount
      postFirstReviewPushesPerPr
      pickupLatencyHours
      reviewCommentsPerLoc
      __typename
    }
    daily {
      bucket
      prsTotal
      reviewsTotal
      reviewsPerPr
      changesRequestedPerPr
      reviewAmplification
      postFirstReviewPushesCount
      postFirstReviewPushesPerPr
      pickupLatencyHours
      reviewCommentsPerLoc
      __typename
    }
    reviewerConcentration {
      dataAvailable
      reviewerCount
      reviewerGini
      __typename
    }
    missingStates {
      key
      title
      guidance
      __typename
    }
    __typename
  }
  aiComparison(orgId: $orgId, dateRange: $dateRange, scope: $scope) {
    orgId
    startDate
    endDate
    dataAvailable
    aiSide {
      bucket
      prsTotal
      prsMerged
      cycleTimeAvgHours
      reviewsPerPr
      reworkRate
      revertRate
      testGapRate
      incidentRate
      __typename
    }
    baselineSide {
      bucket
      prsTotal
      prsMerged
      cycleTimeAvgHours
      reviewsPerPr
      reworkRate
      revertRate
      testGapRate
      incidentRate
      __typename
    }
    delta {
      cycleTimeDeltaHours
      reviewsPerPrDelta
      reworkRateDelta
      revertRateDelta
      testGapRateDelta
      incidentRateDelta
      __typename
    }
    __typename
  }
}`

// registeredAiOpportunitiesDocument is the registered document for the
// `aiOpportunities` operation, the exact wire-form text a real web client sends
// (testdata/wire_capture/aiopportunities_captured.graphql).
const registeredAiOpportunitiesDocument = `query AIOpportunities($orgId: String!, $scope: AIScopeInput, $limit: Int! = 5) {
  aiOpportunities(orgId: $orgId, scope: $scope, limit: $limit) {
    orgId
    detectorReady
    recommendations {
      opportunityId
      kind
      repoId
      repoName
      teamId
      teamName
      title
      rationale
      score
      evidenceRefs
      workGraphDrilldowns {
        rootType
        rootId
        label
        __typename
      }
      __typename
    }
    __typename
  }
}`

// registeredAiOpportunitiesV1Document is the text of `aiOpportunities` BEFORE the AI opportunity list asked for the served repository and team names (CHAOS-8114).
// It stays a legacy text (see legacyDigestsByOperation), so a web build still sending it keeps working while the
// new one rolls out; the operation's ONE current document is registeredAiOpportunitiesDocument above. Remove it
// with the cleanup ticket once no client sends it (testdata/wire_capture/aiopportunities_v1_captured.graphql).
const registeredAiOpportunitiesV1Document = `query AIOpportunities($orgId: String!, $scope: AIScopeInput, $limit: Int! = 5) {
  aiOpportunities(orgId: $orgId, scope: $scope, limit: $limit) {
    orgId
    detectorReady
    recommendations {
      opportunityId
      kind
      repoId
      teamId
      title
      rationale
      score
      evidenceRefs
      workGraphDrilldowns {
        rootType
        rootId
        label
        __typename
      }
      __typename
    }
    __typename
  }
}`

// registeredImproveOpportunitiesDocument is the registered document for the
// `improveOpportunities` operation, the exact wire-form text a real web client sends
// (testdata/wire_capture/improveopportunities_captured.graphql).
const registeredImproveOpportunitiesDocument = `query ImproveOpportunities($scope: AIScopeInput, $limit: Int! = 10, $windowDays: Int! = 30) {
  improveOpportunities(scope: $scope, limit: $limit, windowDays: $windowDays) {
    orgId
    detectorReady
    totalCount
    opportunities {
      opportunityId
      kind
      entityType
      entityId
      title
      rationale
      score
      severity
      evidenceRefs
      recommendedAction
      value
      threshold
      unit
      thresholdDirection
      entityDisplayName
      __typename
    }
    __typename
  }
}`

// registeredImproveOpportunitiesV2Document is the text improveOpportunities accepted BEFORE it selected the CHAOS-8954 name fields. It stays a
// legacy text (see legacyDigestsByOperation), so a web build still sending it keeps working while the new web rolls
// out; the operation's ONE current document is registeredImproveOpportunitiesDocument above.
const registeredImproveOpportunitiesV2Document = `query ImproveOpportunities($scope: AIScopeInput, $limit: Int! = 10, $windowDays: Int! = 30) {
  improveOpportunities(scope: $scope, limit: $limit, windowDays: $windowDays) {
    orgId
    detectorReady
    totalCount
    opportunities {
      opportunityId
      kind
      entityType
      entityId
      title
      rationale
      score
      severity
      evidenceRefs
      recommendedAction
      value
      threshold
      unit
      thresholdDirection
      __typename
    }
    __typename
  }
}`

// registeredImproveOpportunitiesV1Document is the text of `improveOpportunities` BEFORE the Automations table asked
// for `value`, `threshold`, `unit` and `thresholdDirection` (CHAOS-8537; the web half is CHAOS-8500; the fields are CHAOS-7626). It stays a
// legacy text (see legacyDigestsByOperation), so a web build still sending it keeps working while the new one rolls
// out; the operation's ONE current document is registeredImproveOpportunitiesDocument above. Remove it with the
// cleanup ticket once no client sends it (testdata/wire_capture/improveopportunities_v1_captured.graphql).
const registeredImproveOpportunitiesV1Document = `query ImproveOpportunities($scope: AIScopeInput, $limit: Int! = 10, $windowDays: Int! = 30) {
  improveOpportunities(scope: $scope, limit: $limit, windowDays: $windowDays) {
    orgId
    detectorReady
    totalCount
    opportunities {
      opportunityId
      kind
      entityType
      entityId
      title
      rationale
      score
      severity
      evidenceRefs
      recommendedAction
      __typename
    }
    __typename
  }
}`

// registeredAiGovernanceSummaryDocument is the registered document for the
// `aiGovernanceSummary` operation, the exact wire-form text a real web client sends
// (testdata/wire_capture/aigovernancesummary_captured.graphql).
const registeredAiGovernanceSummaryDocument = `query AIGovernanceSummary($orgId: String!, $dateRange: AIDateRangeInput!, $scope: AIScopeInput, $violationLimit: Int! = 50) {
  aiGovernanceSummary(
    orgId: $orgId
    dateRange: $dateRange
    scope: $scope
    violationLimit: $violationLimit
  ) {
    orgId
    startDate
    endDate
    dataAvailable
    recentViolations {
      ruleId
      severity
      subjectType
      subjectId
      teamId
      repoId
      observedAt
      evidence
      repoName
      teamName
      subjectTitle
      ruleName
      __typename
    }
    __typename
  }
}`

// registeredAiGovernanceSummaryV1Document is the text aiGovernanceSummary accepted BEFORE it selected the CHAOS-8954 name fields. It stays a
// legacy text (see legacyDigestsByOperation), so a web build still sending it keeps working while the new web rolls
// out; the operation's ONE current document is registeredAiGovernanceSummaryDocument above.
const registeredAiGovernanceSummaryV1Document = `query AIGovernanceSummary($orgId: String!, $dateRange: AIDateRangeInput!, $scope: AIScopeInput, $violationLimit: Int! = 50) {
  aiGovernanceSummary(
    orgId: $orgId
    dateRange: $dateRange
    scope: $scope
    violationLimit: $violationLimit
  ) {
    orgId
    startDate
    endDate
    dataAvailable
    recentViolations {
      ruleId
      severity
      subjectType
      subjectId
      teamId
      repoId
      observedAt
      evidence
      __typename
    }
    __typename
  }
}`

// registeredAiWorkflowDrilldownDocument is the registered document for the
// `aiWorkflowDrilldown` operation, the exact wire-form text a real web client sends
// (testdata/wire_capture/aiworkflowdrilldown_captured.graphql).
const registeredAiWorkflowDrilldownDocument = `query AIWorkflowDrilldown($orgId: String!, $rootType: AIWorkflowRootTypeInput!, $rootId: String!, $depth: Int! = 3, $limit: Int! = 100) {
  aiWorkflowDrilldown(
    orgId: $orgId
    rootType: $rootType
    rootId: $rootId
    depth: $depth
    limit: $limit
  ) {
    orgId
    rootType
    rootId
    partial
    dataAvailable
    nodes {
      nodeType
      nodeId
      displayName
      nameExpected
      __typename
    }
    edges {
      edgeId
      sourceType
      sourceId
      targetType
      targetId
      edgeType
      confidence
      source
      evidence
      provider
      repoId
      __typename
    }
    __typename
  }
}`

// registeredAiWorkflowDrilldownV1Document is the text of `aiWorkflowDrilldown` BEFORE the AI evidence rows asked for the served node names and whether a node type carries a name (CHAOS-8113).
// It stays a legacy text (see legacyDigestsByOperation), so a web build still sending it keeps working while the
// new one rolls out; the operation's ONE current document is registeredAiWorkflowDrilldownDocument above. Remove it
// with the cleanup ticket once no client sends it (testdata/wire_capture/aiworkflowdrilldown_v1_captured.graphql).
const registeredAiWorkflowDrilldownV1Document = `query AIWorkflowDrilldown($orgId: String!, $rootType: AIWorkflowRootTypeInput!, $rootId: String!, $depth: Int! = 3, $limit: Int! = 100) {
  aiWorkflowDrilldown(
    orgId: $orgId
    rootType: $rootType
    rootId: $rootId
    depth: $depth
    limit: $limit
  ) {
    orgId
    rootType
    rootId
    partial
    dataAvailable
    nodes {
      nodeType
      nodeId
      __typename
    }
    edges {
      edgeId
      sourceType
      sourceId
      targetType
      targetId
      edgeType
      confidence
      source
      evidence
      provider
      repoId
      __typename
    }
    __typename
  }
}`

// registeredAiRiskBreakdownDocument is the registered document for the
// `aiRiskBreakdown` operation, the exact wire-form text a real web client sends
// (testdata/wire_capture/airiskbreakdown_captured.graphql).
const registeredAiRiskBreakdownDocument = `query AIRiskBreakdown($orgId: String!, $dateRange: AIDateRangeInput!, $scope: AIScopeInput) {
  aiRiskBreakdown(orgId: $orgId, dateRange: $dateRange, scope: $scope) {
    orgId
    startDate
    endDate
    dataAvailable
    byBucket {
      bucket
      prsTotal
      reworkPrs
      reworkRate
      revertPrs
      revertRate
      testGapPrs
      testGapRate
      incidentsCount
      incidentRate
      __typename
    }
    hotspotOverlap {
      bucket
      prsTotal
      prsTouchingHotspots
      hotspotOverlapRate
      avgHotspotRiskScore
      __typename
    }
    complexityOverlap {
      bucket
      prsTotal
      prsTouchingHighComplexity
      complexityOverlapRate
      __typename
    }
    missingStates {
      key
      title
      guidance
      __typename
    }
    __typename
  }
  aiComparison(orgId: $orgId, dateRange: $dateRange, scope: $scope) {
    orgId
    startDate
    endDate
    dataAvailable
    aiSide {
      bucket
      prsTotal
      prsMerged
      cycleTimeAvgHours
      reviewsPerPr
      reworkRate
      revertRate
      testGapRate
      incidentRate
      __typename
    }
    baselineSide {
      bucket
      prsTotal
      prsMerged
      cycleTimeAvgHours
      reviewsPerPr
      reworkRate
      revertRate
      testGapRate
      incidentRate
      __typename
    }
    delta {
      cycleTimeDeltaHours
      reviewsPerPrDelta
      reworkRateDelta
      revertRateDelta
      testGapRateDelta
      incidentRateDelta
      __typename
    }
    __typename
  }
}`

// registeredAiAttributedPrsDocument is the registered document for the
// `aiAttributedPrs` operation, the exact wire-form text a real web client sends
// (testdata/wire_capture/aiattributedprs_captured.graphql).
const registeredAiAttributedPrsDocument = `query AIAttributedPrs($orgId: String!, $dateRange: AIDateRangeInput!, $scope: AIScopeInput, $limit: Int! = 50, $offset: Int! = 0) {
  aiAttributedPrs(
    orgId: $orgId
    dateRange: $dateRange
    scope: $scope
    limit: $limit
    offset: $offset
  ) {
    orgId
    startDate
    endDate
    total
    hasMore
    dataAvailable
    rows {
      repoId
      repoName
      number
      title
      kind
      workType
      teamId
      mergedAt
      __typename
    }
    __typename
  }
}`

// registeredAiAttributedPrsV1Document is the text of `aiAttributedPrs` BEFORE the PR Evidence list asked for
// `repoName` (CHAOS-7991, CHAOS-8000 dual accept): a web build still on the old text keeps working while the new
// one rolls out. Listed in legacyDigestsByOperation; remove it with the cleanup ticket once no client sends it
// (testdata/wire_capture/aiattributedprs_v1_captured.graphql).
const registeredAiAttributedPrsV1Document = `query AIAttributedPrs($orgId: String!, $dateRange: AIDateRangeInput!, $scope: AIScopeInput, $limit: Int! = 50, $offset: Int! = 0) {
  aiAttributedPrs(
    orgId: $orgId
    dateRange: $dateRange
    scope: $scope
    limit: $limit
    offset: $offset
  ) {
    orgId
    startDate
    endDate
    total
    hasMore
    dataAvailable
    rows {
      repoId
      number
      title
      kind
      workType
      teamId
      mergedAt
      __typename
    }
    __typename
  }
}`

// registeredAiAttributionOverviewDocument is the registered document for the
// `aiAttributionOverview` operation, the exact wire-form text a real web client sends
// (testdata/wire_capture/aiattributionoverview_captured.graphql).
const registeredAiAttributionOverviewDocument = `query AIAttributionOverview($orgId: String!, $dateRange: AIDateRangeInput!, $scope: AIAttributionScopeInput, $limit: Int! = 50, $offset: Int! = 0) {
  aiAttributionOverview(
    orgId: $orgId
    dateRange: $dateRange
    scope: $scope
    limit: $limit
    offset: $offset
  ) {
    orgId
    startDate
    endDate
    mix {
      kind
      count
      share
      __typename
    }
    totalAttributed
    hasMore
    dataAvailable
    rows {
      subjectType
      subjectId
      repoId
      provider
      kind
      source
      confidence
      actor
      evidence
      observedAt
      teamId
      repoName
      teamName
      subjectTitle
      __typename
    }
    __typename
  }
}`

// registeredAiAttributionOverviewV1Document is the text aiAttributionOverview accepted BEFORE it selected the CHAOS-8954 name fields. It stays a
// legacy text (see legacyDigestsByOperation), so a web build still sending it keeps working while the new web rolls
// out; the operation's ONE current document is registeredAiAttributionOverviewDocument above.
const registeredAiAttributionOverviewV1Document = `query AIAttributionOverview($orgId: String!, $dateRange: AIDateRangeInput!, $scope: AIAttributionScopeInput, $limit: Int! = 50, $offset: Int! = 0) {
  aiAttributionOverview(
    orgId: $orgId
    dateRange: $dateRange
    scope: $scope
    limit: $limit
    offset: $offset
  ) {
    orgId
    startDate
    endDate
    mix {
      kind
      count
      share
      __typename
    }
    totalAttributed
    hasMore
    dataAvailable
    rows {
      subjectType
      subjectId
      repoId
      provider
      kind
      source
      confidence
      actor
      evidence
      observedAt
      teamId
      __typename
    }
    __typename
  }
}`

// registeredSavedReportsDocument is the registered document for the `savedReports`
// operation, the exact wire-form text a real web client sends
// (testdata/wire_capture/saved_reports_captured.graphql).
const registeredSavedReportsDocument = `query savedReports($orgId: String!, $limit: Int, $offset: Int) {
  savedReports(orgId: $orgId, limit: $limit, offset: $offset) {
    items {
      id
      orgId
      name
      description
      reportPlan
      isTemplate
      isActive
      lastRunAt
      lastRunStatus
      createdAt
      updatedAt
      __typename
    }
    total
    __typename
  }
}`

// registeredSavedReportDocument is the registered document for the `savedReport`
// operation, the exact wire-form text a real web client sends
// (testdata/wire_capture/saved_report_captured.graphql).
const registeredSavedReportDocument = `query savedReport($orgId: String!, $reportId: String!) {
  savedReport(orgId: $orgId, reportId: $reportId) {
    id
    orgId
    name
    description
    reportPlan
    isTemplate
    templateSourceId
    parameters
    scheduleId
    isActive
    lastRunAt
    lastRunStatus
    createdAt
    updatedAt
    createdBy
    __typename
  }
}`

// registeredReportRunsDocument is the registered document for the `reportRuns`
// operation, the exact wire-form text a real web client sends
// (testdata/wire_capture/report_runs_captured.graphql).
const registeredReportRunsDocument = `query reportRuns($orgId: String!, $reportId: String!, $limit: Int) {
  reportRuns(orgId: $orgId, reportId: $reportId, limit: $limit) {
    items {
      id
      reportId
      status
      startedAt
      completedAt
      durationSeconds
      renderedMarkdown
      artifactUrl
      provenanceRecords
      error
      triggeredBy
      createdAt
      __typename
    }
    total
    __typename
  }
}`

// The saved-report mutation documents, the exact wire-form text a real web
// client sends (testdata/wire_capture/*_saved_report_captured.graphql and
// trigger_report_captured.graphql), produced by the pinned @urql/core the way the
// web repo's graphql-wire-parity check reproduces it.
const registeredCreateSavedReportDocument = `mutation createSavedReport($orgId: String!, $input: CreateSavedReportInput!) {
  createSavedReport(orgId: $orgId, input: $input) {
    id
    orgId
    name
    description
    reportPlan
    isTemplate
    isActive
    createdAt
    updatedAt
    __typename
  }
}`

const registeredUpdateSavedReportDocument = `mutation updateSavedReport($orgId: String!, $reportId: String!, $input: UpdateSavedReportInput!) {
  updateSavedReport(orgId: $orgId, reportId: $reportId, input: $input) {
    id
    orgId
    name
    description
    reportPlan
    isTemplate
    parameters
    scheduleId
    isActive
    lastRunAt
    lastRunStatus
    createdAt
    updatedAt
    __typename
  }
}`

const registeredCloneSavedReportDocument = `mutation cloneSavedReport($orgId: String!, $input: CloneSavedReportInput!) {
  cloneSavedReport(orgId: $orgId, input: $input) {
    id
    orgId
    name
    description
    isActive
    createdAt
    updatedAt
    __typename
  }
}`

const registeredDeleteSavedReportDocument = `mutation deleteSavedReport($orgId: String!, $reportId: String!) {
  deleteSavedReport(orgId: $orgId, reportId: $reportId)
}`

const registeredTriggerReportDocument = `mutation triggerReport($orgId: String!, $reportId: String!) {
  triggerReport(orgId: $orgId, reportId: $reportId) {
    id
    reportId
    status
    startedAt
    triggeredBy
    __typename
  }
}`

// registeredSecurityOverviewDocument is the registered document for the
// `securityOverview` operation, the exact wire-form text a real web client
// sends (testdata/wire_capture/securityoverview_captured.graphql).
const registeredSecurityOverviewDocument = `query SecurityOverview($orgId: String!, $filters: SecurityAlertFilterInput) {
  securityOverview(orgId: $orgId, filters: $filters) {
    kpis {
      openTotal
      critical
      high
      meanDaysToFix30d
      openDelta30d
      __typename
    }
    severityBreakdown {
      severity
      count
      __typename
    }
    topRepos {
      repoId
      repoName
      repoUrl
      count
      __typename
    }
    trend {
      day
      opened
      fixed
      __typename
    }
    __typename
  }
}`

// registeredSecurityAlertsDocument is the registered document for the
// `securityAlerts` operation, the exact wire-form text a real web client
// sends (testdata/wire_capture/securityalerts_captured.graphql).
const registeredSecurityAlertsDocument = `query SecurityAlerts($orgId: String!, $filters: SecurityAlertFilterInput, $pagination: SecurityPaginationInput) {
  securityAlerts(orgId: $orgId, filters: $filters, pagination: $pagination) {
    edges {
      node {
        alertId
        repoId
        repoName
        repoUrl
        source
        severity
        state
        packageName
        cveId
        url
        title
        description
        createdAt
        fixedAt
        dismissedAt
        __typename
      }
      cursor
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
    __typename
  }
}`

// registeredPrDetailDocument is CHAOS-4991's registered document for the
// `pr` operation -- the PR detail view (core row, reviews, commits,
// linkedIssues). CHAOS-4980 wired the linkedIssues sub-field and the
// nil-for-unknown existence check only, and pr_operation_not_registered_test.go
// (removed by this change) asserted `pr` must stay UNREGISTERED until the
// rest of the port landed -- see workgraph.ResolveLinkedIssues's own doc
// comment for that history. CHAOS-4991 completes the port
// (workgraph.FetchPRCoreRow/ResolveReviews/ResolveCommits, wired into
// schema.resolvers.go's Pr resolver) and registers the document here,
// satisfying that guard's condition.
//
// REGISTRATION SERVES THE DOCUMENT (CHAOS-8702; see registeredInvestmentBreakdownDocument's doc comment): the catalog
// switch serves it on /query and on /query/proof, the measurement-only plane (CHAOS-5425).
//
// Same "registered documents only" contract, same "sourced from the real
// client file, not reconstructed" discipline as every const above: this
// is the urql WIRE FORM, not the source text in
// web/src/lib/graphql/queries.ts (PR_DETAIL_QUERY, queries.ts:94-138,
// operation name "PrDetail", variables `$orgId: String!` and `$id: ID!`).
// Verified by IMPORTING the web repo's own, live, pinned tooling --
// scripts/graphql-wire-parity.ts's exported `wireForm()`, the SAME
// function that repo's own CI parity gate calls -- rather than a
// hand-reflowed guess: `wireForm(PR_DETAIL_QUERY)` was invoked directly
// (via `tsx`, resolving `@urql/core` from the web repo's own
// node_modules) and its sha256(strings.TrimSpace(...)) digest recorded
// alongside the byte-exact captured text under
// testdata/wire_capture/pr_captured.graphql -- see that file and
// query_route_wire_capture_test.go's
// TestRegisteredPrDetailDocument_MatchesCapturedWireFixture, which
// asserts this const's digest against that fixture independently of this
// comment's own claim.
const registeredPrDetailDocument = `query PrDetail($orgId: String!, $id: ID!) {
  pr(orgId: $orgId, id: $id) {
    id
    orgId
    repoId
    repoName
    number
    title
    body
    state
    authorName
    authorEmail
    createdAt
    mergedAt
    closedAt
    headBranch
    baseBranch
    additions
    deletions
    changedFiles
    firstReviewAt
    firstCommentAt
    changesRequestedCount
    reviewsCount
    commentsCount
    reviews {
      reviewId
      reviewer
      state
      submittedAt
      __typename
    }
    commits {
      hash
      message
      authorName
      authorEmail
      authorWhen
      confidence
      provenance
      evidence
      __typename
    }
    linkedIssues {
      workItemId
      confidence
      provenance
      evidence
      __typename
    }
    __typename
  }
}`

// registeredCatalogValuesDocument is the registered document for the
// filter-dropdown read of the `catalog` operation: the distinct values of
// one dimension for the authorized org. It is the urql WIRE FORM of the
// web client's CatalogValues query (`$orgId: String!`,
// `$dimension: DimensionInput!`), captured under
// testdata/wire_capture/catalog_values_captured.graphql and asserted equal
// by TestRegisteredCatalogDocuments_MatchCapturedWireFixtures.
//
// Registration serves it: the catalog switch reads no routing row (CHAOS-8702).
const registeredCatalogValuesDocument = `query CatalogValues($orgId: String!, $dimension: DimensionInput!) {
  catalog(orgId: $orgId, dimension: $dimension) {
    values {
      value
      count
      __typename
    }
    __typename
  }
}`

// registeredAcrRepositoryScopesDocument is the registered document for the
// repository-scope read the agent-context runtime performs: `catalog` with
// the REPO dimension fixed in the document text. It is a separate operation
// from registeredCatalogValuesDocument because each registered document
// carries its own digest and routing row. Wire form captured under
// testdata/wire_capture/acr_repository_scopes_captured.graphql.
const registeredAcrRepositoryScopesDocument = `query ACRRepositoryScopes($orgId: String!) {
  catalog(orgId: $orgId, dimension: REPO) {
    values {
      value
      count
      __typename
    }
    __typename
  }
}`

// registeredConnectorsDataHealthDocument is the registered document for the connector-health read on the data-health connectors page: the
// urql wire form of the web client's query, captured under
// testdata/wire_capture/data_health_connectors_captured.graphql and asserted equal by
// TestRegisteredDataHealthDocuments_MatchCapturedWireFixtures. Registration
// serves it: the catalog switch reads no routing row (CHAOS-8702).
const registeredConnectorsDataHealthDocument = `query GetConnectorsDataHealth($teamId: ID!) {
  dataHealth(team: $teamId) {
    connectors {
      provider
      scope
      lastSyncAt
      rowsIngested
      lastFailure {
        occurredAt
        message
        stage
        __typename
      }
      __typename
    }
    __typename
  }
}`

// registeredDataHealthIdentityDocument is the registered document for the identity-mapping read on the data-health identity page: the
// urql wire form of the web client's query, captured under
// testdata/wire_capture/data_health_identity_captured.graphql and asserted equal by
// TestRegisteredDataHealthDocuments_MatchCapturedWireFixtures. Registration
// serves it: the catalog switch reads no routing row (CHAOS-8702).
const registeredDataHealthIdentityDocument = `query DataHealthIdentity($team: ID!) {
  dataHealth(team: $team) {
    identityMapping {
      unmappedCount
      unmappedIdentities {
        provider
        email
        displayName
        observedCount
        __typename
      }
      suggestedAliases {
        unmappedIdentity {
          provider
          email
          displayName
          __typename
        }
        suggestedCanonicalId
        suggestedCanonicalName
        confidence
        __typename
      }
      __typename
    }
    __typename
  }
}`

// registeredDataHealthIdentityV1Document is the text dataHealthIdentity accepted BEFORE it selected the CHAOS-8954 name fields. It stays a
// legacy text (see legacyDigestsByOperation), so a web build still sending it keeps working while the new web rolls
// out; the operation's ONE current document is registeredDataHealthIdentityDocument above.
const registeredDataHealthIdentityV1Document = `query DataHealthIdentity($team: ID!) {
  dataHealth(team: $team) {
    identityMapping {
      unmappedCount
      unmappedIdentities {
        provider
        email
        displayName
        observedCount
        __typename
      }
      suggestedAliases {
        unmappedIdentity {
          provider
          email
          displayName
          __typename
        }
        suggestedCanonicalId
        confidence
        __typename
      }
      __typename
    }
    __typename
  }
}`

// registeredMetricLineageDocument is the registered document for the metric-lineage read the data-health popover issues under team ALL: the
// urql wire form of the web client's query, captured under
// testdata/wire_capture/data_health_metric_lineage_captured.graphql and asserted equal by
// TestRegisteredDataHealthDocuments_MatchCapturedWireFixtures. Registration
// serves it: the catalog switch reads no routing row (CHAOS-8702).
const registeredMetricLineageDocument = `query MetricLineage($metricId: ID!) {
  dataHealth(team: 
"ALL") {
    metricLineage(metricId: $metricId) {
      metricId
      sourceTables
      computeWindow {
        kind
        durationDays
        __typename
      }
      computedAt
      rowCount
      __typename
    }
    __typename
  }
}`

// registeredMappingCoverageHealthDocument is the registered document for the mapping-coverage read on the data-health mapping page: the
// urql wire form of the web client's query, captured under
// testdata/wire_capture/data_health_mapping_coverage_captured.graphql and asserted equal by
// TestRegisteredDataHealthDocuments_MatchCapturedWireFixtures. Registration
// serves it: the catalog switch reads no routing row (CHAOS-8702).
const registeredMappingCoverageHealthDocument = `query GetMappingCoverageHealth($teamId: ID!) {
  dataHealth(team: $teamId) {
    mappingCoverage {
      deployments {
        totalRepos
        coveredRepos
        coveragePct
        __typename
      }
      workItems {
        totalRepos
        coveredRepos
        coveragePct
        __typename
      }
      __typename
    }
    __typename
  }
}`

// registeredExperimentsDocument is the registered document for the experiments read on the improve experiments page: the
// urql wire form of the web client's query, captured under
// testdata/wire_capture/experiments_captured.graphql and asserted equal by
// TestRegisteredExperimentsDocument_MatchesCapturedWireFixture. Registration
// serves it: the catalog switch reads no routing row (CHAOS-8702).
const registeredExperimentsDocument = `query Experiments($orgId: String!, $filters: FilterInput) {
  experiments(orgId: $orgId, filters: $filters) {
    items {
      id
      opportunityId
      hypothesis
      metric
      owner
      stopCondition
      status
      startDate
      stopDate
      outcome
      __typename
    }
    derivedFromOpportunities
    __typename
  }
}`

// registeredProductTelemetryDashboardDocument is the registered document for the per-org product telemetry dashboard read: the
// urql wire form of the web client's query, captured under
// testdata/wire_capture/product_telemetry_dashboard_captured.graphql and asserted equal by
// TestRegisteredProductTelemetryDocuments_MatchCapturedWireFixtures. Registration
// serves it: the catalog switch reads no routing row (CHAOS-8702).
const registeredProductTelemetryDashboardDocument = `query ProductTelemetryDashboard($orgId: String!, $input: ProductTelemetryDashboardInput!) {
  productTelemetryDashboard(orgId: $orgId, input: $input) {
    dailyActiveUsers {
      day
      activeAnonymousUsers
      __typename
    }
    topRoutes {
      routePattern
      events
      sessions
      anonymousUsers
      __typename
    }
    featureViews {
      feature
      surface
      views
      anonymousUsers
      __typename
    }
    filterChanges {
      view
      filterKey
      changes
      avgValueCount
      __typename
    }
    chartInteractions {
      chart
      action
      surface
      interactions
      sessions
      __typename
    }
    clientErrors {
      routePattern
      boundary
      errorClass
      errors
      affectedAnonymousUsers
      __typename
    }
    sessionSummary {
      p50DurationMs
      p75DurationMs
      p90DurationMs
      p95DurationMs
      avgPagesViewed
      avgInteractions
      __typename
    }
    __typename
  }
}`

// registeredProductTelemetryPlatformDashboardDocument is the registered document for the cross-org platform product telemetry dashboard read: the
// urql wire form of the web client's query, captured under
// testdata/wire_capture/product_telemetry_platform_dashboard_captured.graphql and asserted equal by
// TestRegisteredProductTelemetryDocuments_MatchCapturedWireFixtures. Registration
// serves it: the catalog switch reads no routing row (CHAOS-8702).
const registeredProductTelemetryPlatformDashboardDocument = `query ProductTelemetryPlatformDashboard($input: ProductTelemetryDashboardInput!) {
  productTelemetryPlatformDashboard(input: $input) {
    totals {
      activeOrgs
      anonymousUsers
      sessions
      events
      __typename
    }
    dailyActiveUsers {
      day
      activeAnonymousUsers
      __typename
    }
    topRoutes {
      routePattern
      events
      sessions
      anonymousUsers
      __typename
    }
    featureViews {
      feature
      surface
      views
      anonymousUsers
      __typename
    }
    filterChanges {
      view
      filterKey
      changes
      avgValueCount
      __typename
    }
    chartInteractions {
      chart
      action
      surface
      interactions
      sessions
      __typename
    }
    clientErrors {
      routePattern
      boundary
      errorClass
      errors
      affectedAnonymousUsers
      __typename
    }
    sessionSummary {
      p50DurationMs
      p75DurationMs
      p90DurationMs
      p95DurationMs
      avgPagesViewed
      avgInteractions
      __typename
    }
    topOrgs {
      orgIdHash
      events
      sessions
      anonymousUsers
      orgId
      orgName
      orgSlug
      __typename
    }
    __typename
  }
}`

// registeredWorkItemTeamAttributionsDocument is the registered document
// for the `workItemTeamAttributions` operation (CHAOS-7066). Not a
// captured real web query -- same CHAOS-7042/7065 precedent: no web
// caller today, so this is hand-written against the published SDL,
// selecting every field model.WorkItemTeamAttribution carries.
const registeredWorkItemTeamAttributionsDocument = `query WorkItemTeamAttributions($orgId: String!, $workItemIds: [String!], $teamId: String) {
  workItemTeamAttributions(
    orgId: $orgId
    workItemIds: $workItemIds
    teamId: $teamId
  ) {
    workItemId
    provider
    teamId
    teamName
    source
    confidence
    isPrimary
    evidence
    __typename
  }
}`

// registeredRecommendationsDocument is the registered document for the
// `recommendations` operation (CHAOS-7065). Not a captured real web
// query -- like `home` (CHAOS-7042), the web consumer is a separate,
// not-yet-built ticket (CHAOS-7068), so this document is hand-written
// against the published SDL, selecting every field model.Recommendation
// carries. It exists to prove the field is reachable and correctly
// shaped, not to certify a live wire-parity comparison against a real
// caller's exact selection set -- that certification is CHAOS-7068's
// job, against its own real query, same sequencing as CHAOS-7070/7064.
const registeredRecommendationsDocument = `query Recommendations($orgId: String!, $team: ID!, $window: WindowInput!) {
  recommendations(orgId: $orgId, team: $team, window: $window) {
    ruleId
    teamId
    orgId
    computedAt
    windowStart
    windowEnd
    severity
    title
    rationale
    successCriterion
    evidence {
      teamId
      metricTable
      windowStart
      windowEnd
      field
      value
      __typename
    }
    __typename
  }
}`

// digestHex is a thin wrapper over the ONE canonical document-digest
// algorithm (CHAOS-4696): sha256(strings.TrimSpace(text)), hex-encoded,
// now shared code in internal/queryapi/digest so
// cmd/registrydump computes the EXACT SAME digest this
// running process does -- not a second hand-typed copy of a two-line
// function that could silently drift from this one. (The schema-digest
// half -- digest.Schema(schemav1.SDL), computed in buildQueryRoute for
// routeswitch.PostgresSwitch's own routing key -- is a separate concern;
// this function is document digest only.)
func digestHex(document string) string {
	return digest.Document(document)
}

// queryRouteConfig is env-sourced configuration for the live /query
// route. All fields are required together -- see loadQueryRouteConfig.
//
// There is deliberately no operator-supplied schema-digest field here
// (CHAOS-5013 removed GO_API_SCHEMA_DIGEST and the startup gate that
// verified it): the schema digest routeswitch.PostgresSwitch needs for its
// own routing-state lookups is computed directly from the embedded SDL
// (digest.Schema(schemav1.SDL), see buildQueryRoute) -- a build-time
// constant, not something an operator configures or a process can
// mis-start over.
type queryRouteConfig struct {
	ClickHouseURI       string
	RegistryPostgresURI string
	EnvelopeJWKSPath    string
	EnvelopeIssuer      string
	EnvelopeAudience    string
}

// loadQueryRouteConfig reads the /query route's configuration from the
// environment. ok is false when ANY required variable is unset -- Wave
// 0's default ("nothing mounted, only /healthz and /readyz live") is
// preserved for any caller that does not set these, rather than the
// process failing to start. This mirrors the deployment shape: a plain
// `go build`/`go vet`/CI run, or an operator who has not yet configured
// this service's dependencies, must not be forced to also configure
// ClickHouse/Postgres/JWKS just to build or run the binary.
func loadQueryRouteConfig(getenv getenvFunc) (queryRouteConfig, bool) {
	cfg := queryRouteConfig{
		ClickHouseURI:       getenv("CLICKHOUSE_URI"),
		RegistryPostgresURI: getenv("GO_API_REGISTRY_POSTGRES_URI"),
		EnvelopeJWKSPath:    getenv("GO_API_ENVELOPE_JWKS_PATH"),
		EnvelopeIssuer:      getenv("GO_API_ENVELOPE_ISSUER"),
		EnvelopeAudience:    getenv("GO_API_ENVELOPE_AUDIENCE"),
	}
	if cfg.ClickHouseURI == "" || cfg.RegistryPostgresURI == "" || cfg.EnvelopeJWKSPath == "" ||
		cfg.EnvelopeIssuer == "" || cfg.EnvelopeAudience == "" {
		return cfg, false
	}
	return cfg, true
}

// newUnrestrictedReadClickHouseOptions is the ONE place any query-api read
// client gets ClickHouse's own "unrestricted" MaxBytesToRead posture
// (CHAOS-4651/CHAOS-4653, below) -- every read client this service builds
// must start from this, not a hand-rolled dhclickhouse.Options{DSN: dsn}
// literal, because a bare literal's MaxBytesToRead is nil, which
// dev-health-go/clickhouse silently defaults to a 64 MiB per-query ceiling
// (CHAOS-4647's original defect). CHAOS-5610 pulled this out of
// newQueryRouteClickHouseClient below -- /query was the only route that
// applied this fix; investment_explain_route.go's own read client
// (buildInvestmentExplainRoute) built a bare Options{DSN: ...} and hit the
// SAME 64 MiB ceiling in prod (code 307 at 64.46 MiB, `investment/explain
// streaming error: ... iterate work unit investment rows: ClickHouse row
// iteration failed`) -- the earlier row-iteration incident recurring in a
// second route because the fix lived on one call site instead of the
// class. Callers may layer additional, route-specific options (e.g.
// queryRouteMaxResultRows below) onto the returned value; none of them may
// overwrite MaxBytesToRead -- except the MCP caller class
// (applyMCPClickHouseCeilings, CHAOS-7091), whose whole point is a read
// ceiling, and which is served on its own listener to one caller class.
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
//     default, i.e. CHAOS-4647 again. newUnrestrictedReadClickHouseOptions
//     below therefore returns an explicit pointer to a zero-valued local,
//     never an absent field. See query_route_integration_test.go's
//     tip_config_sends_unrestricted_max_bytes_to_read subtest, which
//     reads system.settings back through this exact constructor and
//     failed RED (observed "67108864", not "0") against a deliberate
//     "just delete the field" version of this function before this fix
//     was written.
func newUnrestrictedReadClickHouseOptions(dsn string) dhclickhouse.Options {
	// The one constructor path (CHAOS-9126, D5879): MaxBytesToRead unrestricted
	// and the shared result-row bound, from package chclient. No route keeps its
	// own MaxResultRows default.
	return chclient.Options(dsn)
}

// queryRouteMaxResultRows overrides dev-health-go/clickhouse's per-request
// safety-net default (Options.MaxResultRows=1,000) for THIS route only --
// unlike MaxBytesToRead above, this protects THIS PROCESS's own memory,
// not ClickHouse, and its value is derived from /query's own workload
// (workgraph.MaxEdgesLimit fan-out); it is intentionally NOT part of
// newUnrestrictedReadClickHouseOptions, so investment/explain and any
// future route each reason about their own row-buffering workload rather
// than inheriting a number derived from a different endpoint's fan-out --
// exactly the CHAOS-4647 mistake (borrowing CHAOS-3848's untouched
// numbers for a different endpoint) this PR exists to stop repeating.
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
const queryRouteMaxResultRows uint = chclient.MaxResultRows // = 500,000 -- PROVISIONAL, workload derivation, successor CHAOS-4654; the SAME bound as every REST client (CHAOS-9126)

// newQueryRouteClickHouseClient is the ONE place this route constructs its
// ClickHouse client -- pulled out of buildQueryRoute so a test can exercise
// the REAL production wiring (these exact options reaching the real
// driver) instead of a hand-copied literal that could silently drift from
// what buildQueryRoute actually does (codex review round 1, P3). Layers
// this route's own MaxResultRows on top of
// newUnrestrictedReadClickHouseOptions's shared MaxBytesToRead posture,
// per that function's doc comment.
func newQueryRouteClickHouseClient(dsn string) (*dhclickhouse.Client, error) {
	return dhclickhouse.NewClickHouseQueryClientWithOptions(newUnrestrictedReadClickHouseOptions(dsn))
}

// buildQueryRoute wires the real featureFlags path from env-sourced
// config: the shared dev-health-go ClickHouse client, a real Postgres
// pool, and the effective-principal verifier, then hands them to
// newQueryHandler. The returned cleanup func closes the Postgres pool;
// call it on shutdown. The returned ready func is CHAOS-4512's fix,
// extended by CHAOS-4708: a live liveness check of the THREE dependencies
// /query actually reads (ClickHouse via chClient.Ping, the registry
// Postgres pool via pgPool.Ping, the envelope JWKS via
// verifier.CheckJWKS), for main.go's readyzHandler to call on every
// /readyz request -- co-located here, not in main.go, because these are
// the only places this route's live dependency handles exist. See
// readyzHandler's doc comment in main.go for the full contract this
// closes over. CHAOS-4708 also adds an EAGER JWKS check below, matching
// the ClickHouse Ping's fail-fast-at-boot discipline -- see that check's
// own comment for why it does not substitute for the live one in
// readinessCheck.
// queryRouteHandlers groups what buildQueryRoute mounts.
//
// A struct rather than a longer positional return list: CHAOS-5425 added
// the third and fourth handlers (/query/proof and /buildinfo), and a
// six-value positional return is exactly where two same-typed
// http.HandlerFunc values get swapped at a call site with nothing to
// catch it -- /query and /query/proof differ ONLY in reachability, so a
// swap would silently make shadow operations reachable to real traffic.
type queryRouteHandlers struct {
	// RegistryPool is the process's ONE registry Postgres pool (the query-api
	// role's own login), owned and closed by buildQueryRoute's cleanup. Build
	// hands it to the edge-token users check (CHAOS-6290) rather than opening a
	// second pool; a caller must not close it.
	RegistryPool *pgxpool.Pool
	// Query is /query: production reachability, canary|primary only.
	Query http.HandlerFunc
	// RunOperation is POST /query/run-operation (CHAOS-7831): the SAME serving pipeline as Query, wrapped so the MCP class rows gate it
	// (markClassGated). Mounted by mountQueryRouteSets on Plane.InternalHandler ONLY; acr's run_operation posts here. /query itself stays
	// ungated: the Python /graphql edge reaches it over the same internal identity carrier.
	RunOperation http.HandlerFunc
	// Proof is the same pipeline over a measurement-only Switch that also
	// admits shadow. Mounted only by mountProofRoute, which refuses in a
	// production posture and without an explicit opt-in.
	Proof http.HandlerFunc
	// ProofWrite is /query/proof-write (CHAOS-7096): the same pipeline,
	// mutation-only, org-allowlist gated. Mounted only by
	// mountProofWriteRoute, and ONLY on Plane.InternalHandler -- never on
	// Plane.Handler, never on the public listener.
	ProofWrite http.HandlerFunc
	// Registry is GET /registry.
	Registry http.HandlerFunc
	// BuildInfo is GET /buildinfo -- which build this process is,
	// authenticated with the same envelope verifier /query uses.
	BuildInfo http.HandlerFunc
	// MCP is POST /query of the MCP caller class (CHAOS-7085): its own gate,
	// gqlgen server, ClickHouse client and routing rows. Mounted only on
	// Plane.MCPHandler, which only the MCP listener serves.
	MCP http.Handler
	// MCPProof is the proof variant of MCP (CHAOS-7214): POST /query/proof-mcp,
	// mounted only by mountProofMCPRoute and only on Plane.InternalHandler.
	MCPProof http.Handler
	// Probes are /query's live dependency checks, one per dependency class, in the
	// order readinessCheck runs them. dho query-api registers each as its own
	// required readiness check, so the operator /readyz names the failing class
	// (and only that) without a shared body to leak from.
	Probes []ReadinessProbe
}

// ReadinessProbe is one live dependency check of /query. Name is a valid health
// check name and the only thing an unauthenticated /readyz says about a failure.
type ReadinessProbe struct {
	Name  string
	Check func(context.Context) error
}

func buildQueryRoute(getenv getenvFunc, cfg queryRouteConfig) (queryRouteHandlers, func(context.Context) error, func(), error) {
	// CHAOS-5013: the schema digest routeswitch.PostgresSwitch needs for
	// its own routing-state lookups is computed directly from the
	// embedded SDL, not read from an operator-supplied env var and
	// verified against it (that startup gate -- GO_API_SCHEMA_DIGEST,
	// verifySchemaDigest, log.Fatalf-on-mismatch -- is REMOVED: chris's
	// ruling, "you version a schema, but to start something -- no"). This
	// is the same value the removed gate used to verify, computed the
	// same way, just no longer something a missing/wrong env var can
	// prevent this process from starting over.
	schemaDigest := digest.Schema(schemav1.SDL)

	chClient, err := newQueryRouteClickHouseClient(cfg.ClickHouseURI)
	if err != nil {
		return queryRouteHandlers{}, nil, nil, err
	}
	// Eager readiness check, matching internal/workerservice's own
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
		return queryRouteHandlers{}, nil, nil, fmt.Errorf("query-api: ClickHouse readiness check failed (CLICKHOUSE_URI must be the NATIVE protocol port, not the HTTP port -- see deploy/go-workers/README.md): %w", err)
	}

	pgPool, err := pgxpool.New(context.Background(), cfg.RegistryPostgresURI)
	if err != nil {
		return queryRouteHandlers{}, nil, nil, err
	}

	verifier, err := principal.NewVerifier(cfg.EnvelopeJWKSPath, cfg.EnvelopeIssuer, cfg.EnvelopeAudience)
	if err != nil {
		pgPool.Close()
		return queryRouteHandlers{}, nil, nil, err
	}
	// CHAOS-4708: eager readiness check, matching the ClickHouse Ping
	// above's "measurement that did not happen must FAIL, loudly"
	// discipline -- a missing, unreadable, empty, or malformed JWKS is a
	// hopeless config exactly like a wrong ClickHouse protocol port is:
	// refuse to start rather than mount /query behind a verifier that
	// will 401 every authenticated request. This does NOT substitute for
	// readinessCheck's live JWKS re-check below: this one-shot call says
	// nothing about a JWKS that goes bad LATER (deleted, rotated wrong,
	// truncated by a bad mount refresh) -- exactly the gap this ticket
	// exists to close for readiness, the same way the ClickHouse
	// readiness check below does not substitute for THIS eager Ping.
	if err := verifier.CheckJWKS(); err != nil {
		pgPool.Close()
		return queryRouteHandlers{}, nil, nil, fmt.Errorf("query-api: JWKS readiness check failed (GO_API_ENVELOPE_JWKS_PATH must point to a readable, non-empty, valid Ed25519 JWKS document): %w", err)
	}

	// One posture check per process: it proves the role in the background from here
	// on, and both the combined check and the per-class probes read its last answer.
	posture := queryAPIPostureCheck(getenv, pgPool)
	// CHAOS-7831: ONE class-row switch for the MCP listener and the named-operation route (:8091), so a root's class row is one decision.
	classSwitch := newClassRowSwitch(pgPool)
	handler, proofHandler, proofWriteHandler, registryHandler, err := newQueryHandler(analytics.PinInvestmentMembershipScope(chClient), pgPool, verifier, schemaDigest, getenv, classSwitch)
	if err != nil {
		pgPool.Close()
		return queryRouteHandlers{}, nil, nil, err
	}
	// CHAOS-7085/CHAOS-7091: the MCP caller class gets its OWN ClickHouse
	// client (a read-bytes ceiling, not the shared unrestricted setting) and
	// its OWN routing rows (one class row per allowlisted root field).
	mcpClient, err := newMCPClickHouseClient(cfg.ClickHouseURI)
	if err != nil {
		pgPool.Close()
		return queryRouteHandlers{}, nil, nil, fmt.Errorf("query-api: build the MCP caller-class ClickHouse client: %w", err)
	}
	mcpHandler := newMCPHandler(mcpClient, pgPool, classSwitch, getenv)
	// CHAOS-7214: the proof variant over a switch that also admits shadow rows.
	mcpProofHandler := newMCPProofHandler(mcpHandler, routeswitch.NewClassDecisionProofSwitch(pgPool, mcpRoutingDigests()), verifier, newProofOrgAllowed(pgPool))
	handlers := queryRouteHandlers{
		Query:      handler,
		Proof:      proofHandler,
		ProofWrite: proofWriteHandler,
		Registry:   registryHandler,
		BuildInfo:  newBuildInfoHandler(verifier),
		MCP:        mcpHandler,
		MCPProof:   mcpProofHandler,
		Probes:     readinessProbes(chClient, pgPool, verifier, posture),
	}
	// The one registry pool, for Build's edge users check (CHAOS-6290). Set outside the
	// literal above so that literal keeps the shape posture_readiness_test pins.
	handlers.RegistryPool = pgPool
	// Set outside the literal for the same reason (the literal's shape is pinned): the class-gated alias of the serving handler.
	handlers.RunOperation = markClassGated(handler)
	cleanup := func() { pgPool.Close() }
	// CHAOS-6803/CHAOS-6804: a deployment that names query-api's role
	// (QUERY_API_DATABASE_ROLE) is not ready until the pool logs in AS that role
	// and the role holds exactly the query-api manifest the migration leg
	// applies (postgres.QueryAPIPosture): the read plane, the saved-report
	// writes, and nothing else. A deployment that names none has not opted in
	// and is checked for nothing extra.
	ready := readinessCheck(chClient, pgPool, verifier, posture)
	return handlers, ready, cleanup, nil
}

// readinessPinger and jwksChecker are the narrow surfaces readinessCheck needs
// from ClickHouse, the Postgres pool and the envelope verifier
// (*dhclickhouse.Client, *pgxpool.Pool and *principal.Verifier satisfy them),
// so the composition -- which dependency's failure wins, and that the posture
// check is really part of it -- is testable without a live ClickHouse.
type readinessPinger interface {
	Ping(context.Context) error
}

type jwksChecker interface {
	CheckJWKS() error
}

// readinessCheck returns a func that checks ALL THREE of /query's live
// dependencies -- ClickHouse, the registry Postgres pool, and (CHAOS-4708)
// the envelope JWKS -- and returns the first error any of them reports.
// This is CHAOS-4512's actual fix: buildQueryRoute above already pings
// ClickHouse ONCE, eagerly, at startup (so a ClickHouse that is down at
// boot refuses to start the process at all -- "measurement that did not
// happen must FAIL, loudly"). That one-shot check says nothing about a
// ClickHouse that goes unreachable LATER, and says nothing about Postgres
// at all: pgxpool.New above is lazy by design (pgx does not dial until a
// query or Ping is issued), so an unreachable or misconfigured
// GO_API_REGISTRY_POSTGRES_URI has never failed startup -- the process
// comes up, the OLD readyzHandler answered 200 unconditionally, and every
// real request then failed against a pool that has never once connected.
// Calling this on every /readyz request (with a bounded timeout applied
// by the caller) is what closes that gap: readiness now reflects the
// CURRENT reachability of both dependencies, not their state at process
// start.
//
// CHAOS-4708 closes the same gap for the third dependency /query reads:
// principal.NewVerifier loads its JWKS lazily, per Verify call, by
// deliberate design (a rotated key is picked up without a restart -- see
// that constructor's doc comment) -- which means a missing, unreadable,
// empty, or malformed GO_API_ENVELOPE_JWKS_PATH was, before this fix,
// invisible to BOTH startup (buildQueryRoute never read the file) AND
// readiness (nothing here checked it): the process would start, mount
// /query, and answer /readyz with exactly "ready", while every
// authenticated request 401'd. verifier.CheckJWKS() closes that -- see
// its own doc comment for why calling it here, uncached, on every probe,
// preserves the no-restart rotation contract rather than defeating it.
func readinessCheck(chClient readinessPinger, pgPool readinessPinger, verifier jwksChecker, posture func(context.Context) error) func(context.Context) error {
	probes := readinessProbes(chClient, pgPool, verifier, posture)
	return func(ctx context.Context) error {
		for _, probe := range probes {
			if err := probe.Check(ctx); err != nil {
				return err
			}
		}
		return nil
	}
}

// readinessProbes are the checks readinessCheck runs, one per dependency class and in
// that order (the role-posture one only when the deployment named a query-api role).
func readinessProbes(chClient readinessPinger, pgPool readinessPinger, verifier jwksChecker, posture func(context.Context) error) []ReadinessProbe {
	probes := []ReadinessProbe{
		{Name: "query_clickhouse", Check: func(ctx context.Context) error {
			if err := chClient.Ping(ctx); err != nil {
				return &readyzDependencyError{Class: readyzClassClickHouse, Cause: err}
			}
			return nil
		}},
		{Name: "query_postgres", Check: func(ctx context.Context) error {
			if err := pgPool.Ping(ctx); err != nil {
				return &readyzDependencyError{Class: readyzClassPostgres, Cause: err}
			}
			return nil
		}},
		{Name: "query_jwks", Check: func(context.Context) error {
			if err := verifier.CheckJWKS(); err != nil {
				return &readyzDependencyError{Class: readyzClassJWKS, Cause: err}
			}
			return nil
		}},
	}
	if posture != nil {
		probes = append(probes, ReadinessProbe{Name: "query_role_posture", Check: func(ctx context.Context) error {
			return queryAPIPostureReadiness(ctx, posture)
		}})
	}
	return probes
}

// queryAPIPostureCheck returns the readiness check for query-api's Postgres role
// posture, or nil when the deployment names no query-api role
// (QUERY_API_DATABASE_ROLE unset or blank): such a deployment has not opted in
// and is checked for nothing extra.
//
// The whole-catalog posture query takes 1.4-1.9 s on the production catalog
// (CHAOS-6765), longer than the kubelet's 2 s probe timeout, and the readiness
// contract is that a probe answers within its budget from state it already
// holds, never by running a live expensive check. So the check starts proving
// the role in the BACKGROUND at construction (process start, before readiness
// can flip: an unproven role is not ready) and /readyz reads the last answer
// without waiting (CachedPostureCheck.CheckNoWait).
func queryAPIPostureCheck(getenv getenvFunc, pgPool *pgxpool.Pool) func(context.Context) error {
	return queryAPIPostureCheckWith(getenv, pgPool, postgresstore.PostureCheckOptions{})
}

// queryAPIPostureCheckWith is queryAPIPostureCheck with the cache's freshness
// tunable. Production passes the zero value (the defaults: a passing answer is
// re-proven every 300 s, CHAOS-6937); a test that must watch a grant change
// reach readiness passes a short TTL, and still runs the production
// composition (role from the environment, River schema, background warm,
// no-wait read).
func queryAPIPostureCheckWith(
	getenv getenvFunc, pgPool *pgxpool.Pool, options postgresstore.PostureCheckOptions,
) func(context.Context) error {
	role := strings.TrimSpace(getenv("QUERY_API_DATABASE_ROLE"))
	if role == "" {
		return nil
	}
	options.Logger = slog.Default()
	cached := postgresstore.NewCachedQueryAPIPostureCheck(
		pgPool, role, queryAPIRiverSchema(getenv), options,
	)
	cached.Warm()
	return func(context.Context) error { return cached.CheckNoWait() }
}

// queryAPIRiverSchema is the River schema the posture check asserts the role
// holds NO privilege on: RIVER_DATABASE_SCHEMA, defaulting to "river" like every
// other service (config.defaultRiverDatabaseSchema).
func queryAPIRiverSchema(getenv getenvFunc) string {
	if schema := strings.TrimSpace(getenv("RIVER_DATABASE_SCHEMA")); schema != "" {
		return schema
	}
	return "river"
}

// queryAPIPostureReadiness runs the optional posture check and classes its
// failure so /readyz names WHICH dependency failed without echoing the cause.
func queryAPIPostureReadiness(ctx context.Context, posture func(context.Context) error) error {
	if posture == nil {
		return nil
	}
	if err := posture(ctx); err != nil {
		return &readyzDependencyError{Class: readyzClassPosture, Cause: err}
	}
	return nil
}

// The three dependency classes readinessCheck can fail on. These are the
// ONLY strings readyzHandler (main.go) is ever allowed to put in a
// /readyz response body for an unhealthy check -- see
// readyzDependencyError's doc comment for why.
const (
	readyzClassClickHouse = "clickhouse"
	readyzClassPostgres   = "postgres"
	readyzClassJWKS       = "jwks"
	// readyzClassPosture: the pool does not log in as the named query-api role,
	// or that role does not hold exactly the query-api manifest (CHAOS-6803,
	// CHAOS-6804). Only checked when QUERY_API_DATABASE_ROLE names a role.
	readyzClassPosture = "postgres_posture"
)

// readyzDependencyError names WHICH of /query's three live dependencies
// (ClickHouse, the registry Postgres pool, or the envelope JWKS) failed a
// readiness check, while keeping the underlying error -- which can carry
// a ClickHouse/Postgres host:port (pgx and clickhouse-go dial errors) or
// a filesystem path (principal.Verifier.CheckJWKS's errors name
// GO_API_ENVELOPE_JWKS_PATH directly) -- separate from that class name.
//
// CHAOS-4724: /readyz is UNAUTHENTICATED. Before this type existed,
// readyzHandler wrote err.Error() straight into the response body, so
// whatever the wrapped dependency error rendered (a Postgres host:port, a
// JWKS file path) reached any unauthenticated caller. Cause is for the
// log line readyzHandler emits on every failure (the operator-diagnosable
// detail); Class is the only part of this error that is safe to also put
// in the response body a caller outside the trust boundary receives.
// Error() still renders the full "<class>: <cause>" text some other
// caller might reasonably want (e.g. a future internal-only diagnostic
// surface) -- it is readyzHandler's choice to use Class alone for the
// body, not a property of this type -- and Unwrap exposes Cause so
// errors.Is/As still see through to it.
type readyzDependencyError struct {
	Class string
	Cause error
}

func (e *readyzDependencyError) Error() string { return e.Class + ": " + e.Cause.Error() }
func (e *readyzDependencyError) Unwrap() error { return e.Cause }

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
// both served by the catalog switch.
//
// There is no per-org or partial rollout (CHAOS-6807, CHAOS-8702): a registered operation is served to every
// authenticated org, because Switch.Enabled(operation string) takes no org argument and these operations have no
// Python resolver for an org outside a cohort to fall back to.
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

// sortedOperationNames returns m's keys sorted -- a deterministic
// ordering for a log line (map iteration order is intentionally
// randomized by the Go runtime, so printing a map's keys directly would
// make the mounted-route log line non-reproducible between restarts).
func sortedOperationNames(m map[string]string) []string {
	names := make([]string, 0, len(m))
	for operation := range m {
		names = append(names, operation)
	}
	sort.Strings(names)
	return names
}

// mountedRouteLogMessage is the CHAOS-4710 deliverable-3 replacement for
// main.go's former hardcoded, stale literal ("featureFlags, reviewEdges,
// cognitiveLoad, complexityTimeseries, hotspots, operatingReview" -- six
// of the twelve operations digestByOperation below actually registers,
// unmaintained since Wave 3). Following CHAOS-4512's precedent explicitly
// ("remove the false claim rather than correcting the list, so it cannot
// go stale again"): newQueryHandler calls this with the SAME
// digestByOperation map it registers against (see the call site below),
// so the printed set is always exactly the registered set, by
// construction -- a thirteenth operation added to that map appears here
// with no separate edit required, and query_route_mounted_log_test.go's
// TestMountedRouteLogMessage_ListsExactlyRegisteredOperations parses the
// actual produced message and asserts its operation set equals the map
// passed in, independent of any hand-typed expectation.
//
// Deliberately a free function taking the map as a parameter, NOT a
// package-level "build the map" function of its own: digestByOperation's
// composite literal below is cmd/registrydump's parse
// target (see that tool's doc comment -- it asserts EXACTLY ONE
// `digestByOperation := map[string]string{...}` assignment exists in
// this file and reads the map from that literal via go/ast, not via
// import). An earlier version of this fix moved the literal itself into
// a separate function returning it, which silently broke registrydump
// (its walk only matches a composite-literal RHS, not a function-call
// RHS) and, with it, every test that cross-checks against registrydump's
// output (test_go_api_operation_catalog.py,
// test_go_api_document_digest.py) -- caught by the ops pre-push gate's
// full unit suite, not by `go build`/`go vet`/`go test ./cmd/query-api/...`,
// none of which import or run registrydump against this file. The map
// literal below is therefore untouched from its pre-CHAOS-4710 shape;
// only the log line next to it is new.
func mountedRouteLogMessage(digestByOperation map[string]string) string {
	return fmt.Sprintf(
		"query-api: /query route mounted (%s)",
		strings.Join(sortedOperationNames(digestByOperation), ", "),
	)
}

func newQueryHandler(chClient featureflags.QueryClient, pgPool *pgxpool.Pool, verifier *principal.Verifier, schemaDigest string, getenv getenvFunc, classSwitches ...routeswitch.Switch) (http.HandlerFunc, http.HandlerFunc, http.HandlerFunc, http.HandlerFunc, error) {
	// CHAOS-6263 PR (a): /query's edge-access-token carrier, built once
	// here (not per-request) and threaded into every authenticateInternalRequest
	// call this handler makes. (nil, nil, nil) when GO_API_EDGE_JWT_SECRET is
	// unset -- same opt-out contract as buildEdgeVerifierFromEnv; an error
	// here means the secret IS set but malformed, which is a hard config
	// failure this function refuses to start behind, same discipline as
	// the envelope verifier's own JWKS check in buildQueryRoute.
	edgeAuth, edgeStore, err := buildQueryEdgeAuthenticatorFromEnv(getenv, pgPool)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("query-api: edge authenticator: %w", err)
	}
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
	// internally consistent across this map and digestByOperation's reverse
	// index -- it is never compared against request text.
	//
	// This is cmd/registrydump's parse target -- see
	// mountedRouteLogMessage's doc comment above for why this literal's
	// exact shape (a single `digestByOperation := map[string]string{...}`
	// assignment) must stay untouched.
	digestByOperation := map[string]string{
		"featureFlags":                      digestHex(registeredFeatureFlagsDocument),
		"reviewEdges":                       digestHex(registeredReviewEdgesDocument),
		"cognitiveLoad":                     digestHex(registeredCognitiveLoadDocument),
		"complexityTimeseries":              digestHex(registeredComplexityTimeseriesDocument),
		"hotspots":                          digestHex(registeredHotspotsDocument),
		"operatingReview":                   digestHex(registeredOperatingReviewDocument),
		"home":                              digestHex(registeredHomeDocument),
		"releaseImpact":                     digestHex(registeredReleaseImpactDocument),
		"workGraphEdges":                    digestHex(registeredWorkGraphEdgesDocument),
		"workGraphFlow":                     digestHex(registeredWorkGraphFlowDocument),
		"workUnitTeamAttributions":          digestHex(registeredWorkUnitTeamAttributionsDocument),
		"workGraphArtifacts":                digestHex(registeredWorkGraphArtifactsDocument),
		"flowMatrix":                        digestHex(registeredFlowMatrixDocument),
		"investmentBreakdown":               digestHex(registeredInvestmentBreakdownDocument),
		"investmentEvidenceQuality":         digestHex(registeredInvestmentEvidenceQualityDocument),
		"investmentFull":                    digestHex(registeredInvestmentFullDocument),
		"capacityForecast":                  digestHex(registeredCapacityForecastDocument),
		"capacityCompletionDistribution":    digestHex(registeredCapacityCompletionDistributionDocument),
		"capacityForecasts":                 digestHex(registeredCapacityForecastsDocument),
		"throughputForecast":                digestHex(registeredThroughputForecastDocument),
		"featureFlagEvents":                 digestHex(registeredFeatureFlagEventsDocument),
		"pr":                                digestHex(registeredPrDetailDocument),
		"securityOverview":                  digestHex(registeredSecurityOverviewDocument),
		"securityAlerts":                    digestHex(registeredSecurityAlertsDocument),
		"connectorsDataHealth":              digestHex(registeredConnectorsDataHealthDocument),
		"dataHealthIdentity":                digestHex(registeredDataHealthIdentityDocument),
		"metricLineage":                     digestHex(registeredMetricLineageDocument),
		"mappingCoverageHealth":             digestHex(registeredMappingCoverageHealthDocument),
		"catalogValues":                     digestHex(registeredCatalogValuesDocument),
		"acrRepositoryScopes":               digestHex(registeredAcrRepositoryScopesDocument),
		"busFactor":                         digestHex(registeredBusFactorDocument),
		"testOpsPipeline":                   digestHex(registeredTestOpsPipelineDocument),
		"testOpsTest":                       digestHex(registeredTestOpsTestDocument),
		"testOpsCoverage":                   digestHex(registeredTestOpsCoverageDocument),
		"featureFlagTimeseries":             digestHex(registeredFeatureFlagTimeseriesDocument),
		"savedReports":                      digestHex(registeredSavedReportsDocument),
		"savedReport":                       digestHex(registeredSavedReportDocument),
		"reportRuns":                        digestHex(registeredReportRunsDocument),
		"createSavedReport":                 digestHex(registeredCreateSavedReportDocument),
		"updateSavedReport":                 digestHex(registeredUpdateSavedReportDocument),
		"deleteSavedReport":                 digestHex(registeredDeleteSavedReportDocument),
		"cloneSavedReport":                  digestHex(registeredCloneSavedReportDocument),
		"triggerReport":                     digestHex(registeredTriggerReportDocument),
		"productTelemetryDashboard":         digestHex(registeredProductTelemetryDashboardDocument),
		"productTelemetryPlatformDashboard": digestHex(registeredProductTelemetryPlatformDashboardDocument),
		"experiments":                       digestHex(registeredExperimentsDocument),
		"aiImpactSummary":                   digestHex(registeredAiImpactSummaryDocument),
		"aiComparison":                      digestHex(registeredAiComparisonDocument),
		"aiReviewLoad":                      digestHex(registeredAiReviewLoadDocument),
		"compoundingRisk":                   digestHex(registeredCompoundingRiskDocument),
		"aiOpportunities":                   digestHex(registeredAiOpportunitiesDocument),
		"improveOpportunities":              digestHex(registeredImproveOpportunitiesDocument),
		"aiGovernanceSummary":               digestHex(registeredAiGovernanceSummaryDocument),
		"aiWorkflowDrilldown":               digestHex(registeredAiWorkflowDrilldownDocument),
		"aiRiskBreakdown":                   digestHex(registeredAiRiskBreakdownDocument),
		"aiAttributedPrs":                   digestHex(registeredAiAttributedPrsDocument),
		"aiAttributionOverview":             digestHex(registeredAiAttributionOverviewDocument),
		"testopsRisk":                       digestHex(registeredTestopsRiskDocument),
		"testopsJobFailures":                digestHex(registeredTestopsJobFailuresDocument),
		"coverageBaselines":                 digestHex(registeredCoverageBaselinesDocument),
		"coverageScopeBaseline":             digestHex(registeredCoverageScopeBaselineDocument),
		"sourceHealth":                      digestHex(registeredSourceHealthDocument),
		"workItemTeamAttributions":          digestHex(registeredWorkItemTeamAttributionsDocument),
		"recommendations":                   digestHex(registeredRecommendationsDocument),
	}
	// CHAOS-4710 deliverable 3: log the mounted set HERE, where
	// digestByOperation actually lives, rather than handing main.go a
	// hand-typed guess of what this function registers -- see
	// mountedRouteLogMessage's doc comment for why this is not a
	// package-level function main.go can call directly.
	log.Print(mountedRouteLogMessage(digestByOperation))
	// Both are built from THIS map and THIS schemaDigest -- the same two
	// values PostgresSwitch is about to be constructed with -- so neither
	// can describe a registration set this route does not actually have.
	// That is the same by-construction discipline mountedRouteLogMessage
	// above exists to enforce, applied to the two surfaces an operator
	// uses to answer "is anything actually enabled?".
	registryHandler := newRegistryHandler(schemaDigest, digestByOperation)
	// CHAOS-8702: the serving switch reads no routing row -- a registered operation is served
	// (routeswitch/catalog_switch.go). The proof switch below and the class-row switch
	// (newClassRowSwitch) still read rows: a measurement route needs a row and an MCP class root with no
	// row is dark.
	sw := routeswitch.NewCatalogSwitch(digestByOperation)
	routeMux := routeswitch.NewMux(sw)

	// operationByDigest is digestByOperation's reverse index, built once
	// here rather than on every request -- operationForDocument does a
	// single map lookup per request, not a linear scan.
	operationByDigest, err := buildOperationByDigest(digestByOperation, legacyDigestsByOperation)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("query-api: %w", err)
	}

	gqlHandler := newGraphQLServer(&graph.Resolver{ClickHouse: chClient, Postgres: pgPool, ReportWriter: newReportWriter(pgPool, jobContractRoot)})
	for operation := range digestByOperation {
		routeMux.Register(operation, gqlHandler)
	}

	// CHAOS-5425: the SAME pipeline is built a second time for the measurement-only proof route, so a
	// proof exercises the real path. Both handlers share newDocumentDispatchHandler below -- byte-identical
	// ingress, body-size contract, document resolution, bearer/envelope verification and org context. The
	// catalog switch serves every registered operation, as on the serving route: no routing row decides it.
	proofMux := routeswitch.NewMux(routeswitch.NewCatalogSwitch(digestByOperation))
	for operation := range digestByOperation {
		proofMux.Register(operation, gqlHandler)
	}

	// CHAOS-7096: a THIRD instance of the same shared pipeline, over its own
	// Switch, independent of any routing state -- a proof-write operation
	// is by definition one CHAOS-6098 could not yet get a receipt for at its
	// target mode, so gating it on that mode would recreate the exact
	// circularity this route exists to break (routing_enable.go's
	// ErrEnableUnproven). "Off by default" instead lives in the two gates
	// OUTSIDE this Switch: mountProofWriteRoute's posture check (server.go,
	// mirrors mountProofRoute) decides whether this handler is mounted on
	// any listener at all, and orgAllowed decides per-request whether THIS
	// caller may use it (go_api_proof_orgs, empty on every deployment until
	// an operator runs `dho goapi routing proof-org add`). A request
	// reaching proofWriteMux.Dispatch has already passed both.
	proofWriteOperations := make(routeswitch.StaticSwitch, len(digestByOperation))
	for operation := range digestByOperation {
		proofWriteOperations[operation] = true
	}
	proofWriteMux := routeswitch.NewMux(proofWriteOperations)
	for operation := range digestByOperation {
		proofWriteMux.Register(operation, gqlHandler)
	}
	orgAllowed := newProofOrgAllowed(pgPool)

	// CHAOS-7831: the class-row switch is the one the MCP listener uses (same type, same keys, live read); only the serving handler is gated: the
	// proof handlers measure a root BEFORE it is enabled and must keep running on a dark one.
	// buildQueryRoute passes the ONE switch instance it also gives the MCP handler; a caller that passes none (the unit tests) gets one built the same way.
	classSwitch := newClassRowSwitch(pgPool)
	if len(classSwitches) > 0 {
		classSwitch = classSwitches[0]
	}
	classGate := newClassRowGate(classSwitch)
	return newDocumentDispatchHandler(getenv, routeMux, operationByDigest, verifier, edgeAuth, edgeStore, "", nil, classGate),
		newDocumentDispatchHandler(getenv, proofMux, operationByDigest, verifier, edgeAuth, edgeStore, digest.KindQuery, nil),
		newDocumentDispatchHandler(getenv, proofWriteMux, operationByDigest, verifier, edgeAuth, edgeStore, digest.KindMutation, orgAllowed),
		registryHandler, nil
}

// newProofOrgAllowed builds /query/proof-write's org-allowlist check
// (CHAOS-7096) against a real Postgres pool. Named and separated from
// newQueryHandler's body specifically so a test can call it directly with a
// pool forced into an error state (e.g. already closed) and assert the
// closure itself -- not a stand-in -- fails closed: a store error must
// never read as "allowed", and the only way to prove that is to exercise
// this exact function, not a test double standing in for its contract.
func newProofOrgAllowed(pool *pgxpool.Pool) func(context.Context, string) bool {
	return func(ctx context.Context, orgID string) bool {
		allowed, err := postgresstore.ProofOrgAllowed(ctx, pool, orgID)
		if err != nil {
			// Fail closed: a store error is never "let it through". Logged
			// so an operator can tell "nobody is allowlisted yet" (a normal,
			// expected 403) from "the allowlist read itself is broken".
			log.Printf("query-api: proof-write org-allowlist read failed for org_id=%q (refusing): %v", orgID, err)
			return false
		}
		return allowed
	}
}

// newGraphQLServer is the gqlgen server every registered operation runs on:
// the executable schema over the resolvers, the null-argument refusal, and the
// error presenter. The venue oracle builds the same server, so what it measures
// is what serves.
func newGraphQLServer(resolver *graph.Resolver) *gqlhandler.Server {
	return newGraphQLServerWithLimits(resolver, graphQLComplexityLimit, graphQLDepthLimit)
}

// newGraphQLServerWithLimits is newGraphQLServer with its two query limits as
// parameters, so a test can drive the real HTTP pipeline against a limit small
// enough for this schema's shallow documents to exceed (no registered document
// nests past graphQLDepthLimit, so the production depth limit cannot be
// exceeded over HTTP).
func newGraphQLServerWithLimits(resolver *graph.Resolver, complexityLimit, depthMax int) *gqlhandler.Server {
	schema := graph.NewExecutableSchema(graph.Config{Resolvers: resolver})
	// CHAOS-7078: gqlhandler.NewDefaultServer's own doc comment says it
	// plainly -- "Deprecated: This was and is just an example ... Not for
	// prod". It turns ON introspection, Automatic Persisted Queries, GET,
	// websocket and multipart transports, with no depth or complexity
	// limit anywhere. Built explicitly instead:
	//   - POST only. This route is never browsed and every existing
	//     identity/envelope check already assumes a POST body; GET,
	//     websocket, multipart and the OPTIONS transport are all simply
	//     absent, not merely disabled.
	//   - no introspection, no APQ. An unregistered document has no
	//     operation name and no go_api_routing_state row can exist for it
	//     -- operationForDocument's digest gate (below) already refuses
	//     it outright; APQ's hash-registration flow would be a second,
	//     weaker path toward the same shape this route exists to keep
	//     closed, and introspection has no legitimate caller on a route
	//     the Python edge alone forwards to.
	//   - a fixed complexity limit and a depth limit (gqlgen ships no
	//     depth limiter of its own), both sized from the real
	//     registered-document set -- see graphql_server_limits.go's doc
	//     comment for the measurement and graphql_server_limits_test.go
	//     for the pin.
	gqlHandler := gqlhandler.New(schema)
	gqlHandler.AddTransport(transport.POST{})
	// Outermost, so refusals by the limits and the org guard are counted too.
	gqlHandler.AroundOperations(recordOperationErrorCount)
	gqlHandler.Use(extension.FixedComplexityLimit(complexityLimit))
	gqlHandler.Use(depthLimit{Max: depthMax})
	gqlHandler.AroundFields(graph.RefuseNullForNonNullArguments)
	gqlHandler.Use(graph.OperationOrgGuard{})
	gqlHandler.AroundResponses(recordErrorCount)
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
		return withMutationLocation(ctx, graphql.DefaultErrorPresenter(ctx, err))
	})
	return gqlHandler
}

// withMutationLocation gives an error raised by a mutation field the source
// location of that field, as the Python plane's error carries (line and column
// of the field in the document). Queries keep the answer they have always had:
// no location on a resolver error.
func withMutationLocation(ctx context.Context, presented *gqlerror.Error) *gqlerror.Error {
	if presented == nil || len(presented.Locations) > 0 || !graphql.HasOperationContext(ctx) {
		return presented
	}
	operation := graphql.GetOperationContext(ctx).Operation
	field := graphql.GetFieldContext(ctx)
	if operation == nil || operation.Operation != ast.Mutation || field == nil || field.Field.Field == nil || field.Field.Position == nil {
		return presented
	}
	presented.Locations = []gqlerror.Location{{Line: field.Field.Position.Line, Column: field.Field.Position.Column}}
	return presented
}

// newDocumentDispatchHandler builds the per-request pipeline /query,
// /query/proof AND /query/proof-write (CHAOS-7096) serve: method check, the
// Python edge's body-size contract, registered-document resolution,
// bearer/envelope verification, org context, and dispatch through the
// supplied Mux.
//
// It takes the Mux rather than the Switch so the routes cannot drift
// in anything EXCEPT reachability -- the property a measurement/write route
// exists to vary, and the only one it is allowed to.
//
// requireKind constrains which registered document KIND may execute here,
// by digest.DocumentKind's vocabulary ("" means no constraint -- /query's
// shape, unchanged since before CHAOS-7096). /query/proof passes
// digest.KindQuery: it exists to measure an operation without exposing it to
// real traffic, and a mutation cannot be measured that way -- running it
// would apply a real write, and a two-plane comparison would apply it
// twice. /query/proof-write passes digest.KindMutation, the exact inverse,
// for the opposite reason: it exists ONLY to apply one real, bounded write
// so CHAOS-6098's bootstrap circularity has a receipt to point at, and a
// query document has no business on that door. Checked after the request is
// authenticated and resolved to a registered operation, and before the Mux
// is reached, so no switch state can let the wrong kind through.
//
// orgAllowed, when non-nil, gates the AUTHENTICATED claims' OrgID before the
// Mux is reached (CHAOS-7096's proof-org allowlist). nil for /query and
// /query/proof -- both keep today's behavior byte for byte (their handlers are
// built with a nil check). It must fail CLOSED: an empty
// OrgID or a "cannot decide" answer from the caller is a refusal, never a
// fall-through, because a write door is the one place "the check could not
// run" must never read as "the check passed."
func newDocumentDispatchHandler(getenv getenvFunc, routeMux *routeswitch.Mux, operationByDigest map[string]string, verifier *principal.Verifier, edgeAuth *policy.Authenticator, edgeStore policy.Store, requireKind string, orgAllowed func(context.Context, string) bool, gates ...documentGate) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// /graphql (graphql_edge_route.go) runs this same pipeline. Its
		// differences are keyed on a context value only that route sets:
		// GET is a request for a registered query, the identity is checked
		// before the body is parsed, and the refusals the Python edge
		// answered itself keep its status and body.
		edge := isGraphQLEdge(r.Context())
		viaGET := edge && r.Method == http.MethodGet
		if r.Method != http.MethodPost && !viaGET {
			if edge {
				refuseGraphQLEdgeMethod(w)
				return
			}
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var bodyBytes []byte
		if !viaGET {
			// Same body-size contract the Python edge's
			// GraphQLQuerySizeLimitMiddleware enforces for /graphql
			// (security.py's GRAPHQL_MAX_QUERY_BYTES, default 16 KiB) --
			// codex review, 2026-08-28: reading up to 1 MiB unconditionally
			// let a body between the configured limit and 1 MiB through
			// silently, bypassing that existing request-size contract for
			// this canaried operation. LimitReader+1 lets a body of EXACTLY
			// the limit succeed while still detecting one byte over it,
			// without buffering the oversized remainder.
			limit := graphQLMaxQueryBytes(getenv)
			read, readErr := io.ReadAll(io.LimitReader(r.Body, int64(limit)+1))
			_ = r.Body.Close()
			if readErr != nil {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			if len(read) > limit {
				// /graphql's own size refusal ran before this, in
				// graphQLEdgeLimits, outside the security headers.
				http.Error(w, "GraphQL request body exceeds size limit", http.StatusRequestEntityTooLarge)
				return
			}
			bodyBytes = read
		}

		// /graphql checks the identity before it parses the body (the Python
		// app resolved its GraphQL context first); /query keeps its order,
		// below.
		var claims authctx.Claims
		if edge {
			authenticated, ok := authenticateGraphQLEdge(w, r)
			if !ok {
				return
			}
			claims = authenticated
		}

		var query string
		switch {
		case viaGET:
			text, body, refusal := graphQLEdgeGETDocument(r)
			if refusal != nil {
				refusal(w)
				return
			}
			bodyBytes, query = body, text
		case edge:
			text, refusal := graphQLEdgePOSTDocument(r, bodyBytes)
			if refusal != nil {
				refusal(w)
				return
			}
			query = text
		default:
			// The Python edge reads this body with json.loads, which refuses an
			// integer literal past 4300 digits with a ValueError -- an unhandled
			// exception, the generic 500 -- wherever the literal sits (the
			// variables included, which this decode into {query} would not see).
			var intLimit *pyjson.IntLimitError
			if _, decodeErr := pyjson.Decode(bodyBytes); errors.As(decodeErr, &intLimit) {
				policy.WriteDetail(w, http.StatusInternalServerError, "Internal Server Error", nil)
				return
			}

			var parsed struct {
				Query string `json:"query"`
			}
			if err := json.Unmarshal(bodyBytes, &parsed); err != nil {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			query = parsed.Query

			// Authenticate BEFORE the document lookup, so every carrier outcome
			// (ambiguous, none, malformed, invalid, accepted) is decided before the
			// unregistered-document 404 can answer (CHAOS-6757 r1: an ambiguous or
			// unauthenticated request for an unregistered document got 404).
			// Python parity: the edge's GraphQLRouter resolves get_context (401
			// "Authentication required") before it picks an operation. The body
			// refusals above stay where they were: the size 413 and the 4300-digit
			// 500 are documented above as Python-edge behaviour that precedes this
			// route; the malformed-JSON 400 keeps its old position and is NOT
			// verified against Python's order. (/graphql authenticated earlier,
			// before any parse.)
			authenticated, ok := authenticateInternalRequest(w, r, verifier, edgeAuth, edgeStore)
			if !ok {
				return
			}
			claims = authenticated
		}

		operation, ok := operationForDocument(query, operationByDigest)
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
				digestHex(query), truncateForLog(query, maxUnwrapChainLogBytes),
			)
			httpapi.RecordNotFoundCause(r.Context(), httpapi.NotFoundUnregisteredDocument)
			if edge {
				refuseGraphQLEdgeUnregistered(w)
				return
			}
			http.NotFound(w, r)
			return
		}

		if viaGET {
			// GraphQL forbids a mutation over GET, and the Python edge never
			// forwarded one (go_api_dispatcher.py "mutation_over_get"):
			// refused here, before any switch or resolver can run it.
			if kind, _ := digest.DocumentKind(query); kind != digest.KindQuery {
				log.Printf("query-api: /graphql refused a GET for a non-query document: operation=%s kind=%q", operation, kind)
				refuseMutationOverGET(w)
				return
			}
		}

		if requireKind != "" {
			kind, kindErr := digest.DocumentKind(query)
			if kind != requireKind {
				// DocumentKind answers "" for a document whose kind cannot be
				// stated, so that case is refused here too: only a document
				// proven to be the required kind reaches a kind-constrained route.
				log.Printf("query-api: route requiring kind=%q refused a document: operation=%s kind=%q err=%v", requireKind, operation, kind, kindErr)
				http.Error(w, fmt.Sprintf("this route serves %s documents only", requireKind), http.StatusMethodNotAllowed)
				return
			}
		}

		if orgAllowed != nil {
			// Fail closed on the empty case too -- see the doc comment above.
			// claims.OrgID is trusted here for the same reason the rest of this
			// pipeline trusts claims: authenticateInternalRequest already
			// verified them, above, before any of this ran.
			if claims.OrgID == "" || !orgAllowed(r.Context(), claims.OrgID) {
				log.Printf("query-api: proof-write route refused: operation=%s org_id=%q is not on the proof-org allowlist", operation, claims.OrgID)
				http.Error(w, "this org is not enabled for proof-write", http.StatusForbidden)
				return
			}
		}

		// The raw body rides along so a resolver can read a JSON variable in the
		// order the client wrote its keys (see graph.WithRequestBody).
		r = r.WithContext(graph.WithRequestBody(authctx.WithClaims(r.Context(), claims), bodyBytes))
		r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		if edge {
			// What the Python edge forwarded: a POST of this JSON body, with
			// Content-Type application/json whatever the client sent.
			r.Method = http.MethodPost
			r.ContentLength = int64(len(bodyBytes))
			r.Header = r.Header.Clone()
			r.Header.Set("Content-Type", "application/json")
		}

		// CHAOS-7831: the class-row gate runs in front of the document rows (class_row_gate.go). The /graphql edge is not gated (its callers are the
		// Python edge's, never acr's identity headers).
		if !edge {
			for _, gate := range gates {
				if gate(w, r, operation, query) {
					return
				}
			}
		}

		if edge {
			routeMux.DispatchOr(operation, w, r, graphQLEdgeNotEnabled(operation))
			return
		}
		routeMux.Dispatch(operation, w, r)
	}
}

// legacyDigestsByOperation lists, per operation, the digests of the registered texts it accepted BEFORE its
// current one (CHAOS-8000 dual accept). A request carrying a legacy text resolves to the same operation as one
// carrying the current text, so a web build still on the old text keeps working while the new text rolls out;
// the operation keeps ONE current document in digestByOperation. Each legacy text is a
// `registered<Operation>V<n>Document` const (a literal, so cmd/registrydump can read it) named once here and never in
// digestByOperation. The literal below is cmd/registrydump's second parse target: keep its exact shape
// (`"<operation>": {digestHex(<constIdent>), ...}`). Empty = every operation accepts one text.
var legacyDigestsByOperation = map[string][]string{
	"testopsRisk":           {digestHex(registeredTestopsRiskV1Document)},
	"dataHealthIdentity":    {digestHex(registeredDataHealthIdentityV1Document)},
	"aiGovernanceSummary":   {digestHex(registeredAiGovernanceSummaryV1Document)},
	"aiAttributionOverview": {digestHex(registeredAiAttributionOverviewV1Document)},
	"aiAttributedPrs":       {digestHex(registeredAiAttributedPrsV1Document)},
	"aiImpactSummary":       {digestHex(registeredAiImpactSummaryV1Document)},
	"aiOpportunities":       {digestHex(registeredAiOpportunitiesV1Document)},
	"aiWorkflowDrilldown":   {digestHex(registeredAiWorkflowDrilldownV1Document)},
	"capacityForecast":      {digestHex(registeredCapacityForecastV1Document), digestHex(registeredCapacityForecastV2Document)},
	"compoundingRisk":       {digestHex(registeredCompoundingRiskV1Document)},
	"coverageScopeBaseline": {digestHex(registeredCoverageScopeBaselineV1Document)},
	"hotspots":              {digestHex(registeredHotspotsV1Document)},
	"home":                  {digestHex(registeredHomeV1Document), digestHex(registeredHomeV2Document), digestHex(registeredHomeV3Document), digestHex(registeredHomeV4Document), digestHex(registeredHomeV5Document), digestHex(registeredHomeV6Document), digestHex(registeredHomeV7Document)},
	"improveOpportunities":  {digestHex(registeredImproveOpportunitiesV1Document), digestHex(registeredImproveOpportunitiesV2Document)},
	"operatingReview":       {digestHex(registeredOperatingReviewV1Document), digestHex(registeredOperatingReviewV2Document), digestHex(registeredOperatingReviewV3Document)},
	"reviewEdges":           {digestHex(registeredReviewEdgesV1Document)},
}

// buildOperationByDigest is the reverse index digest -> operation over every accepted text: each operation's
// current digest plus its legacy ones. A digest that maps to two operations is refused (the lookup would be
// ambiguous), and so is a legacy entry for an operation digestByOperation does not register.
func buildOperationByDigest(digestByOperation map[string]string, legacy map[string][]string) (map[string]string, error) {
	out := make(map[string]string, len(digestByOperation))
	for operation, digest := range digestByOperation {
		if other, dup := out[digest]; dup {
			return nil, fmt.Errorf("digest %s is registered for both %q and %q", digest, other, operation)
		}
		out[digest] = operation
	}
	operations := make([]string, 0, len(legacy))
	for operation := range legacy {
		operations = append(operations, operation)
	}
	sort.Strings(operations)
	for _, operation := range operations {
		if _, registered := digestByOperation[operation]; !registered {
			return nil, fmt.Errorf("legacy digests for %q, which is not a registered operation", operation)
		}
		for _, digest := range legacy[operation] {
			if other, dup := out[digest]; dup {
				return nil, fmt.Errorf("legacy digest %s of %q is already registered for %q", digest, operation, other)
			}
			out[digest] = operation
		}
	}
	return out, nil
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
func graphQLMaxQueryBytes(getenv getenvFunc) int {
	raw := getenv("GRAPHQL_MAX_QUERY_BYTES")
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

// envelopeRequestID reads the caller-supplied correlation id, the same
// header name (internal/auth/httpapi.RequestIDHeader) that package's own
// RequestID middleware uses -- query-api mounts no such middleware today,
// so this is deliberately just a header read, not a generated fallback:
// principal.Verify's CHAOS-5443 rejection log treats a missing id as an
// empty field rather than fabricating one that would look like a real
// correlation id but correlate nothing.
func envelopeRequestID(r *http.Request) string {
	return r.Header.Get("X-Request-Id")
}

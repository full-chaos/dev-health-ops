package server

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/internalidentity"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
)

// CHAOS-7508: acr calls ops with the throughputForecast, capacityForecast and capacityForecasts texts it pinned. Those are the current
// texts of main before this change, copied here byte for byte from main at 3898256426ac (not from the consts of this
// tree), so ops must keep accepting each of them after the new selections land.

const mainThroughputForecastText = `query ThroughputForecast($orgId: String!, $input: ThroughputForecastInput!) {
  throughputForecast(orgId: $orgId, input: $input) {
    forecastId
    computedAt
    teamId
    workScopeId
    backlogSize
    backlogState
    backlogAsOfDay
    degradedReason
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
      grainCount
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

const mainCapacityForecastsText = `query CapacityForecasts($orgId: String!, $filters: CapacityForecastFilterInput) {
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

const mainCapacityForecastText = `query CapacityForecast($orgId: String!, $input: CapacityForecastInput) {
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

func TestForecastDocumentsOfMainStayAcceptedByteForByte(t *testing.T) {
	texts := []struct{ operation, text, digest string }{
		{"throughputForecast", mainThroughputForecastText, "a265a9550e4cdab2bc77395257777ec366ac554b19c617af094de47c7a9e2b03"},
		{"capacityForecast", mainCapacityForecastText, "d95197012e32056fcece1f8975d953e7465bb112354cf4201e87b39375f933ef"},
		{"capacityForecasts", mainCapacityForecastsText, "43890adbbf75ac3ad71c29f728a82e35d035c3f2c701f0ced116559fa28b6c20"},
	}
	served, byDigest, post, ran := servedRegistryPost(t, "throughputForecast", "capacityForecast", "capacityForecasts")

	for _, c := range texts {
		if got := digestHex(c.text); got != c.digest {
			t.Fatalf("%s: the text of main digests to %s, want %s (main's go_api_operations.json value)", c.operation, got, c.digest)
		}
		if served[c.operation] == c.digest {
			t.Errorf("%s: the served current digest is still main's %s: the new selections did not land", c.operation, c.digest)
		}
		if got, ok := operationForDocument(c.text, byDigest); !ok || got != c.operation {
			t.Errorf("the %s text of main resolves to %q, %v; want %s, true", c.operation, got, ok, c.operation)
		}
		ran[c.operation] = 0
		if got := post(c.text); got.Code != http.StatusOK || ran[c.operation] != 1 {
			t.Errorf("POST the %s text of main: %d %s, handler ran %d; want 200 and one run", c.operation, got.Code, strings.TrimSpace(got.Body.String()), ran[c.operation])
		}
	}
}

// servedRegistryPost is the served registry (current digests plus legacyDigestsByOperation) behind the real /graphql
// edge chain, with a stub handler per named operation that counts its runs.
func servedRegistryPost(t *testing.T, operations ...string) (map[string]string, map[string]string, func(string) *httptest.ResponseRecorder, map[string]int) {
	t.Helper()
	served := servedRegistryDigests(t)
	byDigest, err := buildOperationByDigest(served, legacyDigestsByOperation)
	if err != nil {
		t.Fatal(err)
	}

	enabled := routeswitch.StaticSwitch{}
	mux := routeswitch.NewMux(enabled)
	ran := map[string]int{}
	for _, operation := range operations {
		op := operation
		enabled[op] = true
		mux.Register(op, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			ran[op]++
			w.WriteHeader(http.StatusOK)
		}))
	}
	store := &fakeEdgeStore{
		states:  map[uuid.UUID]policy.UserState{ecUser: {IsActive: true, TokenVersion: 5}},
		found:   map[uuid.UUID]bool{ecUser: true},
		members: map[[2]uuid.UUID]bool{{ecUser, ecOrg}: true},
	}
	verifier, _ := iaVerifier(t)
	auth := ecEdgeAuth(t, store)
	chain := onPattern("/graphql", graphQLEdgeChain(newDocumentDispatchHandler(os.Getenv, mux, byDigest, verifier, auth, store, "", nil),
		graphQLEdgeDeps{auth: auth, corsOrigins: []string{"https://app.example"}, maxBytes: defaultGraphQLMaxQueryBytes}))
	member := ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), tokenVersion: 5})
	post := func(text string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(`{"query": `+jsonQuote(t, text)+`}`))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", "Bearer "+member)
		recorder := httptest.NewRecorder()
		internalidentity.Public(http.HandlerFunc(chain.ServeHTTP)).ServeHTTP(recorder, request)
		return recorder
	}
	return served, byDigest, post, ran
}

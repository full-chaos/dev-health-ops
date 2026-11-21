package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/internalidentity"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
)

// The scoped breakdown texts before CHAOS-12999 (no evidenceQualityStats.sizedUnitCount) are retired: acr and web
// send only the current texts, so a request with an old text gets the standard 404 UNREGISTERED_DOCUMENT. The old
// texts are kept here as literals (they are no longer in query_route.go), each pinned to the digest it was served under.

const scopedBreakdownV1Body = `($orgId: String!, $batch: AnalyticsRequestInput!) {
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
      uncategorizedCount
      __typename
    }
    __typename
  }
}`

var retiredScopedBreakdownV1Texts = []struct{ operation, text, digest string }{
	{"investmentBreakdownTeam", "query InvestmentBreakdownTeam" + scopedBreakdownV1Body, "41128827a7bb7324a6fe3b4f771830ab39b236b237ab182406c30c5151aaf722"},
	{"investmentBreakdownRepository", "query InvestmentBreakdownRepository" + scopedBreakdownV1Body, "d896c57f2425e5eb54262cbb01fdd7d0bf6c17249b8c0d4c140d4d33b492a3e6"},
	{"teamWorkItemBreakdown", "query TeamWorkItemBreakdown" + scopedBreakdownV1Body, "875c9197af4bf566516a679569a59ed30a2763117d6d5c61ba9a362c94be2fd8"},
}

func servedRegistryDigests(t *testing.T) map[string]string {
	t.Helper()
	pgPool, err := pgxpool.New(context.Background(), "postgres://nobody:nobody@127.0.0.1:1/nonexistent")
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pgPool.Close)
	verifier, err := principal.NewVerifier(writeMountedLogTestJWKS(t), "dev-health-ops-edge", "query-api")
	if err != nil {
		t.Fatalf("principal.NewVerifier: %v", err)
	}
	_, _, _, registry, err := newQueryHandler(nopQueryClient{}, pgPool, verifier, "sha256:test-schema-digest", withIssuer)
	if err != nil {
		t.Fatalf("newQueryHandler: %v", err)
	}
	served := map[string]string{}
	for _, entry := range fetchRegistryPayload(t, registry).Operations {
		served[entry.Operation] = entry.DocumentDigest
	}
	return served
}

func TestScopedInvestmentBreakdownV1TextsArePinnedOldTexts(t *testing.T) {
	for _, c := range retiredScopedBreakdownV1Texts {
		if got := digestHex(c.text); got != c.digest {
			t.Errorf("%s: the retired text digests to %s, want %s", c.operation, got, c.digest)
		}
		if strings.Contains(c.text, "sizedUnitCount") {
			t.Errorf("%s: the retired text selects sizedUnitCount: it is not the text before CHAOS-12999", c.operation)
		}
	}
}

func TestLegacyDigestsByOperationNamesNoScopedInvestmentBreakdown(t *testing.T) {
	for _, c := range retiredScopedBreakdownV1Texts {
		if legacy, ok := legacyDigestsByOperation[c.operation]; ok {
			t.Errorf("legacyDigestsByOperation[%s] = %v, want no entry", c.operation, legacy)
		}
	}
}

func TestScopedInvestmentBreakdownV1TextsAreRefused(t *testing.T) {
	served := servedRegistryDigests(t)
	byDigest, err := buildOperationByDigest(served, legacyDigestsByOperation)
	if err != nil {
		t.Fatal(err)
	}
	current := map[string]string{}
	for _, c := range scopedInvestmentBreakdownDocuments {
		current[c.operation] = c.text
	}

	enabled := routeswitch.StaticSwitch{}
	mux := routeswitch.NewMux(enabled)
	ran := map[string]int{}
	for _, c := range retiredScopedBreakdownV1Texts {
		op := c.operation
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

	const unregistered = `{"errors":[{"message":"This GraphQL document is not registered.","extensions":{"code":"UNREGISTERED_DOCUMENT"}}],"data":null}`
	for _, c := range retiredScopedBreakdownV1Texts {
		text, ok := current[c.operation]
		if !ok {
			t.Fatalf("%s: no current text in scopedInvestmentBreakdownDocuments", c.operation)
		}
		if got, ok := operationForDocument(text, byDigest); !ok || got != c.operation {
			t.Errorf("the current %s text resolves to %q, %v; want %s, true", c.operation, got, ok, c.operation)
		}
		if got, ok := operationForDocument(c.text, byDigest); ok {
			t.Errorf("the retired %s text resolves to %q; want no operation", c.operation, got)
		}

		ran[c.operation] = 0
		if got := post(c.text); got.Code != http.StatusNotFound || strings.TrimSpace(got.Body.String()) != unregistered || ran[c.operation] != 0 {
			t.Errorf("POST the retired %s text: %d %s, handler ran %d; want 404 %s and no run", c.operation, got.Code, strings.TrimSpace(got.Body.String()), ran[c.operation], unregistered)
		}
		ran[c.operation] = 0
		if got := post(text); got.Code != http.StatusOK || ran[c.operation] != 1 {
			t.Errorf("POST the current %s text: %d %s, handler ran %d; want 200 and one run", c.operation, got.Code, strings.TrimSpace(got.Body.String()), ran[c.operation])
		}
	}
}

package server

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-go/pep"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
)

const servedOrgShadowDocument = `query Q { dataHealth(team: "t") { __typename } }`

var servedOrgB = uuid.MustParse("44444444-4444-4444-4444-444444444444")

// serveServedOrgShadow sends one /graphql request, token org ecOrg, X-Org-Id header, through the real edge chain (the
// org scope and the edge authentication) into the real GraphQL server with a shadow-mode gate. The one substitution:
// the shadow subject is a user access token, as in production (the test's edge token is hs256 and would be skipped).
// It returns the status and the organizations the shadow asked the decision point about.
func serveServedOrgShadow(t *testing.T, memberOfB bool, header string) (int, []string) {
	t.Helper()
	members := map[[2]uuid.UUID]bool{{ecUser, ecOrg}: true}
	if memberOfB {
		members[[2]uuid.UUID{ecUser, servedOrgB}] = true
	}
	store := &fakeEdgeStore{
		states:  map[uuid.UUID]policy.UserState{ecUser: {IsActive: true, TokenVersion: 5}},
		found:   map[uuid.UUID]bool{ecUser: true},
		members: members,
	}
	var mu sync.Mutex
	var asked []string
	gate := pdp.New(pdp.Config{Mode: pdp.ModeShadow, ExtraShadowRows: graphQLShadowRows(), Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Decider: funcDecider{func(_ context.Context, req pep.Request) pep.Decision {
			mu.Lock()
			asked = append(asked, req.OrganizationID)
			mu.Unlock()
			return pep.Decision{}
		}}})
	server := newGraphQLServerWithLimits(&graph.Resolver{}, 1000, 10)
	mux := routeswitch.NewMux(routeswitch.StaticSwitch{"gql": true})
	mux.Register("gql", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := graph.WithShadowSubject(pdp.WithGate(r.Context(), gate), graph.ShadowSubject{Bearer: "user-bearer", UserAccessToken: true})
		server.ServeHTTP(w, r.WithContext(ctx))
	}))
	verifier, _ := iaVerifier(t)
	auth := ecEdgeAuth(t, store)
	chain := graphQLEdgeChain(newDocumentDispatchHandler(os.Getenv, mux, map[string]string{digestHex(servedOrgShadowDocument): "gql"}, verifier, auth, store, "", nil),
		graphQLEdgeDeps{auth: auth, maxBytes: defaultGraphQLMaxQueryBytes})
	token := ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), tokenVersion: 5})
	request := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(`{"query": `+jsonQuote(t, servedOrgShadowDocument)+`}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("X-Org-Id", header)
	recorder := httptest.NewRecorder()
	onPattern("/graphql", withRouteProfile(chain)).ServeHTTP(recorder, request)
	gate.Wait()
	mu.Lock()
	defer mu.Unlock()
	return recorder.Code, append([]string(nil), asked...)
}

// Token org A, a member of B, X-Org-Id B: the shadow's input organization is B, the served one.
func TestGraphQLShadowServedOrgIsTheHeaderOrgOfAMember(t *testing.T) {
	code, asked := serveServedOrgShadow(t, true, servedOrgB.String())
	if code >= 500 || len(asked) != 1 || asked[0] != servedOrgB.String() {
		t.Fatalf("status %d, the shadow asked about %v, want one ask about %s (the served org, not the token org %s)", code, asked, servedOrgB, ecOrg)
	}
}

// Token org A, not a member of B, X-Org-Id B: the org scope refuses before the operation runs, so no shadow decision.
func TestGraphQLShadowNonMemberHeaderOrgIsRefusedBeforeTheShadow(t *testing.T) {
	code, asked := serveServedOrgShadow(t, false, servedOrgB.String())
	if code != http.StatusForbidden || len(asked) != 0 {
		t.Fatalf("status %d, the shadow asked about %v, want 403 and no ask", code, asked)
	}
}

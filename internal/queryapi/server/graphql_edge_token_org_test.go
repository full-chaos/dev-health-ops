package server

import (
	"bytes"
	"context"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
)

// countingEdgeStore is fakeEdgeStore counting the membership reads.
type countingEdgeStore struct {
	*fakeEdgeStore
	mu    sync.Mutex
	reads map[[2]uuid.UUID]int
}

func (c *countingEdgeStore) Membership(ctx context.Context, userID, orgID uuid.UUID) (string, bool, error) {
	c.mu.Lock()
	c.reads[[2]uuid.UUID{userID, orgID}]++
	c.mu.Unlock()
	return c.fakeEdgeStore.Membership(ctx, userID, orgID)
}

// On /graphql a token whose own org has no membership row keeps main's
// answer: 401, with no X-Org-Id and with an X-Org-Id naming that org, after one
// membership read of it per request. The routed check refuses it (reason
// edge_rejected), before the edge's own membership check; the same removed
// member gets 403 on go-api.
func TestGraphQLEdgeAnswersARemovedMembersTokenAsOnMain(t *testing.T) {
	removedOrg := uuid.MustParse("99999999-9999-9999-9999-999999999999")
	store := &countingEdgeStore{fakeEdgeStore: &fakeEdgeStore{
		states:  map[uuid.UUID]policy.UserState{ecUser: {IsActive: true, TokenVersion: 5}},
		found:   map[uuid.UUID]bool{ecUser: true},
		members: map[[2]uuid.UUID]bool{{ecUser, ecOrg}: true},
	}, reads: map[[2]uuid.UUID]int{}}
	verifier, _ := iaVerifier(t)
	mux := routeswitch.NewMux(routeswitch.StaticSwitch{"probe": true})
	ran := 0
	mux.Register("probe", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { ran++; w.WriteHeader(http.StatusOK) }))
	auth := ecEdgeAuth(t, store)
	handler := onPattern("/graphql", graphQLEdgeChain(newDocumentDispatchHandler(os.Getenv, mux, map[string]string{digestHex(iaDocument): "probe"}, verifier, auth, store, "", nil),
		graphQLEdgeDeps{auth: auth, corsOrigins: []string{"https://app.example"}, maxBytes: defaultGraphQLMaxQueryBytes})).ServeHTTP
	removed := ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: removedOrg.String(), tokenVersion: 5})
	var logged bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&logged)
	t.Cleanup(func() { log.SetOutput(previous) })
	for _, header := range []string{"", removedOrg.String()} {
		t.Run("header="+header, func(t *testing.T) {
			store.reads, ran = map[[2]uuid.UUID]int{}, 0
			logged.Reset()
			c := edgeCell{method: http.MethodPost, carrier: "nonmember", document: "query"}
			request := edgeRequest(t, c, "", removed, "")
			if header != "" {
				request.Header.Set("X-Org-Id", header)
			}
			recorder := serveEdge(handler, c, request)
			if recorder.Code != http.StatusUnauthorized || ran != 0 {
				t.Fatalf("%d %q ran=%d, want 401 and nothing run", recorder.Code, recorder.Body.String(), ran)
			}
			// The routed check refuses the token org without a membership row
			// (accessroute_user.go), before the carrier's own membership read.
			if !strings.Contains(logged.String(), "api user access token for an org the user is not a member of") ||
				!strings.Contains(logged.String(), "reason=edge_rejected carrier=edge") {
				t.Fatalf("logged %q, want the not-a-member refusal and reason=edge_rejected", logged.String())
			}
			if got := store.reads[[2]uuid.UUID{ecUser, removedOrg}]; got != 1 {
				t.Fatalf("membership reads of the token's org: %d, want 1", got)
			}
		})
	}
}

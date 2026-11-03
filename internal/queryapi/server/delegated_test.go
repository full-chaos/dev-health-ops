package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/digest"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
)

var ecActor = uuid.MustParse("77777777-7777-7777-7777-777777777777")

func ecMemberStore() *fakeEdgeStore {
	return &fakeEdgeStore{
		states:  map[uuid.UUID]policy.UserState{ecUser: {IsActive: true, TokenVersion: 5}},
		found:   map[uuid.UUID]bool{ecUser: true},
		members: map[[2]uuid.UUID]bool{{ecUser, ecOrg}: true},
	}
}

// TestEdgeCarrier_DelegatedTokenIsDelegated: a delegated session's bearer (the
// target's token naming the actor) reaches the resolvers as Delegated; the
// same member without the claim does not.
func TestEdgeCarrier_DelegatedTokenIsDelegated(t *testing.T) {
	store := ecMemberStore()
	auth := ecEdgeAuth(t, store)
	handler, seen := iaDispatchWithEdge(t, nil, auth, store)
	plain := ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), role: "admin", tokenVersion: 5})
	delegated := ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), role: "admin", tokenVersion: 5, impersonatingUserID: ecActor.String()})
	for _, token := range []string{plain, delegated} {
		if rec := ecPost(handler, token); rec.Code != http.StatusOK {
			t.Fatalf("got %d, want 200, body=%s", rec.Code, rec.Body.String())
		}
	}
	if len(*seen) != 2 || (*seen)[0].Delegated || !(*seen)[1].Delegated {
		t.Fatalf("claims = %+v, want the plain token not delegated and the delegated one Delegated", *seen)
	}
	if got := (*seen)[1]; got.OrgID != ecOrg.String() || got.Role != "admin" || got.ImpersonationActive {
		t.Fatalf("delegated claims = %+v, want the target's own org and role", got)
	}
}

const proofWriteMutation = "mutation Probe { probe }"

// TestProofWriteRefusesADelegatedCaller: /query/proof-write, the write door,
// refuses a delegated session's bearer before any resolver runs; the same
// caller without the claim is served.
func TestProofWriteRefusesADelegatedCaller(t *testing.T) {
	store := ecMemberStore()
	auth := ecEdgeAuth(t, store)
	ran := 0
	mux := routeswitch.NewMux(routeswitch.StaticSwitch{"probe": true})
	mux.Register("probe", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ran++
		w.WriteHeader(http.StatusOK)
	}))
	allow := func(context.Context, string) bool { return true }
	handler := newDocumentDispatchHandler(os.Getenv, mux, map[string]string{digestHex(proofWriteMutation): "probe"}, nil, auth, store, digest.KindMutation, allow)
	post := func(token string) *httptest.ResponseRecorder {
		body, _ := json.Marshal(map[string]string{"query": proofWriteMutation})
		req := httptest.NewRequest(http.MethodPost, "/query/proof-write", strings.NewReader(string(body)))
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		handler(rec, req)
		return rec
	}
	plain := ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), role: "admin", tokenVersion: 5})
	if rec := post(plain); rec.Code != http.StatusOK || ran != 1 {
		t.Fatalf("plain caller: %d %s, ran %d; want 200 and the resolver run", rec.Code, rec.Body.String(), ran)
	}
	delegated := ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), role: "admin", tokenVersion: 5, impersonatingUserID: ecActor.String()})
	if rec := post(delegated); rec.Code != http.StatusForbidden || ran != 1 {
		t.Fatalf("delegated caller: %d %s, ran %d; want 403 and no resolver", rec.Code, rec.Body.String(), ran)
	}
}

package server

import (
	"context"
	"encoding/json"
	"errors"
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
	plain := ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), tokenVersion: 5})
	delegated := ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), tokenVersion: 5, impersonatingUserID: ecActor.String()})
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
	allow := func(context.Context, string) (bool, error) { return true, nil }
	handler := onPattern("/query/proof-write", newDocumentDispatchHandler(os.Getenv, mux, map[string]string{digestHex(proofWriteMutation): "probe"}, nil, auth, store, digest.KindMutation, allow)).ServeHTTP
	post := func(token string) *httptest.ResponseRecorder {
		body, _ := json.Marshal(map[string]string{"query": proofWriteMutation})
		req := httptest.NewRequest(http.MethodPost, "/query/proof-write", strings.NewReader(string(body)))
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		handler(rec, req)
		return rec
	}
	plain := ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), tokenVersion: 5})
	if rec := post(plain); rec.Code != http.StatusOK || ran != 1 {
		t.Fatalf("plain caller: %d %s, ran %d; want 200 and the resolver run", rec.Code, rec.Body.String(), ran)
	}
	delegated := ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), tokenVersion: 5, impersonatingUserID: ecActor.String()})
	if rec := post(delegated); rec.Code != http.StatusForbidden || ran != 1 {
		t.Fatalf("delegated caller: %d %s, ran %d; want 403 and no resolver", rec.Code, rec.Body.String(), ran)
	}

	// The delegated refusal comes before the allowlist read: a read that fails does not turn it into a 503.
	failing := func(context.Context, string) (bool, error) { return false, errors.New("allowlist read failed") }
	handler = onPattern("/query/proof-write", newDocumentDispatchHandler(os.Getenv, mux, map[string]string{digestHex(proofWriteMutation): "probe"}, nil, auth, store, digest.KindMutation, failing)).ServeHTTP
	if rec := post(delegated); rec.Code != http.StatusForbidden || ran != 1 {
		t.Fatalf("delegated caller, allowlist read failing: %d %s, ran %d; want 403 and no resolver", rec.Code, rec.Body.String(), ran)
	}
}

// D6773 (product rule 1, CHAOS-13206): a delegated superuser (admin.orgs) whose token names an org it holds no
// membership row in gets the legacy (HS256) path's answer on every token class. The legacy answer, executed on
// origin/main ad73c1cc1 (TestDelegatedSuperuserOnANonMemberOrg_ReadServedWriteRefused): the REST read is served
// in the claimed org (200); the write door refuses (401) and runs no resolver.
func TestDelegatedSuperuserOnANonMemberOrg_ReadServedAsOnTheLegacyPath(t *testing.T) {
	quietStdLog(t)
	store := newAccessStore()
	store.states[ecUser] = policy.UserState{IsActive: true, IsSuperuser: true, TokenVersion: 5}
	store.members = nil
	m := newAccessMinter(t)
	f := newRESTFixture(t, store, testAccess(m.keys, nil, accessTestNow))
	if got := f.do(http.MethodGet, m.mint(ecUser, map[string]any{"act": map[string]any{"sub": wirePrincipal(ecActor)}}, nil)); got.Status != http.StatusOK || got.Body != `{"org":"`+ecOrg.String()+`"}` {
		t.Fatalf("REST read: got %+v, want 200 in the claimed org", got)
	}
}

func TestDelegatedSuperuserOnANonMemberOrg_WriteRefused(t *testing.T) {
	quietStdLog(t)
	delegated := ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), tokenVersion: 5, impersonatingUserID: ecActor.String()})
	wstore := &fakeEdgeStore{
		states: map[uuid.UUID]policy.UserState{ecUser: {IsActive: true, IsSuperuser: true, TokenVersion: 5}},
		found:  map[uuid.UUID]bool{ecUser: true},
	}
	ran := 0
	mux := routeswitch.NewMux(routeswitch.StaticSwitch{"probe": true})
	mux.Register("probe", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ran++
		w.WriteHeader(http.StatusOK)
	}))
	allow := func(context.Context, string) (bool, error) { return true, nil }
	handler := onPattern("/query/proof-write", newDocumentDispatchHandler(os.Getenv, mux, map[string]string{digestHex(proofWriteMutation): "probe"}, nil, ecEdgeAuth(t, wstore), wstore, digest.KindMutation, allow)).ServeHTTP
	body, _ := json.Marshal(map[string]string{"query": proofWriteMutation})
	req := httptest.NewRequest(http.MethodPost, "/query/proof-write", strings.NewReader(string(body)))
	req.Header.Set("Authorization", "Bearer "+delegated)
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnauthorized || ran != 0 {
		t.Fatalf("write door: %d %s, ran %d; want 401 and no resolver", rec.Code, rec.Body.String(), ran)
	}
}

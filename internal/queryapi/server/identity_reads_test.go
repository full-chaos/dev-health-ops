package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/digest"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
)

// One request, one read of each fact of the caller's identity: the verified
// token with its users row, each membership, the impersonation session. A
// fact read twice can change between the reads, and then two consumers of
// the same request (the org scope, the impersonation middleware, the
// pipeline, the headers they set, the resolvers' claims) act on two
// different answers. These cells count the store reads of one request, per
// fact, on /graphql and on /query.

// countingIdentityStore is policy.Store over base that counts every read per
// fact. userStates, when set, answers the users row in turn (the last one
// repeats), so the row can change between two reads of one request.
type countingIdentityStore struct {
	base       *fakeEdgeStore
	userStates []policy.UserState
	mu         sync.Mutex
	counts     map[string]int
}

func newCountingIdentityStore(base *fakeEdgeStore) *countingIdentityStore {
	return &countingIdentityStore{base: base, counts: map[string]int{}}
}

func (s *countingIdentityStore) count(fact string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.counts[fact]
	s.counts[fact]++
	return n
}

func (s *countingIdentityStore) UserState(ctx context.Context, id uuid.UUID) (policy.UserState, bool, error) {
	n := s.count("user " + id.String())
	if len(s.userStates) > 0 {
		return s.userStates[min(n, len(s.userStates)-1)], true, nil
	}
	return s.base.UserState(ctx, id)
}

func (s *countingIdentityStore) Membership(ctx context.Context, userID, orgID uuid.UUID) (string, bool, error) {
	s.count("member " + userID.String() + " " + orgID.String())
	return s.base.Membership(ctx, userID, orgID)
}

func (s *countingIdentityStore) ActiveImpersonation(ctx context.Context, adminID uuid.UUID) (*policy.Impersonation, error) {
	s.count("session " + adminID.String())
	return s.base.ActiveImpersonation(ctx, adminID)
}

// reads is every fact read, with its count, in a stable order.
func (s *countingIdentityStore) reads() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	facts := make([]string, 0, len(s.counts))
	for fact, n := range s.counts {
		facts = append(facts, fact+"="+strconv.Itoa(n))
	}
	sort.Strings(facts)
	return strings.Join(facts, "; ")
}

var (
	irOtherOrg  = uuid.MustParse("55555555-5555-5555-5555-555555555555")
	irTargetOrg = uuid.MustParse("44444444-4444-4444-4444-444444444444")
)

func irFacts(facts ...string) string {
	sort.Strings(facts)
	return strings.Join(facts, "; ")
}

func irUser() string                { return "user " + ecUser.String() + "=1" }
func irSession() string             { return "session " + ecUser.String() + "=1" }
func irMember(org uuid.UUID) string { return "member " + ecUser.String() + " " + org.String() + "=1" }
func irToken(t *testing.T, super bool) string {
	return ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), role: "owner", isSuperuser: super, tokenVersion: 5})
}

func TestGraphQLEdgeReadsEachIdentityFactOncePerRequest(t *testing.T) {
	session := &policy.Impersonation{AdminUserID: ecUser, TargetUserID: ecTarget, TargetOrgID: irTargetOrg, TargetRole: "viewer"}
	plain := policy.UserState{IsActive: true, TokenVersion: 5}
	super := policy.UserState{IsActive: true, IsSuperuser: true, TokenVersion: 5}
	for name, tc := range map[string]struct {
		state    policy.UserState
		sessions map[uuid.UUID]*policy.Impersonation
		super    bool
		orgID    string
		want     string
	}{
		"member":                             {plain, nil, false, "", irFacts(irUser(), irMember(ecOrg))},
		"member naming an org it belongs to": {plain, nil, false, irOtherOrg.String(), irFacts(irUser(), irMember(irOtherOrg))},
		"member naming its own org spelled another way": {plain, nil, false, strings.ReplaceAll(ecOrg.String(), "-", ""), irFacts(irUser(), irMember(ecOrg))},
		"superuser":                              {super, nil, true, "", irFacts(irUser(), irSession(), irMember(ecOrg))},
		"superuser naming another org":           {super, nil, true, irOtherOrg.String(), irFacts(irUser(), irSession(), irMember(irOtherOrg))},
		"impersonating superuser":                {super, map[uuid.UUID]*policy.Impersonation{ecUser: session}, true, "", irFacts(irUser(), irSession())},
		"superuser row, token without the claim": {super, map[uuid.UUID]*policy.Impersonation{ecUser: session}, false, "", irFacts(irUser(), irMember(ecOrg))},
	} {
		store := newCountingIdentityStore(&fakeEdgeStore{
			states:   map[uuid.UUID]policy.UserState{ecUser: tc.state},
			found:    map[uuid.UUID]bool{ecUser: true},
			members:  map[[2]uuid.UUID]bool{{ecUser, ecOrg}: true, {ecUser, irOtherOrg}: true},
			sessions: tc.sessions,
		})
		handler, seen := edgeClaimsHarness(t, store)
		recorder := irPost(t, handler, "/graphql", irToken(t, tc.super), tc.orgID)
		if recorder.Code != http.StatusOK || len(*seen) != 1 {
			t.Errorf("%s: %d ran=%v, want 200 and one run", name, recorder.Code, *seen)
			continue
		}
		if got := store.reads(); got != tc.want {
			t.Errorf("%s: reads %q, want %q", name, got, tc.want)
		}
	}
}

// TestGraphQLEdgeConsumersShareOneAnswerWhenTheRowChanges: the users row
// changes between reads (superuser, then not). The org scope accepted a
// foreign X-Org-Id because the caller was a superuser; the resolvers must run
// in that org with that same superuser answer, from the one read.
func TestGraphQLEdgeConsumersShareOneAnswerWhenTheRowChanges(t *testing.T) {
	store := newCountingIdentityStore(&fakeEdgeStore{
		found:   map[uuid.UUID]bool{ecUser: true},
		members: map[[2]uuid.UUID]bool{{ecUser, ecOrg}: true},
	})
	store.userStates = []policy.UserState{
		{IsActive: true, IsSuperuser: true, TokenVersion: 5},
		{IsActive: true, IsSuperuser: false, TokenVersion: 5},
	}
	handler, seen := edgeClaimsHarness(t, store)
	recorder := irPost(t, handler, "/graphql", irToken(t, true), irOtherOrg.String())
	if recorder.Code != http.StatusOK || len(*seen) != 1 {
		t.Fatalf("%d ran=%v, want 200 and one run", recorder.Code, *seen)
	}
	want := authctx.Claims{OrgID: irOtherOrg.String(), Role: "", IsSuperuser: true}
	if (*seen)[0] != want || !strings.Contains(store.reads(), irUser()) {
		t.Fatalf("resolvers ran as %+v after reads %q; want %+v from one read of the users row", (*seen)[0], store.reads(), want)
	}
}

// irPost sends the registered probe document as POST to path, with an edge
// bearer and, when orgID is set, X-Org-Id.
func irPost(t *testing.T, handler http.Handler, path, token, orgID string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"query": `+jsonQuote(t, iaDocument)+`}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+token)
	if orgID != "" {
		request.Header.Set("X-Org-Id", orgID)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

func TestQueryReadsEachIdentityFactOncePerRequest(t *testing.T) {
	session := &policy.Impersonation{AdminUserID: ecUser, TargetUserID: ecTarget, TargetOrgID: irTargetOrg, TargetRole: "viewer"}
	plain := policy.UserState{IsActive: true, TokenVersion: 5}
	super := policy.UserState{IsActive: true, IsSuperuser: true, TokenVersion: 5}
	verifier, priv := iaVerifier(t)
	for name, tc := range map[string]struct {
		state    policy.UserState
		sessions map[uuid.UUID]*policy.Impersonation
		carrier  func(*http.Request)
		want     string
	}{
		"edge token, member":                  {plain, nil, irBearer(irToken(t, false)), irFacts(irUser(), irMember(ecOrg))},
		"edge token, superuser":               {super, nil, irBearer(irToken(t, true)), irFacts(irUser(), irSession(), irMember(ecOrg))},
		"edge token, impersonating superuser": {super, map[uuid.UUID]*policy.Impersonation{ecUser: session}, irBearer(irToken(t, true)), irFacts(irUser(), irSession())},
		"envelope":                            {plain, nil, irBearer(iaEnvelope(t, priv, principal.Claims{OrgID: ecOrg.String(), Role: "admin"})), ""},
		"internal headers":                    {plain, nil, iaHeaders(ecOrg.String(), "admin", false, false), ""},
	} {
		store := newCountingIdentityStore(&fakeEdgeStore{
			states:   map[uuid.UUID]policy.UserState{ecUser: tc.state},
			found:    map[uuid.UUID]bool{ecUser: true},
			members:  map[[2]uuid.UUID]bool{{ecUser, ecOrg}: true},
			sessions: tc.sessions,
		})
		handler, seen := iaDispatchWithEdge(t, verifier, ecEdgeAuth(t, store), store)
		if code := iaPost(t, handler, tc.carrier); code != http.StatusOK || len(*seen) != 1 {
			t.Errorf("%s: %d ran=%v, want 200 and one run", name, code, *seen)
			continue
		}
		if got := store.reads(); got != tc.want {
			t.Errorf("%s: reads %q, want %q", name, got, tc.want)
		}
	}
}

func irBearer(token string) func(*http.Request) {
	return func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+token) }
}

// TestQueryProofWriteReadsTheOrgAllowlistOncePerRequest: the proof-write
// route's org allowlist is a per-request fact of the caller's org, read once.
func TestQueryProofWriteReadsTheOrgAllowlistOncePerRequest(t *testing.T) {
	verifier, priv := iaVerifier(t)
	ran, asked := 0, 0
	mux := routeswitch.NewMux(routeswitch.StaticSwitch{"probeWrite": true})
	mux.Register("probeWrite", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { ran++ }))
	allowed := func(context.Context, string) bool { asked++; return true }
	handler := newDocumentDispatchHandler(os.Getenv, mux, map[string]string{digestHex(edgeWriteDocument): "probeWrite"}, verifier, nil, nil, digest.KindMutation, allowed)
	request := httptest.NewRequest(http.MethodPost, "/query/proof-write", strings.NewReader(`{"query": `+jsonQuote(t, edgeWriteDocument)+`}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+iaEnvelope(t, priv, principal.Claims{OrgID: ecOrg.String(), Role: "admin"}))
	recorder := httptest.NewRecorder()
	handler(recorder, request)
	if recorder.Code != http.StatusOK || ran != 1 || asked != 1 {
		t.Fatalf("%d ran=%d allowlist asked %d time(s), want 200, one run, one ask", recorder.Code, ran, asked)
	}
}

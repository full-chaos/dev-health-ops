package server

import (
	"context"
	"errors"
	"log"
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
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
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
// failMemberAt, when set, fails that membership read (1 = the first).
type countingIdentityStore struct {
	base         *fakeEdgeStore
	userStates   []policy.UserState
	failMemberAt int
	mu           sync.Mutex
	counts       map[string]int
	memberReads  int
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
	s.mu.Lock()
	s.memberReads++
	fail := s.failMemberAt != 0 && s.memberReads == s.failMemberAt
	s.mu.Unlock()
	if fail {
		return "", false, errors.New("membership read failed")
	}
	return s.base.Membership(ctx, userID, orgID)
}

func (s *countingIdentityStore) OrgMembership(ctx context.Context, user, org uuid.UUID) (string, bool, bool, error) {
	role, member, err := s.Membership(ctx, user, org)
	return role, member, true, err
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
func irMember(org uuid.UUID) string { return irMemberReads(org, 1) }

// irMemberReads is n reads of one membership row. /query has no ReadOnce: the
// routed check and the carrier each read the token org's row.
func irMemberReads(org uuid.UUID, n int) string {
	return "member " + ecUser.String() + " " + org.String() + "=" + strconv.Itoa(n)
}
func irToken(t *testing.T, super bool) string {
	return ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), tokenVersion: 5})
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
		"member naming an org it belongs to": {plain, nil, false, irOtherOrg.String(), irFacts(irUser(), irMember(ecOrg), irMember(irOtherOrg))},
		"member naming its own org spelled another way": {plain, nil, false, strings.ReplaceAll(ecOrg.String(), "-", ""), irFacts(irUser(), irMember(ecOrg))},
		"superuser":                              {super, nil, true, "", irFacts(irUser(), irSession(), irMember(ecOrg))},
		"superuser naming another org":           {super, nil, true, irOtherOrg.String(), irFacts(irUser(), irSession(), irMember(ecOrg), irMember(irOtherOrg))},
		"impersonating superuser":                {super, map[uuid.UUID]*policy.Impersonation{ecUser: session}, true, "", irFacts(irUser(), irSession(), irMember(ecOrg))},
		"superuser row, token without the claim": {super, map[uuid.UUID]*policy.Impersonation{ecUser: session}, false, "", irFacts(irUser(), irSession(), irMember(ecOrg))},
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
	want := authctx.Claims{OrgID: irOtherOrg.String(), Role: "", IsSuperuser: true, Scope: pepgate.Legacy(irOtherOrg.String())}
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
	verifier, _ := iaVerifier(t)
	for name, tc := range map[string]struct {
		state    policy.UserState
		sessions map[uuid.UUID]*policy.Impersonation
		carrier  func(*http.Request)
		want     string
	}{
		"edge token, impersonating superuser": {super, map[uuid.UUID]*policy.Impersonation{ecUser: session}, irBearer(irToken(t, true)), irFacts(irUser(), irSession(), irMember(ecOrg))},
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

// TestQueryReadsTheTokenOrgMembershipTwiceAndBothFailClosed: /query has no
// ReadOnce, so an edge token's own org membership row is read twice per
// request, once by the routed check (accessroute_user.go) and once by the
// carrier (checkEdgeCarrier); either read failing refuses the request with
// go-api's 503 (nothing was decided about the credential).
func TestQueryReadsTheTokenOrgMembershipTwiceAndBothFailClosed(t *testing.T) {
	plain := policy.UserState{IsActive: true, TokenVersion: 5}
	super := policy.UserState{IsActive: true, IsSuperuser: true, TokenVersion: 5}
	verifier, _ := iaVerifier(t)
	var logged strings.Builder
	previous := log.Writer()
	log.SetOutput(&logged)
	t.Cleanup(func() { log.SetOutput(previous) })
	for name, tc := range map[string]struct {
		state policy.UserState
		want  string
	}{
		"edge token, member":    {plain, irFacts(irUser(), irMemberReads(ecOrg, 2))},
		"edge token, superuser": {super, irFacts(irUser(), irSession(), irMemberReads(ecOrg, 2))},
	} {
		for failAt := 0; failAt <= 2; failAt++ {
			logged.Reset()
			store := newCountingIdentityStore(&fakeEdgeStore{
				states:  map[uuid.UUID]policy.UserState{ecUser: tc.state},
				found:   map[uuid.UUID]bool{ecUser: true},
				members: map[[2]uuid.UUID]bool{{ecUser, ecOrg}: true},
			})
			store.failMemberAt = failAt
			handler, seen := iaDispatchWithEdge(t, verifier, ecEdgeAuth(t, store), store)
			code := iaPost(t, handler, irBearer(irToken(t, false)))
			if failAt == 0 {
				if code != http.StatusOK || len(*seen) != 1 {
					t.Errorf("%s: %d ran=%v, want 200 and one run", name, code, *seen)
				} else if got := store.reads(); got != tc.want {
					t.Errorf("%s: reads %q, want %q", name, got, tc.want)
				}
				continue
			}
			reason := map[int]string{1: "reason=edge_store_failed", 2: "reason=edge_membership_lookup_failed"}[failAt]
			if code != http.StatusServiceUnavailable || len(*seen) != 0 || !strings.Contains(logged.String(), reason) {
				t.Errorf("%s: membership read %d failing: %d ran=%v logged %q, want 503, nothing run and %s", name, failAt, code, *seen, logged.String(), reason)
			}
		}
	}
}

func irBearer(token string) func(*http.Request) {
	return func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+token) }
}

// TestQueryProofWriteReadsTheOrgAllowlistOncePerRequest: the proof-write
// route's org allowlist is a per-request fact of the caller's org, read once.
func TestQueryProofWriteReadsTheOrgAllowlistOncePerRequest(t *testing.T) {
	verifier, _ := iaVerifier(t)
	ran, asked := 0, 0
	mux := routeswitch.NewMux(routeswitch.StaticSwitch{"probeWrite": true})
	mux.Register("probeWrite", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { ran++ }))
	allowed := func(context.Context, string) bool { asked++; return true }
	handler := newDocumentDispatchHandler(os.Getenv, mux, map[string]string{digestHex(edgeWriteDocument): "probeWrite"}, verifier, nil, nil, digest.KindMutation, allowed)
	request := httptest.NewRequest(http.MethodPost, "/query/proof-write", strings.NewReader(`{"query": `+jsonQuote(t, edgeWriteDocument)+`}`))
	request.Header.Set("Content-Type", "application/json")
	iaHeaders(ecOrg.String(), "admin", false, false)(request)
	recorder := httptest.NewRecorder()
	handler(recorder, request)
	if recorder.Code != http.StatusOK || ran != 1 || asked != 1 {
		t.Fatalf("%d ran=%d allowlist asked %d time(s), want 200, one run, one ask", recorder.Code, ran, asked)
	}
}

func (*countingIdentityStore) ReadsPlatformRoles() bool { return false }

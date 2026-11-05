package server

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// The legacy impersonation caller: the admin's own bearer while go-api holds a
// live impersonation row for the admin. With AUTH_PROXY_IMPERSONATION on, the
// impersonation is a delegated session whose own bearer is the target, so
// query-api refuses the legacy caller (policy.Scope.RefuseLegacyImpersonation)
// and serves the delegated bearer as the target. Off, every answer is main's.

const legacyRefusalBody = `{"detail":"Not allowed in an impersonation session"}`

// legacyAnswer is one answer with every header, so two answers compare byte-equal.
type legacyAnswer struct {
	Status int
	Body   string
	Header http.Header
}

func (a legacyAnswer) equal(b legacyAnswer) bool {
	return a.Status == b.Status && a.Body == b.Body && reflect.DeepEqual(a.Header, b.Header)
}

// legacyStore: ecUser is the admin (a live superuser, a member of ecOrg) with
// session as its live go-api row (nil: none) and sessionErr as the row read's
// failure; ecActor is a second admin whose row names ecUser as the target,
// for the delegated bearer cells.
func legacyStore(session *policy.Impersonation, sessionErr error) *accessStore {
	store := newAccessStore()
	store.states[ecUser] = policy.UserState{IsActive: true, IsSuperuser: true, TokenVersion: 5}
	store.sessions = map[uuid.UUID]*policy.Impersonation{}
	if session != nil {
		store.sessions[ecUser] = session
	}
	store.errSession = sessionErr
	return store
}

// delegatedStore: ecUser is the target (a plain member of ecOrg); ecActor, a
// live superuser, holds a live row naming ecUser.
func delegatedStore() *accessStore {
	store := newAccessStore()
	store.states[ecActor] = policy.UserState{IsActive: true, IsSuperuser: true, TokenVersion: 5}
	store.found[ecActor] = true
	store.sessions = map[uuid.UUID]*policy.Impersonation{
		ecActor: {AdminUserID: ecActor, TargetUserID: ecUser, TargetOrgID: ecOrg, TargetRole: "admin"},
	}
	return store
}

func legacySession() *policy.Impersonation {
	return &policy.Impersonation{AdminUserID: ecUser, TargetUserID: ecTarget, TargetOrgID: irTargetOrg, TargetRole: "viewer"}
}

// legacyTokens is the admin's own bearer in each signing: the HS256 edge
// token and the EdDSA user access token.
func legacyTokens(t *testing.T, minter *accessMinter) map[string]string {
	t.Helper()
	return map[string]string{
		"HS256": ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), role: "admin", isSuperuser: true, tokenVersion: 5}),
		"EdDSA": minter.mint(ecUser, nil, nil),
	}
}

// delegatedTokens is the target's delegated bearer (ecUser, actor ecActor) in
// each signing.
func delegatedTokens(t *testing.T, minter *accessMinter) map[string]string {
	t.Helper()
	return map[string]string{
		"HS256": ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), role: "admin", tokenVersion: 5, impersonatingUserID: ecActor.String()}),
		"EdDSA": minter.mint(ecUser, map[string]any{"act": map[string]any{"sub": wirePrincipal(ecActor)}}, nil),
	}
}

// legacyStack is the real public listener over store, with the access-token
// issuer set (so both signings are carriers) and the refusal set to refuse.
func legacyStack(t *testing.T, minter *accessMinter, store *accessStore, refuse bool) *behaviourStack {
	t.Helper()
	return newAccessStackRefusing(t, newQueryAPIUserStoreFrom(store), testAccess(minter.keys, nil, accessTestNow), true, refuse)
}

// legacyGraphQL sends one /graphql request (POST body or GET query string) on
// the public listener.
func legacyGraphQL(s *behaviourStack, method, bearer string) legacyAnswer {
	var request *http.Request
	if method == http.MethodGet {
		request = httptest.NewRequest(method, "/graphql?query="+url.QueryEscape(iaDocument), nil)
	} else {
		request = httptest.NewRequest(method, "/graphql", strings.NewReader(`{"query":`+jsonQuoteString(iaDocument)+`}`))
		request.Header.Set("Content-Type", "application/json")
	}
	request.Header.Set("Authorization", "Bearer "+bearer)
	recorder := httptest.NewRecorder()
	s.handlers["query"].ServeHTTP(recorder, request)
	header := recorder.Header().Clone()
	header.Del("X-Request-ID")
	return legacyAnswer{recorder.Code, strings.TrimSpace(recorder.Body.String()), header}
}

var legacyMethods = []string{http.MethodPost, http.MethodGet}

// TestGraphQLRefusesTheLegacyImpersonationCaller: with the switch on, the
// admin's own bearer beside a live row is refused on /graphql with go-api's
// 403, no impersonation header and no resolver run; unset, the same request is
// served as the target (the repro).
func TestGraphQLRefusesTheLegacyImpersonationCaller(t *testing.T) {
	quietStdLog(t)
	minter := newAccessMinter(t)
	for signing, token := range legacyTokens(t, minter) {
		for _, method := range legacyMethods {
			t.Run(signing+" "+method, func(t *testing.T) {
				unset := legacyStack(t, minter, legacyStore(legacySession(), nil), false)
				was := legacyGraphQL(unset, method, token)
				if was.Status != http.StatusOK || was.Header.Get("X-Impersonated-User-Id") != ecTarget.String() ||
					len(*unset.seen) != 1 || (*unset.seen)[0].OrgID != irTargetOrg.String() || !(*unset.seen)[0].ImpersonationActive {
					t.Fatalf("switch unset: got %d %v claims=%+v, want the target served (main's behavior)", was.Status, was.Header, *unset.seen)
				}

				on := legacyStack(t, minter, legacyStore(legacySession(), nil), true)
				got := legacyGraphQL(on, method, token)
				if got.Status != http.StatusForbidden || got.Body != legacyRefusalBody {
					t.Fatalf("switch on: got %d %s, want 403 %s", got.Status, got.Body, legacyRefusalBody)
				}
				if got.Header.Get("X-Impersonating") != "" || got.Header.Get("X-Impersonated-User-Id") != "" {
					t.Fatalf("switch on: impersonation headers %v on a refusal", got.Header)
				}
				if len(*on.seen) != 0 {
					t.Fatalf("switch on: resolver ran %d times, want 0", len(*on.seen))
				}
			})
		}
	}
}

// TestGraphQLServesTheDelegatedBearerWithTheSwitchOn: the delegated bearer is
// the target, with or without the actor's live row, in both signings.
func TestGraphQLServesTheDelegatedBearerWithTheSwitchOn(t *testing.T) {
	quietStdLog(t)
	minter := newAccessMinter(t)
	for signing, token := range delegatedTokens(t, minter) {
		for _, method := range legacyMethods {
			t.Run(signing+" "+method, func(t *testing.T) {
				on := legacyStack(t, minter, delegatedStore(), true)
				got := legacyGraphQL(on, method, token)
				if got.Status != http.StatusOK || len(*on.seen) != 1 {
					t.Fatalf("got %d %s, resolver ran %d; want 200 and one run", got.Status, got.Body, len(*on.seen))
				}
				claims := (*on.seen)[0]
				if !claims.Delegated || claims.ImpersonationActive || claims.OrgID != ecOrg.String() || claims.IsSuperuser {
					t.Fatalf("claims = %+v, want the target (delegated, its own org, no superuser, no legacy impersonation)", claims)
				}
				unset := legacyGraphQL(legacyStack(t, minter, delegatedStore(), false), method, token)
				if !got.equal(unset) {
					t.Fatalf("switch on %+v differs from unset %+v", got, unset)
				}
			})
		}
	}
}

// TestGraphQLLegacyRefusalLeavesEveryOtherAnswer: a superuser with no live row,
// a plain member, and a failed row read answer byte-equal with the switch on
// and unset (the read failure is main's 500 in both).
func TestGraphQLLegacyRefusalLeavesEveryOtherAnswer(t *testing.T) {
	quietStdLog(t)
	minter := newAccessMinter(t)
	member := map[string]string{
		"HS256": ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), role: "admin", tokenVersion: 5}),
		"EdDSA": minter.mint(ecUser, nil, nil),
	}
	cases := map[string]struct {
		store  func() *accessStore
		tokens map[string]string
		status int
	}{
		"superuser, no live row": {func() *accessStore { return legacyStore(nil, nil) }, legacyTokens(t, minter), http.StatusOK},
		"plain member":           {newAccessStore, member, http.StatusOK},
		"row read fails":         {func() *accessStore { return legacyStore(nil, errors.New("session store timeout")) }, legacyTokens(t, minter), http.StatusInternalServerError},
	}
	for name, tc := range cases {
		for signing, token := range tc.tokens {
			for _, method := range legacyMethods {
				t.Run(name+" "+signing+" "+method, func(t *testing.T) {
					on := legacyGraphQL(legacyStack(t, minter, tc.store(), true), method, token)
					unset := legacyGraphQL(legacyStack(t, minter, tc.store(), false), method, token)
					if on.Status != tc.status || !on.equal(unset) {
						t.Fatalf("switch on %+v, unset %+v; want both %d and byte-equal", on, unset, tc.status)
					}
				})
			}
		}
	}
}

// TestRefusesLegacyImpersonationReadsTheSwitchAsGoAPI: the one parse of
// AUTH_PROXY_IMPERSONATION, and the /graphql dependencies Build mounts carry it.
func TestRefusesLegacyImpersonationReadsTheSwitchAsGoAPI(t *testing.T) {
	for raw, want := range map[string]struct {
		on  bool
		err bool
	}{
		"": {false, false}, "  ": {false, false}, "false": {false, false}, "0": {false, false},
		"true": {true, false}, " TRUE ": {true, false}, "1": {true, false},
		"yes": {false, true}, "on": {false, true},
	} {
		getenv := getenvFunc(func(name string) string {
			if name == impersonationProxyEnv {
				return raw
			}
			return ""
		})
		lookup := func(name string) (string, bool) { value := getenv(name); return value, value != "" }
		on, err := refusesLegacyImpersonation(getenv)
		if on != want.on || (err != nil) != want.err {
			t.Errorf("%s=%q: got %v, %v; want %v, error %v", impersonationProxyEnv, raw, on, err, want.on, want.err)
		}
		deps, depsErr := newGraphQLEdgeDeps(getenv, lookup, nil)
		if deps.refuseLegacyImpersonation != want.on || (depsErr != nil) != want.err {
			t.Errorf("%s=%q: /graphql deps refuse=%v error=%v; want %v, error %v", impersonationProxyEnv, raw, deps.refuseLegacyImpersonation, depsErr, want.on, want.err)
		}
	}
}

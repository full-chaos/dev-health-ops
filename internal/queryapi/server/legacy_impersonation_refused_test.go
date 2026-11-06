package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
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
// and unset (the read failure is the policy scope's 503 in both).
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
		"row read fails":         {func() *accessStore { return legacyStore(nil, errors.New("session store timeout")) }, legacyTokens(t, minter), http.StatusServiceUnavailable},
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
		deps, depsErr := newGraphQLEdgeDeps(getenv, lookup, nil, nil)
		if deps.refuseLegacyImpersonation != want.on || (depsErr != nil) != want.err {
			t.Errorf("%s=%q: /graphql deps refuse=%v error=%v; want %v, error %v", impersonationProxyEnv, raw, deps.refuseLegacyImpersonation, depsErr, want.on, want.err)
		}
	}
}

// legacyMutationDocument is a registered mutation on the access stack (the same resolver as iaDocument).
const legacyMutationDocument = "mutation LegacyImpersonationWrite { probe }"

// The same two kinds behind a leading comment: the operation type is the AST's, not the text's first word.
const (
	legacyNotedQueryDocument    = "# reads\nquery LegacyImpersonationNotedRead { probe }"
	legacyNotedMutationDocument = "# writes\nmutation LegacyImpersonationNotedWrite { probe }"
)

// revokedHolderStore is legacyStore for an admin whose admin.impersonate was revoked (it still holds admin.orgs)
// while its go-api row may stay live: the policy scope applies no row to it.
func revokedHolderStore(session *policy.Impersonation, sessionErr error) *accessStore {
	store := legacyStore(session, sessionErr)
	platform := rule.PlatformOf(rule.ActionAdminOrgs)
	state := store.states[ecUser]
	state.Platform = &platform
	store.states[ecUser] = state
	return store
}

// legacyGraphQLDocument is legacyGraphQL with the document named (its POST body JSON-encoded).
func legacyGraphQLDocument(s *behaviourStack, method, bearer, document string) legacyAnswer {
	var request *http.Request
	if method == http.MethodGet {
		request = httptest.NewRequest(method, "/graphql?query="+url.QueryEscape(document), nil)
	} else {
		body, _ := json.Marshal(map[string]string{"query": document})
		request = httptest.NewRequest(method, "/graphql", bytes.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
	}
	request.Header.Set("Authorization", "Bearer "+bearer)
	recorder := httptest.NewRecorder()
	s.handlers["query"].ServeHTTP(recorder, request)
	header := recorder.Header().Clone()
	header.Del("X-Request-ID")
	return legacyAnswer{recorder.Code, strings.TrimSpace(recorder.Body.String()), header}
}

// servedAsItself fails unless the resolver ran once for the admin in its own org, with no impersonation.
func servedAsItself(t *testing.T, label string, got legacyAnswer, s *behaviourStack) {
	t.Helper()
	if got.Status != http.StatusOK || len(*s.seen) != 1 || (*s.seen)[0].OrgID != ecOrg.String() || (*s.seen)[0].ImpersonationActive ||
		got.Header.Get("X-Impersonated-User-Id") != "" {
		t.Fatalf("%s: got %d %s %v claims=%+v, want 200 served as the admin itself", label, got.Status, got.Body, got.Header, *s.seen)
	}
}

// TestGraphQLRevokedHolderIsRefusedByOperationType: with the switch on, a caller that no longer holds
// admin.impersonate beside a live row is refused on a mutation document (the read limit) and served as itself on a
// query document. A POST goes by the document's operation type (its AST), not its method; a GET reads by its method,
// as before (a mutation over GET is the dispatch's 400). A query whose row read fails answers as the GET of that query.
func TestGraphQLRevokedHolderIsRefusedByOperationType(t *testing.T) {
	quietStdLog(t)
	minter := newAccessMinter(t)
	readFails := errors.New("session store timeout")
	for signing, token := range legacyTokens(t, minter) {
		for _, method := range legacyMethods {
			t.Run(signing+" "+method+" query, live row", func(t *testing.T) {
				on := legacyStack(t, minter, revokedHolderStore(legacySession(), nil), true)
				got := legacyGraphQLDocument(on, method, token, iaDocument)
				servedAsItself(t, "switch on", got, on)
				unset := legacyStack(t, minter, revokedHolderStore(legacySession(), nil), false)
				if was := legacyGraphQLDocument(unset, method, token, iaDocument); !got.equal(was) {
					t.Fatalf("switch on %+v differs from unset %+v", got, was)
				}
			})
			t.Run(signing+" "+method+" query, row read fails", func(t *testing.T) {
				on := legacyStack(t, minter, revokedHolderStore(nil, readFails), true)
				got := legacyGraphQLDocument(on, method, token, iaDocument)
				servedAsItself(t, "switch on", got, on)
				get := legacyStack(t, minter, revokedHolderStore(nil, readFails), true)
				if viaGET := legacyGraphQLDocument(get, http.MethodGet, token, iaDocument); got.Status != viaGET.Status || got.Body != viaGET.Body {
					t.Fatalf("%s answer %d %s differs from the GET answer %d %s", method, got.Status, got.Body, viaGET.Status, viaGET.Body)
				}
			})
			t.Run(signing+" "+method+" mutation, live row", func(t *testing.T) {
				on := legacyStack(t, minter, revokedHolderStore(legacySession(), nil), true)
				got := legacyGraphQLDocument(on, method, token, legacyMutationDocument)
				if method == http.MethodGet {
					// GET reads by its method, as before: the dispatch refuses the mutation over GET, byte-equal to unset.
					unset := legacyStack(t, minter, revokedHolderStore(legacySession(), nil), false)
					if was := legacyGraphQLDocument(unset, method, token, legacyMutationDocument); got.Status != http.StatusBadRequest || !got.equal(was) || len(*on.seen) != 0 {
						t.Fatalf("switch on %+v, unset %+v; want both the 400 mutation-over-GET refusal, byte-equal, no run", got, was)
					}
					return
				}
				if got.Status != http.StatusForbidden || got.Body != legacyRefusalBody || len(*on.seen) != 0 {
					t.Fatalf("switch on: got %d %s, resolver ran %d; want 403 %s and no run", got.Status, got.Body, len(*on.seen), legacyRefusalBody)
				}
				if got.Header.Get("X-Impersonating") != "" || got.Header.Get("X-Impersonated-User-Id") != "" {
					t.Fatalf("switch on: impersonation headers %v on a refusal", got.Header)
				}
			})
		}
		t.Run(signing+" POST, a comment before the operation", func(t *testing.T) {
			read := legacyStack(t, minter, revokedHolderStore(legacySession(), nil), true)
			servedAsItself(t, "commented query", legacyGraphQLDocument(read, http.MethodPost, token, legacyNotedQueryDocument), read)
			write := legacyStack(t, minter, revokedHolderStore(legacySession(), nil), true)
			if got := legacyGraphQLDocument(write, http.MethodPost, token, legacyNotedMutationDocument); got.Status != http.StatusForbidden || len(*write.seen) != 0 {
				t.Fatalf("commented mutation: got %d %s, resolver ran %d; want 403 and no run", got.Status, got.Body, len(*write.seen))
			}
		})
		t.Run(signing+" POST mutation, unset or no live row: served as itself", func(t *testing.T) {
			for name, s := range map[string]*behaviourStack{
				"switch unset":      legacyStack(t, minter, revokedHolderStore(legacySession(), nil), false),
				"switch on, no row": legacyStack(t, minter, revokedHolderStore(nil, nil), true),
			} {
				servedAsItself(t, name, legacyGraphQLDocument(s, http.MethodPost, token, legacyMutationDocument), s)
			}
		})
		t.Run(signing+" POST mutation, row read fails", func(t *testing.T) {
			on := legacyStack(t, minter, revokedHolderStore(nil, readFails), true)
			if got := legacyGraphQLDocument(on, http.MethodPost, token, legacyMutationDocument); got.Status != http.StatusServiceUnavailable || len(*on.seen) != 0 {
				t.Fatalf("switch on: got %d %s, resolver ran %d; want 503 and no run", got.Status, got.Body, len(*on.seen))
			}
		})
	}
}

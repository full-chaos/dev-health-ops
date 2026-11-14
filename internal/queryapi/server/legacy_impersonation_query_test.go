package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/digest"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
)

// /query and /query/proof-write read the go-api row themselves (liveScope).
// With AUTH_PROXY_IMPERSONATION on they refuse the legacy caller with the
// same 403 as /graphql (policy.WriteImpersonationRefused); every other /query
// refusal keeps its bare 401. REST rows never read the row: the admin's own
// bearer acts as the admin there, switch or not.

// legacyRaw is one answer as written: status, body and Content-Type.
type legacyRaw struct {
	Status      int
	Body        string
	ContentType string
}

func legacyQuery(s *behaviourStack, listener, path, bearer string) (legacyAnswer, legacyRaw) {
	request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"query":`+jsonQuoteString(iaDocument)+`}`))
	request.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		request.Header.Set("Authorization", "Bearer "+bearer)
	}
	recorder := httptest.NewRecorder()
	s.handlers[listener].ServeHTTP(recorder, request)
	header := recorder.Header().Clone()
	header.Del("X-Request-ID")
	return legacyAnswer{recorder.Code, strings.TrimSpace(recorder.Body.String()), header},
		legacyRaw{recorder.Code, recorder.Body.String(), recorder.Header().Get("Content-Type")}
}

// legacyQueryRoutes are the mounts of the /query pipeline (newDocumentDispatchHandler), on each listener that serves them.
var legacyQueryRoutes = []struct{ listener, path string }{
	{"query", "/query"},
	{"internal", "/query"},
	{"query", "/query/proof"},
	{"internal", "/query/proof-write"},
	{"internal", "/query/run-operation"},
}

// TestQueryRefusesTheLegacyImpersonationCaller: switch unset, the admin's own
// bearer beside a live row is served on /query as the target (the repro); on,
// it gets the impersonation refusal and no resolver runs.
func TestQueryRefusesTheLegacyImpersonationCaller(t *testing.T) {
	quietStdLog(t)
	minter := newAccessMinter(t)
	for signing, token := range legacyTokens(t, minter) {
		for _, route := range legacyQueryRoutes {
			t.Run(signing+" "+route.listener+" "+route.path, func(t *testing.T) {
				unset := legacyStack(t, minter, legacyStore(legacySession(), nil), false)
				if was, _ := legacyQuery(unset, route.listener, route.path, token); was.Status != http.StatusOK ||
					len(*unset.seen) != 1 || (*unset.seen)[0].OrgID != irTargetOrg.String() || !(*unset.seen)[0].ImpersonationActive {
					t.Fatalf("switch unset: got %d %s claims=%+v, want the target served (main's behavior)", was.Status, was.Body, *unset.seen)
				}
				on := legacyStack(t, minter, legacyStore(legacySession(), nil), true)
				if got, _ := legacyQuery(on, route.listener, route.path, token); got.Status != http.StatusForbidden || got.Body != legacyRefusalBody {
					t.Fatalf("switch on: got %d %s, want 403 %s", got.Status, got.Body, legacyRefusalBody)
				}
				if len(*on.seen) != 0 {
					t.Fatalf("switch on: resolver ran %d times, want 0", len(*on.seen))
				}
			})
		}
	}
}

// TestQueryLegacyRefusalIsTheGraphQLRefusal: the /query refusal and the
// /graphql one (policy.Scope) are the same status, body and content type, and
// so is policy.WriteImpersonationRefused: the text has one source.
func TestQueryLegacyRefusalIsTheGraphQLRefusal(t *testing.T) {
	quietStdLog(t)
	minter := newAccessMinter(t)
	writer := httptest.NewRecorder()
	policy.WriteImpersonationRefused(writer)
	want := legacyRaw{writer.Code, writer.Body.String(), writer.Header().Get("Content-Type")}
	for signing, token := range legacyTokens(t, minter) {
		stack := legacyStack(t, minter, legacyStore(legacySession(), nil), true)
		graphQL := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(`{"query":`+jsonQuoteString(iaDocument)+`}`))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", "Bearer "+token)
		stack.handlers["query"].ServeHTTP(graphQL, request)
		if got := (legacyRaw{graphQL.Code, graphQL.Body.String(), graphQL.Header().Get("Content-Type")}); got != want {
			t.Errorf("%s /graphql refusal %+v, want the writer's %+v", signing, got, want)
		}
		for _, route := range legacyQueryRoutes {
			if _, got := legacyQuery(stack, route.listener, route.path, token); got != want {
				t.Errorf("%s %s %s refusal %+v, want the writer's %+v", signing, route.listener, route.path, got, want)
			}
		}
	}
}

// TestQueryLegacyRefusalLeavesEveryOtherAnswer: the delegated bearer, a
// superuser with no live row, a plain member, a failed row read (go-api's 503,
// fail closed), a bad token and no credential answer byte-equal with the switch
// on and unset; the refusals among them stay the bare 401.
func TestQueryLegacyRefusalLeavesEveryOtherAnswer(t *testing.T) {
	quietStdLog(t)
	minter := newAccessMinter(t)
	member := map[string]string{
		"EdDSA": minter.mint(ecUser, nil, nil),
	}
	cases := map[string]struct {
		store  func() *accessStore
		tokens map[string]string
		status int
	}{
		"delegated bearer":       {delegatedStore, delegatedTokens(t, minter), http.StatusOK},
		"superuser, no live row": {func() *accessStore { return legacyStore(nil, nil) }, legacyTokens(t, minter), http.StatusOK},
		"plain member":           {newAccessStore, member, http.StatusOK},
		"row read fails":         {func() *accessStore { return legacyStore(nil, errors.New("session store timeout")) }, legacyTokens(t, minter), http.StatusServiceUnavailable},
		"bad token":              {func() *accessStore { return legacyStore(legacySession(), nil) }, map[string]string{"garbage": "not-a-token"}, http.StatusUnauthorized},
		"no credential":          {func() *accessStore { return legacyStore(legacySession(), nil) }, map[string]string{"none": ""}, http.StatusUnauthorized},
	}
	for name, tc := range cases {
		for signing, token := range tc.tokens {
			for _, route := range legacyQueryRoutes {
				t.Run(name+" "+signing+" "+route.listener+" "+route.path, func(t *testing.T) {
					on, _ := legacyQuery(legacyStack(t, minter, tc.store(), true), route.listener, route.path, token)
					unset, _ := legacyQuery(legacyStack(t, minter, tc.store(), false), route.listener, route.path, token)
					if on.Status != tc.status || !on.equal(unset) {
						t.Fatalf("switch on %+v, unset %+v; want both %d and byte-equal", on, unset, tc.status)
					}
					if tc.status == http.StatusUnauthorized && on.Body != strings.TrimSpace(queryRefusal) {
						t.Fatalf("switch on: refusal body %q, want the bare %q", on.Body, queryRefusal)
					}
					if tc.status == http.StatusServiceUnavailable && on.Body != restUnavailable {
						t.Fatalf("switch on: failed read body %q, want %q", on.Body, restUnavailable)
					}
				})
			}
		}
	}
}

// TestProofWriteDoorRefusesTheLegacyImpersonationCaller: the write door
// (digest.KindMutation, the org allowlist) refuses the legacy caller with the
// switch on, or set to a value that is not a bool; unset, it is served in the
// target org (the repro).
func TestProofWriteDoorRefusesTheLegacyImpersonationCaller(t *testing.T) {
	quietStdLog(t)
	minter := newAccessMinter(t)
	door := func(t *testing.T, value string) (http.HandlerFunc, *[]authctx.Claims) {
		t.Helper()
		store := newQueryAPIUserStoreFrom(legacyStore(legacySession(), nil))
		auth := ecEdgeAuth(t, store)
		if err := routeAuthenticator(auth, testAccess(minter.keys, nil, accessTestNow).opts); err != nil {
			t.Fatal(err)
		}
		seen := &[]authctx.Claims{}
		mux := routeswitch.NewMux(routeswitch.StaticSwitch{"probe": true})
		mux.Register("probe", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			claims, _ := authctx.FromContext(r.Context())
			*seen = append(*seen, claims)
			w.WriteHeader(http.StatusOK)
		}))
		getenv := getenvFunc(func(name string) string {
			if name == impersonationProxyEnv {
				return value
			}
			return ""
		})
		allow := func(context.Context, string) (bool, error) { return true, nil }
		// Mounted at its pattern: the access-token rows are keyed on the route the mux matched.
		routes := http.NewServeMux()
		routes.Handle("/query/proof-write", newDocumentDispatchHandler(getenv, mux, map[string]string{digestHex(proofWriteMutation): "probe"}, nil, auth, store, digest.KindMutation, allow))
		return routes.ServeHTTP, seen
	}
	post := func(handler http.HandlerFunc, token string) *httptest.ResponseRecorder {
		body, _ := json.Marshal(map[string]string{"query": proofWriteMutation})
		request := httptest.NewRequest(http.MethodPost, "/query/proof-write", strings.NewReader(string(body)))
		request.Header.Set("Authorization", "Bearer "+token)
		recorder := httptest.NewRecorder()
		handler(recorder, request)
		return recorder
	}
	for signing, token := range legacyTokens(t, minter) {
		t.Run(signing, func(t *testing.T) {
			unset, seenUnset := door(t, "")
			if got := post(unset, token); got.Code != http.StatusOK || len(*seenUnset) != 1 || (*seenUnset)[0].OrgID != irTargetOrg.String() {
				t.Fatalf("switch unset: %d %s claims=%+v, want the mutation served in the target org (main's behavior)", got.Code, got.Body.String(), *seenUnset)
			}
			// "yes" starts the process with one WARN and fails closed (TestInvalidImpersonationSwitchStartsWithOneWarnAndRefuses).
			for _, value := range []string{"true", "yes"} {
				on, seenOn := door(t, value)
				if got := post(on, token); got.Code != http.StatusForbidden || strings.TrimSpace(got.Body.String()) != legacyRefusalBody || len(*seenOn) != 0 {
					t.Fatalf("switch %q: %d %s, ran %d; want 403 %s and no resolver", value, got.Code, got.Body.String(), len(*seenOn), legacyRefusalBody)
				}
			}
		})
	}
}

// restAs sends GET /api/v1/home with bearer (and X-Org-Id when org is set)
// and returns the answer with the claims the route ran with.
func restAs(t *testing.T, store *accessStore, minter *accessMinter, bearer string, org uuid.UUID) (answer, []authctx.Claims) {
	t.Helper()
	return restAsWith(t, store, testAccess(minter.keys, nil, accessTestNow), bearer, org)
}

// restAsWith is restAs over setup (nil: no access-token issuer, the edge token's own path).
func restAsWith(t *testing.T, store *accessStore, setup *accessSetup, bearer string, org uuid.UUID) (answer, []authctx.Claims) {
	t.Helper()
	var seen []authctx.Claims
	f := newRESTFixtureServing(t, store, setup, func(w http.ResponseWriter, r *http.Request) {
		claims, _ := authctx.FromContext(r.Context())
		seen = append(seen, claims)
		w.WriteHeader(http.StatusOK)
	})
	request := httptest.NewRequest(http.MethodGet, "/api/v1/home", nil)
	request.Header.Set("Authorization", "Bearer "+bearer)
	if org != uuid.Nil {
		request.Header.Set("X-Org-Id", org.String())
	}
	recorder := httptest.NewRecorder()
	f.handler.ServeHTTP(recorder, request)
	return answer{recorder.Code, strings.TrimSpace(recorder.Body.String())}, seen
}

// TestRESTServesTheLegacyAdminBearerAsTheAdmin: a REST row never reads the
// go-api row, so the admin's own bearer beside a live row acts as the admin:
// (a) in the admin's own org, never the target's; (b) an X-Org-Id naming the
// target org is not read: the same answer, the admin's org (resource_resolver
// organization_from_credential: the org is the credential's, rest_access.go:42
// (user access token));
// (c) an admin who is a member of the target org and names it in the token acts
// there with its own membership, never the target user's role or the impersonation.
func TestRESTServesTheLegacyAdminBearerAsTheAdmin(t *testing.T) {
	quietStdLog(t)
	minter := newAccessMinter(t)
	for signing, token := range legacyTokens(t, minter) {
		t.Run(signing, func(t *testing.T) {
			got, seen := restAs(t, legacyStore(legacySession(), nil), minter, token, uuid.Nil)
			if got.Status != http.StatusOK || len(seen) != 1 || seen[0].OrgID != ecOrg.String() || seen[0].ImpersonationActive || seen[0].Role == "viewer" {
				t.Fatalf("(a) got %+v claims=%+v, want the admin in its own org", got, seen)
			}
			withHeader, seenHeader := restAs(t, legacyStore(legacySession(), nil), minter, token, irTargetOrg)
			if withHeader != got || len(seenHeader) != 1 || seenHeader[0] != seen[0] {
				t.Fatalf("(b) X-Org-Id target: got %+v claims=%+v, want %+v claims=%+v (the header is not read)", withHeader, seenHeader, got, seen)
			}
		})
	}
	member := legacyStore(legacySession(), nil)
	member.members[[2]uuid.UUID{ecUser, irTargetOrg}] = true
	member.roles = map[[2]uuid.UUID]string{{ecUser, irTargetOrg}: "owner"}
	inTarget := map[string]string{
		"EdDSA": minter.mint(ecUser, map[string]any{"org_id": irTargetOrg.String()}, nil),
	}
	for signing, token := range inTarget {
		got, seen := restAs(t, member, minter, token, uuid.Nil)
		if got.Status != http.StatusOK || len(seen) != 1 || seen[0].OrgID != irTargetOrg.String() || seen[0].ImpersonationActive || seen[0].Role == "viewer" {
			t.Errorf("(c) %s: got %+v claims=%+v, want the admin in the target org as itself", signing, got, seen)
		}
	}
}

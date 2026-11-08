package server

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/internalidentity"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/edtokens"
)

type paCarrier struct {
	name string
	// serve posts the probe document with bearer and the extra headers.
	serve func(t *testing.T, bearer string, headers map[string]string) (*httptest.ResponseRecorder, []authctx.Claims)
}

func paSet(actions ...string) *rule.Platform {
	set := rule.PlatformOf(actions...)
	return &set
}

// paCarriers is /query and /graphql over one live store: the probe operation
// records the claims the pipeline hands to a resolver.
func paCarriers(store *fakeEdgeStore) []paCarrier {
	build := func(graphql bool) func(*testing.T, string, map[string]string) (*httptest.ResponseRecorder, []authctx.Claims) {
		return func(t *testing.T, bearer string, headers map[string]string) (*httptest.ResponseRecorder, []authctx.Claims) {
			t.Helper()
			var seen []authctx.Claims
			mux := routeswitch.NewMux(routeswitch.StaticSwitch{"probe": true})
			mux.Register("probe", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				claims, _ := authctx.FromContext(r.Context())
				seen = append(seen, claims)
				w.WriteHeader(http.StatusOK)
			}))
			auth := ecEdgeAuth(t, store)
			verifier, _ := iaVerifier(t)
			dispatch := newDocumentDispatchHandler(os.Getenv, mux, map[string]string{digestHex(iaDocument): "probe"}, verifier, auth, store, "", nil)
			path, handler := "/query", onPattern("/query", dispatch)
			if graphql {
				path = "/graphql"
				handler = onPattern("/graphql", graphQLEdgeChain(dispatch, graphQLEdgeDeps{auth: auth, corsOrigins: []string{"https://app.example"}, maxBytes: defaultGraphQLMaxQueryBytes}))
			}
			request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"query":"`+iaDocument+`"}`))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Authorization", "Bearer "+bearer)
			for name, value := range headers {
				request.Header.Set(name, value)
			}
			recorder := httptest.NewRecorder()
			internalidentity.Public(handler).ServeHTTP(recorder, request)
			return recorder, seen
		}
	}
	return []paCarrier{{"/query", build(false)}, {"/graphql", build(true)}}
}

func paStore(superuser bool, platform *rule.Platform) *fakeEdgeStore {
	return &fakeEdgeStore{
		states:  map[uuid.UUID]policy.UserState{ecUser: {IsActive: true, IsSuperuser: superuser, Platform: platform, TokenVersion: 5}},
		found:   map[uuid.UUID]bool{ecUser: true},
		members: map[[2]uuid.UUID]bool{{ecUser, ecOrg}: true},
	}
}

var paActions = []string{rule.ActionAdminTelemetry, rule.ActionAdminOrgs, rule.ActionAdminStats}

// The edge carrier hands the resolvers the action set of the LIVE users row,
// on /query and on /graphql: each consumer's named action is held by its
// holder only.
func TestEdgeClaimsCarryTheLiveActionSet(t *testing.T) {
	for _, c := range []struct {
		name      string
		superuser bool
		platform  *rule.Platform
	}{
		{"only admin.telemetry", false, paSet(rule.ActionAdminTelemetry)},
		{"only admin.orgs", false, paSet(rule.ActionAdminOrgs)},
		{"only admin.stats", false, paSet(rule.ActionAdminStats)},
		{"platform_admin set", false, paSet(rule.PlatformActions...)},
		{"flag only", true, nil},
		{"no flag, no set", false, nil},
	} {
		store := paStore(c.superuser, c.platform)
		for _, carrier := range paCarriers(store) {
			t.Run(carrier.name+"/"+c.name, func(t *testing.T) {
				token := ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), tokenVersion: 5})
				recorder, seen := carrier.serve(t, token, nil)
				if recorder.Code != http.StatusOK || len(seen) != 1 {
					t.Fatalf("status %d, %d runs, body %q", recorder.Code, len(seen), recorder.Body.String())
				}
				for _, action := range paActions {
					if got, want := seen[0].Holds(action), rule.Holds(c.platform, c.superuser, action); got != want {
						t.Errorf("claims hold %s = %v, want %v (claims %+v)", action, got, want, seen[0])
					}
				}
			})
		}
	}
}

// The action set never comes from the request: a token claim or a header
// naming an action adds nothing to the row's answer.
func TestEdgeClaimsNeverTakeTheActionSetFromTheRequest(t *testing.T) {
	store := paStore(false, nil)
	for _, carrier := range paCarriers(store) {
		t.Run(carrier.name, func(t *testing.T) {
			// A routed token naming actions or superuser in its claims is refused at the contract: nothing runs.
			claimed := ecTokens.MustMint(t, edtokens.Claims{UserID: ecUser, OrgID: ecOrg.String(), TokenVersion: 5,
				Extra: map[string]any{"platform": rule.PlatformActions, "actions": rule.PlatformActions, "is_superuser": true, "role": "member"}})
			if recorder, seen := carrier.serve(t, claimed, nil); recorder.Code != http.StatusUnauthorized || len(seen) != 0 {
				t.Fatalf("action claims: status %d, %d runs, want the 401 and no run", recorder.Code, len(seen))
			}
			token := ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), tokenVersion: 5})
			recorder, seen := carrier.serve(t, token, map[string]string{"X-Platform-Actions": strings.Join(rule.PlatformActions, ","), "X-DH-Platform": "admin.stats"})
			if recorder.Code != http.StatusOK || len(seen) != 1 {
				t.Fatalf("status %d, %d runs", recorder.Code, len(seen))
			}
			for _, action := range paActions {
				if seen[0].Holds(action) {
					t.Errorf("claims hold %s from the request: %+v", action, seen[0])
				}
			}
		})
	}
}

// An org-less caller needs the live row to carry a platform set (or the
// flag). A flag-less caller with no set keeps the bare refusal.
func TestEdgeOrglessAdmissionNeedsASetOrTheFlag(t *testing.T) {
	for _, c := range []struct {
		name      string
		superuser bool
		platform  *rule.Platform
		admitted  bool
	}{
		{"only admin.telemetry", false, paSet(rule.ActionAdminTelemetry), true},
		{"only admin.stats, left to the operation guard", false, paSet(rule.ActionAdminStats), true},
		{"an empty set, left to the operation guard", false, paSet(), true},
		{"platform_admin set", false, paSet(rule.PlatformActions...), true},
		{"flag only", true, nil, true},
		{"no flag, no set", false, nil, false},
	} {
		store := paStore(c.superuser, c.platform)
		for _, carrier := range paCarriers(store) {
			t.Run(carrier.name+"/"+c.name, func(t *testing.T) {
				token := ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: "", tokenVersion: 5})
				recorder, seen := carrier.serve(t, token, nil)
				if c.admitted {
					if recorder.Code != http.StatusOK || len(seen) != 1 || seen[0].OrgID != "" {
						t.Fatalf("status %d, claims %+v, want an org-less run", recorder.Code, seen)
					}
					return
				}
				if recorder.Code != http.StatusUnauthorized || len(seen) != 0 {
					t.Fatalf("status %d, %d runs, want the 401 and no run", recorder.Code, len(seen))
				}
			})
		}
	}
}

// Through the real gqlgen server every carrier shares: an org-less caller
// with a platform set but without admin.telemetry is refused by the operation
// guard before a resolver runs, for the dashboard and for a credential-org
// field alike.
func TestOrglessPlatformSetIsRefusedByTheServedGuard(t *testing.T) {
	server := newGraphQLServer(&graph.Resolver{})
	dashboard := registeredProductTelemetryPlatformDashboardDocument
	for _, c := range []struct {
		name     string
		document string
		claims   authctx.Claims
	}{
		{"admin.stats holder, the dashboard", dashboard, authctx.Claims{Platform: paSet(rule.ActionAdminStats)}},
		{"admin.telemetry holder, dataHealth", `query Q { dataHealth(team: "x") { __typename } }`, authctx.Claims{Platform: paSet(rule.ActionAdminTelemetry)}},
		{"admin.telemetry holder, an introspection root", `query Q { __schema { types { name } } }`, authctx.Claims{Platform: paSet(rule.ActionAdminTelemetry)}},
	} {
		t.Run(c.name, func(t *testing.T) {
			body := `{"query":` + jsonQuote(t, c.document) + `,"variables":{"input":{"startDate":"2026-01-01","endDate":"2026-01-31"}}}`
			request := httptest.NewRequest(http.MethodPost, "/query", strings.NewReader(body))
			request.Header.Set("Content-Type", "application/json")
			request = request.WithContext(authctx.WithClaims(request.Context(), c.claims))
			recorder := httptest.NewRecorder()
			server.ServeHTTP(recorder, request)
			if !strings.Contains(recorder.Body.String(), "Authorization required") {
				t.Fatalf("status %d body %q, want the guard's refusal", recorder.Code, recorder.Body.String())
			}
		})
	}
}

// The MCP carrier never carries a platform set: its claims always have an org
// and hold no platform action, so the org-less rule cannot be reached there.
func TestMCPCarrierHoldsNoPlatformAction(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/query", nil)
	iaHeaders(ecOrg.String(), "viewer", false, false)(request)
	claims, status, reason := mcpAuthenticate(request)
	if status != 0 || reason != "" || claims.OrgID != ecOrg.String() || claims.Platform != nil {
		t.Fatalf("claims %+v status %d reason %q", claims, status, reason)
	}
	for _, action := range rule.PlatformActions {
		if claims.Holds(action) {
			t.Errorf("MCP claims hold %s", action)
		}
	}
}

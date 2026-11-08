package authapi

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/local"
)

// bearerGroup is one route group whose routes verify a user bearer through
// Issuer.bearer: bearer are the routes that must not mount without an access
// check, kept the group's routes that mount either way.
type bearerGroup struct {
	build  func(t *testing.T, f *fixture, issuer *Issuer, logger *slog.Logger) ([]httpapi.Route, error)
	bearer []groupRoute
	kept   []groupRoute
}

// groupRoute is a request and the status it gets when its route is not mounted
// (404, or 405 when the same path stays mounted for another method).
type groupRoute struct {
	method, path, pattern string
	unmounted             int
}

func bearerGroups() map[string]bearerGroup {
	return map[string]bearerGroup{
		"change-password": {
			build: func(t *testing.T, f *fixture, issuer *Issuer, logger *slog.Logger) ([]httpapi.Route, error) {
				return CredentialRoutes(CredentialDeps{Flows: &local.Flows{}, Sessions: f.sessions, Users: f.users, Issuer: issuer, Logger: logger, Limits: mustLimits(t, httpapi.NewMemoryCounters(nil))})
			},
			bearer: []groupRoute{{http.MethodPost, PatternChangePassword, PatternChangePassword, http.StatusNotFound}},
			kept: []groupRoute{
				{http.MethodPost, PatternForgotPassword, PatternForgotPassword, 0},
				{http.MethodPost, PatternResetPassword, PatternResetPassword, 0},
				{http.MethodGet, PatternVerifyEmail, PatternVerifyEmail, 0},
				{http.MethodPost, PatternResendVerification, PatternResendVerification, 0},
			},
		},
		"social-link": {
			build: func(t *testing.T, f *fixture, issuer *Issuer, logger *slog.Logger) ([]httpapi.Route, error) {
				return SocialLinkRoutes(SocialLinkDeps{Sessions: f.sessions, Users: f.users, Issuer: issuer, Logger: logger, Social: &fakeLinkDomain{}, Limits: mustLimits(t, httpapi.NewMemoryCounters(nil))})
			},
			bearer: []groupRoute{
				{http.MethodPost, PatternSocialLink, PatternSocialLink, http.StatusNotFound},
				{http.MethodDelete, "/v1/identities/social/0b6f3c55-6a1e-4c9e-9d39-8f2d1c3b7a10", PatternSocialUnlink, http.StatusNotFound},
			},
		},
		"oauth-consent": {
			build: func(t *testing.T, f *fixture, issuer *Issuer, logger *slog.Logger) ([]httpapi.Route, error) {
				signer, _, _ := fileSigner(t)
				return oauthRoutesRows(OAuthDeps{Store: &oauthStoreFake{}, Registry: oauthRegistry(t), Limits: mustLimits(t, httpapi.NewMemoryCounters(nil)),
					JWKS: documentOf(signer), IssuerURL: oauthIssuer, WebOrigin: oauthWeb, ResourceACRAPI: oauthAPI, ResourceACRMCP: oauthMCP,
					Session: Deps{Sessions: f.sessions, Users: f.users, Issuer: issuer, Logger: logger}}, oauthRows())
			},
			bearer: []groupRoute{{http.MethodPost, PatternOAuthAuthorize, PatternOAuthAuthorize, http.StatusMethodNotAllowed}},
			kept: []groupRoute{
				{http.MethodGet, PatternOAuthAuthorize, PatternOAuthAuthorize, 0},
				{http.MethodGet, PatternOAuthMetadata, PatternOAuthMetadata, 0},
				{http.MethodGet, PatternOAuthJWKS, PatternOAuthJWKS, 0},
			},
		},
	}
}

func serveGroup(t *testing.T, routes []httpapi.Route, route groupRoute) int {
	t.Helper()
	mux := http.NewServeMux()
	for _, r := range routes {
		mux.Handle(r.Method+" "+r.Pattern, r.Handler)
	}
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, httptest.NewRequest(route.method, route.path, strings.NewReader("")))
	return response.Code
}

// Each route group that verifies a user bearer mounts its bearer routes only
// when the issuer holds the access check. Without it they are not mounted, one
// line names AUTH_TOKEN_ISSUER_URL, the group and its routes, and the group's
// other routes stay mounted. With it they mount and no such line is written.
func TestEveryBearerRouteGroupMountsOnlyWithAnAccessCheck(t *testing.T) {
	const line = "auth bearer routes not mounted: AUTH_TOKEN_ISSUER_URL is unset"
	for name, group := range bearerGroups() {
		t.Run(name, func(t *testing.T) {
			signer, _, _ := fileSigner(t)
			f := newFixtureWith(t, newEdDSAIssuer(t, signer, AccessTokenAudience))
			logs := &bytes.Buffer{}
			routes, err := group.build(t, f, newEdDSAIssuer(t, signer, AccessTokenAudience), slog.New(slog.NewJSONHandler(logs, nil)))
			if err != nil {
				t.Fatal(err)
			}
			for _, route := range group.bearer {
				if code := serveGroup(t, routes, route); code != route.unmounted {
					t.Errorf("no access check: %s %s = %d, want %d (not mounted)", route.method, route.path, code, route.unmounted)
				}
			}
			for _, route := range group.kept {
				if code := serveGroup(t, routes, route); code == http.StatusNotFound || code == http.StatusMethodNotAllowed {
					t.Errorf("no access check: %s %s = %d, want it mounted", route.method, route.path, code)
				}
			}
			if got := strings.Count(logs.String(), line); got != 1 {
				t.Errorf("no access check: %d not-mounted lines, want 1: %s", got, logs)
			}
			if !strings.Contains(logs.String(), `"group":"`+name+`"`) {
				t.Errorf("the not-mounted line names no group %s: %s", name, logs)
			}
			for _, route := range group.bearer {
				if !strings.Contains(logs.String(), route.method+" "+route.pattern) {
					t.Errorf("the not-mounted line names no %s %s: %s", route.method, route.pattern, logs)
				}
			}

			checked, _ := withSessionBearer(t, newEdDSAIssuer(t, signer, AccessTokenAudience))
			logs.Reset()
			routes, err = group.build(t, f, checked, slog.New(slog.NewJSONHandler(logs, nil)))
			if err != nil {
				t.Fatal(err)
			}
			for _, route := range append(group.bearer, group.kept...) {
				if code := serveGroup(t, routes, route); code == http.StatusNotFound || code == http.StatusMethodNotAllowed {
					t.Errorf("with the access check: %s %s = %d, want it mounted", route.method, route.path, code)
				}
			}
			if got := strings.Count(logs.String(), line); got != 0 {
				t.Errorf("with the access check: %d not-mounted lines, want 0", got)
			}
		})
	}
}

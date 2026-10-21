package policy

import (
	"net/http"
	"strings"
	"testing"

	"github.com/golang-jwt/jwt/v5"
)

const orglessRefusal = `{"detail":"Organization context required"}`

func orglessClaims(c jwt.MapClaims) { delete(c, "org_id") }

func TestOrglessNonSuperuserReachesOnlyTheAuthRoutes(t *testing.T) {
	paths := map[string]int{
		"/api/v1/auth/onboard":                     200,
		"/api/v1/auth/onboarding/state":            200,
		"/api/v1/auth/onboarding/skip-integration": 200,
		"/api/v1/auth/accept-invite":               200,
		"/api/v1/auth/me/organizations":            200,
		"/api/v1/auth/change-password":             200,
		"/api/v1/auth/me":                          200,
		"/api/v1/auth/switch-org":                  200,
		"/api/v1/auth/refresh":                     200,
		"/api/v1/auth/logout":                      200,
		"/api/v1/auth/sso/providers":               403,
		"/api/v1/auth/sso/providers/p-1/activate":  403,
		"/api/v1/auth/onboard/extra":               403,
		"/api/v1/auth/validate":                    403,
		"/api/v1/orgs/me":                          403,
		"/api/v1/authx/onboard":                    403,
		"/api/v1/auth":                             403,
		"/api/v1/anything-new":                     403,
		"/api/v1/x/api/v1/auth/onboard":            403,
		"/graphql":                                 403,
		"/api/v1/auth/../orgs/me":                  403,
	}
	for _, level := range []Authz{Authenticated, Admin, AdminOrg} {
		for path, want := range paths {
			t.Run(level.String()+" "+path, func(t *testing.T) {
				store := newStore()
				headers := map[string]string{"Authorization": "Bearer " + sign(t, claims(func(c jwt.MapClaims) { orglessClaims(c); c["role"] = "admin" }))}
				got := serve(NewGuard(authenticator(t, store), quiet()).Wrap(level, echo), http.MethodGet, path, headers)
				if level == AdminOrg && want == 200 {
					want = 403
				}
				if got.status != want {
					t.Fatalf("status %d %s, want %d", got.status, got.body, want)
				}
				if want == 403 && got.body != orglessRefusal {
					t.Fatalf("body %s", got.body)
				}
			})
		}
	}
}

func TestOrglessSuperuserAndOrgTokensAreUnchanged(t *testing.T) {
	superStore := newStore()
	superStore.users[userID] = UserState{IsActive: true, IsSuperuser: true}
	cases := map[string]struct {
		store  *fakeStore
		mutate func(jwt.MapClaims)
		level  Authz
		path   string
		status int
	}{
		"superuser without an org, other route":     {superStore, orglessClaims, Authenticated, "/api/v1/orgs/me", 200},
		"superuser without an org, superuser route": {superStore, orglessClaims, Superuser, "/api/v1/admin/x", 200},
		"member with an org, other route":           {newStore(), nil, Authenticated, "/api/v1/orgs/me", 200},
		"member with an org, auth route":            {newStore(), nil, Authenticated, "/api/v1/auth/onboard", 200},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			headers := map[string]string{"Authorization": "Bearer " + sign(t, claims(tc.mutate))}
			got := serve(NewGuard(authenticator(t, tc.store), quiet()).Wrap(tc.level, echo), http.MethodGet, tc.path, headers)
			if got.status != tc.status || (tc.status == 200 && !strings.Contains(got.body, "user="+userID.String())) {
				t.Fatalf("status %d %s, want %d", got.status, got.body, tc.status)
			}
		})
	}
}

func TestOrglessOptionalRouteStaysOpen(t *testing.T) {
	headers := map[string]string{"Authorization": "Bearer " + sign(t, claims(orglessClaims))}
	got := serve(NewGuard(authenticator(t, newStore()), quiet()).Wrap(Optional, echo), http.MethodGet, "/api/v1/public-thing", headers)
	if got.status != 200 {
		t.Fatalf("status %d %s", got.status, got.body)
	}
}

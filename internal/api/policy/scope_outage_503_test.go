package policy

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/golang-jwt/jwt/v5"
)

const scopeOutageBody = `{"detail":{"message":"Database temporarily unavailable"}}`

// TestScopeCallerLookupOutageIsTheGuard503 is D6666: OrgScope and Impersonation
// answer the route guard's 503 and body when the caller lookup is
// ErrUnavailable, with the issuer unset and set, and never call the handler.
// Any other lookup error stays the generic 500.
func TestScopeCallerLookupOutageIsTheGuard503(t *testing.T) {
	outage := fmt.Errorf("lookup: %w", ErrUnavailable)
	other := errors.New("lookup: syntax")
	type build func(*Scope) http.Handler
	layers := map[string]build{
		"org scope":     func(s *Scope) http.Handler { return s.Principal(s.OrgScope(failIfCalled(t))) },
		"impersonation": func(s *Scope) http.Handler { return s.Principal(s.Impersonation(failIfCalled(t))) },
	}
	for layer, mount := range layers {
		for name, tc := range map[string]struct {
			err    error
			status int
			body   string
		}{
			"unavailable":        {outage, http.StatusServiceUnavailable, scopeOutageBody},
			"other lookup error": {other, http.StatusInternalServerError, `{"detail":"Internal Server Error"}`},
		} {
			t.Run(layer+"/issuer unset/"+name, func(t *testing.T) {
				store := newStore()
				store.errUser = tc.err
				scope := NewScope(authenticator(t, store), quiet())
				token := sign(t, claims(func(c jwt.MapClaims) { c["is_superuser"] = true }))
				got := serve(mount(scope), http.MethodGet, "/x", map[string]string{"Authorization": "Bearer " + token})
				if got.status != tc.status || got.body != tc.body {
					t.Fatalf("%d %s, want %d %s", got.status, got.body, tc.status, tc.body)
				}
			})
			t.Run(layer+"/issuer set/"+name, func(t *testing.T) {
				header, _ := jwt.NewWithClaims(jwt.SigningMethodEdDSA, jwt.MapClaims{}).SigningString()
				rig := newScopeRig(t, true, &BearerRow{AccessToken: true}, func(c *countedStore) { c.errUser = tc.err })
				handler := RouteProfiles(func(*http.Request) (string, bool) { return rigProfile, true })(mount(rig.scope))
				got := serveRaw(handler, "Bearer "+header+".sig", nil)
				if got.status != tc.status || got.body != tc.body {
					t.Fatalf("%d %s, want %d %s", got.status, got.body, tc.status, tc.body)
				}
			})
		}
	}
}

func failIfCalled(t *testing.T) http.Handler {
	return http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("the handler ran after a failed caller lookup")
	})
}

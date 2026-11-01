package authapi

import (
	"net/http"
	"testing"
)

// goAPIChangePasswordRefusals are go-api's own answers on POST /api/v1/auth/change-password when its guard refuses the
// bearer (AUTH_PROXY_CREDENTIALS off). With the flag on go-api forwards the route unread and auth-service answers these
// requests, so auth-service must answer them byte for byte the same; the go-api twin of this table is pinned in
// internal/apiservice/authproxy_owner_auth_test.go.
var goAPIChangePasswordRefusals = map[string]struct {
	bearer string
	status int
	body   string
}{
	"no token":  {"", http.StatusUnauthorized, `{"detail":{"message":"Not authenticated"}}`},
	"bad token": {"not-a-token", http.StatusUnauthorized, `{"detail":{"message":"Invalid or expired token"}}`},
	"bad jws":   {"eyJhbGciOiJIUzI1NiJ9.e30.EXAMPLE", http.StatusUnauthorized, `{"detail":{"message":"Invalid or expired token"}}`},
}

func TestChangePasswordRefusalsAreGoAPIsBytes(t *testing.T) {
	for name, want := range goAPIChangePasswordRefusals {
		t.Run(name, func(t *testing.T) {
			c := newCredFixture(t)
			var header map[string]string
			if want.bearer != "" {
				header = map[string]string{"Authorization": "Bearer " + want.bearer}
			}
			r := c.call(http.MethodPost, PatternChangePassword, `{"current_password": "old", "new_password": "long-enough-1"}`, header)
			if r.Code != want.status || r.Body.String() != want.body {
				t.Fatalf("auth-service %d %s, go-api %d %s", r.Code, r.Body.String(), want.status, want.body)
			}
			if r.Header().Get("WWW-Authenticate") != "Bearer" || r.Header().Get("Content-Type") != "application/json" {
				t.Fatalf("headers %v, want go-api's WWW-Authenticate: Bearer and application/json", r.Header())
			}
			if c.fakes.pwdSet != 0 {
				t.Fatal("a refused change wrote a password")
			}
		})
	}
}

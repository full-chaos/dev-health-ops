package authapi

import (
	"net/http"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

func TestChangePasswordRefusesAnAudDevHealthAPIBearerWith401(t *testing.T) {
	const body = `{"current_password":"current-password-1","new_password":"Brand-new-pass-77!"}`
	post := func(t *testing.T, audience string) (*credFixture, int, string) {
		t.Helper()
		a := newAccessFixture(t)
		c := newCredFixtureOn(t, httpapi.NewMemoryCounters(nil), a.fixture)
		claims := a.claims()
		claims.Aud = audience
		if audience == contracts.PlatformAPIAudience {
			claims.Scope = "api:access"
		}
		r := c.call(http.MethodPost, PatternChangePassword, body, a.signClaims(claims))
		return c, r.Code, r.Body.String()
	}

	t.Run("aud auth-service is accepted", func(t *testing.T) {
		if _, code, out := post(t, contracts.SessionRouteAudience); code != http.StatusOK {
			t.Fatalf("%d %s, want 200", code, out)
		}
	})
	t.Run("aud dev-health-api is refused with 401 and nothing changes", func(t *testing.T) {
		c, code, out := post(t, contracts.PlatformAPIAudience)
		if code != http.StatusUnauthorized {
			t.Fatalf("%d %s, want 401", code, out)
		}
		if c.fakes.pwdSet != 0 || c.fakes.created != 0 {
			t.Fatalf("password writes %d, sessions %d, want 0 and 0", c.fakes.pwdSet, c.fakes.created)
		}
	})
}

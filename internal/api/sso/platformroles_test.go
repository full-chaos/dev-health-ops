package sso

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
)

// The SSO sign-in reads of a users row take is_superuser by the platform
// roles switch: byte-equal while off, the platform role rule while on.
func TestSSOUserReadsFollowTheSwitch(t *testing.T) {
	for _, columns := range []string{`id, email, username, full_name, avatar_url, `, `id, email, username, full_name, `} {
		want := `SELECT ` + columns + `is_superuser, token_version
FROM users WHERE id = $1::uuid`
		if got, err := (handlers{}).userReadSQL(columns); got != want || err != nil {
			t.Errorf("off: %q %v", got, err)
		}
		got, err := handlers{Deps: Deps{PlatformRoles: policy.PlatformRoleRead{Owned: true}}}.userReadSQL(columns)
		if err != nil || !strings.Contains(got, rule.HoldsSQL(`"auth"`, "users.id", "'platform_admin'")) || strings.Contains(got, "is_superuser") {
			t.Errorf("on: %q %v", got, err)
		}
	}
}

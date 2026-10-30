package session

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
)

// The session users reads take is_superuser by the platform roles switch:
// byte-equal while off, the platform role rule while on.
func TestSessionUserReadsFollowTheSwitch(t *testing.T) {
	off := handlers{}
	for alias, want := range map[string]string{"": userColumns, "u": prefixed("u", userColumns)} {
		if got, err := off.userColumnsFor(alias); got != want || err != nil {
			t.Errorf("off (%q): %q %v", alias, got, err)
		}
	}
	on := handlers{Deps: Deps{PlatformRoles: policy.PlatformRoleRead{Owned: true}}}
	for alias, id := range map[string]string{"": "users.id", "u": "u.id"} {
		got, err := on.userColumnsFor(alias)
		if err != nil || !strings.Contains(got, rule.HoldsSQL(`"auth"`, id, "'platform_admin'")) || strings.Contains(got, "is_superuser") {
			t.Errorf("on (%q): %q %v", alias, got, err)
		}
	}
}

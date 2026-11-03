package billing

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
)

// The entitlement access check reads is_superuser by the platform roles
// switch: byte-equal while off, the platform role rule while on.
func TestEntitlementAccessSQLFollowsTheSwitch(t *testing.T) {
	if got, err := (handlers{}).entitlementAccessSQL(); got != `SELECT is_active, is_superuser FROM users WHERE id = $1` || err != nil {
		t.Errorf("off: %q %v", got, err)
	}
	got, err := handlers{platformRoles: policy.PlatformRoleRead{Owned: true}}.entitlementAccessSQL()
	if err != nil || !strings.Contains(got, rule.HoldsSQL(`"auth"`, "users.id", "'platform_admin'")) || strings.Contains(got, "is_superuser") {
		t.Errorf("on: %q %v", got, err)
	}
}

// newHandlers carries the switch from Deps.
func TestNewHandlersCarriesThePlatformRoleRead(t *testing.T) {
	read := policy.PlatformRoleRead{Owned: true, AuthSchema: "acp"}
	if got := newHandlers(Deps{PlatformRoles: read}).platformRoles; got != read {
		t.Errorf("platformRoles = %+v, want %+v", got, read)
	}
}

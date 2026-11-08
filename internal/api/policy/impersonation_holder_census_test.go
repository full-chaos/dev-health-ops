package policy

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

// The guard's admin-or `holds` closure is never reached on a live-row read while every holder of admin.impersonate
// also holds admin.orgs: such a caller passes User.IsAdmin first. The closure itself is not pinned; this census fails
// first if a platform role makes it reachable (a role with admin.impersonate and no admin.orgs).
func TestEveryPlatformRoleThatHoldsAdminImpersonateHoldsAdminOrgs(t *testing.T) {
	catalog := policycatalog.Default()
	holders := catalog.RolesHolding(rule.ActionAdminImpersonate, policycatalog.ScopePlatform)
	if len(holders) == 0 {
		t.Fatalf("no platform role holds %s in the catalog", rule.ActionAdminImpersonate)
	}
	for _, role := range holders {
		if !catalog.Allows(role, rule.ActionAdminOrgs) {
			t.Errorf("catalog platform role %s holds %s and not %s", role, rule.ActionAdminImpersonate, rule.ActionAdminOrgs)
		}
	}
	for _, role := range catalog.Roles() {
		if role.Scope != policycatalog.ScopePlatform {
			continue
		}
		if held := rule.PlatformOfRole(role.ID); held.Has(rule.ActionAdminImpersonate) && !held.Has(rule.ActionAdminOrgs) {
			t.Errorf("platform role %s holds %s and not %s at run time (rule.PlatformOfRole)", role.ID, rule.ActionAdminImpersonate, rule.ActionAdminOrgs)
		}
	}
}

package platformrole

import (
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

// rule lists the platform actions as constants so it needs no catalog import;
// this pins that list to the catalog's platform_admin role, each a platform
// action of the catalog (a platform action no role holds, such as authz.decide,
// is not listed).
func TestRulePlatformActionsAreTheCatalogs(t *testing.T) {
	catalog := policycatalog.Default()
	for _, action := range rule.PlatformActions {
		act, err := catalog.Action(action)
		if err != nil || act.Scope != policycatalog.ScopePlatform {
			t.Fatalf("rule lists %s, which is not a platform action of the catalog: %v", action, err)
		}
	}
	role, err := catalog.Role(rule.PlatformAdmin)
	if err != nil {
		t.Fatal(err)
	}
	held := append([]string(nil), role.Actions...)
	listed := append([]string(nil), rule.PlatformActions...)
	for _, list := range [][]string{held, listed} {
		sort.Strings(list)
	}
	if strings.Join(listed, ",") != strings.Join(held, ",") {
		t.Fatalf("rule.PlatformActions %v, platform_admin holds %v", listed, held)
	}
}

// The flag means platform_admin: its action set is the role's, and nothing
// else is held (an organization action, an unknown action, no flag).
func TestPlatformOfSuperuserIsPlatformAdminsActions(t *testing.T) {
	catalog := policycatalog.Default()
	on, off := rule.PlatformOfSuperuser(true), rule.PlatformOfSuperuser(false)
	for _, action := range catalog.Actions() {
		if want := catalog.Allows(rule.PlatformAdmin, action.ID); on.Has(action.ID) != want {
			t.Errorf("flag on: Has(%s) = %v, want %v", action.ID, on.Has(action.ID), want)
		}
		if off.Has(action.ID) {
			t.Errorf("flag off holds %s", action.ID)
		}
	}
	for _, action := range []string{"", "admin.unknown", "org.delete", "admin.*", "ADMIN.USERS"} {
		if on.Has(action) || rule.PlatformOf(action).Has(action) {
			t.Errorf("an unknown or organization action %q is held", action)
		}
	}
	if rule.PlatformOfRole("owner").Has(rule.ActionAdminOrgs) || rule.PlatformOfRole("").Has(rule.ActionAdminOrgs) {
		t.Error("a role that is not platform_admin holds a platform action")
	}
	var zero rule.Platform
	if zero.Has(rule.ActionAdminUsers) {
		t.Error("the zero value holds an action")
	}
	only := rule.PlatformOf(rule.ActionAdminStats)
	if !rule.Holds(&only, false, rule.ActionAdminStats) || rule.Holds(&only, true, rule.ActionAdminUsers) {
		t.Error("an explicit set must decide alone, whatever the flag")
	}
	if !rule.Holds(nil, true, rule.ActionAdminUsers) || rule.Holds(nil, false, rule.ActionAdminUsers) {
		t.Error("a carrier with only the flag must mean platform_admin or nothing")
	}
}

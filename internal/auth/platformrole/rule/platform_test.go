package rule

import "testing"

// The flag means platform_admin: every platform action; no flag, nothing.
func TestPlatformOfSuperuserHoldsEveryPlatformActionOrNone(t *testing.T) {
	on, off := PlatformOfSuperuser(true), PlatformOfSuperuser(false)
	for _, action := range PlatformActions {
		if !on.Has(action) || off.Has(action) {
			t.Errorf("%s: flag on %v, flag off %v; want true false", action, on.Has(action), off.Has(action))
		}
	}
}

// Has fails closed: an unknown action, an organization action and the empty
// name are never held, whatever set is asked for.
func TestPlatformHasFailsClosedOnAnActionThatIsNotAPlatformAction(t *testing.T) {
	for _, action := range []string{"", "admin.unknown", "org.delete", "admin.*", "ADMIN.USERS", "admin.impersonate "} {
		if PlatformOfSuperuser(true).Has(action) || PlatformOf(action).Has(action) || PlatformOfRole(PlatformAdmin).Has(action) {
			t.Errorf("%q is held", action)
		}
	}
	if PlatformOfRole("owner").Has(ActionAdminOrgs) || PlatformOfRole("").Has(ActionAdminOrgs) {
		t.Error("a role that is not platform_admin holds a platform action")
	}
	var zero Platform
	if zero.Has(ActionAdminUsers) {
		t.Error("the zero value holds an action")
	}
}

// An explicit set decides alone, whatever the flag; a carrier with only the
// flag means platform_admin or nothing.
func TestHoldsDecidesByTheExplicitSetElseTheFlag(t *testing.T) {
	only := PlatformOf(ActionAdminStats)
	if !Holds(&only, false, ActionAdminStats) || Holds(&only, true, ActionAdminUsers) {
		t.Error("an explicit set must decide alone, whatever the flag")
	}
	if !Holds(nil, true, ActionAdminUsers) || Holds(nil, false, ActionAdminUsers) {
		t.Error("a carrier with only the flag must mean platform_admin or nothing")
	}
}

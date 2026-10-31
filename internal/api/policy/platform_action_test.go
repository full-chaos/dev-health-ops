package policy

import (
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
)

// allBut is every platform action except the named one.
func allBut(action string) *rule.Platform {
	var rest []string
	for _, a := range rule.PlatformActions {
		if a != action {
			rest = append(rest, a)
		}
	}
	p := rule.PlatformOf(rest...)
	return &p
}

func only(action string) *rule.Platform {
	p := rule.PlatformOf(action)
	return &p
}

type platformCase struct {
	name      string
	superuser bool
	platform  *rule.Platform
	want      bool
}

// platformCases is the matrix every site of one action is held to: the flag
// alone means platform_admin (the switch-off outcome, unchanged), the named
// action alone is enough, every other action is not, and neither is a flag
// beside a set that lacks it.
func platformCases(action string) []platformCase {
	empty := rule.PlatformOf()
	return []platformCase{
		{"flag only, as before the change", true, nil, true},
		{"only the mapped action", false, only(action), true},
		{"the flag beside only the mapped action", true, only(action), true},
		{"every other action", true, allBut(action), false},
		{"an empty set beside the flag", true, &empty, false},
		{"no flag, no set", false, nil, false},
	}
}

// Mix 1: User.IsAdmin. A platform principal passes an org Admin guard only by
// admin.orgs; an org owner or admin passes by the role.
func TestPlatformAction_IsAdminNeedsAdminOrgs(t *testing.T) {
	for _, c := range platformCases(rule.ActionAdminOrgs) {
		user := &User{Role: "member", IsSuperuser: c.superuser, Platform: c.platform}
		if got := user.IsAdmin(); got != c.want {
			t.Errorf("%s: IsAdmin = %v, want %v", c.name, got, c.want)
		}
	}
	for _, role := range []string{"owner", "admin"} {
		if !(&User{Role: role, Platform: allBut(rule.ActionAdminOrgs)}).IsAdmin() {
			t.Errorf("an org %s lost the guard", role)
		}
	}
	if (&User{Role: "viewer"}).IsAdmin() {
		t.Error("a viewer passes the Admin guard")
	}
}

// Mix 4: mayUseOrg. A caller names an org they hold no membership of only by
// admin.orgs.
func TestPlatformAction_MayUseOrgNeedsAdminOrgs(t *testing.T) {
	scope := NewScope(authenticator(t, newStore()), quiet())
	for _, c := range platformCases(rule.ActionAdminOrgs) {
		user := &User{UserID: userID.String(), OrgID: ownOrg, IsSuperuser: c.superuser, Platform: c.platform}
		got, err := scope.mayUseOrg(context.Background(), user, strangerOrg)
		if err != nil || got != c.want {
			t.Errorf("%s: mayUseOrg = %v, %v; want %v", c.name, got, err, c.want)
		}
	}
}

// Switch off (and on, for platform_admin): a caller that carries only the flag
// decides exactly as one that carries platform_admin's full action set, at
// every platform action, and a caller without the flag holds none. Every site
// of the nine mix rows reads Holds, so this pins the unchanged outcome.
func TestPlatformAction_FlagOnlyDecidesAsPlatformAdminDid(t *testing.T) {
	full := rule.PlatformOfSuperuser(true)
	for _, action := range rule.PlatformActions {
		flagOnly := &User{IsSuperuser: true}
		explicit := &User{IsSuperuser: true, Platform: &full}
		none := &User{}
		if !flagOnly.Holds(action) || !explicit.Holds(action) || none.Holds(action) {
			t.Errorf("%s: flag only %v, full set %v, no flag %v; want true true false", action, flagOnly.Holds(action), explicit.Holds(action), none.Holds(action))
		}
	}
	if !(&User{Role: "member", IsSuperuser: true}).IsAdmin() || (&User{Role: "member"}).IsAdmin() {
		t.Error("the flag no longer passes an org Admin guard, or a plain member does")
	}
	for _, unknown := range []string{"", "org.delete", "admin.unknown"} {
		if (&User{IsSuperuser: true}).Holds(unknown) {
			t.Errorf("the flag holds %q", unknown)
		}
	}
}

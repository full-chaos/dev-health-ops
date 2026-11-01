package server

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
)

// Mix 5: roleInOrg. A non-member acts in an org other than the token's own
// only by admin.orgs; the token's own org always needs the membership.
func TestPlatformAction_ActingInAnotherOrgNeedsAdminOrgs(t *testing.T) {
	empty := rule.PlatformOf()
	only := rule.PlatformOf(rule.ActionAdminOrgs)
	var rest []string
	for _, a := range rule.PlatformActions {
		if a != rule.ActionAdminOrgs {
			rest = append(rest, a)
		}
	}
	others := rule.PlatformOf(rest...)
	for _, c := range []struct {
		name      string
		superuser bool
		platform  *rule.Platform
		want      bool
	}{
		{"flag only, as before the change", true, nil, true},
		{"only admin.orgs", false, &only, true},
		{"every other action", true, &others, false},
		{"an empty set beside the flag", true, &empty, false},
		{"no flag, no set", false, nil, false},
	} {
		user := &policy.User{OrgID: "own", IsSuperuser: c.superuser, Platform: c.platform}
		role, ok := roleInOrg(user, "other", "", false)
		if ok != c.want || role != "" {
			t.Errorf("%s: roleInOrg = %q, %v; want allowed %v", c.name, role, ok, c.want)
		}
		if _, ok := roleInOrg(user, "own", "", false); ok {
			t.Errorf("%s: the token's own org was open without a membership", c.name)
		}
	}
}

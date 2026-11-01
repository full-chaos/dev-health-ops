package datahealth

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
)

// Mix 6: RequireOperator. A principal with no operator role passes only by
// admin.stats.
func TestPlatformAction_OperatorGateNeedsAdminStats(t *testing.T) {
	empty := rule.PlatformOf()
	only := rule.PlatformOf(rule.ActionAdminStats)
	var rest []string
	for _, a := range rule.PlatformActions {
		if a != rule.ActionAdminStats {
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
		{"only admin.stats", false, &only, true},
		{"every other action", true, &others, false},
		{"an empty set beside the flag", true, &empty, false},
		{"no flag, no set", false, nil, false},
	} {
		err := RequireOperator(Principal{Present: true, Role: "member", IsSuperuser: c.superuser, Platform: c.platform})
		if got := err == nil; got != c.want {
			t.Errorf("%s: err %v, want allowed %v", c.name, err, c.want)
		}
	}
	for _, role := range []string{"admin", "OWNER", "operator"} {
		if err := RequireOperator(Principal{Present: true, Role: role}); err != nil {
			t.Errorf("role %s refused: %v", role, err)
		}
	}
}

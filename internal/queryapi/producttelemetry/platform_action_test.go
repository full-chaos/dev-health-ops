package producttelemetry

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
)

// RequirePlatformAdmin admits a present, non-impersonating principal by
// admin.telemetry alone.
func TestPlatformAction_PlatformDashboardNeedsAdminTelemetry(t *testing.T) {
	var others []string
	for _, a := range rule.PlatformActions {
		if a != rule.ActionAdminTelemetry {
			others = append(others, a)
		}
	}
	rest, only, empty := rule.PlatformOf(others...), rule.PlatformOf(rule.ActionAdminTelemetry), rule.PlatformOf()
	for _, c := range []struct {
		name string
		p    Principal
		want bool
	}{
		{"the flag only, as before", Principal{Present: true, IsSuperuser: true}, true},
		{"only admin.telemetry", Principal{Present: true, Platform: &only}, true},
		{"the flag beside every other action", Principal{Present: true, IsSuperuser: true, Platform: &rest}, false},
		{"an empty set beside the flag", Principal{Present: true, IsSuperuser: true, Platform: &empty}, false},
		{"no power", Principal{Present: true}, false},
		{"a holder while impersonating", Principal{Present: true, IsSuperuser: true, ImpersonationActive: true}, false},
		{"not present", Principal{IsSuperuser: true}, false},
	} {
		if got := RequirePlatformAdmin(c.p) == nil; got != c.want {
			t.Errorf("%s: admitted = %v, want %v", c.name, got, c.want)
		}
	}
}

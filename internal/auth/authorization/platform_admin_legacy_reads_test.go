package authorization

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

// platformAdminLegacyReads are the query-api actions (and the saved-report writes) whose legacy path admits a
// superuser with no membership row in an active or a suspended organization (CHAOS-13240; legacy cells
// TestPlatformAdminNoMembershipQueryAPIAnswersEqualLegacy and TestLegacyQueryAPISuperuserInAnInactiveOrganization).
var platformAdminLegacyReads = []string{"analytics.read", "git.read", "metrics.read", "reports.write", "work_items.read"}

// Product rule 1: a platform admin with no membership row gets the legacy answer on these actions, an allow with the
// tenant scope only, in an active or a suspended organization. A member keeps the suspended-organization deny, an
// unknown organization denies, and a platform role without admin.orgs gets nothing.
func TestPlatformAdminNoMembershipGetsTheLegacyQueryAPIActions(t *testing.T) {
	e := NewEvaluator()
	for _, a := range platformAdminLegacyReads {
		act, err := policycatalog.Default().Action(a)
		if err != nil {
			t.Fatalf("%s: %v", a, err)
		}
		if act.RepositoryScope == policycatalog.RepositoryScopeFiltered {
			t.Errorf("%s is repository_scope filtered", a)
		}
		req := orgRequest(a)
		for _, org := range []Status{StatusActive, StatusSuspended} {
			for _, member := range []string{"", "viewer"} {
				wantOwnerShapeAllow(t, a+" platform_admin + "+member+" in org "+statusName(org),
					e.Evaluate(req, platformAdminSnapshot("platform_admin", member, org)), ReasonAllowedByPlatformAdmin)
			}
		}
		if d := e.Evaluate(req, platformAdminSnapshot("platform_admin", "", StatusUnknown)); d.Allowed || d.Reason != ReasonOrganizationInactive {
			t.Errorf("%s platform_admin, unknown org: %+v, want deny organization_inactive", a, d)
		}
		for _, member := range []string{"viewer", "owner"} {
			if d := e.Evaluate(req, platformAdminSnapshot("", member, StatusSuspended)); d.Allowed || d.Reason != ReasonOrganizationInactive {
				t.Errorf("%s %s member, suspended org: %+v, want deny organization_inactive", a, member, d)
			}
		}
		if d := e.Evaluate(req, platformAdminSnapshot("platform_owner", "", StatusActive)); d.Allowed || d.Reason != ReasonNoMembership {
			t.Errorf("%s platform_owner + no membership row: %+v, want deny no_membership", a, d)
		}
	}
}

func statusName(s Status) string {
	if s == StatusSuspended {
		return "suspended"
	}
	return "active"
}

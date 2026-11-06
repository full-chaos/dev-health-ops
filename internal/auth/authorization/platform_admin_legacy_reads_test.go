package authorization

import (
	"reflect"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

// platformAdminLegacyReads are the organization actions of the query-api rows (and the saved-report writes) whose
// legacy path admits a superuser in any organization with no membership row (CHAOS-13209 B3, acp parity cells
// TestParityCellsQueryAPI / TestParityPDPAlone).
var platformAdminLegacyReads = []string{"analytics.read", "git.read", "metrics.read", "reports.write", "work_items.read"}

// Product rule 1 (D6773): a platform admin with no membership row gets the legacy answer on these actions, an allow
// with the tenant scope and, for a repository-filtered action, every repository of the organization (the legacy
// scope). An inactive organization still denies, and a platform role without admin.orgs gets nothing.
func TestPlatformAdminNoMembershipGetsTheLegacyQueryAPIActions(t *testing.T) {
	e := NewEvaluator()
	for _, a := range platformAdminLegacyReads {
		act, err := policycatalog.Default().Action(a)
		if err != nil {
			t.Fatalf("%s: %v", a, err)
		}
		want := &Obligations{TenantScope: testOrg}
		if act.RepositoryScope == policycatalog.RepositoryScopeFiltered {
			want.RepositoryScope = RepositoryScopeOrganization
		}
		req := orgRequest(a)
		for _, member := range []string{"", "viewer"} {
			d := e.Evaluate(req, platformAdminSnapshot("platform_admin", member, StatusActive))
			if !d.Allowed || d.Reason != ReasonAllowedByPlatformAdmin || !reflect.DeepEqual(d.Obligations, want) {
				t.Errorf("%s platform_admin + %q: %+v (obligations %+v), want allow %s with %+v", a, member, d, d.Obligations,
					ReasonAllowedByPlatformAdmin, want)
			}
		}
		if d := e.Evaluate(req, platformAdminSnapshot("platform_admin", "", StatusSuspended)); d.Allowed || d.Reason != ReasonOrganizationInactive {
			t.Errorf("%s platform_admin, suspended org: %+v, want deny organization_inactive", a, d)
		}
		if d := e.Evaluate(req, platformAdminSnapshot("platform_owner", "", StatusActive)); d.Allowed || d.Reason != ReasonNoMembership {
			t.Errorf("%s platform_owner + no membership row: %+v, want deny no_membership", a, d)
		}
	}
}

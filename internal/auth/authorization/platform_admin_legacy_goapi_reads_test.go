package authorization

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

// platformAdminLegacyGoAPIReads are the go-api billing and license reads whose legacy handlers admit an admin.billing
// holder (or a users.is_superuser user) with no membership row: billing invoiceOrg / resolveOrg, entitlementAccess and
// the licensing entitlements handler. Legacy cell: apiservice
// TestPlatformAdminNoMembershipBillingAndLicenseReadsEqualLegacyWired.
var platformAdminLegacyGoAPIReads = []string{"billing.read", "license.read"}

// C1. Product rule 1: a platform admin with no membership row gets the legacy answer on the go-api billing and
// license reads, an allow with the tenant scope only, in an active, a suspended or an unknown organization. A member
// without admin.orgs keeps its own answer, and a platform role without admin.orgs gets nothing.
func TestPlatformAdminNoMembershipGetsTheLegacyGoAPIBillingAndLicenseReads(t *testing.T) {
	e := NewEvaluator()
	for _, a := range platformAdminLegacyGoAPIReads {
		act, err := policycatalog.Default().Action(a)
		if err != nil {
			t.Fatalf("%s: %v", a, err)
		}
		if act.RepositoryScope == policycatalog.RepositoryScopeFiltered {
			t.Errorf("%s is repository_scope filtered", a)
		}
		req := orgRequest(a)
		for _, org := range []Status{StatusActive, StatusSuspended, StatusUnknown} {
			for _, member := range []string{"", "viewer"} {
				wantOwnerShapeAllow(t, a+" platform_admin + "+member+" in org "+statusName(org),
					e.Evaluate(req, platformAdminSnapshot("platform_admin", member, org)), ReasonAllowedByPlatformAdmin)
			}
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

// C6. The legacy billing and license handlers key on admin.billing (or users.is_superuser); the flag keys on
// admin.orgs. The two answers are equal only while every platform role that holds admin.orgs also holds admin.billing.
func TestEveryAdminOrgsHolderHoldsAdminBilling(t *testing.T) {
	cat := policycatalog.Default()
	holders := 0
	for _, id := range cat.RolesHolding(platformAdminAction, policycatalog.ScopePlatform) {
		holders++
		if !cat.Allows(id, "admin.billing") {
			t.Errorf("platform role %s holds admin.orgs but not admin.billing: the billing.read and license.read flag would be wider than legacy", id)
		}
	}
	if holders == 0 {
		t.Fatal("no platform role holds admin.orgs; the check is vacuous")
	}
}

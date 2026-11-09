package authorization

import "testing"

// Legacy refuses ACR context and evidence reads to a platform admin with no membership row in the organization of
// its own token (query-api edge auth: the token's own organization always needs the membership; web answers 503
// unavailable). The decision point gives the same refusal: no repository action takes the platform-admin flag,
// so a no-row admin.orgs holder is denied no_membership on both reads, in every organization status.
func TestPlatformAdminWithNoMembershipIsRefusedTheAcrRepositoryReads(t *testing.T) {
	e := NewEvaluator()
	for _, action := range []string{"acr.context.read", "acr.evidence.read"} {
		req := Request{Action: action, ResourceKind: "repository", OrganizationID: testOrg, ResourceKey: "acme/api"}
		got := e.Evaluate(req, grantFake{fakeSnapshot: platformAdminSnapshot("platform_admin", "", StatusActive), now: nowNano})
		if got.Allowed || got.Reason != ReasonNoMembership {
			t.Errorf("%s, no-row platform_admin, active org: %+v, want deny no_membership", action, got)
		}
		for _, org := range []Status{StatusSuspended, StatusUnknown} {
			if got := e.Evaluate(req, grantFake{fakeSnapshot: platformAdminSnapshot("platform_admin", "", org), now: nowNano}); got.Allowed {
				t.Errorf("%s, no-row platform_admin, org %v: %+v, want deny", action, org, got)
			}
		}
	}
}

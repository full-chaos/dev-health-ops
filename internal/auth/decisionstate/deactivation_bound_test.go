package decisionstate

import (
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
)

// The PDP reads a user's state. A service account or a workload has no membership row and the snapshot reports
// neither a service-account row nor workload limits, so every action of such a principal denies at the PDP,
// whatever grants the principal holds. These are pins: they hold on main and must keep holding.
func TestDeactivationBound_NonUserPrincipals_PDPDeny(t *testing.T) {
	const org = "11111111-1111-4111-8111-111111111111"
	now := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC).UnixNano()
	e := authorization.NewEvaluator()
	live := &snapshot{
		principal: authorization.StatusActive, org: org, orgStatus: authorization.StatusActive, now: now,
		grants: []authorization.Grant{{OrgID: org, Role: "viewer", ResourceKind: "repository", ResourceKey: "acme/api"}},
	}
	for _, tc := range []struct {
		name string
		req  authorization.Request
	}{
		{"repository read by grant alone", authorization.Request{Action: "acr.context.read", ResourceKind: "repository", OrganizationID: org, ResourceKey: "acme/api"}},
		{"organization action", authorization.Request{Action: "analytics.read", ResourceKind: "organization", OrganizationID: org}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := e.Evaluate(tc.req, live)
			if d.Allowed {
				t.Fatalf("a principal with no membership and no service-account row was allowed: %+v", d)
			}
		})
	}
	if _, found := live.ServiceAccount(org); found {
		t.Fatal("the snapshot reports a service-account row")
	}
	if _, found := live.Workload(); found {
		t.Fatal("the snapshot reports workload limits")
	}
}

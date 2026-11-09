package policy

import (
	"context"
	"testing"

	"github.com/full-chaos/dev-health-go/pep"
	"github.com/full-chaos/dev-health-go/pep/pepfake"

	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
)

type productRule2Snapshot struct {
	role   string
	grants []authorization.Grant
}

func (s productRule2Snapshot) PrincipalStatus() authorization.Status {
	return authorization.StatusActive
}
func (s productRule2Snapshot) PlatformRole() string { return "" }
func (s productRule2Snapshot) OrganizationStatus(string) authorization.Status {
	return authorization.StatusActive
}
func (s productRule2Snapshot) Membership(org string) (authorization.Membership, bool) {
	return authorization.Membership{OrgID: org, Role: s.role, Status: authorization.StatusActive}, true
}
func (s productRule2Snapshot) Grants(string) ([]authorization.Grant, error) { return s.grants, nil }
func (s productRule2Snapshot) Workload() (authorization.WorkloadLimits, bool) {
	return authorization.WorkloadLimits{}, false
}
func (s productRule2Snapshot) ServiceAccount(string) (authorization.ServiceAccount, bool) {
	return authorization.ServiceAccount{}, false
}
func (s productRule2Snapshot) NowUnixNano() int64 { return 1_800_000_000_000_000_000 }

// A member with one repository grant reads these actions org-wide: the allow is carried out on a route that has a
// repository dimension (a route not in the no-repository-dimension table), so go-api serves the whole organization.
func TestProductRule2GoAPIMemberWithOneRepositoryGrantReadsOrgWideMetrics(t *testing.T) {
	const org = "0b6a1c9e-55d4-4a43-9e34-6f4c1d2b3a10"
	snap := productRule2Snapshot{role: "member", grants: []authorization.Grant{
		{OrgID: org, Role: "viewer", ResourceKind: "repository", ResourceKey: "acme/api"}}}
	for _, action := range []string{"metrics.read", "analytics.read", "analytics.export", "git.read", "work_items.read"} {
		t.Run(action, func(t *testing.T) {
			d := authorization.NewEvaluator().Evaluate(authorization.Request{Action: action, ResourceKind: "organization", OrganizationID: org}, snap)
			if !d.Allowed || d.Obligations == nil {
				t.Fatalf("decision point answered %+v", d)
			}
			request := pep.Request{PrincipalReference: "p", Action: action, ResourceType: "organization", ResourceKey: org, OrganizationID: org}
			rule := pepfake.Rule{PrincipalReference: "p", Action: action, ResourceType: "organization", ResourceKey: org, OrganizationID: org,
				TenantScope: d.Obligations.TenantScope, RepositoryScope: string(d.Obligations.RepositoryScope), RepositoryAllowlist: d.Obligations.RepositoryAllowlist}
			decision := pepfake.New(rule).Decide(context.Background(), request)
			if !decision.Allowed() {
				t.Fatalf("fake decision point refused: %v", decision.Reason())
			}
			if !obligationsCarriedOut(decision.Obligations(), request, false, true) {
				t.Fatalf("%s: allow for a member with one repository grant is not carried out on a route with a repository dimension (allowlist %v)", action, d.Obligations.RepositoryAllowlist)
			}
		})
	}
}

package server

import (
	"testing"

	"github.com/full-chaos/dev-health-go/pep/pepfake"

	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
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

// A member with one non-wildcard repository grant reads org-wide through the real REST routes: the PDP-off run is the
// baseline, and the enforced run, decided by the real Evaluator over the catalog, answers the same bytes with the same
// reads.
func TestProductRule2MemberWithOneRepositoryGrantReadsOrgWideOnEnforcedRESTRoutes(t *testing.T) {
	org := ecOrg.String()
	snap := productRule2Snapshot{role: "member", grants: []authorization.Grant{
		{OrgID: org, Role: "viewer", ResourceKind: "repository", ResourceKey: "acme/api"}}}
	for _, route := range seamRoutes() {
		t.Run(route.name, func(t *testing.T) {
			legacy := legacyRun(t, route, userBearer)
			s := newSeamStack(t, route)
			d := authorization.NewEvaluator().Evaluate(authorization.Request{Action: route.action, ResourceKind: "organization", OrganizationID: org}, snap)
			if !d.Allowed || d.Obligations == nil {
				t.Fatalf("decision point answered %+v", d)
			}
			rule := seamAllow(s, func(r *pepfake.Rule) {
				r.TenantScope, r.RepositoryScope, r.RepositoryAllowlist = d.Obligations.TenantScope, string(d.Obligations.RepositoryScope), d.Obligations.RepositoryAllowlist
			})
			gate, _ := seamGate(t, pdp.ModeEnforce, namespaceOf(route.action), pepfake.New(rule))
			got := s.run(gate, s.user)
			if !got.same(legacy) {
				t.Fatalf("enforced run differs from the PDP-off run (an allowlist %v is refused):\n got %d %q %d statements\nwant %d %q %d statements",
					d.Obligations.RepositoryAllowlist, got.status, got.body, len(got.statements), legacy.status, legacy.body, len(legacy.statements))
			}
			if len(s.seen) != 1 || s.seen[0].Scope != pepgate.Organization(org) {
				t.Fatalf("handler saw %+v, want Scope Organization(%s)", s.seen, org)
			}
		})
	}
}

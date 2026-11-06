package pdp

import (
	"context"
	"slices"
	"testing"

	"github.com/full-chaos/dev-health-go/pep/pepfake"

	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
)

type productRule2Snapshot struct {
	role   string
	grants []authorization.Grant
}

func (s productRule2Snapshot) PrincipalStatus() authorization.Status { return authorization.StatusActive }
func (s productRule2Snapshot) PlatformRole() string                  { return "" }
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

// productRule2Rule is the decision point's real answer for a member with one repository grant, as a fake PDP rule.
func productRule2Rule(t *testing.T, action string) pepfake.Rule {
	t.Helper()
	snap := productRule2Snapshot{role: "member", grants: []authorization.Grant{
		{OrgID: seamOrg, Role: "viewer", ResourceKind: "repository", ResourceKey: "acme/api"}}}
	d := authorization.NewEvaluator().Evaluate(authorization.Request{Action: action, ResourceKind: "organization", OrganizationID: seamOrg}, snap)
	if !d.Allowed || d.Obligations == nil {
		t.Fatalf("%s: decision point answered %+v", action, d)
	}
	return pepfake.Rule{PrincipalReference: testBearer, Action: action, ResourceType: "organization", ResourceKey: seamOrg,
		OrganizationID: seamOrg, TenantScope: d.Obligations.TenantScope, RepositoryScope: string(d.Obligations.RepositoryScope),
		RepositoryAllowlist: d.Obligations.RepositoryAllowlist}
}

func TestProductRule2QueryAPIMemberWithOneRepositoryGrantReadsOrgWideMetrics(t *testing.T) {
	g0 := New(Config{Mode: ModeOff})
	for _, action := range []string{"metrics.read", "analytics.read", "analytics.export", "git.read", "work_items.read"} {
		var profile string
		for _, p := range slices.Sorted(mapKeys(g0.actions)) {
			if g0.actions[p] == action {
				profile = p
				break
			}
		}
		if profile == "" {
			t.Logf("no query-api row asks %s", action)
			continue
		}
		t.Run(action, func(t *testing.T) {
			_, cfg := newMeter(t)
			cfg.Mode, cfg.Decider, cfg.Switch = ModeEnforce, pepfake.New(productRule2Rule(t, action)), cells("analytics,metrics,git,work_items")
			g := New(cfg)
			scope, err := g.Authorize(context.Background(), profile, testBearer, seamOrg)
			if err != nil {
				t.Fatalf("%s %s: answered %v, want an org-wide scope", profile, action, err)
			}
			if scope.Kind() != pepgate.KindOrganization || scope.Restricted() {
				t.Fatalf("%s %s: scope kind %v keys %v, want Organization", profile, action, scope.Kind(), scope.RepoKeys())
			}
		})
	}
}

func mapKeys[M ~map[K]V, K comparable, V any](m M) func(func(K) bool) {
	return func(yield func(K) bool) {
		for k := range m {
			if !yield(k) {
				return
			}
		}
	}
}

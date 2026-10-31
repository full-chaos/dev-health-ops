package authorization

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

func FuzzEvaluateDelegated(f *testing.F) {
	f.Add("billing.manage", "owner", "platform_owner", "billing.manage", 1)
	f.Add("org.delete", "owner", "platform_admin", "org.delete", 1)
	f.Add("org.read", "viewer", "", "org.read", 2)
	f.Add("org.read", "viewer", "", "", 1)
	f.Fuzz(func(t *testing.T, action, role, actorRole, permitted string, hops int) {
		chain := make([]Hop, 0, 3)
		for i := 0; i < hops%4 && i < 3; i++ {
			chain = append(chain, Hop{ActorPrincipalID: testActor, PermittedActions: []string{permitted}})
		}
		s := delegatedSnapshot{
			fakeSnapshot: orgSnapshot(role),
			d:            Delegation{Chain: chain, ActorPlatformRole: actorRole, TargetOrganizationID: testOrg, Assurance: "aal1"},
			on:           true,
		}
		d := NewEvaluator().Evaluate(orgRequest(action), s)
		if !d.Allowed {
			if d.Obligations != nil || d.Audit != nil {
				t.Fatalf("deny carries obligations or audit: %+v", d)
			}
			return
		}
		if len(chain) != 1 {
			t.Fatalf("allowed with a chain of %d hops", len(chain))
		}
		if action != permitted {
			t.Fatalf("allowed %q outside permitted %q", action, permitted)
		}
		if !policycatalog.Default().Allows(role, action) {
			t.Fatalf("allowed %q for role %q that does not hold it", action, role)
		}
		floor := floorOrgActions[action] || credentialShape.MatchString(action) || action == "admin.impersonate"
		if floor && actorRole != ownerActorRol {
			t.Fatalf("allowed floor action %q for actor role %q", action, actorRole)
		}
		if d.Obligations == nil || d.Obligations.TenantScope != testOrg {
			t.Fatalf("delegated allow without tenant scope: %+v", d)
		}
	})
}

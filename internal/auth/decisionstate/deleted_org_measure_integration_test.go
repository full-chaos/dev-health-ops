//go:build integration

package decisionstate

import (
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
)

// CHAOS-13214, a measurement: the organization status the real snapshot reads for an organization whose
// public.organizations row was deleted (public only; and with an auth copy left behind), and the production
// evaluator's answer over that snapshot for a superuser with no membership row. CELL lines; the test fails only on a
// read error.
func TestMeasureSnapshotOfADeletedOrganization(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	superuser := e.user(t, ctx, true, true)
	names := map[authorization.Status]string{
		authorization.StatusUnknown: "Unknown", authorization.StatusActive: "Active", authorization.StatusInvited: "Invited",
		authorization.StatusSuspended: "Suspended", authorization.StatusRemoved: "Removed",
	}
	evaluator := authorization.NewEvaluator()
	for _, c := range []struct {
		label string
		org   func() string
	}{
		{"public row only, deleted", func() string {
			id := e.publicOrg(t, ctx)
			e.exec(t, ctx, `DELETE FROM public.organizations WHERE id = $1`, id)
			return id.String()
		}},
		{"auth copy kept, public row deleted", func() string {
			id := e.org(t, ctx)
			e.exec(t, ctx, `DELETE FROM public.organizations WHERE id = $1`, id)
			return id.String()
		}},
	} {
		org := c.org()
		snap := e.snapshot(t, ctx, superuser, org)
		t.Logf("CELL|decisionstate|%s|org_status=%s|platform_role=%q", c.label, names[snap.OrganizationStatus(org)], snap.PlatformRole())
		for _, action := range []string{"billing.read", "license.read", "members.read", "analytics.read"} {
			d := evaluator.Evaluate(authorization.Request{Action: action, ResourceKind: "organization", OrganizationID: org}, snap)
			t.Logf("CELL|decisionstate|%s|superuser no membership|%s|allowed=%v|reason=%s", c.label, action, d.Allowed, d.Reason)
		}
	}
}

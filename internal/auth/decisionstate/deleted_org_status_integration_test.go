//go:build integration

package decisionstate

import (
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
)

// The real snapshot reads an organization whose public.organizations row was deleted (public only; and with an auth
// copy left behind) as Unknown, never Removed, and the production evaluator admits a superuser with no membership
// row there on a flagged action (as the legacy paths do) and refuses an unflagged one with organization_inactive.
func TestSnapshotOfADeletedOrganizationIsUnknown(t *testing.T) {
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
		if got := snap.OrganizationStatus(org); got != authorization.StatusUnknown {
			t.Errorf("%s: organization status %s, want Unknown", c.label, names[got])
		}
		for _, w := range []struct {
			action  string
			allowed bool
			reason  authorization.Reason
		}{
			{"billing.read", true, authorization.ReasonAllowedByPlatformAdmin},
			{"license.read", true, authorization.ReasonAllowedByPlatformAdmin},
			{"analytics.read", true, authorization.ReasonAllowedByPlatformAdmin},
			{"members.read", false, authorization.ReasonOrganizationInactive},
		} {
			d := evaluator.Evaluate(authorization.Request{Action: w.action, ResourceKind: "organization", OrganizationID: org}, snap)
			t.Logf("CELL|decisionstate|%s|superuser no membership|%s|allowed=%v|reason=%s", c.label, w.action, d.Allowed, d.Reason)
			if d.Allowed != w.allowed || d.Reason != w.reason {
				t.Errorf("%s: superuser with no membership row, %s: allowed=%v reason=%s, want allowed=%v reason=%s", c.label,
					w.action, d.Allowed, d.Reason, w.allowed, w.reason)
			}
		}
	}
}

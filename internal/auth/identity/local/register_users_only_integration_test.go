//go:build integration

package local

import (
	"context"
	"testing"
)

func TestRegisterUnderUsersOwnedWritesUsersOnlyNoOrgOrMembership(t *testing.T) {
	ctx := context.Background()
	rig := newEndRig(t, ctx)
	before := func(table string) int {
		return rig.env.count(t, ctx, `SELECT count(*) FROM public.`+table)
	}
	orgs, members := before("organizations"), before("memberships")
	outcome, err := rig.flows.Register(ctx, RegisterRequest{Email: "newcomer@example.com", Password: "Correct-horse-battery-9"})
	if err != nil || outcome.Kind != FlowOK {
		t.Fatalf("Register = %v, %v", outcome.Kind, err)
	}
	if n := rig.env.count(t, ctx, `SELECT count(*) FROM public.users WHERE email = 'newcomer@example.com' AND auth_provider = 'local' AND NOT is_verified`); n != 1 {
		t.Fatalf("public.users rows = %d, want 1 unverified local user", n)
	}
	if n := rig.env.count(t, ctx, `SELECT count(*) FROM auth.users WHERE email_lower = 'newcomer@example.com' AND email_verified_at IS NULL`); n != 1 {
		t.Fatalf("auth.users rows = %d, want 1", n)
	}
	if got := before("organizations"); got != orgs {
		t.Fatalf("public.organizations %d -> %d: register wrote an organization", orgs, got)
	}
	if got := before("memberships"); got != members {
		t.Fatalf("public.memberships %d -> %d: register wrote a membership", members, got)
	}
	if n := rig.env.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.principal.provisioned'`); n != 1 {
		t.Fatalf("auth.principal.provisioned rows = %d, want 1", n)
	}
	if n := rig.env.count(t, ctx, `SELECT count(*) FROM auth.link_credentials WHERE purpose = $1`, string(PurposeEmailVerification)); n != 1 {
		t.Fatalf("verification credentials = %d, want 1", n)
	}
}

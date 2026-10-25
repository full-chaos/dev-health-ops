//go:build integration

package local

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/registerorg"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

func publicCount(t *testing.T, rig *endRig, ctx context.Context, table string) int {
	t.Helper()
	return rig.env.count(t, ctx, `SELECT count(*) FROM public.`+table)
}

func TestRegisterCreatesTheOrganizationAndOwnerMembershipWithTheUser(t *testing.T) {
	ctx := context.Background()
	rig := newEndRig(t, ctx)
	name := "Acme Corp"
	outcome, err := rig.flows.Register(ctx, RegisterRequest{Email: "newcomer@example.com", Password: "Correct-horse-battery-9", OrganizationName: &name})
	if err != nil || outcome.Kind != FlowOK {
		t.Fatalf("Register = %v, %v", outcome.Kind, err)
	}
	var userID uuid.UUID
	if err := rig.env.admin.QueryRow(ctx, `SELECT id FROM public.users WHERE email = 'newcomer@example.com' AND auth_provider = 'local' AND NOT is_verified`).Scan(&userID); err != nil {
		t.Fatalf("public.users row: %v", err)
	}
	if n := rig.env.count(t, ctx, `SELECT count(*) FROM auth.users WHERE email_lower = 'newcomer@example.com' AND email_verified_at IS NULL`); n != 1 {
		t.Fatalf("auth.users rows = %d, want 1", n)
	}
	var slug, orgName, tier, managedBy, settings string
	var active bool
	var description, stripe, skipped *string
	var orgID uuid.UUID
	if err := rig.env.admin.QueryRow(ctx, `SELECT id, slug, name, tier, managed_by, is_active, settings::text, description, stripe_customer_id, onboarding_integration_skipped_at::text
FROM public.organizations`).Scan(&orgID, &slug, &orgName, &tier, &managedBy, &active, &settings, &description, &stripe, &skipped); err != nil {
		t.Fatalf("exactly one organization expected: %v", err)
	}
	if slug != registerorg.Slug(name, userID) || slug != "acme-corp-"+userID.String()[:8] || orgName != name || tier != "community" ||
		managedBy != "stripe" || !active || settings != "{}" || description != nil || stripe != nil || skipped != nil {
		t.Fatalf("organization row differs from go-api's: slug %q name %q tier %q managed_by %q active %v settings %q", slug, orgName, tier, managedBy, active, settings)
	}
	var role string
	var invitedBy *uuid.UUID
	if err := rig.env.admin.QueryRow(ctx, `SELECT role, invited_by_id FROM public.memberships WHERE user_id = $1 AND org_id = $2`, userID, orgID).Scan(&role, &invitedBy); err != nil || role != "owner" || invitedBy != nil {
		t.Fatalf("membership: role %q invited_by %v err %v, want one owner membership", role, invitedBy, err)
	}
	if n := publicCount(t, rig, ctx, "memberships"); n != 1 {
		t.Fatalf("memberships = %d, want 1", n)
	}
	if n := rig.env.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.principal.provisioned'`); n != 1 {
		t.Fatalf("auth.principal.provisioned rows = %d, want 1", n)
	}
}

func TestRegisterWithoutAnOrganizationNameCreatesNoOrganization(t *testing.T) {
	ctx := context.Background()
	rig := newEndRig(t, ctx)
	outcome, err := rig.flows.Register(ctx, RegisterRequest{Email: "quiet@example.com", Password: "Correct-horse-battery-9"})
	if err != nil || outcome.Kind != FlowOK {
		t.Fatalf("Register = %v, %v", outcome.Kind, err)
	}
	if n := rig.env.count(t, ctx, `SELECT count(*) FROM public.users WHERE email = 'quiet@example.com'`); n != 1 {
		t.Fatalf("users = %d, want 1", n)
	}
	if o, m := publicCount(t, rig, ctx, "organizations"), publicCount(t, rig, ctx, "memberships"); o != 0 || m != 0 {
		t.Fatalf("organizations %d, memberships %d, want none", o, m)
	}
}

func TestAnEmailCollisionRollsBackTheOrganizationAndMembership(t *testing.T) {
	ctx := context.Background()
	rig := newEndRig(t, ctx)
	hash, err := cheapHash("Correct-horse-battery-9")
	if err != nil {
		t.Fatal(err)
	}
	store := rig.accounts.Store
	first, err := store.ProvisionLocal(ctx, identity.LocalProvisionRequest{Email: "taken@example.com", PasswordHash: &hash, Organization: &identity.NewOrganization{Name: "First"}})
	if err != nil || first == uuid.Nil {
		t.Fatalf("first ProvisionLocal: %v", err)
	}
	if _, err := store.ProvisionLocal(ctx, identity.LocalProvisionRequest{Email: "taken@example.com", PasswordHash: &hash, Organization: &identity.NewOrganization{Name: "Second"}}); !errors.Is(err, identity.ErrEmailTaken) {
		t.Fatalf("second ProvisionLocal err = %v, want ErrEmailTaken", err)
	}
	if _, err := rig.env.admin.Exec(ctx, `INSERT INTO public.users (id, email, auth_provider, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES (gen_random_uuid(), 'legacy@example.com', 'local', true, false, false, 0, now(), now())`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ProvisionLocal(ctx, identity.LocalProvisionRequest{Email: "legacy@example.com", PasswordHash: &hash, Organization: &identity.NewOrganization{Name: "Third"}}); !errors.Is(err, identity.ErrEmailTaken) {
		t.Fatalf("a go-api held address: err = %v, want ErrEmailTaken", err)
	}
	if o, m := publicCount(t, rig, ctx, "organizations"), publicCount(t, rig, ctx, "memberships"); o != 1 || m != 1 {
		t.Fatalf("organizations %d, memberships %d, want only the first sign-up's 1 and 1", o, m)
	}
	if n := rig.env.count(t, ctx, `SELECT count(*) FROM public.organizations WHERE name IN ('Second', 'Third')`); n != 0 {
		t.Fatalf("a rolled-back sign-up left %d organizations", n)
	}
}

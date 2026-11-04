//go:build integration

package social

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

// TestSocialNewUserOnboardsIntoDefaultOrg is the real-PG chain of D6145 (as corrected): a first social
// sign-in creates the user and no organization or membership; the onboarding create_org then creates
// the organization and the owner membership in both schemas; the next sign-in of the same account
// finds the same principal, now a member of that one organization.
func TestSocialNewUserOnboardsIntoDefaultOrg(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, ctx)
	svc := e.service(t, true)
	acct := account{provider: GitHub, id: 1400, email: "chain-user@example.com", verified: true, public: true}

	first, err := svc.Exchange(ctx, GitHub, e.providers.token("chain-first", acct), nil)
	if err != nil || !first.Created {
		t.Fatalf("first Exchange = %+v, %v; want a created user", first, err)
	}
	user := first.PrincipalID
	e.wantNoMembership(t, ctx, user)

	born, err := e.store(true).OnboardOrganization(ctx, identity.OnboardOrganizationRequest{Principal: user, Name: "Chain Org"})
	if err != nil {
		t.Fatalf("OnboardOrganization: %v", err)
	}
	if born.MembershipID == nil {
		t.Fatal("onboarding made no membership")
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM public.organizations p JOIN auth.organizations a ON a.id = p.id
WHERE p.id = $1 AND p.name = 'Chain Org' AND a.name = 'Chain Org' AND p.is_active AND a.disabled_at IS NULL`, born.ID); n != 1 {
		t.Fatalf("organization in both schemas: %d rows, want 1", n)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM public.memberships p JOIN auth.memberships a ON a.id = p.id
WHERE p.id = $1 AND p.org_id = $2 AND p.user_id = $3 AND a.organization_id = $2 AND a.principal_id = $3
  AND p.role = 'owner' AND a.role = 'owner' AND a.status = 'active'`, *born.MembershipID, born.ID, user); n != 1 {
		t.Fatalf("owner membership in both schemas: %d rows, want 1", n)
	}

	again, err := svc.Exchange(ctx, GitHub, e.providers.token("chain-again", acct), nil)
	if err != nil || again.Created || again.PrincipalID != user {
		t.Fatalf("second Exchange = %+v, %v; want the same existing principal %v", again, err, user)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.memberships WHERE principal_id = $1 AND status = 'active'`, user); n != 1 {
		t.Fatalf("active memberships after onboarding = %d, want 1", n)
	}
	var org uuid.UUID
	if err := e.admin.QueryRow(ctx, `SELECT organization_id FROM auth.memberships WHERE principal_id = $1 AND status = 'active'`, user).Scan(&org); err != nil || org != born.ID {
		t.Fatalf("the member's organization = %v, %v; want %v", org, err, born.ID)
	}
}

func (e *env) wantNoMembership(t *testing.T, ctx context.Context, user uuid.UUID) {
	t.Helper()
	if n := e.count(t, ctx, `SELECT (SELECT count(*) FROM public.memberships WHERE user_id = $1)
+ (SELECT count(*) FROM auth.memberships WHERE principal_id = $1)`, user); n != 0 {
		t.Fatalf("a first social sign-in left %d membership rows, want 0", n)
	}
}

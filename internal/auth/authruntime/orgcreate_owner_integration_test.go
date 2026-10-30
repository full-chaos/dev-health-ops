//go:build integration

package authruntime

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

// bareOrg inserts a go-api organization with no member, as go-api's create
// commits it before it sends the first owner.
func (e *routesEnv) bareOrg(t *testing.T, ctx context.Context) uuid.UUID {
	t.Helper()
	org := uuid.New()
	if _, err := e.admin.Exec(ctx, `INSERT INTO organizations (id, slug, name, tier, is_active, settings, managed_by, created_at, updated_at)
VALUES ($1, $2, 'Created Org', 'community', true, '{}', 'stripe', now(), now())`, org, "created-"+org.String()[:8]); err != nil {
		t.Fatalf("seed org: %v", err)
	}
	return org
}

func (e *routesEnv) orgCounts(t *testing.T, ctx context.Context, org uuid.UUID) (authActive, authDisabled, authMembers, publicMembers int) {
	t.Helper()
	if err := e.admin.QueryRow(ctx, `SELECT
  (SELECT count(*) FROM auth.organizations WHERE id = $1 AND disabled_at IS NULL),
  (SELECT count(*) FROM auth.organizations WHERE id = $1 AND disabled_at IS NOT NULL),
  (SELECT count(*) FROM auth.memberships WHERE organization_id = $1 AND status = 'active'),
  (SELECT count(*) FROM public.memberships WHERE org_id = $1)`, org).Scan(&authActive, &authDisabled, &authMembers, &publicMembers); err != nil {
		t.Fatal(err)
	}
	return
}

// I1: go-api's org create sends the first owner with the calling superuser as
// the actor. The superuser holds no membership of the new organization, which
// has none: auth projects the organization and writes the owner in both
// tables. A deactivated owner is added too, as go-api adds one today.
func TestProxyClientAddsTheFirstOwnerOfAnOrganizationWithNoMembers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e, header := newProvisioningEnv(t, ctx)
	client := proxyClient(t, e, header)
	super := e.account(t, ctx, acctMail(), true, true)
	for name, active := range map[string]bool{"active owner": true, "deactivated owner": false} {
		t.Run(name, func(t *testing.T) {
			owner := e.account(t, ctx, acctMail(), false, active)
			org := e.bareOrg(t, ctx)
			added, err := client.AddMember(ctx, org, owner, super, "owner", nil)
			if err != nil || added.Role != "owner" || added.Status != "active" {
				t.Fatalf("add the first owner: %+v %v", added, err)
			}
			if pub, auth := e.memberRows(t, ctx, org, owner); pub != "owner" || auth != "active" {
				t.Fatalf("owner rows: public %q auth %q", pub, auth)
			}
			if a, d, am, pm := e.orgCounts(t, ctx, org); a != 1 || d != 0 || am != 1 || pm != 1 {
				t.Fatalf("auth org active %d disabled %d, auth members %d, public members %d; want 1 0 1 1", a, d, am, pm)
			}
			if pub, auth := e.memberRows(t, ctx, org, super); pub != "" || auth != "" {
				t.Fatalf("the actor got a membership: public %q auth %q", pub, auth)
			}
		})
	}
}

// I2: an owner auth-service does not hold is refused before anything is
// written; the cleanup's deprovision then leaves no active auth organization
// (a 404 when auth never projected it).
func TestProxyClientRefusedFirstOwnerWritesNothingAndDeprovisions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e, header := newProvisioningEnv(t, ctx)
	client := proxyClient(t, e, header)
	super := e.account(t, ctx, acctMail(), true, true)
	owner := uuid.New()
	if _, err := e.admin.Exec(ctx, `INSERT INTO users (id, email, password_hash, auth_provider, is_active, is_verified, is_superuser,
token_version, created_at, updated_at) VALUES ($1, $2, NULL, 'local', true, true, false, 0, now(), now())`, owner, acctMail()); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	org := e.bareOrg(t, ctx)
	_, err := client.AddMember(ctx, org, owner, super, "owner", nil)
	wantRefused(t, "add an owner auth does not hold", err, 404, "principal_not_found")
	if _, _, am, pm := e.orgCounts(t, ctx, org); am != 0 || pm != 0 {
		t.Fatalf("after the refusal: auth members %d, public members %d; want none", am, pm)
	}
	if err := client.Deprovision(ctx, org, super); err != nil {
		wantRefused(t, "deprovision", err, 404, "organization_not_found")
	}
	if a, _, am, pm := e.orgCounts(t, ctx, org); a != 0 || am != 0 || pm != 0 {
		t.Fatalf("after the deprovision: active auth org %d, auth members %d, public members %d; want none", a, am, pm)
	}
}

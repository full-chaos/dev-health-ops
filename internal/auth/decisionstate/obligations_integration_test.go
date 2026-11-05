//go:build integration

package decisionstate

import (
	"context"
	"slices"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
)

// The allow of a repository-filtered action takes its repository member from the grant rows the real snapshot
// read: live keys only (expired, revoked and other-organization rows are left out), repository_scope organization
// with no grant row, and [] when no row is live.
func TestTheRepositoryObligationIsTheLiveGrantsOfTheRealSnapshot(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	decide := func(user, org uuid.UUID) authorization.Decision {
		t.Helper()
		d := authorization.NewEvaluator().Evaluate(authorization.Request{Action: "analytics.read", ResourceKind: "organization", OrganizationID: org.String()},
			e.snapshot(t, ctx, user, org.String()))
		if !d.Allowed || d.Obligations == nil || d.Obligations.TenantScope != org.String() {
			t.Fatalf("decision %+v obligations %+v, want an allow with the tenant scope", d, d.Obligations)
		}
		return d
	}
	member := func(org uuid.UUID) uuid.UUID {
		user := e.user(t, ctx, true, false)
		e.exec(t, ctx, `INSERT INTO public.memberships (id, user_id, org_id, role, joined_at) VALUES ($1, $2, $3, 'member', now())`, uuid.New(), user, org)
		return user
	}
	org, other := e.org(t, ctx), e.org(t, ctx)
	owner := e.user(t, ctx, true, false)

	plain := member(org)
	if o := decide(plain, org).Obligations; o.RepositoryScope != authorization.RepositoryScopeOrganization || o.RepositoryRestricted {
		t.Fatalf("no grant row: %+v, want repository_scope organization", o)
	}

	narrowed := member(org)
	e.armed(t, ctx, `INSERT INTO auth.resource_grants (principal_id, organization_id, role_key, resource_kind, resource_id) VALUES ($1, $2, 'viewer', 'repository', 'acme/web')`, narrowed, org)
	e.armed(t, ctx, `INSERT INTO auth.resource_grants (principal_id, organization_id, role_key, resource_kind, resource_id) VALUES ($1, $2, 'viewer', 'repository', 'acme/api')`, narrowed, org)
	e.armed(t, ctx, `INSERT INTO auth.resource_grants (principal_id, organization_id, role_key, resource_kind, resource_id, expires_at) VALUES ($1, $2, 'viewer', 'repository', 'acme/old', now() - interval '1 day')`, narrowed, org)
	e.armed(t, ctx, `INSERT INTO auth.resource_grants (principal_id, organization_id, role_key, resource_kind, resource_id, revoked_at, revoked_by) VALUES ($1, $2, 'viewer', 'repository', 'acme/gone', now(), $3)`, narrowed, org, owner)
	e.armed(t, ctx, `INSERT INTO auth.resource_grants (principal_id, organization_id, role_key, resource_kind, resource_id) VALUES ($1, $2, 'viewer', 'repository', 'acme/elsewhere')`, narrowed, other)
	if o := decide(narrowed, org).Obligations; o.RepositoryScope != "" || !o.RepositoryRestricted || !slices.Equal(o.RepositoryAllowlist, []string{"acme/api", "acme/web"}) {
		t.Fatalf("live and dead rows: %+v, want [acme/api acme/web]", o)
	}

	e.armed(t, ctx, `UPDATE auth.resource_grants SET revoked_at = now(), revoked_by = $3 WHERE principal_id = $1 AND organization_id = $2 AND revoked_at IS NULL`, narrowed, org, owner)
	if o := decide(narrowed, org).Obligations; o.RepositoryScope != "" || !o.RepositoryRestricted || len(o.RepositoryAllowlist) != 0 {
		t.Fatalf("every row revoked: %+v, want the empty allowlist (none)", o)
	}
}

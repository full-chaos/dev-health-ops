//go:build integration

package decisionstate

import (
	"context"
	"slices"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
)

// Product rule 2 (catalog version 14): a repository grant never narrows an organization read. On the real snapshot a
// member with no grant row, one with live and dead rows, and one whose rows are all revoked each get the tenant scope
// only for analytics.read. The snapshot still reads every grant row of the pair in any state (the evaluator decides
// which are live), and no row of another organization.
func TestProductRule2RealSnapshotGrantRowsNeverNarrowAnOrgRead(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	orgWide := func(name string, user, org uuid.UUID) {
		t.Helper()
		d := authorization.NewEvaluator().Evaluate(authorization.Request{Action: "analytics.read", ResourceKind: "organization", OrganizationID: org.String()},
			e.snapshot(t, ctx, user, org.String()))
		if !d.Allowed || d.Obligations == nil || d.Obligations.TenantScope != org.String() {
			t.Fatalf("%s: decision %+v obligations %+v, want an allow with the tenant scope", name, d, d.Obligations)
		}
		if o := d.Obligations; o.RepositoryScope != "" || o.RepositoryRestricted || len(o.RepositoryAllowlist) != 0 {
			t.Fatalf("%s: %+v, want the tenant scope only (org-wide)", name, o)
		}
	}
	member := func(org uuid.UUID) uuid.UUID {
		user := e.user(t, ctx, true, false)
		e.exec(t, ctx, `INSERT INTO public.memberships (id, user_id, org_id, role, joined_at) VALUES ($1, $2, $3, 'member', now())`, uuid.New(), user, org)
		return user
	}
	org, other := e.org(t, ctx), e.org(t, ctx)
	owner := e.user(t, ctx, true, false)

	orgWide("no grant row", member(org), org)

	narrowed := member(org)
	e.armed(t, ctx, `INSERT INTO auth.resource_grants (principal_id, organization_id, role_key, resource_kind, resource_id) VALUES ($1, $2, 'viewer', 'repository', 'acme/web')`, narrowed, org)
	e.armed(t, ctx, `INSERT INTO auth.resource_grants (principal_id, organization_id, role_key, resource_kind, resource_id) VALUES ($1, $2, 'viewer', 'repository', 'acme/api')`, narrowed, org)
	e.armed(t, ctx, `INSERT INTO auth.resource_grants (principal_id, organization_id, role_key, resource_kind, resource_id, expires_at) VALUES ($1, $2, 'viewer', 'repository', 'acme/old', now() - interval '1 day')`, narrowed, org)
	e.armed(t, ctx, `INSERT INTO auth.resource_grants (principal_id, organization_id, role_key, resource_kind, resource_id, revoked_at, revoked_by) VALUES ($1, $2, 'viewer', 'repository', 'acme/gone', now(), $3)`, narrowed, org, owner)
	e.armed(t, ctx, `INSERT INTO auth.resource_grants (principal_id, organization_id, role_key, resource_kind, resource_id) VALUES ($1, $2, 'viewer', 'repository', 'acme/elsewhere')`, narrowed, other)
	rows, err := e.snapshot(t, ctx, narrowed, org.String()).Grants(org.String())
	if err != nil {
		t.Fatalf("grants: %v", err)
	}
	var keys []string
	for _, g := range rows {
		keys = append(keys, g.ResourceKey)
	}
	slices.Sort(keys)
	if !slices.Equal(keys, []string{"acme/api", "acme/gone", "acme/old", "acme/web"}) {
		t.Fatalf("snapshot grant rows %v, want every row of the pair in any state and none of another organization", keys)
	}
	orgWide("live and dead rows", narrowed, org)

	e.armed(t, ctx, `UPDATE auth.resource_grants SET revoked_at = now(), revoked_by = $3 WHERE principal_id = $1 AND organization_id = $2 AND revoked_at IS NULL`, narrowed, org, owner)
	orgWide("every row revoked", narrowed, org)
}

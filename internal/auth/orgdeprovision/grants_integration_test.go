//go:build integration

package orgdeprovision

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

// A deprovision ends every live grant of the organization in its commit, with
// an audit row and an outbox event each, so a reactivated organization and a
// re-added member do not get old repository grants back.
func TestDeprovisionEndsTheOrganizationsLiveGrants(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	p := e.populate(t, ctx)
	rows, err := e.admin.Query(ctx, `SELECT principal_id FROM auth.memberships WHERE organization_id = $1 ORDER BY id LIMIT 2`, p.org)
	if err != nil {
		t.Fatal(err)
	}
	var members []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		members = append(members, id)
	}
	rows.Close()
	if len(members) == 0 {
		t.Fatal("populate made no membership")
	}
	actor, _ := e.seedUser(t, ctx)
	var grants []uuid.UUID
	for i, m := range members {
		id := uuid.New()
		grants = append(grants, id)
		if _, err := e.admin.Exec(ctx, `INSERT INTO auth.resource_grants (id, principal_id, organization_id, role_key, resource_kind, resource_id, granted_by)
VALUES ($1, $2, $3, 'viewer', 'repository', $4, $5)`, id, m, p.org, []string{"acme/api", "*"}[i%2], actor); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := e.service.Deprovision(ctx, Input{OrgID: p.org, Actor: Actor{PrincipalID: &actor}, Authorize: allowOrg}); err != nil {
		t.Fatalf("Deprovision: %v", err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.resource_grants WHERE organization_id = $1 AND revoked_at IS NULL`, p.org); n != 0 {
		t.Fatalf("%d live grants after the deprovision, want 0", n)
	}
	for _, id := range grants {
		if n := e.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events WHERE event_type = 'resource_grant.revoked' AND aggregate_id = $1`, "grt_"+id.String()); n != 1 {
			t.Fatalf("grant %s outbox rows = %d, want 1", id, n)
		}
		if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.resource_grant.revoked' AND attributes->>'target_id' = $1`, id.String()); n != 1 {
			t.Fatalf("grant %s audit rows = %d, want 1", id, n)
		}
	}
}

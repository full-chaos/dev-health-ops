//go:build integration

package membership

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

func (e *env) seedGrant(t *testing.T, ctx context.Context, org, principal, by uuid.UUID, key string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	e.adminTx(t, ctx, `INSERT INTO auth.resource_grants (id, principal_id, organization_id, role_key, resource_kind, resource_id, granted_by)
VALUES ($1, $2, $3, 'viewer', 'repository', $4, $5)`, id, principal, org, key, by)
	return id
}

// adminTx runs a fixture write on a table the commit guard protects: the
// guard's triggers are off for this one transaction.
func (e *env) adminTx(t *testing.T, ctx context.Context, sql string, args ...any) {
	t.Helper()
	tx, err := e.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SET LOCAL session_replication_role = replica`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, sql, args...); err != nil {
		t.Fatalf("fixture write: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

// I5, I6: removing a member ends that pair's live grants in the same
// transaction, with an audit row and an outbox event each; a re-added member
// holds none; another member's and another organization's grants stay.
func TestRemoveEndsThePairsLiveGrantsAndAReAddHasNone(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	org, other := e.seedOrg(t, ctx), e.seedOrg(t, ctx)
	owner, user, peer := e.seedUser(t, ctx), e.seedUser(t, ctx), e.seedUser(t, ctx)
	e.add(t, ctx, org, owner, "owner")
	e.add(t, ctx, org, user, "member")
	e.add(t, ctx, org, peer, "member")
	e.add(t, ctx, other, user, "member")
	g1, g2 := e.seedGrant(t, ctx, org, user, owner, "acme/api"), e.seedGrant(t, ctx, org, user, owner, "*")
	keep, otherOrg := e.seedGrant(t, ctx, org, peer, owner, "acme/api"), e.seedGrant(t, ctx, other, user, owner, "acme/api")
	done := e.seedGrant(t, ctx, org, user, owner, "acme/old")
	e.adminTx(t, ctx, `UPDATE auth.resource_grants SET revoked_at = now(), revoked_by = $2 WHERE id = $1`, done, owner)
	actor := owner
	if _, err := e.store.Remove(ctx, Change{OrgID: org, PrincipalID: user, Actor: Actor{PrincipalID: &actor}, Authorize: allowAll}); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	for _, id := range []uuid.UUID{g1, g2} {
		if n := e.count(t, ctx, `SELECT count(*) FROM auth.resource_grants WHERE id = $1 AND revoked_at IS NOT NULL AND revoked_by = $2`, id, owner); n != 1 {
			t.Fatalf("grant %s not revoked by the actor", id)
		}
		if n := e.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events WHERE event_type = 'resource_grant.revoked' AND aggregate_id = $1`, "grt_"+id.String()); n != 1 {
			t.Fatalf("grant %s outbox rows = %d, want 1", id, n)
		}
		if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.resource_grant.revoked' AND attributes->>'target_id' = $1`, id.String()); n != 1 {
			t.Fatalf("grant %s audit rows = %d, want 1", id, n)
		}
	}
	for _, id := range []uuid.UUID{keep, otherOrg} {
		if n := e.count(t, ctx, `SELECT count(*) FROM auth.resource_grants WHERE id = $1 AND revoked_at IS NULL`, id); n != 1 {
			t.Fatalf("grant %s was ended by another pair's removal", id)
		}
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events WHERE event_type = 'resource_grant.revoked' AND aggregate_id = $1`, "grt_"+done.String()); n != 0 {
		t.Fatal("an already revoked grant got a second event")
	}
	e.add(t, ctx, org, user, "member")
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.resource_grants WHERE organization_id = $1 AND principal_id = $2 AND revoked_at IS NULL`, org, user); n != 0 {
		t.Fatalf("a re-added member holds %d live grants, want 0", n)
	}
}

// A removal of a member who holds no grant writes no grant row or event.
func TestRemoveWithoutGrantsWritesNoGrantRows(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	org := e.seedOrg(t, ctx)
	owner, user := e.seedUser(t, ctx), e.seedUser(t, ctx)
	e.add(t, ctx, org, owner, "owner")
	e.add(t, ctx, org, user, "member")
	if _, err := e.store.Remove(ctx, Change{OrgID: org, PrincipalID: user, Authorize: allowAll}); err != nil {
		t.Fatal(err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.resource_grant.revoked' AND organization_id = $1`, org.String()); n != 0 {
		t.Fatalf("grant audit rows = %d, want 0", n)
	}
}

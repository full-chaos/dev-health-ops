//go:build integration

package admin

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

// removeMembership deletes the membership and revokes the member's refresh
// tokens for the org in one transaction (D6507): when the revoke fails, the
// membership is still there. Called on a pool, not inside a caller's
// transaction, so only removeMembership's own transaction can roll it back.
func TestRemoveMembershipRollsBackWhenTheTokenRevokeFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	pgschema.Apply(ctx, t, pool)

	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, sql)
		}
	}
	org, owner, member := uuid.New(), uuid.New(), uuid.New()
	exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, 'remove-tx', 'remove-tx', 'community', 'stripe', true, now(), now())`, org)
	for _, user := range []struct {
		id    uuid.UUID
		email string
		role  string
	}{{owner, "remove-tx-owner@example.com", "owner"}, {member, "remove-tx-member@example.com", "member"}} {
		exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, true, true, false, 0, now(), now())`, user.id, user.email)
		exec(`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, now(), now(), now())`, uuid.New(), org, user.id, user.role)
	}
	exec(`INSERT INTO refresh_tokens (id, user_id, org_id, token_hash, family_id, expires_at)
VALUES ($1, $2, $3, 'remove-tx-hash', $4, now() + interval '1 day')`, uuid.New(), member, org, uuid.New())
	exec(`CREATE FUNCTION remove_tx_refuse_revoke() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'refresh token revoke refused by the test'; END $$`)
	exec(`CREATE TRIGGER remove_tx_refuse_revoke BEFORE UPDATE ON refresh_tokens FOR EACH ROW EXECUTE FUNCTION remove_tx_refuse_revoke()`)

	state := func() (memberships, live int) {
		t.Helper()
		if err := pool.QueryRow(ctx, `SELECT
  (SELECT count(*) FROM memberships WHERE org_id = $1 AND user_id = $2),
  (SELECT count(*) FROM refresh_tokens WHERE org_id = $1 AND user_id = $2 AND revoked_at IS NULL)`, org, member).Scan(&memberships, &live); err != nil {
			t.Fatal(err)
		}
		return memberships, live
	}
	store := pgStore{Pool: pool}

	removed, err := store.removeMembership(ctx, org, member)
	if err == nil || removed {
		t.Fatalf("removeMembership with a failing revoke = (%v, %v), want an error", removed, err)
	}
	if memberships, live := state(); memberships != 1 || live != 1 {
		t.Fatalf("after the failed revoke: %d memberships, %d live tokens; want 1, 1 (rolled back)", memberships, live)
	}

	exec(`DROP TRIGGER remove_tx_refuse_revoke ON refresh_tokens`)
	removed, err = store.removeMembership(ctx, org, member)
	if err != nil || !removed {
		t.Fatalf("removeMembership = (%v, %v), want (true, nil)", removed, err)
	}
	if memberships, live := state(); memberships != 0 || live != 0 {
		t.Fatalf("after the removal: %d memberships, %d live tokens; want 0, 0", memberships, live)
	}
}

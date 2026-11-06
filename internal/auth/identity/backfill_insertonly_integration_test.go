//go:build integration

package identity

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

// authRows is the auth schema's rows of one principal as text: principals, users and external identities.
func (e *env) authRows(t *testing.T, ctx context.Context, id uuid.UUID) string {
	t.Helper()
	var text string
	if err := e.admin.QueryRow(ctx, `SELECT
  coalesce((SELECT row_to_json(p)::text FROM auth.principals p WHERE p.id = $1), '') || '|' ||
  coalesce((SELECT row_to_json(u)::text FROM auth.users u WHERE u.principal_id = $1), '') || '|' ||
  coalesce((SELECT json_agg(row_to_json(x) ORDER BY row_to_json(x)::text)::text FROM auth.external_identities x WHERE x.principal_id = $1), '')`, id).Scan(&text); err != nil {
		t.Fatal(err)
	}
	return text
}

// CHAOS-13107 (acp ruling on #5795): compose's auth-users-backfill is insert-only. An auth user that auth-service changed
// survives a second insert-only run from the stale public.users values byte-equal, and the run writes no audit row. The
// changes go through auth-service's own write paths: the password through the owned store's SetPasswordHash, the name
// through audit.Commit as the runtime login (the projection's commit; the security tables refuse any other write). The
// default run would set both back (applyBackfillCredentials).
func TestInsertOnlyBackfillKeepsAnAuthUserChangedThroughAuthService(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	email := uniqueEmail("bfio")
	id := e.seedUser(t, ctx, email, true, true)
	stale := BackfillTarget{ID: id, Email: email, DisplayName: "Public Name", Active: true, Verified: true,
		PasswordHash: bcryptFor(t, "public-test-material"), InsertOnly: true}
	if res, err := e.store.BackfillUser(ctx, stale, false); err != nil || !res.Inserted {
		t.Fatalf("first insert-only run: %+v %v", res, err)
	}
	// auth-service changes the password and the name; the backfill target keeps the old values.
	if err := e.owned.SetPasswordHash(ctx, id, *bcryptFor(t, "changed-through-auth-service"), Actor{}); err != nil {
		t.Fatalf("change the password through auth-service: %v", err)
	}
	principal := id.String()
	if err := audit.Commit(ctx, e.runtime, e.owned.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			_, err := tx.Exec(ctx, `UPDATE auth.principals SET display_name = 'Changed Through Auth', updated_at = clock_timestamp() WHERE id = $1`, id)
			return err
		},
		Audit:  localAudit("auth.principal.projection_updated", "principal_projected", principal, Actor{}),
		Events: []audit.OutboxEvent{userEvent(principal, "user.credentials_changed")},
	}); err != nil {
		t.Fatalf("change the name through audit.Commit: %v", err)
	}
	before := e.authRows(t, ctx, id)
	if !strings.Contains(before, "Changed Through Auth") {
		t.Fatalf("the name change did not land: %s", before)
	}
	auditBefore := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE subject_principal_id = $1`, id)
	res, err := e.store.BackfillUser(ctx, stale, false)
	if after := e.authRows(t, ctx, id); after != before {
		t.Fatalf("the insert-only run changed the auth user (%+v, %v):\n before %s\n after  %s", res, err, before, after)
	}
	if after := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE subject_principal_id = $1`, id); after != auditBefore {
		t.Fatalf("the insert-only run wrote audit rows: %d -> %d", auditBefore, after)
	}
	if err != nil || !res.Kept || res.Updated || res.Inserted {
		t.Fatalf("second insert-only run: %+v %v", res, err)
	}
}

// A public user with no auth twin is inserted by the insert-only run, as by the default one.
func TestInsertOnlyBackfillInsertsAMissingUser(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	target := BackfillTarget{ID: uuid.New(), Email: uniqueEmail("bfin"), DisplayName: "New User", Active: true, Verified: true,
		PasswordHash: bcryptFor(t, "insert-only-test-material"), InsertOnly: true}
	res, err := e.store.BackfillUser(ctx, target, false)
	if err != nil || !res.Inserted || res.Kept {
		t.Fatalf("insert-only run: %+v %v", res, err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.principals p JOIN auth.users u ON u.principal_id = p.id
WHERE p.id = $1 AND p.display_name = 'New User' AND u.password_hash IS NOT NULL AND u.email_verified_at IS NOT NULL`, target.ID); n != 1 {
		t.Fatalf("the missing user was not inserted as mapped: %d", n)
	}
}

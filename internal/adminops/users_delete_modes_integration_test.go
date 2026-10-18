//go:build integration

package adminops

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// CHAOS-10019 (D6051): `dho admin users delete` is a soft delete by default and a permanent delete with --force, on a real
// PostgreSQL with the full migration chain.

func (db *database) userID(t *testing.T, email string) string {
	t.Helper()
	var id string
	if err := db.conn.QueryRow(context.Background(), `SELECT id::text FROM users WHERE email = $1`, email).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

func (db *database) exec(t *testing.T, query string, args ...any) {
	t.Helper()
	if _, err := db.conn.Exec(context.Background(), query, args...); err != nil {
		t.Fatalf("%v\n%s", err, query)
	}
}

func createUser(t *testing.T, db *database, email, username string) string {
	t.Helper()
	if code, out := goVerb(t, db, []string{"users", "create", "--email", email, "--password", "password1", "--username", username}); code != 0 {
		t.Fatalf("create %s: exit %d %s", email, code, out)
	}
	return db.userID(t, email)
}

func TestUsersDeleteDefaultIsASoftDelete(t *testing.T) {
	db := startDatabase(t)
	userID := createUser(t, db, "soft@example.com", "soft")
	db.seedRefreshToken(t, "soft@example.com")
	db.exec(t, `INSERT INTO password_reset_tokens (id, user_id, token_hash, expires_at, created_at) VALUES ($1, $2, 'h1', now() + interval '1 hour', now())`, uuid.New(), userID)
	db.exec(t, `INSERT INTO email_verification_tokens (id, user_id, token_hash, expires_at, created_at) VALUES ($1, $2, 'h2', now() + interval '1 hour', now())`, uuid.New(), userID)
	var versionBefore int
	if err := db.conn.QueryRow(context.Background(), `SELECT token_version FROM users WHERE id = $1`, userID).Scan(&versionBefore); err != nil {
		t.Fatal(err)
	}

	code, out, logs := executeAdmin(t, db, "users", "delete", "--email", "soft@example.com", "--yes")
	if code != 0 || !strings.HasPrefix(out, "Soft-deleted user: soft@example.com (id: ") {
		t.Fatalf("soft delete: exit %d, stdout %q, stderr %q", code, out, logs)
	}
	if !strings.Contains(logs, `"msg":"admin: user soft-deleted"`) || strings.Contains(logs, "@example.com") {
		t.Fatalf("the log is %q", logs)
	}
	var active bool
	var versionAfter int
	if err := db.conn.QueryRow(context.Background(), `SELECT is_active, token_version FROM users WHERE id = $1`, userID).Scan(&active, &versionAfter); err != nil {
		t.Fatalf("the user row is gone: %v", err)
	}
	if active || versionAfter != versionBefore+1 {
		t.Fatalf("is_active %v, token_version %d -> %d; want false and +1", active, versionBefore, versionAfter)
	}
	if n := db.count(t, `SELECT count(*) FROM refresh_tokens WHERE user_id = $1 AND revoked_at IS NULL`, userID); n != 0 {
		t.Fatalf("%d live refresh tokens left", n)
	}
	if n := db.count(t, `SELECT count(*) FROM password_reset_tokens WHERE user_id = $1`, userID) +
		db.count(t, `SELECT count(*) FROM email_verification_tokens WHERE user_id = $1`, userID); n != 0 {
		t.Fatalf("%d one-time tokens left", n)
	}

	// the same call again is a no-op that says so; the row is still there
	code, out, _ = executeAdmin(t, db, "users", "delete", "--email", "soft@example.com", "--yes")
	if code != 0 || !strings.HasPrefix(out, "User already soft-deleted: soft@example.com") {
		t.Fatalf("second soft delete: exit %d, stdout %q", code, out)
	}
	// a soft delete does not free the row: --force then deletes it
	code, out, _ = executeAdmin(t, db, "users", "delete", "--email", "soft@example.com", "--force", "--yes")
	if code != 0 || !strings.HasPrefix(out, "Permanently deleted user: soft@example.com") {
		t.Fatalf("force after soft: exit %d, stdout %q", code, out)
	}
	if n := db.count(t, `SELECT count(*) FROM users WHERE id = $1`, userID); n != 0 {
		t.Fatal("the user is still there after --force")
	}
}

func TestUsersDeleteForceKeepsBillingRefundAndImpersonationRowsWithANullUser(t *testing.T) {
	db := startDatabase(t)
	gone := createUser(t, db, "gone@example.com", "gone")
	other := createUser(t, db, "other@example.com", "other")
	var orgID string
	if err := db.conn.QueryRow(context.Background(), `SELECT id::text FROM organizations LIMIT 1`).Scan(&orgID); err != nil {
		if code, out := goVerb(t, db, []string{"orgs", "create", "--name", "Acme", "--owner-email", "other@example.com"}); code != 0 {
			t.Fatalf("org: exit %d %s", code, out)
		}
		if err := db.conn.QueryRow(context.Background(), `SELECT id::text FROM organizations LIMIT 1`).Scan(&orgID); err != nil {
			t.Fatal(err)
		}
	}
	db.exec(t, `INSERT INTO billing_audit_log (id, org_id, actor_id, action, resource_type, resource_id, description, created_at)
VALUES ($1, $2, $3, 'checkout.created', 'checkout', $2, 'Checkout', now()), ($4, $2, $5, 'checkout.created', 'checkout', $2, 'Other actor', now())`,
		uuid.New(), orgID, gone, uuid.New(), other)
	db.exec(t, `INSERT INTO refunds (id, org_id, stripe_refund_id, stripe_charge_id, amount, currency, status, initiated_by, metadata, created_at, updated_at)
VALUES ($1, $2, 're_gone', 'ch_gone', 100, 'usd', 'succeeded', $3, '{}', now(), now())`, uuid.New(), orgID, gone)
	db.exec(t, `INSERT INTO impersonation_sessions (id, admin_user_id, target_user_id, target_org_id, target_role, created_at, expires_at, ended_at)
VALUES ($1, $2, $3, $4, 'member', now(), now() + interval '1 hour', now()), ($5, $3, $2, $4, 'member', now(), now() + interval '1 hour', now())`,
		uuid.New(), other, gone, orgID, uuid.New())
	db.exec(t, `INSERT INTO audit_logs (id, org_id, user_id, action, resource_type, resource_id, created_at) VALUES ($1, $2, $3::uuid, 'user.login', 'user', $4, now())`, uuid.New(), orgID, gone, gone)
	db.seedRefreshToken(t, "gone@example.com")

	code, out, logs := executeAdmin(t, db, "users", "delete", "--email", "gone@example.com", "--force", "--yes")
	if code != 0 || !strings.HasPrefix(out, "Permanently deleted user: gone@example.com") {
		t.Fatalf("force delete: exit %d, stdout %q, stderr %q", code, out, logs)
	}
	for _, want := range []string{"billing_audit_log", "refunds", "impersonation_sessions.admin_user_id", "impersonation_sessions.target_user_id"} {
		if !strings.Contains(out, want) {
			t.Errorf("the output does not name %s: %q", want, out)
		}
	}
	if n := db.count(t, `SELECT count(*) FROM users WHERE id = $1`, gone); n != 0 {
		t.Fatal("the user is still there")
	}
	if n := db.count(t, `SELECT count(*) FROM billing_audit_log WHERE actor_id IS NULL AND description = 'Checkout'`); n != 1 {
		t.Fatalf("billing audit row kept with a null actor: %d", n)
	}
	if n := db.count(t, `SELECT count(*) FROM billing_audit_log WHERE actor_id = $1`, other); n != 1 {
		t.Fatal("a billing audit row of another actor was touched")
	}
	if n := db.count(t, `SELECT count(*) FROM refunds WHERE stripe_refund_id = 're_gone' AND initiated_by IS NULL`); n != 1 {
		t.Fatalf("refund row kept with a null initiator: %d", n)
	}
	if n := db.count(t, `SELECT count(*) FROM impersonation_sessions WHERE admin_user_id IS NULL`) +
		db.count(t, `SELECT count(*) FROM impersonation_sessions WHERE target_user_id IS NULL`); n != 2 {
		t.Fatalf("impersonation rows kept with a null user: %d, want 2", n)
	}
	if n := db.count(t, `SELECT count(*) FROM audit_logs WHERE user_id IS NULL AND action = 'user.login'`); n != 1 {
		t.Fatalf("audit_logs row kept with a null user: %d", n)
	}
	if n := db.count(t, `SELECT count(*) FROM refresh_tokens rt WHERE NOT EXISTS (SELECT 1 FROM users u WHERE u.id = rt.user_id)`); n != 0 {
		t.Fatal("a refresh token outlived its user")
	}
	if strings.Contains(logs, "@example.com") || !strings.Contains(logs, `"msg":"admin: user permanently deleted"`) {
		t.Fatalf("the log is %q", logs)
	}
}

func TestUsersDeleteForceFailsLoudlyOnAReferenceThatCannotBeKept(t *testing.T) {
	db := startDatabase(t)
	userID := createUser(t, db, "pinned2@example.com", "pinned2")
	db.exec(t, `CREATE TABLE zz_pinning_records (id uuid PRIMARY KEY, user_id uuid NOT NULL REFERENCES users(id) ON DELETE RESTRICT)`)
	db.exec(t, `INSERT INTO zz_pinning_records (id, user_id) VALUES ($1, $2)`, uuid.New(), userID)

	code, out, logs := executeAdmin(t, db, "users", "delete", "--email", "pinned2@example.com", "--force", "--yes")
	if code == 0 || !strings.Contains(out+logs, "zz_pinning_records") || !strings.Contains(out+logs, "NOT NULL") {
		t.Fatalf("force delete over a NOT NULL RESTRICT reference: exit %d, stdout %q, stderr %q", code, out, logs)
	}
	if n := db.count(t, `SELECT count(*) FROM users WHERE id = $1`, userID); n != 1 {
		t.Fatal("the refused delete removed the user")
	}
	// nothing was half-done: every reference that could be nulled is still in place
	if n := db.count(t, `SELECT count(*) FROM zz_pinning_records WHERE user_id = $1`, userID); n != 1 {
		t.Fatal("the refused delete changed the pinning row")
	}
	// the soft delete is not stopped by it
	if code, out, logs := executeAdmin(t, db, "users", "delete", "--email", "pinned2@example.com", "--yes"); code != 0 {
		t.Fatalf("soft delete over the same reference: exit %d, stdout %q, stderr %q", code, out, logs)
	}
}

func TestUsersDeleteSoftOfADeactivatedUserStillEndsItsCredentials(t *testing.T) {
	db := startDatabase(t)
	userID := createUser(t, db, "deactivated@example.com", "deactivated")
	db.seedRefreshToken(t, "deactivated@example.com")
	db.exec(t, `INSERT INTO password_reset_tokens (id, user_id, token_hash, expires_at, created_at) VALUES ($1, $2, 'h1', now() + interval '1 hour', now())`, uuid.New(), userID)
	if code, out := goVerb(t, db, []string{"users", "update", "--email", "deactivated@example.com", "--no-active"}); code != 0 {
		t.Fatalf("deactivate: exit %d %s", code, out)
	}
	var versionBefore int
	if err := db.conn.QueryRow(context.Background(), `SELECT token_version FROM users WHERE id = $1`, userID).Scan(&versionBefore); err != nil {
		t.Fatal(err)
	}
	code, out, logs := executeAdmin(t, db, "users", "delete", "--email", "deactivated@example.com", "--yes")
	if code != 0 || !strings.HasPrefix(out, "Soft-deleted user: deactivated@example.com") {
		t.Fatalf("soft delete of a deactivated user: exit %d, stdout %q, stderr %q", code, out, logs)
	}
	var versionAfter int
	if err := db.conn.QueryRow(context.Background(), `SELECT token_version FROM users WHERE id = $1`, userID).Scan(&versionAfter); err != nil {
		t.Fatal(err)
	}
	if versionAfter != versionBefore+1 {
		t.Fatalf("token_version %d -> %d; want +1", versionBefore, versionAfter)
	}
	if n := db.count(t, `SELECT count(*) FROM refresh_tokens WHERE user_id = $1 AND revoked_at IS NULL`, userID) +
		db.count(t, `SELECT count(*) FROM password_reset_tokens WHERE user_id = $1`, userID); n != 0 {
		t.Fatalf("%d live credentials left on a deactivated user after delete", n)
	}
	// a repeat has nothing left to end and says so
	code, out, _ = executeAdmin(t, db, "users", "delete", "--email", "deactivated@example.com", "--yes")
	if code != 0 || !strings.HasPrefix(out, "User already soft-deleted: deactivated@example.com") {
		t.Fatalf("repeat soft delete: exit %d, stdout %q", code, out)
	}
}

func TestUsersDeleteForceCountsRowsTheDatabaseNullsWithTheUser(t *testing.T) {
	db := startDatabase(t)
	userID := createUser(t, db, "setnull@example.com", "setnull")
	db.exec(t, `CREATE TABLE zz_setnull_records (id uuid PRIMARY KEY, user_id uuid REFERENCES users(id) ON DELETE SET NULL)`)
	db.exec(t, `INSERT INTO zz_setnull_records (id, user_id) VALUES ($1, $2)`, uuid.New(), userID)

	code, out, logs := executeAdmin(t, db, "users", "delete", "--email", "setnull@example.com", "--force", "--yes")
	if code != 0 || !strings.Contains(out, "kept 1 row(s) in zz_setnull_records.user_id") {
		t.Fatalf("force delete: exit %d, stdout %q, stderr %q", code, out, logs)
	}
	if !strings.Contains(logs, `"rows_kept_with_null_user":1`) {
		t.Fatalf("the Info line does not count the SET NULL row: %q", logs)
	}
	if n := db.count(t, `SELECT count(*) FROM zz_setnull_records WHERE user_id IS NULL`); n != 1 {
		t.Fatalf("the SET NULL row was not kept with a null user: %d", n)
	}
}

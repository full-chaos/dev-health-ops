//go:build integration

package authschema

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// m18Key inserts one signing_keys row as the migration role inside an armed
// transaction (the way the rotation operator writes).
func m18Key(t *testing.T, ctx context.Context, env *authFixture, kid, x, status string) error {
	t.Helper()
	return m13Armed(t, ctx, env.migration, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO auth.signing_keys (kid, algorithm, public_key_jwk, custody_ref, custody_kind, status, revoked_at)
			VALUES ($1, 'EdDSA', jsonb_build_object('kty', 'OKP', 'crv', 'Ed25519', 'x', $2::text), $1, 'file', $3::text,
			        CASE WHEN $3::text = 'compromised' THEN now() END)`, kid, x, status)
		return err
	})
}

func m18Armed(t *testing.T, ctx context.Context, env *authFixture, sql string, args ...any) error {
	t.Helper()
	return m13Armed(t, ctx, env.migration, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, sql, args...)
		return err
	})
}

// TestMigration0018SigningKeyRules: the five rotation states only, at most one
// active key, one public key under one kid, a compromised row terminal, a
// key's identity immutable, and every write inside audit.Commit.
func TestMigration0018SigningKeyRules(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuthFixture(t, ctx)

	for _, state := range []string{"pending", "active", "retiring", "retired", "compromised"} {
		if err := m18Key(t, ctx, env, "k-"+state, "x-"+state, state); err != nil {
			t.Fatalf("a %s key was refused: %v", state, err)
		}
	}
	m10Fail(t, m18Key(t, ctx, env, "k-revoked", "x-revoked", "revoked"), "the retired 'revoked' state")
	m10Fail(t, m18Key(t, ctx, env, "k-active-2", "x-active-2", "active"), "a second active key")
	m10Fail(t, m18Key(t, ctx, env, "k-same-x", "x-compromised", "pending"), "a compromised public key under a new kid")
	m10Fail(t, m18Armed(t, ctx, env, `UPDATE auth.signing_keys SET revoked_at = now() WHERE kid = 'k-pending'`),
		"revoked_at on a key that is not compromised")

	// A compromised row never changes and is never removed, inside audit.Commit
	// or not.
	for what, sql := range map[string]string{
		"re-activating a compromised key":   `UPDATE auth.signing_keys SET status = 'pending', revoked_at = NULL WHERE kid = 'k-compromised'`,
		"touching a compromised key":        `UPDATE auth.signing_keys SET updated_at = now() WHERE kid = 'k-compromised'`,
		"deleting a compromised key":        `DELETE FROM auth.signing_keys WHERE kid = 'k-compromised'`,
		"changing a kid":                    `UPDATE auth.signing_keys SET kid = 'renamed' WHERE kid = 'k-pending'`,
		"changing a public key":             `UPDATE auth.signing_keys SET public_key_jwk = '{"x":"other"}'::jsonb WHERE kid = 'k-pending'`,
		"changing an algorithm":             `UPDATE auth.signing_keys SET algorithm = 'RS256' WHERE kid = 'k-pending'`,
		"compromising without a revoked_at": `UPDATE auth.signing_keys SET status = 'compromised' WHERE kid = 'k-pending'`,
	} {
		m10Fail(t, m18Armed(t, ctx, env, sql), what)
	}
	var status string
	if err := env.admin.QueryRow(ctx, `SELECT status FROM auth.signing_keys WHERE kid = 'k-compromised'`).Scan(&status); err != nil || status != "compromised" {
		t.Fatalf("the compromised row = %q, %v", status, err)
	}

	// A legal state change inside audit.Commit is accepted.
	if err := m18Armed(t, ctx, env, `UPDATE auth.signing_keys SET status = 'retired', state_since = now() WHERE kid = 'k-retiring'`); err != nil {
		t.Fatalf("retiring -> retired was refused: %v", err)
	}

	// Outside audit.Commit every write is refused, even the owner's.
	for what, sql := range map[string]string{
		"an unarmed insert": `INSERT INTO auth.signing_keys (kid, algorithm, public_key_jwk, custody_ref, custody_kind, status)
			VALUES ('k-raw', 'EdDSA', '{"x":"raw"}'::jsonb, 'k-raw', 'file', 'pending')`,
		"an unarmed update": `UPDATE auth.signing_keys SET status = 'retired' WHERE kid = 'k-pending'`,
		"an unarmed delete": `DELETE FROM auth.signing_keys WHERE kid = 'k-retired'`,
	} {
		_, err := env.migration.Exec(ctx, sql)
		if code := asPGCode(err); code != insufficientPrivilege {
			t.Errorf("%s by the owner = %v, want SQLSTATE %s from the commit guard", what, err, insufficientPrivilege)
		}
	}

	// The runtime role reads and never writes.
	for what, sql := range map[string]string{
		"insert": `INSERT INTO auth.signing_keys (kid, algorithm, public_key_jwk, custody_ref, custody_kind, status)
			VALUES ('k-rt', 'EdDSA', '{"x":"rt"}'::jsonb, 'k-rt', 'file', 'pending')`,
		"update": `UPDATE auth.signing_keys SET status = 'retired' WHERE kid = 'k-pending'`,
		"delete": `DELETE FROM auth.signing_keys WHERE kid = 'k-retired'`,
	} {
		err := m13Armed(t, ctx, env.runtime, func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx, sql)
			return err
		})
		if code := asPGCode(err); code != insufficientPrivilege {
			t.Errorf("runtime %s inside an armed transaction = %v, want SQLSTATE %s", what, err, insufficientPrivilege)
		}
	}
	var count int
	if err := env.runtime.QueryRow(ctx, `SELECT count(*) FROM auth.signing_keys`).Scan(&count); err != nil || count != 5 {
		t.Fatalf("runtime read of signing_keys = %d, %v; want 5 rows", count, err)
	}
}

// TestMigration0018TurnsRevokedRowsCompromised: a row 0003 allowed as
// 'revoked' becomes 'compromised' with its revoked_at kept (or set).
func TestMigration0018TurnsRevokedRowsCompromised(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuthMigrationOnly(t, ctx)
	migrations, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations {
		if migration.Version == 18 {
			if _, err := env.conn.Exec(ctx, `
				INSERT INTO auth.signing_keys (kid, algorithm, public_key_jwk, custody_ref, custody_kind, status, revoked_at)
				VALUES ('old-revoked', 'EdDSA', '{"x":"a"}'::jsonb, 'r', 'file', 'revoked', NULL),
				       ('old-active-revoked-at', 'EdDSA', '{"x":"b"}'::jsonb, 'r', 'file', 'active', '2026-01-01T00:00:00Z'),
				       ('old-pending', 'EdDSA', '{"x":"c"}'::jsonb, 'r', 'file', 'pending', NULL)`); err != nil {
				t.Fatalf("seed 0017 rows: %v", err)
			}
		}
		if err := applyOne(ctx, env.conn, env.options, migration); err != nil {
			t.Fatalf("apply %04d: %v", migration.Version, err)
		}
		if migration.Version == 18 {
			break
		}
	}
	rows, err := env.conn.Query(ctx, `SELECT kid, status, revoked_at IS NOT NULL FROM auth.signing_keys ORDER BY kid`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	want := map[string]string{"old-active-revoked-at": "compromised", "old-pending": "pending", "old-revoked": "compromised"}
	seen := 0
	for rows.Next() {
		var kid, status string
		var revoked bool
		if err := rows.Scan(&kid, &status, &revoked); err != nil {
			t.Fatal(err)
		}
		seen++
		if status != want[kid] || revoked != (status == "compromised") {
			t.Errorf("%s = %s (revoked_at set %t), want %s", kid, status, revoked, want[kid])
		}
	}
	if err := rows.Err(); err != nil || seen != 3 {
		t.Fatalf("read %d rows, %v", seen, err)
	}
}

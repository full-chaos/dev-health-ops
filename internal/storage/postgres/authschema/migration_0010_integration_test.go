//go:build integration

package authschema

import (
	"context"
	"strings"
	"testing"
	"time"
)

const (
	m10Principal = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	m10Session   = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
)

func m10Fail(t *testing.T, err error, what string) {
	t.Helper()
	if err == nil {
		t.Errorf("%s was accepted, want a refusal", what)
	}
}

func m10Seed(t *testing.T, ctx context.Context, env *authFixture) {
	t.Helper()
	for _, statement := range []string{
		`INSERT INTO auth.principals (id, kind, display_name) VALUES ('` + m10Principal + `', 'user', 'm10')`,
		`INSERT INTO auth.sessions (id, principal_id, token_hash, expires_at)
		 VALUES ('` + m10Session + `', '` + m10Principal + `', '\x0a'::bytea, now() + interval '1 hour')`,
	} {
		if _, err := env.admin.Exec(ctx, statement); err != nil {
			t.Fatalf("seed %q: %v", statement, err)
		}
	}
}

func TestMigration0010SessionAndRefreshColumns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuthFixture(t, ctx)
	m10Seed(t, ctx, env)

	var revision int
	var amr []string
	var authTime *time.Time
	if err := env.admin.QueryRow(ctx,
		`SELECT revision, amr, auth_time FROM auth.sessions WHERE id = $1`, m10Session,
	).Scan(&revision, &amr, &authTime); err != nil {
		t.Fatalf("read the new session columns: %v", err)
	}
	if revision != 1 || len(amr) != 0 || authTime != nil {
		t.Errorf("session defaults = revision %d, amr %v, auth_time %v; want 1, empty, null", revision, amr, authTime)
	}
	err := guardedExec(ctx, env.admin, `UPDATE auth.sessions SET revision = 0 WHERE id = $1`, m10Session)
	m10Fail(t, err, "session revision 0")
	if err == nil || !strings.Contains(err.Error(), "revision") {
		t.Errorf("revision 0 must be refused by the CHECK on revision, got %v", err)
	}

	insert := func(token string) error {
		_, err := env.admin.Exec(ctx, `INSERT INTO auth.refresh_credentials
			(session_id, principal_id, token_hash, expires_at)
			VALUES ($1, $2, $3::bytea, now() + interval '1 day')`,
			m10Session, m10Principal, token)
		return err
	}
	if err := insert(`\x01`); err != nil {
		t.Fatalf("insert a refresh credential: %v", err)
	}
	if err := insert(`\x02`); err != nil {
		t.Fatalf("insert a second refresh credential: %v", err)
	}
	var families int
	if err := env.admin.QueryRow(ctx, `SELECT count(DISTINCT family_id) FROM auth.refresh_credentials`).Scan(&families); err != nil {
		t.Fatalf("read family_id: %v", err)
	}
	if families != 2 {
		t.Errorf("two credentials inserted without a family share %d family id(s); each must start its own", families)
	}

	// legacy_jti_hash is single-use: unique when present, repeatable when null.
	for _, token := range []string{`\x03`, `\x04`} {
		if _, err := env.admin.Exec(ctx, `INSERT INTO auth.refresh_credentials
			(session_id, principal_id, token_hash, expires_at) VALUES ($1, $2, $3::bytea, now() + interval '1 day')`,
			m10Session, m10Principal, token); err != nil {
			t.Fatalf("insert %s: %v", token, err)
		}
	}
	if err := guardedExec(ctx, env.admin, `UPDATE auth.refresh_credentials SET legacy_jti_hash = '\xbeef'::bytea WHERE token_hash = '\x03'::bytea`); err != nil {
		t.Fatalf("set legacy_jti_hash: %v", err)
	}
	err = guardedExec(ctx, env.admin, `UPDATE auth.refresh_credentials SET legacy_jti_hash = '\xbeef'::bytea WHERE token_hash = '\x04'::bytea`)
	m10Fail(t, err, "a second credential with the same legacy_jti_hash")
	if err == nil || !strings.Contains(err.Error(), "legacy_jti_hash") {
		t.Errorf("duplicate legacy_jti_hash must be refused by its unique index, got %v", err)
	}
}

// parent_id is a self-FK with ON DELETE SET NULL: the sweep that deletes an
// expired credential must not fail, and must not take its live children.
func TestMigration0010ParentDeleteKeepsChildrenAndRuntimeCanSweep(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuthFixture(t, ctx)
	m10Seed(t, ctx, env)

	var parent, child string
	if err := env.admin.QueryRow(ctx, `INSERT INTO auth.refresh_credentials
		(session_id, principal_id, token_hash, expires_at) VALUES ($1, $2, '\x11'::bytea, now() - interval '1 day') RETURNING id::text`,
		m10Session, m10Principal).Scan(&parent); err != nil {
		t.Fatalf("insert parent: %v", err)
	}
	if err := env.admin.QueryRow(ctx, `INSERT INTO auth.refresh_credentials
		(session_id, principal_id, token_hash, expires_at, parent_id)
		VALUES ($1, $2, '\x12'::bytea, now() + interval '1 day', $3) RETURNING id::text`,
		m10Session, m10Principal, parent).Scan(&child); err != nil {
		t.Fatalf("insert child with parent_id: %v", err)
	}
	tag, err := env.runtime.Exec(ctx, `DELETE FROM auth.refresh_credentials WHERE expires_at < now()`)
	if err != nil || tag.RowsAffected() != 1 {
		t.Fatalf("runtime sweep = %v, %v; want it to delete exactly the expired parent", tag, err)
	}
	var parentID *string
	if err := env.admin.QueryRow(ctx, `SELECT parent_id::text FROM auth.refresh_credentials WHERE id = $1`, child).Scan(&parentID); err != nil {
		t.Fatalf("the child did not survive its parent's deletion: %v", err)
	}
	if parentID != nil {
		t.Errorf("child parent_id = %v after the parent was deleted, want NULL", *parentID)
	}
}

func TestMigration0010ExternalIdentityIsUniquePerIssuer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuthFixture(t, ctx)
	m10Seed(t, ctx, env)

	insert := func(issuer string) error {
		return guardedExec(ctx, env.admin, `INSERT INTO auth.external_identities (principal_id, provider, subject, issuer)
			VALUES ($1, 'oidc', 'sub-1', $2)`, m10Principal, issuer)
	}
	if err := insert("https://issuer-a.example"); err != nil {
		t.Fatalf("first issuer: %v", err)
	}
	if err := insert("https://issuer-b.example"); err != nil {
		t.Errorf("the same provider and subject under a different issuer was refused: %v", err)
	}
	m10Fail(t, insert("https://issuer-a.example"), "the same (issuer, provider, subject) twice")

	var legacy int
	if err := env.admin.QueryRow(ctx, `SELECT count(*) FROM pg_indexes
		WHERE schemaname = 'auth' AND indexname = 'external_identities_provider_subject_key'`).Scan(&legacy); err != nil {
		t.Fatal(err)
	}
	if legacy != 0 {
		t.Error("the (provider, subject) unique index still exists, so issuer cannot distinguish subjects")
	}
}

func TestMigration0010LinkCredentialsAndLoginAttempts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuthFixture(t, ctx)
	m10Seed(t, ctx, env)

	link := func(purpose, hash string) error {
		_, err := env.admin.Exec(ctx, `INSERT INTO auth.link_credentials (principal_id, purpose, token_hash, expires_at)
			VALUES ($1, $2, $3::bytea, now() + interval '1 hour')`, m10Principal, purpose, hash)
		return err
	}
	for _, purpose := range []string{"email_verification", "password_reset"} {
		if err := link(purpose, `\x`+map[string]string{"email_verification": "a1", "password_reset": "a2"}[purpose]); err != nil {
			t.Errorf("purpose %s refused: %v", purpose, err)
		}
	}
	m10Fail(t, link("login", `\xa3`), "an unknown link purpose")
	m10Fail(t, link("password_reset", `\xa2`), "a duplicate link token_hash")

	if _, err := env.admin.Exec(ctx, `INSERT INTO auth.login_attempts (identifier_hash) VALUES ('\xc1'::bytea)`); err != nil {
		t.Fatalf("insert a login attempt: %v", err)
	}
	var count int
	if err := env.admin.QueryRow(ctx, `SELECT attempt_count FROM auth.login_attempts`).Scan(&count); err != nil || count != 0 {
		t.Errorf("attempt_count default = %d, %v; want 0", count, err)
	}
	_, err := env.admin.Exec(ctx, `INSERT INTO auth.login_attempts (identifier_hash) VALUES ('\xc1'::bytea)`)
	m10Fail(t, err, "a second lockout row for the same identifier_hash")
	_, err = env.admin.Exec(ctx, `UPDATE auth.login_attempts SET attempt_count = -1`)
	m10Fail(t, err, "a negative attempt_count")

	// Link credentials die with their principal.
	if _, err := env.admin.Exec(ctx, `DELETE FROM auth.principals WHERE id = $1`, m10Principal); err != nil {
		t.Fatalf("delete principal: %v", err)
	}
	if err := env.admin.QueryRow(ctx, `SELECT count(*) FROM auth.link_credentials`).Scan(&count); err != nil || count != 0 {
		t.Errorf("link_credentials left %d row(s) after the principal was deleted (%v)", count, err)
	}
}

// 0010 must apply over a populated v9 schema without touching existing rows.
func TestMigration0010AppliesOverPopulatedSchema(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuthMigrationOnly(t, ctx)
	migrations, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	var tenth *Migration
	for index := range migrations {
		switch {
		case migrations[index].Version < 10:
			if err := applyOne(ctx, env.conn, env.options, migrations[index]); err != nil {
				t.Fatalf("apply %04d: %v", migrations[index].Version, err)
			}
		case migrations[index].Version == 10:
			tenth = &migrations[index]
		}
	}
	if tenth == nil {
		t.Fatal("the lineage has no migration 0010")
	}
	for _, statement := range []string{
		`INSERT INTO auth.principals (id, kind, display_name) VALUES ('` + m10Principal + `', 'user', 'm10')`,
		`INSERT INTO auth.sessions (id, principal_id, token_hash, expires_at)
		 VALUES ('` + m10Session + `', '` + m10Principal + `', '\x0a'::bytea, now() + interval '1 hour')`,
		`INSERT INTO auth.refresh_credentials (session_id, principal_id, token_hash, expires_at)
		 VALUES ('` + m10Session + `', '` + m10Principal + `', '\x0b'::bytea, now() + interval '1 day')`,
	} {
		if _, err := env.conn.Exec(ctx, statement); err != nil {
			t.Fatalf("seed v9 row %q: %v", statement, err)
		}
	}
	// external_identities is a guarded table: seed it the way audit.Commit does.
	tx, err := env.conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `SELECT set_config('dev_health.audit_txid', pg_current_xact_id()::text, true)`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO auth.external_identities (principal_id, provider, subject)
		VALUES ($1, 'oidc', 'sub-1')`, m10Principal); err != nil {
		t.Fatalf("seed v9 external identity: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := applyOne(ctx, env.conn, env.options, *tenth); err != nil {
		t.Fatalf("0010 over populated rows: %v", err)
	}
	var revision int
	var issuer string
	var family string
	if err := env.conn.QueryRow(ctx, `SELECT s.revision, e.issuer, r.family_id::text
		FROM auth.sessions s, auth.external_identities e, auth.refresh_credentials r`).Scan(&revision, &issuer, &family); err != nil {
		t.Fatalf("read migrated rows: %v", err)
	}
	if revision != 1 || issuer != "" || family == "" {
		t.Errorf("migrated defaults = revision %d, issuer %q, family %q; want 1, empty, set", revision, issuer, family)
	}
}

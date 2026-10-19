//go:build integration

package authschema

import (
	"context"
	"strings"
	"testing"
	"time"
)

// m10Hash is a well-formed 32-byte digest, hex-prefixed for a bytea parameter.
func m10Hash(tag string) string { return `\x` + tag + strings.Repeat("0", 64-len(tag)) }

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
	if err := guardedExec(ctx, env.admin, `UPDATE auth.refresh_credentials SET legacy_jti_hash = $1::bytea WHERE token_hash = '\x03'::bytea`, m10Hash("beef")); err != nil {
		t.Fatalf("set legacy_jti_hash: %v", err)
	}
	err = guardedExec(ctx, env.admin, `UPDATE auth.refresh_credentials SET legacy_jti_hash = $1::bytea WHERE token_hash = '\x04'::bytea`, m10Hash("beef"))
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
		if err := link(purpose, m10Hash(map[string]string{"email_verification": "a1", "password_reset": "a2"}[purpose])); err != nil {
			t.Errorf("purpose %s refused: %v", purpose, err)
		}
	}
	m10Fail(t, link("login", m10Hash("a3")), "an unknown link purpose")
	m10Fail(t, link("password_reset", m10Hash("a2")), "a duplicate link token_hash")

	if _, err := env.admin.Exec(ctx, `INSERT INTO auth.login_attempts (identifier_hash) VALUES ($1::bytea)`, m10Hash("c1")); err != nil {
		t.Fatalf("insert a login attempt: %v", err)
	}
	var count int
	if err := env.admin.QueryRow(ctx, `SELECT attempt_count FROM auth.login_attempts`).Scan(&count); err != nil || count != 0 {
		t.Errorf("attempt_count default = %d, %v; want 0", count, err)
	}
	_, err := env.admin.Exec(ctx, `INSERT INTO auth.login_attempts (identifier_hash) VALUES ($1::bytea)`, m10Hash("c1"))
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
	tables := []string{"principals", "sessions", "refresh_credentials", "external_identities"}
	snapshot := func(strip string) map[string]string {
		out := map[string]string{}
		for _, table := range tables {
			var doc string
			if err := env.conn.QueryRow(ctx, `SELECT coalesce(jsonb_agg(to_jsonb(t) `+strip+` ORDER BY t.created_at, t.id::text), '[]')::text FROM auth.`+table+` t`).Scan(&doc); err != nil {
				t.Fatalf("snapshot %s: %v", table, err)
			}
			if doc == "[]" {
				t.Fatalf("snapshot of %s is empty; the control proves nothing", table)
			}
			out[table] = doc
		}
		return out
	}
	before := snapshot("")
	if err := applyOne(ctx, env.conn, env.options, *tenth); err != nil {
		t.Fatalf("0010 over populated rows: %v", err)
	}
	after := snapshot("- 'revision' - 'auth_time' - 'amr' - 'family_id' - 'parent_id' - 'legacy_jti_hash' - 'issuer'")
	for _, table := range tables {
		if before[table] != after[table] {
			t.Errorf("0010 changed existing %s rows:\nbefore %s\nafter  %s", table, before[table], after[table])
		}
	}
	var revision, rows int
	var issuer, family string
	if err := env.conn.QueryRow(ctx, `SELECT s.revision, e.issuer, r.family_id::text, (SELECT count(*) FROM auth.refresh_credentials)
		FROM auth.sessions s, auth.external_identities e, auth.refresh_credentials r`).Scan(&revision, &issuer, &family, &rows); err != nil {
		t.Fatalf("read migrated rows: %v", err)
	}
	if revision != 1 || issuer != "" || family == "" || rows != 1 {
		t.Errorf("migrated defaults = revision %d, issuer %q, family %q, rows %d; want 1, empty, set, 1", revision, issuer, family, rows)
	}
}

// Hash columns take digests only: an address or a token written by mistake is
// refused by the runtime role's own insert.
func TestMigration0010HashColumnsRefuseNonDigests(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuthFixture(t, ctx)
	m10Seed(t, ctx, env)

	_, err := env.runtime.Exec(ctx, `INSERT INTO auth.login_attempts (identifier_hash) VALUES ('alice@example.com'::bytea)`)
	m10Fail(t, err, "an email address in login_attempts.identifier_hash")
	_, err = env.runtime.Exec(ctx, `INSERT INTO auth.link_credentials (principal_id, purpose, token_hash, expires_at)
		VALUES ($1, 'password_reset', 'plaintext-reset-token'::bytea, now() + interval '1 hour')`, m10Principal)
	m10Fail(t, err, "a plaintext token in link_credentials.token_hash")
	_, err = env.runtime.Exec(ctx, `INSERT INTO auth.refresh_credentials (session_id, principal_id, token_hash, expires_at, legacy_jti_hash)
		VALUES ($1, $2, '\x21'::bytea, now() + interval '1 day', 'a-raw-jti'::bytea)`, m10Session, m10Principal)
	m10Fail(t, err, "a raw jti in refresh_credentials.legacy_jti_hash")

	if _, err := env.runtime.Exec(ctx, `INSERT INTO auth.login_attempts (identifier_hash) VALUES ($1::bytea)`, m10Hash("d1")); err != nil {
		t.Errorf("a 32-byte digest was refused: %v", err)
	}
}

// Every new security column refuses a write outside audit.Commit and accepts
// the same write inside it, so the guard is exercised, not read from the
// trigger text. Setting parent_id to NULL (the sweep's ON DELETE SET NULL)
// stays allowed.
func TestMigration0010SecurityColumnsAreGuardedByBehaviour(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuthFixture(t, ctx)
	m10Seed(t, ctx, env)

	var first, second string
	for i, token := range []string{`\x31`, `\x32`} {
		var id string
		if err := env.admin.QueryRow(ctx, `INSERT INTO auth.refresh_credentials (session_id, principal_id, token_hash, expires_at)
			VALUES ($1, $2, $3::bytea, now() + interval '1 day') RETURNING id::text`, m10Session, m10Principal, token).Scan(&id); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			first = id
		} else {
			second = id
		}
	}
	var linkID string
	if err := env.admin.QueryRow(ctx, `INSERT INTO auth.link_credentials (principal_id, purpose, token_hash, expires_at)
		VALUES ($1, 'password_reset', $2::bytea, now() + interval '1 hour') RETURNING id::text`, m10Principal, m10Hash("e1")).Scan(&linkID); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name string
		sql  string
		args []any
	}{
		{"sessions.revision", `UPDATE auth.sessions SET revision = 2 WHERE id = $1`, []any{m10Session}},
		{"sessions.auth_time", `UPDATE auth.sessions SET auth_time = now() WHERE id = $1`, []any{m10Session}},
		{"sessions.amr", `UPDATE auth.sessions SET amr = '{pwd}' WHERE id = $1`, []any{m10Session}},
		{"refresh_credentials.family_id", `UPDATE auth.refresh_credentials SET family_id = gen_random_uuid() WHERE id = $1`, []any{first}},
		{"refresh_credentials.legacy_jti_hash", `UPDATE auth.refresh_credentials SET legacy_jti_hash = $2::bytea WHERE id = $1`, []any{first, m10Hash("f1")}},
		{"refresh_credentials.parent_id", `UPDATE auth.refresh_credentials SET parent_id = $2::uuid WHERE id = $1`, []any{first, second}},
		{"link_credentials.consumed_at", `UPDATE auth.link_credentials SET consumed_at = now() WHERE id = $1`, []any{linkID}},
		{"link_credentials.expires_at", `UPDATE auth.link_credentials SET expires_at = now() + interval '1 year' WHERE id = $1`, []any{linkID}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := env.runtime.Exec(ctx, testCase.sql, testCase.args...)
			if err == nil || !strings.Contains(err.Error(), "audit.Commit") {
				t.Fatalf("an unguarded write was accepted or failed for another reason: %v", err)
			}
			if err := guardedExec(ctx, env.runtime, testCase.sql, testCase.args...); err != nil {
				t.Fatalf("the same write inside a commit transaction was refused: %v", err)
			}
		})
	}
	if _, err := env.runtime.Exec(ctx, `UPDATE auth.refresh_credentials SET parent_id = NULL WHERE id = $1`, first); err != nil {
		t.Errorf("clearing parent_id outside a commit transaction was refused: %v", err)
	}
}

// consumed_at is write-once on link_credentials and refresh_credentials: a
// second consume and a clear are refused even inside a commit transaction, and
// the atomic "WHERE consumed_at IS NULL" consume matches no row on replay.
// Other columns of a consumed row stay writable (revoked_at, the ON DELETE SET
// NULL actions), so a legitimate later write is not blocked.
func TestMigration0010ConsumedAtIsWriteOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuthFixture(t, ctx)
	m10Seed(t, ctx, env)

	var linkID, refreshID, otherID string
	if err := env.admin.QueryRow(ctx, `INSERT INTO auth.link_credentials (principal_id, purpose, token_hash, expires_at)
		VALUES ($1, 'password_reset', $2::bytea, now() + interval '1 hour') RETURNING id::text`, m10Principal, m10Hash("b1")).Scan(&linkID); err != nil {
		t.Fatal(err)
	}
	if err := env.admin.QueryRow(ctx, `INSERT INTO auth.refresh_credentials (session_id, principal_id, token_hash, expires_at)
		VALUES ($1, $2, '\x41'::bytea, now() + interval '1 day') RETURNING id::text`, m10Session, m10Principal).Scan(&otherID); err != nil {
		t.Fatal(err)
	}
	if err := env.admin.QueryRow(ctx, `INSERT INTO auth.refresh_credentials (session_id, principal_id, token_hash, expires_at, superseded_by)
		VALUES ($1, $2, '\x42'::bytea, now() + interval '1 day', $3) RETURNING id::text`, m10Session, m10Principal, otherID).Scan(&refreshID); err != nil {
		t.Fatal(err)
	}

	for _, table := range []string{"link_credentials", "refresh_credentials"} {
		id := map[string]string{"link_credentials": linkID, "refresh_credentials": refreshID}[table]
		consume := func() (int64, error) {
			tx, err := env.runtime.Begin(ctx)
			if err != nil {
				return 0, err
			}
			defer func() { _ = tx.Rollback(ctx) }()
			if _, err := tx.Exec(ctx, `SELECT set_config('dev_health.audit_txid', pg_current_xact_id()::text, true)`); err != nil {
				return 0, err
			}
			tag, err := tx.Exec(ctx, `UPDATE auth.`+table+` SET consumed_at = now() WHERE id = $1 AND consumed_at IS NULL`, id)
			if err != nil {
				return 0, err
			}
			return tag.RowsAffected(), tx.Commit(ctx)
		}
		if n, err := consume(); err != nil || n != 1 {
			t.Fatalf("%s: first consume = %d, %v; want 1", table, n, err)
		}
		if n, err := consume(); err != nil || n != 0 {
			t.Errorf("%s: replayed atomic consume = %d, %v; want 0 rows", table, n, err)
		}
		for name, statement := range map[string]string{
			"a second consume":     `UPDATE auth.` + table + ` SET consumed_at = now() WHERE id = $1`,
			"clearing consumed_at": `UPDATE auth.` + table + ` SET consumed_at = NULL WHERE id = $1`,
		} {
			err := guardedExec(ctx, env.runtime, statement, id)
			if err == nil || !strings.Contains(err.Error(), "consumed_at") {
				t.Errorf("%s: %s: err = %v, want a refusal naming consumed_at", table, name, err)
			}
		}
	}

	// Legitimate later writes to a consumed row still work.
	if err := guardedExec(ctx, env.runtime, `UPDATE auth.refresh_credentials SET revoked_at = now() WHERE id = $1`, refreshID); err != nil {
		t.Errorf("revoking a consumed refresh credential was refused: %v", err)
	}
	if _, err := env.runtime.Exec(ctx, `DELETE FROM auth.refresh_credentials WHERE id = $1`, otherID); err != nil {
		t.Fatalf("delete the superseding credential: %v", err)
	}
	var superseded *string
	if err := env.admin.QueryRow(ctx, `SELECT superseded_by::text FROM auth.refresh_credentials WHERE id = $1`, refreshID).Scan(&superseded); err != nil || superseded != nil {
		t.Errorf("ON DELETE SET NULL on a consumed row: superseded_by = %v, %v; want NULL", superseded, err)
	}
	if _, err := env.admin.Exec(ctx, `DELETE FROM auth.principals WHERE id = $1`, m10Principal); err != nil {
		t.Errorf("deleting the principal of consumed credentials was refused: %v", err)
	}
}

// A live lockout cannot be erased by the runtime role: the identifier is
// immutable, and while locked_until is in the future the row can neither lose
// its lock or count nor be deleted. Clearing after expiry, counting up and the
// idle sweep of unlocked rows stay legal.
func TestMigration0010LiveLockoutCannotBeErasedByTheRuntime(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuthFixture(t, ctx)

	var id string
	if err := env.runtime.QueryRow(ctx, `INSERT INTO auth.login_attempts (identifier_hash, attempt_count, first_attempt_at, locked_until)
		VALUES ($1::bytea, 5, now(), now() + interval '1 hour') RETURNING id::text`, m10Hash("d2")).Scan(&id); err != nil {
		t.Fatalf("insert a locked row: %v", err)
	}
	refused := map[string]string{
		"retargeting the identifier": `UPDATE auth.login_attempts SET identifier_hash = decode(rpad('d3', 64, '0'), 'hex') WHERE id = $1`,
		"clearing the lock":          `UPDATE auth.login_attempts SET locked_until = NULL WHERE id = $1`,
		"shortening the lock":        `UPDATE auth.login_attempts SET locked_until = now() + interval '1 second' WHERE id = $1`,
		"resetting the count":        `UPDATE auth.login_attempts SET attempt_count = 0 WHERE id = $1`,
		"deleting the locked row":    `DELETE FROM auth.login_attempts WHERE id = $1`,
	}
	for name, statement := range refused {
		if _, err := env.runtime.Exec(ctx, statement, id); err == nil {
			t.Errorf("%s on a live lockout was accepted", name)
		}
	}
	if _, err := env.runtime.Exec(ctx, `UPDATE auth.login_attempts SET locked_until = now() + interval '2 hours', attempt_count = 6 WHERE id = $1`, id); err != nil {
		t.Errorf("extending a live lock was refused: %v", err)
	}

	// After expiry the same row can be cleared and deleted.
	if _, err := env.admin.Exec(ctx, `UPDATE auth.login_attempts SET locked_until = now() - interval '1 minute' WHERE id = $1`, id); err != nil {
		t.Fatalf("age the lock: %v", err)
	}
	if _, err := env.runtime.Exec(ctx, `UPDATE auth.login_attempts SET attempt_count = 0, first_attempt_at = NULL, locked_until = NULL WHERE id = $1`, id); err != nil {
		t.Errorf("clearing an expired lock was refused: %v", err)
	}
	if _, err := env.runtime.Exec(ctx, `DELETE FROM auth.login_attempts WHERE id = $1`, id); err != nil {
		t.Errorf("deleting an unlocked row was refused: %v", err)
	}
}

// A child credential always belongs to its parent's family, so a family-wide
// revoke reaches it.
func TestMigration0010RefreshChildInheritsTheParentsFamily(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuthFixture(t, ctx)
	m10Seed(t, ctx, env)

	var parent, parentFamily, other string
	if err := env.runtime.QueryRow(ctx, `INSERT INTO auth.refresh_credentials (session_id, principal_id, token_hash, expires_at)
		VALUES ($1, $2, '\x51'::bytea, now() + interval '1 day') RETURNING id::text, family_id::text`, m10Session, m10Principal).Scan(&parent, &parentFamily); err != nil {
		t.Fatal(err)
	}
	if err := env.runtime.QueryRow(ctx, `INSERT INTO auth.refresh_credentials (session_id, principal_id, token_hash, expires_at)
		VALUES ($1, $2, '\x52'::bytea, now() + interval '1 day') RETURNING id::text`, m10Session, m10Principal).Scan(&other); err != nil {
		t.Fatal(err)
	}

	for name, insert := range map[string]string{
		"family_id omitted":       `INSERT INTO auth.refresh_credentials (session_id, principal_id, token_hash, expires_at, parent_id) VALUES ($1, $2, $3::bytea, now() + interval '1 day', $4) RETURNING family_id::text`,
		"another family supplied": `INSERT INTO auth.refresh_credentials (session_id, principal_id, token_hash, expires_at, parent_id, family_id) VALUES ($1, $2, $3::bytea, now() + interval '1 day', $4, gen_random_uuid()) RETURNING family_id::text`,
	} {
		var family string
		token := m10Hash(map[string]string{"family_id omitted": "61", "another family supplied": "62"}[name])
		if err := env.runtime.QueryRow(ctx, insert, m10Session, m10Principal, token, parent).Scan(&family); err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if family != parentFamily {
			t.Errorf("%s: child family %s, want the parent's %s", name, family, parentFamily)
		}
	}

	// Re-pointing a credential at a parent of another family is refused.
	err := guardedExec(ctx, env.runtime, `UPDATE auth.refresh_credentials SET parent_id = $2::uuid WHERE id = $1`, other, parent)
	if err == nil || !strings.Contains(err.Error(), "family") {
		t.Errorf("re-parenting across families: err = %v, want a refusal naming the family", err)
	}
	err = guardedExec(ctx, env.runtime, `UPDATE auth.refresh_credentials SET family_id = gen_random_uuid() WHERE parent_id = $1`, parent)
	if err == nil || !strings.Contains(err.Error(), "family") {
		t.Errorf("moving a child to another family: err = %v, want a refusal naming the family", err)
	}
}

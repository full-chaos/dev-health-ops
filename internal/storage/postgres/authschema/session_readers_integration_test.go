//go:build integration

package authschema

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// newSessionReader creates a login role with CONNECT only and returns its pool.
func newSessionReader(t *testing.T, ctx context.Context, env *authFixture, base string) (string, *pgxpool.Pool) {
	t.Helper()
	role, err := containers.RoleName(base, env.instance)
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD '%s'", Quote(mustValidatedIdentifier(role)), fixturePassword),
		fmt.Sprintf("GRANT CONNECT ON DATABASE %s TO %s", Quote(mustValidatedIdentifier(env.database)), Quote(mustValidatedIdentifier(role))),
	} {
		if _, err := env.admin.Exec(ctx, statement); err != nil {
			t.Fatalf("setup %q: %v", statement, err)
		}
	}
	t.Cleanup(func() { containers.DropRole(env.admin, role, t.Logf) })
	pool, err := pgxpool.New(ctx, fixtureDSN(t, env.instance.URI, role))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return role, pool
}

func readerStatement(ctx context.Context, pool *pgxpool.Pool, statement string) error {
	rows, err := pool.Query(ctx, statement)
	if err != nil {
		return err
	}
	for rows.Next() {
	}
	rows.Close()
	return rows.Err()
}

func readerOptions(env *authFixture, readers ...string) Options {
	return Options{Schema: env.schema, RuntimeRole: env.runtimeRole, SessionReaderRoles: readers,
		Logger: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))}
}

func readerViolations(t *testing.T, ctx context.Context, env *authFixture, readers ...string) []Violation {
	t.Helper()
	conn, err := env.migration.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	found, err := VerifyRuntimePosture(ctx, conn.Conn(), readerOptions(env, readers...))
	if err != nil {
		t.Fatalf("VerifyRuntimePosture: %v", err)
	}
	return found
}

// A session reader (go-api, query-api) may SELECT exactly id, principal_id,
// revoked_at, legacy_token_version, created_at and web_session_id of the auth sessions
// and principal_id and password_updated_at of the auth users (the session rule's revoke),
// column-level, and nothing else in the auth schema; a re-apply restores exactly that,
// and the posture names any widening.
func TestSessionReaderRolesReadExactlyTheSessionColumns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	env := newAuthFixture(t, ctx)
	reader, pool := newSessionReader(t, ctx, env, "auth_session_reader")
	if _, err := Apply(ctx, env.migration, readerOptions(env, reader)); err != nil {
		t.Fatalf("Apply with a session reader: %v", err)
	}

	if err := readerStatement(ctx, pool, `SELECT id, principal_id, revoked_at, legacy_token_version, created_at, web_session_id FROM auth.sessions`); err != nil {
		t.Fatalf("the reader cannot read its columns: %v", err)
	}
	if err := readerStatement(ctx, pool, `SELECT principal_id, password_updated_at FROM auth.users`); err != nil {
		t.Fatalf("the reader cannot read the password stamp: %v", err)
	}
	for _, refused := range []string{
		`SELECT password_hash FROM auth.users`,
		`SELECT * FROM auth.users`,
		`SELECT expires_at FROM auth.sessions`,
		`SELECT * FROM auth.sessions`,
		`SELECT id FROM auth.principals`,
		`SELECT 1 FROM auth.refresh_credentials`,
		`INSERT INTO auth.sessions (id) VALUES (gen_random_uuid())`,
		`UPDATE auth.sessions SET revoked_at = now()`,
		`CREATE TABLE auth.reader_made (id int)`,
	} {
		if err := readerStatement(ctx, pool, refused); err == nil || !strings.Contains(err.Error(), "permission denied") {
			t.Errorf("%s as the reader: err = %v, want permission denied", refused, err)
		}
	}

	var tableGrants int
	if err := env.admin.QueryRow(ctx, `SELECT count(*) FROM information_schema.role_table_grants
		WHERE grantee = $1 AND table_schema = $2`, reader, env.schema).Scan(&tableGrants); err != nil {
		t.Fatal(err)
	}
	if tableGrants != 0 {
		t.Errorf("the reader holds %d table-level grants in the auth schema, want 0", tableGrants)
	}
	rows, err := env.admin.Query(ctx, `SELECT table_name || '.' || column_name || ':' || privilege_type
		FROM information_schema.column_privileges WHERE grantee = $1 AND table_schema = $2 ORDER BY 1`, reader, env.schema)
	if err != nil {
		t.Fatal(err)
	}
	var columns []string
	for rows.Next() {
		var entry string
		if err := rows.Scan(&entry); err != nil {
			t.Fatal(err)
		}
		columns = append(columns, entry)
	}
	rows.Close()
	if want := "sessions.created_at:SELECT,sessions.id:SELECT,sessions.legacy_token_version:SELECT,sessions.principal_id:SELECT,sessions.revoked_at:SELECT,sessions.web_session_id:SELECT,users.password_updated_at:SELECT,users.principal_id:SELECT"; strings.Join(columns, ",") != want {
		t.Errorf("the reader's column grants = %v, want %s", columns, want)
	}
	if found := readerViolations(t, ctx, env, reader); len(found) != 0 {
		t.Fatalf("posture after Apply: %s", describe(found))
	}

	for _, widen := range []string{
		fmt.Sprintf(`GRANT SELECT ON auth.sessions TO %s`, Quote(mustValidatedIdentifier(reader))),
		fmt.Sprintf(`GRANT SELECT (expires_at) ON auth.sessions TO %s`, Quote(mustValidatedIdentifier(reader))),
		fmt.Sprintf(`GRANT SELECT ON auth.principals TO %s`, Quote(mustValidatedIdentifier(reader))),
	} {
		if _, err := env.migration.Exec(ctx, widen); err != nil {
			t.Fatalf("%s: %v", widen, err)
		}
		if found := readerViolations(t, ctx, env, reader); !strings.Contains(describe(found), reader) {
			t.Errorf("after %q the posture does not name the reader:\n%s", widen, describe(found))
		}
		if _, err := Apply(ctx, env.migration, readerOptions(env, reader)); err != nil {
			t.Fatalf("re-Apply: %v", err)
		}
		if found := readerViolations(t, ctx, env, reader); len(found) != 0 {
			t.Errorf("after re-Apply the posture is not exact: %s", describe(found))
		}
	}
}

func TestSessionReaderRolesAreRefusedWhenUnusable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	env := newAuthFixture(t, ctx)
	if _, err := Apply(ctx, env.migration, readerOptions(env, "no_such_reader_role")); err == nil ||
		!strings.Contains(err.Error(), "no_such_reader_role") {
		t.Errorf("a reader role that does not exist: err = %v, want a refusal naming it", err)
	}
	if _, err := Apply(ctx, env.migration, readerOptions(env, env.runtimeRole)); !errors.Is(err, ErrInvalidOptions) {
		t.Errorf("the runtime role as a reader: err = %v, want ErrInvalidOptions", err)
	}
	if _, err := Apply(ctx, env.migration, readerOptions(env, "bad role")); !errors.Is(err, ErrInvalidOptions) {
		t.Errorf("an unusable role name: err = %v, want ErrInvalidOptions", err)
	}
}

// A reader that holds a NOINHERIT membership in the runtime role can SET ROLE
// to it, and has_*_privilege never shows that. The posture names it. A reader
// with a risky role attribute is named too.
func TestSessionReaderPostureNamesSetOnlyMembershipsAndRiskyAttributes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	env := newAuthFixture(t, ctx)
	member, _ := newSessionReader(t, ctx, env, "rdr_setonly")
	if _, err := env.admin.Exec(ctx, fmt.Sprintf("GRANT %s TO %s WITH INHERIT FALSE, SET TRUE",
		Quote(mustValidatedIdentifier(env.runtimeRole)), Quote(mustValidatedIdentifier(member)))); err != nil {
		t.Fatalf("grant membership: %v", err)
	}
	attributeRoles := map[string]string{}
	for attribute, alter := range map[string]func(string) string{
		"CREATEROLE": func(r string) string {
			return fmt.Sprintf("ALTER ROLE %s CREATEROLE", Quote(mustValidatedIdentifier(r)))
		},
		"CREATEDB": func(r string) string { return fmt.Sprintf("ALTER ROLE %s CREATEDB", Quote(mustValidatedIdentifier(r))) },
		"BYPASSRLS": func(r string) string {
			return fmt.Sprintf("ALTER ROLE %s BYPASSRLS", Quote(mustValidatedIdentifier(r)))
		},
		"REPLICATION": func(r string) string {
			return fmt.Sprintf("ALTER ROLE %s REPLICATION", Quote(mustValidatedIdentifier(r)))
		},
	} {
		role, _ := newSessionReader(t, ctx, env, "rdr_"+strings.ToLower(attribute))
		if _, err := env.admin.Exec(ctx, alter(role)); err != nil {
			t.Fatalf("alter %s: %v", attribute, err)
		}
		attributeRoles[attribute] = role
	}
	clean, _ := newSessionReader(t, ctx, env, "rdr_clean")
	has := func(found []Violation, kind, object, privilege string) bool {
		for _, v := range found {
			if v.Kind == kind && v.Object == object && v.Privilege == privilege {
				return true
			}
		}
		return false
	}
	found := readerViolations(t, ctx, env, member)
	if !has(found, "session_reader", env.runtimeRole, "MEMBER") {
		t.Fatalf("SET-only membership in the runtime role not reported: %v", found)
	}
	for attribute, role := range attributeRoles {
		found := readerViolations(t, ctx, env, role)
		if !has(found, "session_reader", role, attribute) {
			t.Errorf("%s not reported for %s: %v", attribute, role, found)
		}
	}
	for _, v := range readerViolations(t, ctx, env, clean) {
		if v.Kind == "session_reader" && (v.Privilege == "MEMBER" || v.Object == clean) {
			t.Errorf("clean reader reported: %v", v)
		}
	}
}

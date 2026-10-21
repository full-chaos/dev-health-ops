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

// A session reader (go-api, query-api) may SELECT exactly id, principal_id
// and revoked_at of the auth sessions, column-level, and nothing else in the
// auth schema; a re-apply restores exactly that, and the posture names any
// widening.
func TestSessionReaderRolesReadExactlyTheSessionColumns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	env := newAuthFixture(t, ctx)
	reader, pool := newSessionReader(t, ctx, env, "auth_session_reader")
	if _, err := Apply(ctx, env.migration, readerOptions(env, reader)); err != nil {
		t.Fatalf("Apply with a session reader: %v", err)
	}

	if err := readerStatement(ctx, pool, `SELECT id, principal_id, revoked_at FROM auth.sessions`); err != nil {
		t.Fatalf("the reader cannot read its columns: %v", err)
	}
	for _, refused := range []string{
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
	if want := "sessions.id:SELECT,sessions.principal_id:SELECT,sessions.revoked_at:SELECT"; strings.Join(columns, ",") != want {
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

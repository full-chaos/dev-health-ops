//go:build integration

package authschema

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func projectionOptions(env *authFixture, readers ...string) Options {
	options := readerOptions(env)
	options.ProjectionReaderRoles = readers
	return options
}

func projectionViolations(t *testing.T, ctx context.Context, env *authFixture, readers ...string) []Violation {
	t.Helper()
	conn, err := env.migration.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	found, err := VerifyRuntimePosture(ctx, conn.Conn(), projectionOptions(env, readers...))
	if err != nil {
		t.Fatalf("VerifyRuntimePosture: %v", err)
	}
	return found
}

// A projection reader (the worker login that runs the acr reconcile) may SELECT exactly the six columns of the
// credential projection the reader needs, column-level, and nothing else in the auth schema: it reads no other
// table, no other column, and cannot write, truncate or create (CHAOS-11199).
func TestProjectionReaderRolesReadExactlyTheProjectionColumns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	env := newAuthFixture(t, ctx)
	reader, pool := newSessionReader(t, ctx, env, "auth_projection_reader")
	if _, err := Apply(ctx, env.migration, projectionOptions(env, reader)); err != nil {
		t.Fatalf("Apply with a projection reader: %v", err)
	}
	if err := readerStatement(ctx, pool, `SELECT domain_owner, organization_id, external_reference, revision, status, expires_at
		FROM auth.credential_projection WHERE domain_owner = 'acr' ORDER BY external_reference`); err != nil {
		t.Fatalf("the reader cannot read its columns: %v", err)
	}
	for _, refused := range []string{
		`SELECT fingerprint FROM auth.credential_projection`,
		`SELECT scopes FROM auth.credential_projection`,
		`SELECT owner_subject FROM auth.credential_projection`,
		`SELECT * FROM auth.credential_projection`,
		`SELECT id FROM auth.sessions`,
		`SELECT id FROM auth.principals`,
		`SELECT 1 FROM auth.credential_projection_digest`,
		`SELECT 1 FROM auth.credential_registry`,
		`SELECT 1 FROM auth.projection_retry`,
		`INSERT INTO auth.credential_projection (domain_owner) VALUES ('acr')`,
		`UPDATE auth.credential_projection SET status = 'revoked'`,
		`DELETE FROM auth.credential_projection`,
		`TRUNCATE auth.credential_projection`,
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
	want := "credential_projection.domain_owner:SELECT,credential_projection.expires_at:SELECT,credential_projection.external_reference:SELECT," +
		"credential_projection.organization_id:SELECT,credential_projection.revision:SELECT,credential_projection.status:SELECT"
	if strings.Join(columns, ",") != want {
		t.Errorf("the reader's column grants = %v, want %s", columns, want)
	}
	if found := projectionViolations(t, ctx, env, reader); len(found) != 0 {
		t.Fatalf("posture after Apply: %s", describe(found))
	}

	quoted := Quote(mustValidatedIdentifier(reader))
	for _, widen := range []string{
		fmt.Sprintf(`GRANT SELECT (fingerprint) ON auth.credential_projection TO %s`, quoted),
		fmt.Sprintf(`GRANT SELECT ON auth.credential_projection_digest TO %s`, quoted),
		fmt.Sprintf(`GRANT UPDATE (status) ON auth.credential_projection TO %s`, quoted),
	} {
		if _, err := env.migration.Exec(ctx, widen); err != nil {
			t.Fatalf("%s: %v", widen, err)
		}
		found := projectionViolations(t, ctx, env, reader)
		if len(found) == 0 || !strings.Contains(describe(found), "projection_reader") {
			t.Errorf("after %s the posture names no projection_reader violation: %s", widen, describe(found))
		}
		if _, err := Apply(ctx, env.migration, projectionOptions(env, reader)); err != nil {
			t.Fatalf("re-apply after %s: %v", widen, err)
		}
		if found := projectionViolations(t, ctx, env, reader); len(found) != 0 {
			t.Errorf("a re-apply did not restore the grant after %s: %s", widen, describe(found))
		}
	}
	if _, err := env.migration.Exec(ctx, fmt.Sprintf(`REVOKE SELECT (status) ON auth.credential_projection FROM %s`, quoted)); err != nil {
		t.Fatal(err)
	}
	if found := projectionViolations(t, ctx, env, reader); !strings.Contains(describe(found), "missing grant") {
		t.Errorf("a missing declared column is not named: %s", describe(found))
	}
}

// The runtime role keeps its own grants and the reader grant is not given to it.
func TestProjectionReaderGrantLeavesTheRuntimeRoleAlone(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	env := newAuthFixture(t, ctx)
	reader, _ := newSessionReader(t, ctx, env, "auth_projection_reader")
	if _, err := Apply(ctx, env.migration, projectionOptions(env, reader)); err != nil {
		t.Fatal(err)
	}
	var can bool
	if err := env.admin.QueryRow(ctx, `SELECT has_table_privilege($1, 'auth.credential_projection', 'INSERT, UPDATE, SELECT')`, env.runtimeRole).Scan(&can); err != nil || !can {
		t.Fatalf("the runtime role lost its projection grants: %v %v", can, err)
	}
	var readerCan bool
	if err := env.admin.QueryRow(ctx, `SELECT has_table_privilege($1, 'auth.credential_projection', 'INSERT') OR has_table_privilege($1, 'auth.credential_projection', 'UPDATE') OR has_table_privilege($1, 'auth.credential_projection', 'DELETE') OR has_table_privilege($1, 'auth.credential_projection', 'SELECT')`, reader).Scan(&readerCan); err != nil || readerCan {
		t.Fatalf("the reader holds a table-level privilege: %v %v", readerCan, err)
	}
}

func TestPresentReadsKeepsOnlyTablesThatExist(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	env := newAuthFixture(t, ctx)
	got, err := presentReads(ctx, env.admin, env.schema, []ColumnGrant{{Table: "credential_projection"}, {Table: "no_such_table"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Table != "credential_projection" {
		t.Fatalf("presentReads = %+v", got)
	}
}

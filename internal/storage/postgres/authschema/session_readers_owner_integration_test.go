//go:build integration

package authschema

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"
)

// authACLs lists every ACL in the auth schema (the schema, its relations and
// sequences) as text, in a stable order.
func authACLs(t *testing.T, ctx context.Context, env *authFixture) []string {
	t.Helper()
	rows, err := env.admin.Query(ctx, `
		SELECT 'schema:' || n.nspname || '=' || coalesce(n.nspacl::text, '') FROM pg_namespace n WHERE n.nspname = $1
		UNION ALL
		SELECT 'rel:' || c.relname || '=' || coalesce(c.relacl::text, '')
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relkind IN ('r', 'p', 'S', 'v', 'm', 'f')
		ORDER BY 1`, env.schema)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var acls []string
	for rows.Next() {
		var acl string
		if err := rows.Scan(&acl); err != nil {
			t.Fatal(err)
		}
		acls = append(acls, acl)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return acls
}

// A reader that inherits from the role that owns the auth schema is refused
// before any ACL changes.
func TestSessionReaderThatInheritsTheSchemaOwnerKeepsEveryACL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	env := newAuthFixture(t, ctx)
	reader, _ := newSessionReader(t, ctx, env, "auth_owner_reader")
	if _, err := env.admin.Exec(ctx, fmt.Sprintf(`GRANT %s TO %s`,
		Quote(mustValidatedIdentifier(env.migrationRole)), Quote(mustValidatedIdentifier(reader)))); err != nil {
		t.Fatal(err)
	}
	before := authACLs(t, ctx, env)
	if _, err := Apply(ctx, env.migration, readerOptions(env, reader)); !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("Apply with an owner reader: err = %v, want ErrInvalidOptions", err)
	}
	if after := authACLs(t, ctx, env); !reflect.DeepEqual(before, after) {
		t.Errorf("the auth ACLs changed under a refused reader:\nbefore %v\nafter  %v", before, after)
	}
}

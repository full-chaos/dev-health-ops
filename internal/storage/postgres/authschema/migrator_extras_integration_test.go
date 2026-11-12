//go:build integration

package authschema

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"
)

const migratorLeftoverQuery = `
SELECT c.relname::text, ''::text, a.privilege_type
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
CROSS JOIN LATERAL aclexplode(coalesce(c.relacl, acldefault('r', c.relowner))) a
WHERE n.nspname = 'public' AND c.relkind IN ('r','p') AND a.grantee = (SELECT oid FROM pg_roles WHERE rolname = $1)
UNION ALL
SELECT c.relname::text, att.attname::text, a.privilege_type
FROM pg_attribute att JOIN pg_class c ON c.oid = att.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
CROSS JOIN LATERAL aclexplode(att.attacl) a
WHERE n.nspname = 'public' AND c.relkind IN ('r','p') AND att.attnum > 0 AND NOT att.attisdropped
  AND a.grantee = (SELECT oid FROM pg_roles WHERE rolname = $1)`

// migratorRowsOutsideTheSet lists the public privileges the migrator holds that are neither in PublicPrivilegeSet nor the
// anchor privilege on a table of the set.
func migratorRowsOutsideTheSet(t *testing.T, ctx context.Context, env *ownerMoveEnv) []string {
	t.Helper()
	allowed := keysOfPrivilegeSet(PublicPrivilegeSet())
	for _, table := range PublicAnchorTables() {
		allowed[table+"|"+PublicAnchorPrivilege+"|"] = true
	}
	rows, err := env.admin.Query(ctx, migratorLeftoverQuery, env.migratorRole)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var outside []string
	for rows.Next() {
		var table, column, privilege string
		if err := rows.Scan(&table, &column, &privilege); err != nil {
			t.Fatal(err)
		}
		key := table + "|" + privilege + "|" + column
		if !allowed[key] {
			outside = append(outside, key)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	sort.Strings(outside)
	return outside
}

func seedMigratorLeftovers(t *testing.T, ctx context.Context, env *ownerMoveEnv) {
	t.Helper()
	migrator := Quote(mustValidatedIdentifier(env.migratorRole))
	for _, statement := range []string{
		`GRANT INSERT (is_superuser) ON public.users TO ` + migrator + ` WITH GRANT OPTION`,
		`GRANT SELECT (token_hash, user_id, org_id, family_id, expires_at, revoked_at) ON public.refresh_tokens TO ` + migrator + ` WITH GRANT OPTION`,
		`GRANT UPDATE (revoked_at) ON public.refresh_tokens TO ` + migrator + ` WITH GRANT OPTION`,
		`GRANT REFERENCES ON public.refresh_tokens TO ` + migrator,
	} {
		if _, err := env.old.Exec(ctx, statement); err != nil {
			t.Fatalf("seed %q: %v", statement, err)
		}
	}
}

// Prod shape (CHAOS-13499): the owner once gave the migrator INSERT(users.is_superuser) and the old refresh_tokens
// privileges, with grant option. A forward owner move (here the grants-only form) must leave the migrator holding the
// set and nothing else.
func TestOwnerMoveGrantsOnlyRevokesTheMigratorsLeftoverPublicPrivileges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	env := newOwnerMoveEnv(t, ctx, false)
	seedMigratorLeftovers(t, ctx, env)
	if got := migratorRowsOutsideTheSet(t, ctx, env); len(got) != 9 {
		t.Fatalf("setup: the migrator holds %d privileges outside the set, want 9: %v", len(got), got)
	}
	o := env.moveOptions(true)
	o.GrantsOnly = true
	env.move(t, ctx, o)
	if got := migratorRowsOutsideTheSet(t, ctx, env); len(got) != 0 {
		t.Errorf("the migrator holds %d public privileges outside the set after the move: %s", len(got), strings.Join(got, ", "))
	}
}

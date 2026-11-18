//go:build integration

package identity

import (
	"context"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// forceDeleteBlockingKeys are the four auth keys the ledger row names: a user a RESTRICT key names refuses the erase.
var forceDeleteBlockingKeys = []string{
	"auth.platform_role_assignments.(granted_by)", "auth.resource_grants.(principal_id)",
	"auth.resource_grants.(revoked_by)", "auth.service_accounts.(owner_principal_id)",
}

// erasedOwnRowKeys are the auth keys that name the principal's own row (or a principal of another kind): the erase
// deletes the own rows first, so they never refuse a user.
var erasedOwnRowKeys = []string{
	"auth.credential_projection.(principal_id)", "auth.memberships.(principal_id)",
	"auth.platform_role_assignments.(principal_id)", "auth.push_sources.(principal_id)",
	"auth.service_accounts.(principal_id)", "auth.users.(principal_id)", "auth.workloads.(principal_id)",
}

// The key set is read from the catalog after the whole migration chain ran on a real PostgreSQL: every foreign key (any
// schema, partition copies skipped) into a users or principals table whose delete action is RESTRICT or NO ACTION, keyed
// by schema.table.(columns in key order). It must equal, as an exact set, the blocking keys + the own-row keys + the
// public.users columns the erase nulls; an empty or short set fails. The ledger row must name exactly the blocking keys.
func TestForceDeleteBlockingKeysAreTheCatalogAndTheLedgerRow(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	rows, err := e.admin.Query(ctx, `SELECT n.nspname || '.' || cl.relname || '.(' ||
  (SELECT string_agg(a.attname, ',' ORDER BY k.ord) FROM unnest(c.conkey) WITH ORDINALITY AS k(attnum, ord)
   JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.attnum) || ')'
FROM pg_constraint c
JOIN pg_class cl ON cl.oid = c.conrelid
JOIN pg_namespace n ON n.oid = cl.relnamespace
JOIN pg_class rf ON rf.oid = c.confrelid
WHERE c.contype = 'f' AND c.conparentid = 0 AND c.confdeltype IN ('r', 'a') AND rf.relname IN ('principals', 'users')
  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
ORDER BY 1`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			t.Fatal(err)
		}
		got = append(got, key)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	want := slices.Concat(forceDeleteBlockingKeys, erasedOwnRowKeys)
	for _, c := range authschema.UserEraseNullColumns() {
		want = append(want, "public."+c.Table+".("+c.Column+")")
	}
	slices.Sort(want)
	if len(got) == 0 || !slices.Equal(got, want) {
		t.Fatalf("RESTRICT / NO ACTION keys into users and principals after the full chain:\n got  %v\n want %v", got, want)
	}

	raw, err := os.ReadFile("../../../ci/auth_exception_ledger.tsv")
	if err != nil {
		t.Fatal(err)
	}
	var rowsFound []string
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "known-stricter-force-delete-reference-blocked\t") {
			rowsFound = append(rowsFound, line)
		}
	}
	if len(rowsFound) != 1 {
		t.Fatalf("ledger rows for known-stricter-force-delete-reference-blocked = %d, want 1", len(rowsFound))
	}
	row := rowsFound[0]
	list := regexp.MustCompile(`The keys are ([^;]+);`).FindStringSubmatch(row)
	if list == nil {
		t.Fatal("the ledger row has no 'The keys are ...;' list")
	}
	var named []string
	for _, token := range regexp.MustCompile(`,\s*|\s+and\s+`).Split(list[1], -1) {
		if !regexp.MustCompile(`^[a-z_]+\.[a-z_]+$`).MatchString(token) {
			t.Fatalf("ledger key token %q is not an exact table.column", token)
		}
		parts := strings.SplitN(token, ".", 2)
		named = append(named, "auth."+parts[0]+".("+parts[1]+")")
	}
	slices.Sort(named)
	wantNamed := slices.Clone(forceDeleteBlockingKeys)
	slices.Sort(wantNamed)
	if !slices.Equal(named, wantNamed) {
		t.Errorf("ledger row names %v, want exactly %v", named, wantNamed)
	}
	if !strings.Contains(row, "\tpermanent\t") || !strings.Contains(row, "closed: accepted difference") {
		t.Error("ledger row is not a permanent row that states closed: accepted difference")
	}
}

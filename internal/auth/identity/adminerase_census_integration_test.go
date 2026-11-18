//go:build integration

package identity

import (
	"context"
	"fmt"
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
// public.users columns the erase nulls; an empty or short set fails. The ledger reason must name exactly the blocking keys, as a set of table.column tokens anywhere in the text.
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
	slices.Sort(got)
	got, want = slices.Compact(got), slices.Compact(want)
	if len(got) == 0 || !slices.Equal(got, want) {
		t.Fatalf("RESTRICT / NO ACTION keys into users and principals after the full chain:\n got  %v\n want %v", got, want)
	}

	raw, err := os.ReadFile("../../../ci/auth_exception_ledger.tsv")
	if err != nil {
		t.Fatal(err)
	}
	var rowsFound [][]string
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "known-stricter-force-delete-reference-blocked\t") {
			rowsFound = append(rowsFound, strings.Split(line, "\t"))
		}
	}
	if len(rowsFound) != 1 || len(rowsFound[0]) != 7 {
		t.Fatalf("ledger rows for known-stricter-force-delete-reference-blocked = %d (want 1 row of 7 fields)", len(rowsFound))
	}
	row := rowsFound[0]
	if err := checkLedgerKeys(row[6]); err != nil {
		t.Error(err)
	}
	if row[1] != "permanent" || !strings.Contains(row[6], "closed: accepted difference") {
		t.Error("ledger row is not a permanent row that states closed: accepted difference")
	}
}

// ledgerNamedKeys is the set of table.column tokens anywhere in a ledger reason: split on every character outside
// [a-z_.], trim a trailing '.', keep the exact table.column shapes. Punctuation and sentence order do not matter; a
// repeated token is one key.
func ledgerNamedKeys(reason string) []string {
	shape := regexp.MustCompile(`^[a-z_]+\.[a-z_]+$`)
	var keys []string
	for _, token := range regexp.MustCompile(`[^a-z_.]+`).Split(reason, -1) {
		token = strings.TrimRight(token, ".")
		if shape.MatchString(token) {
			parts := strings.SplitN(token, ".", 2)
			keys = append(keys, "auth."+parts[0]+".("+parts[1]+")")
		}
	}
	slices.Sort(keys)
	return slices.Compact(keys)
}

// checkLedgerKeys fails closed both ways: an extra token, a missing blocking key, or no token at all.
func checkLedgerKeys(reason string) error {
	named := ledgerNamedKeys(reason)
	want := slices.Clone(forceDeleteBlockingKeys)
	slices.Sort(want)
	if len(named) == 0 || !slices.Equal(named, want) {
		return fmt.Errorf("ledger reason names %v, want exactly %v", named, want)
	}
	return nil
}

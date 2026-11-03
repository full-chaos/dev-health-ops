package postgres

import (
	"regexp"
	"testing"
)

// A table named in both manifests (users while the platform roles are owned:
// table-wide SELECT and DELETE, column-scoped INSERT and UPDATE) must not read
// as an excess INSERT. The UPDATE half has this carve-out (CHAOS-6747); the
// INSERT half must have it too, in the readiness query and in its diagnostic.
var (
	mixedInsertReadiness = regexp.MustCompile(`NOT required\.allow_insert\s+AND 'INSERT' = ANY \(held\.any_column\)(?:[^O]|O[^R])*?declared\.privilege = 'INSERT'`)
	// The diagnostic carve-out is exact: a column INSERT is excess unless the (table, column) pair is declared ($7, $8).
	mixedInsertDiagnosis = regexp.MustCompile(`(?s)NOT allow_insert AND \(\s*has_table_privilege\(\$1, relation, 'INSERT'\)\s+OR EXISTS \(.*?pg_attribute.*?has_column_privilege\(\$1, relation, \w+\.attname, 'INSERT'\)\s+AND NOT EXISTS \(\s+SELECT 1 FROM unnest\(\$7::text\[\], \$8::text\[\]\).*?declared\.table_name = resolved\.table_name AND declared\.column_name = attribute\.attname.*?\) AS excess_insert`)
	// The column sweep refuses an INSERT on any undeclared column; only a table-wide INSERT flag exempts it.
	undeclaredInsertSweep = regexp.MustCompile(`privilege_type\.privilege = 'INSERT' AND mixed\.allow_insert`)
)

func TestRolePostureQueryAllowsDeclaredColumnInsertOnMixedTable(t *testing.T) {
	t.Parallel()
	if !mixedInsertReadiness.MatchString(rolePostureQuery) {
		t.Fatal("rolePostureQuery refuses a column INSERT the posture declares on a table that is also in RequiredTables: carve it out as the UPDATE half does")
	}
}

func TestDiagnoseTablePostureQueryAllowsDeclaredColumnInsertOnMixedTable(t *testing.T) {
	t.Parallel()
	if !mixedInsertDiagnosis.MatchString(diagnoseTablePostureQuery) {
		t.Fatal("diagnoseTablePostureQuery reports excess_insert for a declared column INSERT on a mixed table: mirror the UPDATE carve-out ($7 = tables with an INSERT column)")
	}
}

func TestRolePostureQueryColumnSweepRefusesUndeclaredColumnInsertOnMixedTable(t *testing.T) {
	t.Parallel()
	if !undeclaredInsertSweep.MatchString(rolePostureQuery) {
		t.Fatal("rolePostureQuery's column sweep no longer ties its INSERT exemption to the table-wide allow_insert flag: an undeclared column INSERT on a mixed table must stay excess")
	}
	if got := len(regexp.MustCompile(`privilege_type\.privilege = 'INSERT'`).FindAllString(rolePostureQuery, -1)); got != 1 {
		t.Fatalf("rolePostureQuery's column sweep names INSERT %d times, want 1 (the allow_insert exemption alone)", got)
	}
}

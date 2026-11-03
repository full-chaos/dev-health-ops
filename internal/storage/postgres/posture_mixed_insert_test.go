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
	mixedInsertDiagnosis = regexp.MustCompile(`NOT allow_insert AND \(\s*has_table_privilege\(\$1, relation, 'INSERT'\)\s+OR \(has_any_column_privilege\(\$1, relation, 'INSERT'\) AND NOT table_name = ANY\(COALESCE\(\$7::text\[\], '\{\}'::text\[\]\)\)\)`)
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

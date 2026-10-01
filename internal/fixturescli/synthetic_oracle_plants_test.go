//go:build integration

package fixturescli

import (
	"strings"
	"testing"
)

func oracleFixtureTable(mutate func(row []any)) FrozenTable {
	columns := []FrozenColumn{
		{Name: "org_id", Type: "String"},
		{Name: "source_version_at", Type: "DateTime64(6, 'UTC')"},
		{Name: "source_revision", Type: "UInt128"},
		{Name: "source_conflict_key", Type: "String"},
		{Name: "ingest_revision", Type: "UInt128"},
		{Name: "ordering_contract", Type: "UInt8"},
		{Name: "id", Type: "String"},
		{Name: "service_id", Type: "String"},
	}
	row := []any{"org", "2026-08-01 00:00:00.000000", "33024892065891515822595855507067670", "6f70", "33024892065891515822595855507067670", 2, "m1", "svc"}
	if mutate != nil {
		mutate(row)
	}
	return FrozenTable{Name: "operational_service_repository_mappings", Columns: columns, Rows: [][]any{row}}
}

// The oracle's comparison keeps the stamp itself tested and excludes only the
// clock-derived values: a different revision, conflict key and clock agree; a
// wrong non-clock value, a missing stamp and an unexpected column do not.
func TestOracleComparisonExcludesOnlyTheClockDerivedOrderingValues(t *testing.T) {
	live := oracleFixtureTable(nil)
	clockDifferent := oracleFixtureTable(func(row []any) {
		row[1] = "2026-08-02 00:00:00.000000"
		row[2] = "33024892188676198799736574376954156"
		row[3] = "6f71"
		row[4] = "33024892188676198799736574376954156"
	})
	if problem := oracleTableProblem(t, live, clockDifferent); problem != "" {
		t.Fatalf("clock-derived ordering values must not fail the comparison: %s", problem)
	}

	for name, tc := range map[string]struct {
		loaded func() FrozenTable
		want   string
	}{
		"a wrong non-clock value": {
			func() FrozenTable { return oracleFixtureTable(func(row []any) { row[7] = "other-service" }) },
			"rows differ",
		},
		"a row with no stamp (ordering_contract 0)": {
			func() FrozenTable { return oracleFixtureTable(func(row []any) { row[5] = 0 }) },
			"ordering_contract 0, want 2",
		},
		"a row with an empty revision": {
			func() FrozenTable { return oracleFixtureTable(func(row []any) { row[2] = "0" }) },
			"source_revision 0 is empty",
		},
		"a row with an empty conflict key": {
			func() FrozenTable { return oracleFixtureTable(func(row []any) { row[3] = "" }) },
			"source_conflict_key is empty",
		},
		"a fifth unexpected column": {
			func() FrozenTable {
				table := oracleFixtureTable(nil)
				table.Columns = append(table.Columns, FrozenColumn{Name: "surprise", Type: "String"})
				table.Rows[0] = append(table.Rows[0], "x")
				return table
			},
			"columns differ",
		},
	} {
		problem := oracleTableProblem(t, live, tc.loaded())
		if problem == "" || !strings.Contains(problem, tc.want) {
			t.Errorf("%s: problem %q, want one containing %q", name, problem, tc.want)
		}
	}
}

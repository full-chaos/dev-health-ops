//go:build integration

package fixturescli

import (
	"fmt"
	"math/big"
	"reflect"
	"testing"
)

// ordering columns whose VALUES the live-producer oracle does not compare: they
// are derived from each side's own clock (the producer reads the clock of its
// run; the loader stamps from the frozen row), so two runs never agree on them.
// They are checked separately, as a stamp (stampProblem), so a missing or wrong
// stamp still fails. The exclusion is closed: exactly the four columns of
// orderingColumnDefs, removed by withoutOrdering, which refuses a table that
// holds only some of them.

// stampProblem returns "" when every row of an operational table carries a
// well-formed contract-2 stamp: ordering_contract 2, a non-zero source_revision
// and ingest_revision, and a non-empty source_conflict_key. A table that is not
// an operational entity table is not checked.
func stampProblem(table FrozenTable) string {
	if _, operational := operationalFamilies[table.Name]; !operational {
		return ""
	}
	position := map[string]int{}
	for index, column := range table.Columns {
		position[column.Name] = index
	}
	for _, ordering := range orderingColumnDefs {
		if _, ok := position[ordering.Name]; !ok {
			return fmt.Sprintf("%s has no %s column", table.Name, ordering.Name)
		}
	}
	for number, row := range table.Rows {
		if got := fmt.Sprint(row[position["ordering_contract"]]); got != "2" {
			return fmt.Sprintf("%s row %d: ordering_contract %s, want 2", table.Name, number, got)
		}
		for _, name := range []string{"source_revision", "ingest_revision"} {
			value, ok := new(big.Int).SetString(fmt.Sprint(row[position[name]]), 10)
			if !ok || value.Sign() == 0 {
				return fmt.Sprintf("%s row %d: %s %v is empty", table.Name, number, name, row[position[name]])
			}
		}
		if fmt.Sprint(row[position["source_conflict_key"]]) == "" {
			return fmt.Sprintf("%s row %d: source_conflict_key is empty", table.Name, number)
		}
	}
	return ""
}

// oracleTableProblem compares one table the live Python producer wrote with the
// same table the loader wrote: both must carry a contract-2 stamp (stampProblem),
// and the rest of the table, the four ordering columns excluded and timestamps
// masked, must be equal column for column and row for row. "" means they agree.
func oracleTableProblem(t *testing.T, live, loaded FrozenTable) string {
	t.Helper()
	if problem := stampProblem(live); problem != "" {
		return "live producer: " + problem
	}
	if problem := stampProblem(loaded); problem != "" {
		return "loader: " + problem
	}
	live, loaded = withoutOrdering(t, live), withoutOrdering(t, loaded)
	if !reflect.DeepEqual(loaded.Columns, live.Columns) {
		return fmt.Sprintf("%s: columns differ\n live   %v\n loader %v", live.Name, live.Columns, loaded.Columns)
	}
	if diff := rowsDiff(maskTimes(live), maskTimes(loaded)); diff != "" {
		return fmt.Sprintf("the loader's rows differ from the live Python producer's (timestamps masked, ordering columns excluded):\n%s", diff)
	}
	return ""
}

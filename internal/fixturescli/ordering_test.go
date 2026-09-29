package fixturescli

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestStampOrderingAddsTheContractTwoColumns(t *testing.T) {
	table := WorldTable{FrozenTable: FrozenTable{
		Name: "operational_incidents",
		Columns: []FrozenColumn{
			{Name: "org_id", Type: "UUID"}, {Name: "source_version_at", Type: "DateTime64(6, 'UTC')"},
			{Name: "id", Type: "UUID"}, {Name: "observed_at", Type: "DateTime64(6, 'UTC')"},
			{Name: "last_synced", Type: "DateTime64(6, 'UTC')"}, {Name: "relationship_confidence", Type: "Nullable(Float64)"},
			{Name: "is_deleted", Type: "UInt8"}, {Name: "title", Type: "String"},
		},
	}}
	live := []any{"11111111-2222-4333-8444-555555555555", "2026-09-01 10:00:00.000000", "aaaaaaaa-2222-4333-8444-555555555555",
		"2026-09-01 10:00:01.000000", "2026-09-01 10:00:02.000000", nil, json.Number("0"), "db down"}
	tombstone := append([]any{}, live...)
	tombstone[6] = json.Number("1")

	stamped, rows, err := stampOrdering(table, [][]any{live, tombstone})
	if err != nil {
		t.Fatal(err)
	}
	if len(stamped.Columns) != len(table.Columns)+4 || len(rows[0]) != len(stamped.Columns) {
		t.Fatalf("columns %d, row %d", len(stamped.Columns), len(rows[0]))
	}
	tail := rows[0][len(rows[0])-4:]
	if tail[3] != 2 || tail[0] == "0" || !strings.HasPrefix(tail[1].(string), "6f7065726174696f6e616c2d636f6e666c6963742d7631") {
		t.Fatalf("stamp %v: want contract 2, a revision and a hex key led by the conflict domain", tail)
	}
	if rows[0][len(rows[0])-4] == rows[1][len(rows[1])-4] {
		t.Fatalf("a tombstone and a live row of one entity share a source revision: the rank is not applied")
	}
	if len(table.Columns) != 8 {
		t.Fatalf("the input table was changed")
	}
	if again, _, _ := stampOrdering(stamped, rows); len(again.Columns) != len(stamped.Columns) {
		t.Fatalf("a stamped table was stamped twice")
	}
	other := WorldTable{FrozenTable: FrozenTable{Name: "git_commits", Columns: []FrozenColumn{{Name: "a", Type: "String"}}}}
	if same, _, _ := stampOrdering(other, nil); len(same.Columns) != 1 {
		t.Fatalf("a table that is not an operational entity table was stamped")
	}
}

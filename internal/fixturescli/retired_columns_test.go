package fixturescli

import (
	"reflect"
	"testing"
)

// CHAOS-9161: the frozen worlds still carry teams.members, which migration 118 dropped. The load
// never names a retired column, and the rows are projected with it, value by value.
func TestFrozenTeamsLoadWithoutTheRetiredRosterColumn(t *testing.T) {
	table := FrozenTable{
		Name:    "teams",
		Columns: []FrozenColumn{{Name: "id", Type: "String"}, {Name: "members", Type: "Array(String)"}, {Name: "manual_members", Type: "Array(String)"}},
		Rows:    [][]any{{"t1", []any{"a"}, []any{"b"}}},
	}
	for name, got := range map[string]FrozenTable{"load": table.WithoutServerStamped(), "compare": table.WithoutRetired()} {
		var names []string
		for _, column := range got.Columns {
			names = append(names, column.Name)
		}
		if !reflect.DeepEqual(names, []string{"id", "manual_members"}) {
			t.Errorf("%s: columns = %v, want [id manual_members]", name, names)
		}
		if !reflect.DeepEqual(got.Rows, [][]any{{"t1", []any{"b"}}}) {
			t.Errorf("%s: rows = %v, want the manual_members value kept in its own position", name, got.Rows)
		}
	}
	other := FrozenTable{Name: "projects", Columns: []FrozenColumn{{Name: "members", Type: "String"}}}
	if got := other.WithoutRetired(); len(got.Columns) != 1 {
		t.Errorf("a column named members of another table was dropped: %v", got.Columns)
	}
}

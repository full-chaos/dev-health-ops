package clickhouse

import (
	"reflect"
	"testing"
)

// With the ACR access read's project scope off, the posture is exactly
// APIPosture: the readiness check and the generated users.d are unchanged.
// With it on, the posture adds SELECT on projects and nothing else.
func TestAPIPostureForAddsOnlyTheProjectsSelectWithTheProjectScopeOn(t *testing.T) {
	const database = "default"
	if got := APIPostureFor(database, false); !reflect.DeepEqual(got, APIPosture(database)) {
		t.Fatalf("project scope off: posture differs from APIPosture:\n%+v", got)
	}
	on := APIPostureFor(database, true)
	base := APIPosture(database)
	if len(on.RequiredTables) != len(base.RequiredTables)+1 || !reflect.DeepEqual(on.RequiredTables[:len(base.RequiredTables)], base.RequiredTables) {
		t.Fatalf("project scope on: want APIPosture plus one row, got %d rows", len(on.RequiredTables))
	}
	added := on.RequiredTables[len(on.RequiredTables)-1]
	if added != (TableGrant{Database: database, Table: "projects", AllowSelect: true}) || added != ProjectScopeGrant(database) {
		t.Fatalf("project scope on: added row %+v, want SELECT on %s.projects only", added, database)
	}
	for _, row := range base.RequiredTables {
		if row.Table == "projects" {
			t.Fatal("APIPosture already grants projects: the project scope grant must stay out of it while the flag is off")
		}
	}
	// Repeated calls do not share or grow the slice.
	if again := APIPostureFor(database, true); len(again.RequiredTables) != len(on.RequiredTables) {
		t.Fatalf("a second call has %d rows, want %d", len(again.RequiredTables), len(on.RequiredTables))
	}
}

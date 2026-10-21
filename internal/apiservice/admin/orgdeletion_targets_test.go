package admin

import (
	"encoding/json"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// pythonDeletionTarget is one entry of the introspection script's JSON
// output below -- the SHAPE of org_deletion.py's predicate for one table,
// not just its name.
type pythonDeletionTarget struct {
	Table string `json:"table"`
	Kind  string `json:"kind"` // "direct" or "via"
	// Direct fields.
	Column string `json:"column"`
	UUID   bool   `json:"uuid"`
	// Via fields.
	SubTable     string `json:"sub_table"`
	SubIDColumn  string `json:"sub_id_column"`
	SubOrgColumn string `json:"sub_org_column"`
	SubOrgUUID   bool   `json:"sub_org_uuid"`
}

// TestOrgDeletionTargetsMatchThePythonList is the rot guard team-lead's
// CHAOS-6306 condition 3 names: it reads org_deletion.py's own
// _postgres_targets() list and fails loudly if orgDeletionTargets drifts
// from it in table set, order, or shape. It names no lane, PR, or ticket:
// only org_deletion.py's own module and this package's own type.
//
// An adversarial review found the original version compared only table
// names plus "exactly one of Direct/Via is set" -- a predicate mistake on
// the actual scoping COLUMN, its UUID-vs-string bind type, or (for an
// indirect target) the subquery's table/column shape would pass silently
// as long as it picked a real column on the right table. This version
// The frozen answer was produced by executing each Python predicate lambda with two distinguishable sentinel
// values (a fixed uuid.UUID and a non-UUID string) and inspecting the
// resulting SQLAlchemy expression tree directly -- for a direct predicate,
// the bound column name and whether the bound sentinel it compares against
// was the uuid.UUID one or the string one; for an indirect (`.in_(select
// ...)`) predicate, the outer foreign-id column plus the subquery's own
// table/id-column/org-column/bind-type -- rather than trusting the
// predicate's declared table name alone.
// orgDeletionTargetsPythonBuild is the build whose org_deletion._postgres_targets
// answered the frozen list: main when it was recorded.
const orgDeletionTargetsPythonBuild = "a4847c5e93607451a0c987b314d37e02fc43ce85"

func TestOrgDeletionTargetsMatchThePythonList(t *testing.T) {
	golden := venueoracle.OpenGolden(t, venueoracle.GoldenSpec{
		Path:        "testdata/admin/org_deletion_targets.json",
		PythonBuild: orgDeletionTargetsPythonBuild,
		SHA256:      "f98db083aff5545c6c00857c406bd08df051e02815f802854a5ebf03618c1733",
	})
	answers := golden.Produce(t, []venueoracle.Request{venueoracle.ProgramRequest("org deletion targets", nil)})
	golden.Consumed(t, answers...)
	output := []byte(answers[0].Body)

	var pythonTargets []pythonDeletionTarget
	if err := json.Unmarshal(output, &pythonTargets); err != nil {
		t.Fatalf("decode python output: %v\n%s", err, output)
	}

	if len(pythonTargets) != len(orgDeletionTargets) {
		pythonTables := make([]string, len(pythonTargets))
		for i, target := range pythonTargets {
			pythonTables[i] = target.Table
		}
		goTables := make([]string, len(orgDeletionTargets))
		for i, target := range orgDeletionTargets {
			goTables[i] = target.Table
		}
		t.Fatalf("table count: python=%d go=%d\npython=%v\ngo=%v", len(pythonTargets), len(goTables), pythonTables, goTables)
	}

	seen := map[string]bool{}
	for i, py := range pythonTargets {
		goTarget := orgDeletionTargets[i]
		if py.Table != goTarget.Table {
			t.Fatalf("table[%d]: python=%q go=%q (order matters -- it is the response's key order)", i, py.Table, goTarget.Table)
		}
		if seen[goTarget.Table] {
			t.Fatalf("duplicate table in orgDeletionTargets: %s", goTarget.Table)
		}
		seen[goTarget.Table] = true
		if (goTarget.Direct == nil) == (goTarget.Via == nil) {
			t.Fatalf("%s: exactly one of Direct/Via must be set", goTarget.Table)
		}

		switch py.Kind {
		case "direct":
			if goTarget.Direct == nil {
				t.Fatalf("%s: python predicate is direct (column=%s), go has Via instead", goTarget.Table, py.Column)
			}
			if goTarget.Direct.Column != py.Column {
				t.Fatalf("%s: direct column: python=%q go=%q", goTarget.Table, py.Column, goTarget.Direct.Column)
			}
			if goTarget.Direct.UUIDType != py.UUID {
				t.Fatalf("%s: direct column %q uuid-bind: python=%v go=%v", goTarget.Table, py.Column, py.UUID, goTarget.Direct.UUIDType)
			}
		case "via":
			if goTarget.Via == nil {
				t.Fatalf("%s: python predicate is indirect (column=%s -> %s.%s where %s), go has Direct instead", goTarget.Table, py.Column, py.SubTable, py.SubIDColumn, py.SubOrgColumn)
			}
			if goTarget.Via.Column != py.Column {
				t.Fatalf("%s: via foreign column: python=%q go=%q", goTarget.Table, py.Column, goTarget.Via.Column)
			}
			if goTarget.Via.SubTable != py.SubTable {
				t.Fatalf("%s: via sub-table: python=%q go=%q", goTarget.Table, py.SubTable, goTarget.Via.SubTable)
			}
			if goTarget.Via.SubIDColumn != py.SubIDColumn {
				t.Fatalf("%s: via sub-id-column: python=%q go=%q", goTarget.Table, py.SubIDColumn, goTarget.Via.SubIDColumn)
			}
			if goTarget.Via.SubOrgColumn != py.SubOrgColumn {
				t.Fatalf("%s: via sub-org-column: python=%q go=%q", goTarget.Table, py.SubOrgColumn, goTarget.Via.SubOrgColumn)
			}
			if goTarget.Via.UUIDOrgColumn != py.SubOrgUUID {
				t.Fatalf("%s: via sub-org-column %q uuid-bind: python=%v go=%v", goTarget.Table, py.SubOrgColumn, py.SubOrgUUID, goTarget.Via.UUIDOrgColumn)
			}
		default:
			t.Fatalf("%s: python predicate introspection returned an unrecognized shape: %s", goTarget.Table, py.Kind)
		}
	}
	golden.SkipDiff(t)
	golden.Finish(t)
}

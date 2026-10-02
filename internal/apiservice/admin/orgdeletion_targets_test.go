package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// orgDeletionRepoRoot walks up from this file to the directory holding
// src/dev_health_ops (this repo's Python source root) -- same walk as
// venuehelpers_test.go's own repoRoot, duplicated here because that one is
// integration-tagged and this test deliberately is not: it needs Python to
// import one module, never a database or container.
func orgDeletionRepoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate this test's source file")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}

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
// EXECUTES each Python predicate lambda with two distinguishable sentinel
// values (a fixed uuid.UUID and a non-UUID string) and inspects the
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
		Recipe: "git worktree add --detach $DIR " + orgDeletionTargetsPythonBuild + " (with its .venv: uv sync --frozen --no-install-project); then from the repository root: " +
			"go run ./internal/testsupport/venueoracle/goldenrecord -pkg ./internal/apiservice/admin/ -test '^TestOrgDeletionTargetsMatchThePythonList$' -python-root $DIR",
	})
	root := golden.PythonRoot(t, orgDeletionRepoRoot(t))

	script := `
import json
import sys
import uuid
sys.path.insert(0, "src")
from dev_health_ops.api.services import org_deletion
from sqlalchemy.sql import operators

sentinel_uuid = uuid.UUID(int=1)
sentinel_str = "sentinel-org-id-string-not-a-uuid"

def describe(target):
    expr = target.predicate(sentinel_uuid, sentinel_str)
    if expr.operator is operators.eq:
        return {
            "table": target.table,
            "kind": "direct",
            "column": expr.left.name,
            "uuid": expr.right.value == sentinel_uuid,
        }
    if expr.operator is operators.in_op:
        sub = expr.right.element
        where = sub.whereclause
        return {
            "table": target.table,
            "kind": "via",
            "column": expr.left.name,
            "sub_table": sub.get_final_froms()[0].name,
            "sub_id_column": sub.selected_columns[0].name,
            "sub_org_column": where.left.name,
            "sub_org_uuid": where.right.value == sentinel_uuid,
        }
    return {"table": target.table, "kind": "unrecognized-operator:" + str(expr.operator)}

out = [describe(t) for t in org_deletion._postgres_targets()]
print(json.dumps(out))
`
	answers := golden.Produce(t, root, []venueoracle.Request{venueoracle.ProgramRequest("org deletion targets", script, nil, nil)},
		func(producer *venueoracle.Producer, _ []venueoracle.Request) []venueoracle.Response {
			// The harness starts the child in the closed environment: nothing
			// ambient shapes its answer.
			cmd, err := producer.Command(context.Background(), nil, nil, "-c", script)
			if err != nil {
				t.Fatal(err)
			}
			cmd.Dir = producer.Root
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			output, err := cmd.Output()
			if err != nil {
				t.Fatalf("python: %v\n%s", err, stderr.String())
			}
			return []venueoracle.Response{{Status: 0, Body: string(output)}}
		})
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

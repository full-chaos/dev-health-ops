package admin

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
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
	directory := filepath.Dir(file)
	for {
		if info, statErr := os.Stat(filepath.Join(directory, "src", "dev_health_ops")); statErr == nil && info.IsDir() {
			return directory
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatalf("no src/dev_health_ops above %s", file)
		}
		directory = parent
	}
}

// orgDeletionInterpreter resolves a Python interpreter with this repo's
// dependencies importable: the repo .venv first, PATH python3 as a
// fallback (mirrors the venue oracle's own pyoracle interpreter
// resolution, without pulling in that package's testcontainers/database
// machinery for what is otherwise a fast, no-DB test).
func orgDeletionInterpreter(t *testing.T, root string) string {
	t.Helper()
	venv := filepath.Join(root, ".venv", "bin", "python3")
	if info, err := os.Stat(venv); err == nil && !info.IsDir() {
		return venv
	}
	path, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("no python3 interpreter available")
	}
	return path
}

// TestOrgDeletionTargetsMatchThePythonList is the rot guard team-lead's
// CHAOS-6306 condition 3 names: it reads org_deletion.py's own
// _postgres_targets() list -- table name, and whether the scoping
// predicate is direct (this table's own org_id/id/target_org_id column,
// and whether that column is a native uuid) or indirect (one level
// through another table's own org_id column) -- and fails loudly if
// orgDeletionTargets drifts from it in table set, order, or shape. It
// names no lane, PR, or ticket: only org_deletion.py's own module and
// this package's own type.
func TestOrgDeletionTargetsMatchThePythonList(t *testing.T) {
	root := orgDeletionRepoRoot(t)
	interpreter := orgDeletionInterpreter(t, root)

	script := `
import json
import sys
sys.path.insert(0, "src")
from dev_health_ops.api.services import org_deletion

out = []
for target in org_deletion._postgres_targets():
    out.append(target.table)
print(json.dumps(out))
`
	cmd := exec.Command(interpreter, "-c", script)
	cmd.Dir = root
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("python: %v\n%s", err, output)
	}

	var pythonTables []string
	if err := json.Unmarshal(output, &pythonTables); err != nil {
		t.Fatalf("decode python output: %v\n%s", err, output)
	}

	goTables := make([]string, len(orgDeletionTargets))
	for i, target := range orgDeletionTargets {
		goTables[i] = target.Table
	}

	if len(pythonTables) != len(goTables) {
		t.Fatalf("table count: python=%d go=%d\npython=%v\ngo=%v", len(pythonTables), len(goTables), pythonTables, goTables)
	}
	for i := range pythonTables {
		if pythonTables[i] != goTables[i] {
			t.Fatalf("table[%d]: python=%q go=%q (order matters -- it is the response's key order)", i, pythonTables[i], goTables[i])
		}
	}

	seen := map[string]bool{}
	for _, target := range orgDeletionTargets {
		if seen[target.Table] {
			t.Fatalf("duplicate table in orgDeletionTargets: %s", target.Table)
		}
		seen[target.Table] = true
		if (target.Direct == nil) == (target.Via == nil) {
			t.Fatalf("%s: exactly one of Direct/Via must be set", target.Table)
		}
	}
}

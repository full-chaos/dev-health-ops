package docstools_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func driftRepoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above the test directory")
		}
		dir = parent
	}
}

var driftPythonFiles = []string{
	"scripts/check_investment_docs_drift.py",
	"scripts/check_queue_mapping_docs_drift.py",
	"tests/docs/test_investment_drift.py",
	"tests/docs/test_queue_mapping_drift.py",
}

func TestDocsDriftGuardsRunNoPython(t *testing.T) {
	root := driftRepoRoot(t)
	for _, rel := range driftPythonFiles {
		if _, err := os.Stat(filepath.Join(root, rel)); !os.IsNotExist(err) {
			t.Errorf("%s still exists (stat err: %v); the drift guard is `cigate docs`", rel, err)
		}
	}
	raw, err := os.ReadFile(filepath.Join(root, "Makefile"))
	if err != nil {
		t.Fatal(err)
	}
	for number, line := range strings.Split(string(raw), "\n") {
		for _, rel := range driftPythonFiles[:2] {
			if strings.Contains(line, rel) {
				t.Errorf("Makefile:%d runs %s: %s", number+1, rel, strings.TrimSpace(line))
			}
		}
	}
}

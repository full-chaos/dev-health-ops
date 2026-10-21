package integrationshards_test

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// Verbatim copies of helpers that live in a sibling package; this package needs them and cannot import another package's _test files.

func repoRoot(t *testing.T) string {
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

func rowFiles(t *testing.T, dir string) []string {
	t.Helper()
	names, err := filepath.Glob(filepath.Join(dir, "*.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(names)
	return names
}

func dataRows(t *testing.T, dir string) [][]string {
	var rows [][]string
	for _, f := range rowFiles(t, dir) {
		for _, line := range strings.Split(readText(t, f), "\n") {
			if strings.HasPrefix(line, "<<<<<<<") || strings.HasPrefix(line, "=======") || strings.HasPrefix(line, ">>>>>>>") {
				t.Fatalf("%s holds a conflict marker", f)
			}
			if strings.TrimSpace(line) != "" && !strings.HasPrefix(line, "#") {
				rows = append(rows, strings.Split(line, "\t"))
			}
		}
	}
	return rows
}

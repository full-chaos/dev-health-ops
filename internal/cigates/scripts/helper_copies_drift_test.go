package scripts_test

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The packages under internal/cigates keep their own copies of shared test helpers, because a _test.go file cannot be
// imported. This guard fails when a copy drifts from its source, so a fix made in one copy cannot silently diverge.

func withoutPackageClause(s string) string {
	_, rest, _ := strings.Cut(s, "\n")
	return rest
}

func funcText(t *testing.T, path, name string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`(?ms)^func ` + regexp.QuoteMeta(name) + `\(.*?^}\n`).Find(raw)
	if m == nil {
		t.Fatalf("%s: func %s not found", path, name)
	}
	return string(m)
}

func TestHelperCopiesAreByteIdenticalAcrossPackages(t *testing.T) {
	root := repoRoot(t)
	base := filepath.Join(root, "internal/cigates")
	read := func(rel string) string {
		raw, err := os.ReadFile(filepath.Join(base, rel))
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	want := sha256.Sum256([]byte(withoutPackageClause(read("scripts/ci_script_helpers_test.go"))))
	for _, pkg := range []string{"affected", "checkgo", "workflows"} {
		rel := pkg + "/ci_script_helpers_test.go"
		if got := sha256.Sum256([]byte(withoutPackageClause(read(rel)))); got != want {
			t.Errorf("%s differs from scripts/ci_script_helpers_test.go beyond the package clause", rel)
		}
	}
	copies := []struct{ copyFile, name, homeFile string }{
		{"affected/support_test.go", "repoRoot", "workflows/aggregate_gate_results_test.go"},
		{"scripts/support_test.go", "repoRoot", "workflows/aggregate_gate_results_test.go"},
		{"checkgo/support_test.go", "repoRoot", "workflows/aggregate_gate_results_test.go"},
		{"scripts/support_test.go", "tail", "checkgo/check_go_script_test.go"},
		{"checkgo/support_test.go", "rowFiles", "scripts/shard_row_files_merge_test.go"},
		{"checkgo/support_test.go", "dataRows", "scripts/shard_row_files_merge_test.go"},
	}
	for _, c := range copies {
		if funcText(t, filepath.Join(base, c.copyFile), c.name) != funcText(t, filepath.Join(base, c.homeFile), c.name) {
			t.Errorf("%s: func %s differs from its source in %s", c.copyFile, c.name, c.homeFile)
		}
	}
}

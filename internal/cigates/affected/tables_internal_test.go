package affected

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The glob and refusal cases of the deleted ci/go_relevance.sh corpus, as classes.tsv globs answer them.
func TestClassGlobs(t *testing.T) {
	cases := []struct {
		glob, path string
		want       bool
	}{
		{"docs/*.md", "docs/sub/a.md", false},
		{"docs/*.md", "docs/a.md", true},
		{"scripts/**", "scripts/a/b/c.sh", true},
		{"**/testdata/**", "testdata/x", true},
		{"**/testdata/**", "pkg/sub/testdata/x.txt", true},
		{"docs/c.rgo", "docs/cargo", false},
		{"Makefile", "docs/Makefile", false},
		{"internal/a+b(c)/*", "internal/a+b(c)/x.txt", true},
		{"docs/*.md", "docs/r\u00e9sum\u00e9.md", true},
		{"docs/?.md", "docs/\u0663.md", true},
		{".codex-review*", ".codex-review-context.md", true},
		{"ci/check_go.sh", "ci/check_go.sh.bak", false},
	}
	for _, c := range cases {
		if got := globMatch(c.glob, c.path); got != c.want {
			t.Errorf("globMatch(%q, %q) = %v, want %v", c.glob, c.path, got, c.want)
		}
	}
	for _, bad := range []string{"", "/abs/**", "docs/", "docs/**x", "docs/["} {
		if err := checkGlob(bad); err == nil {
			t.Errorf("checkGlob(%q) accepted a malformed glob", bad)
		}
	}
}

// A walk's filter bounds what it covers; an unexpressed walk covers everything.
func TestWalkRowFilterCovers(t *testing.T) {
	cases := []struct {
		root, depth, filter, path string
		want                      bool
	}{
		{".", depthTree, "*", "README.md", true},
		{".", depthTree, "*.go", "README.md", false},
		{".", depthTree, "*.go", "internal/x/x.go", true},
		{".", depthTree, "*_test.go", "internal/x/x.go", false},
		{"docs", depthDir, "*.md", "docs/a.md", true},
		{"docs", depthDir, "*.md", "docs/sub/a.md", false},
		{"docs", depthTree, "*.go", "docs/a.md", false},
		{unexpressed, "-", "-", "README.md", true},
	}
	for _, c := range cases {
		w := walkRow{root: c.root, depth: c.depth, filter: c.filter}
		if got := w.covers(c.path); got != c.want {
			t.Errorf("walk %s %s %s covers(%q) = %v, want %v", c.root, c.depth, c.filter, c.path, got, c.want)
		}
	}
	for _, f := range []string{"*", "*.go", "*_test.go"} {
		if !validFilter(f) {
			t.Errorf("validFilter(%q) = false", f)
		}
	}
	for _, f := range []string{"", "-", ".go", "**.go", "*/x.go", "*.[ch]"} {
		if validFilter(f) {
			t.Errorf("validFilter(%q) = true", f)
		}
	}
}

// CHAOS-13101: reach.d holds one file per package, so two changes that add rows for different packages touch different
// files. The writer drops the file of a package with no rows and the legacy reach.tsv; the loader reads the union and
// refuses a row filed under another package or a reach.tsv left beside it.
func TestReachDirSplitsByPackage(t *testing.T) {
	dir := t.TempDir()
	rows := map[string]bool{"a/b\tx.go\tc": true, "a/b\tx.go\td": true, "e\ty.go\t*": true, ".\tz.go\t-": true}
	if err := os.MkdirAll(filepath.Join(dir, reachDir), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{reachLegacyFile, reachDir + "/gone.tsv"} {
		if err := os.WriteFile(filepath.Join(dir, f), []byte("x\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := writeReach(dir, rows); err != nil {
		t.Fatal(err)
	}
	names, err := reachFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(names, " "); got != "_root.tsv a__b.tsv e.tsv" {
		t.Fatalf("reach.d files %q", got)
	}
	if _, err := os.Stat(filepath.Join(dir, reachLegacyFile)); !os.IsNotExist(err) {
		t.Fatalf("the writer kept %s: %v", reachLegacyFile, err)
	}
	if b, _ := os.ReadFile(filepath.Join(dir, reachDir, "a__b.tsv")); string(b) != reachHeader+"a/b\tx.go\tc\na/b\tx.go\td\n" {
		t.Fatalf("a__b.tsv:\n%s", b)
	}
	tb, err := loadTables(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(tb.reachRows) != len(rows) || tb.reachRows["e\ty.go\t*"] != "reach.d/e.tsv:2" {
		t.Fatalf("loaded rows %v", tb.reachRows)
	}
	if err := os.WriteFile(filepath.Join(dir, reachDir, "e.tsv"), []byte("a/b\tx.go\tc\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadTables(dir); err == nil || !strings.Contains(err.Error(), "belongs in reach.d/a__b.tsv") {
		t.Fatalf("a row filed under another package: %v", err)
	}
	if err := writeReach(dir, rows); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, reachLegacyFile), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadTables(dir); err == nil || !strings.Contains(err.Error(), "the reach table is reach.d/") {
		t.Fatalf("a legacy reach.tsv: %v", err)
	}
}

package affected

import (
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// Every path behind a reads.tsv row is a repository-root path: clean and relative, and, when it is a first-level name
// (no slash), a tracked file or directory of the checkout or a declared class glob. A deeper path may name a file that
// does not exist yet (a guard that a file stays absent, a fixture path): it stays a read,
// and a Markdown-only plan treats a package whose every named path is absent as a reader of unknown shape.
func TestReadsPathsAreTrackedRepositoryPaths(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	tracked := repoFiles(root)
	if tracked == nil {
		t.Skip("not a git checkout")
	}
	known := map[string]bool{}
	for _, f := range tracked {
		for p := f; p != "."; p = filepath.ToSlash(filepath.Dir(p)) {
			known[p] = true
		}
	}
	tb, err := loadTables(filepath.Join(root, "internal", "cigates", "affected"))
	if err != nil {
		t.Fatal(err)
	}
	globs := map[string]bool{}
	for _, g := range tb.globs {
		globs[g.glob] = true
	}
	reads, err := ScanReads(root)
	if err != nil {
		t.Fatal(err)
	}
	var bad []string
	for _, r := range reads {
		if r.Kind != ReadPath || tb.suggest(r.Path) == "" {
			continue
		}
		clean := cleanRel(r.Path) && r.Path != "."
		bare := !strings.Contains(r.Path, "/")
		if !clean || (bare && !known[r.Path] && !globs[r.Path]) {
			bad = append(bad, r.String())
		}
	}
	sort.Strings(bad)
	if len(bad) > 0 {
		t.Fatalf("%d read(s) behind a reads.tsv row name no tracked repository path:\n%s", len(bad), strings.Join(bad, "\n"))
	}
}

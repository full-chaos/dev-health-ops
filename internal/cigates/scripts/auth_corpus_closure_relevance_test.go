package scripts_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// CHAOS-11670: ci/auth_corpus_queue_relevance.sh is relevant when a changed file sits in a directory listed in
// ci/auth_corpus_closure.txt (one repo package directory per row), besides the rows of ci/auth_corpus_paths.txt.
// The table is read fail-closed: a missing, empty or malformed table prints corpus=true.

// corpusClosureRun runs the script over a group that changes paths, with a closure table of exactly table (nil = no file).
func corpusClosureRun(t *testing.T, table *string, paths ...string) string {
	t.Helper()
	dir := corpusQueueRepo(t, true)
	tablePath := filepath.Join(dir, "ci", "auth_corpus_closure.txt")
	if table == nil {
		if err := os.Remove(tablePath); err != nil {
			t.Fatal(err)
		}
	} else if err := os.WriteFile(tablePath, []byte(*table), 0o644); err != nil {
		t.Fatal(err)
	}
	corpusQueueGit(t, dir, "add", "-A")
	corpusQueueGit(t, dir, "commit", "-q", "-m", "table")
	tip := corpusQueueGit(t, dir, "rev-parse", "HEAD")
	head := corpusQueueCommit(t, dir, paths...)
	return corpusQueueRun(t, dir, tip, head)
}

func TestAuthCorpusQueueClosureTable(t *testing.T) {
	table := "# a comment\n\ninternal/api/sessionissue\ncmd/dho\n"
	cases := []struct {
		name  string
		paths []string
		want  string
	}{
		{"a Go file of a closure package", []string{"internal/api/sessionissue/issue.go"}, "corpus=true"},
		{"a test file of a closure package", []string{"internal/api/sessionissue/issue_test.go"}, "corpus=true"},
		{"a non-Go file of a closure package", []string{"internal/api/sessionissue/testdata.json"}, "corpus=true"},
		{"a file of a second row", []string{"cmd/dho/other.go"}, "corpus=true"},
		{"a file name with a newline in a closure package", []string{"internal/api/sessionissue/a\nb.go"}, "corpus=true"},
		{"a file of a package outside the table", []string{"internal/providersync/sync.go"}, "corpus=false"},
		{"a subdirectory of a closure package is another package", []string{"internal/api/sessionissue/sub/x.go"}, "corpus=false"},
		{"a sibling that shares the prefix of a row", []string{"internal/api/sessionissue2/x.go"}, "corpus=false"},
		{"the parent of a row", []string{"internal/api/x.go"}, "corpus=false"},
		{"a file at the repository root", []string{"README.md"}, "corpus=false"},
		{"docs only", []string{"docs/index.md"}, "corpus=false"},
		{"outside and inside in one group", []string{"docs/a.md", "cmd/dho/x.go"}, "corpus=true"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := corpusClosureRun(t, &table, c.paths...); got != c.want {
				t.Fatalf("change %q: got %q, want %q", c.paths, got, c.want)
			}
		})
	}
}

func TestAuthCorpusQueueClosureTableFailsClosed(t *testing.T) {
	if got := corpusClosureRun(t, nil, "docs/a.md"); got != "corpus=true" {
		t.Errorf("missing table: got %q, want corpus=true", got)
	}
	for name, table := range map[string]string{
		"empty":         "",
		"only comments": "# a\n# b\n",
	} {
		if got := corpusClosureRun(t, &table, "docs/a.md"); got != "corpus=true" {
			t.Errorf("%s table: got %q, want corpus=true", name, got)
		}
	}
	bad := []string{"internal/a/*", "internal/a?", "internal/[a]", "internal\\a", "/internal/a", "./internal/a", "internal/./a", "internal/../a",
		"..", ".", "internal//a", "internal/a/", "internal/a\r", "internal/a ", " internal/a", "\tinternal/a", "internal/a\v", "internal/a # c", "internal/a\f", "\u00a0internal/a", "\ufeffinternal/a", "internal/{a,b}", "!(internal)", "internal/\u00e9"}
	for _, row := range bad {
		t.Run(fmt.Sprintf("%q", row), func(t *testing.T) {
			table := "internal/good\n" + row + "\n"
			if got := corpusClosureRun(t, &table, "docs/a.md"); got != "corpus=true" {
				t.Fatalf("row %q: got %q, want corpus=true (refused, fails closed)", row, got)
			}
		})
	}
	good := "internal/good\ninternal/a-b_c.d/x\n"
	if got := corpusClosureRun(t, &good, "docs/a.md"); got != "corpus=false" {
		t.Errorf("a well-formed table and an unrelated change: got %q, want corpus=false", got)
	}
}

func TestAuthCorpusQueueClosureTableReadWithoutALastNewline(t *testing.T) {
	table := "internal/a\ninternal/b"
	if got := corpusClosureRun(t, &table, "internal/b/x.go"); got != "corpus=true" {
		t.Errorf("a last row without a newline is still a row: got %q", got)
	}
}

func TestAuthCorpusQueueClosureSkipsFileNamesItCannotNameAsADirectory(t *testing.T) {
	table := "internal/a\n"
	for _, p := range []string{"internal/a", "x"} {
		if got := corpusClosureRun(t, &table, p); got != "corpus=false" {
			t.Errorf("change %q: got %q, want corpus=false (a file named like a row is not in the row's directory)", p, got)
		}
	}
}

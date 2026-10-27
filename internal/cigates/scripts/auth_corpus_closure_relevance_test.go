package scripts_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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

// The real table (not the fixed one of the other cases): a change to a file that a closure package embeds from a
// subdirectory (go:embed) runs the corpus.
func TestAuthCorpusQueueRealTableCoversEmbeddedFiles(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), "ci", "auth_corpus_closure.txt"))
	if err != nil {
		t.Fatal(err)
	}
	table := string(raw)
	cases := []struct {
		path, want string
	}{
		{"internal/mail/templates/new_template.html", "corpus=true"},
		{"internal/pgmigrate/sql/9999_new.sql", "corpus=true"},
		{"internal/pgmigrate/sql/down/9999_new.sql", "corpus=true"},
		{"internal/chmigrate/sql/9999_new.sql", "corpus=true"},
		{"docs/index.md", "corpus=false"},
	}
	for _, c := range cases {
		t.Run(c.path, func(t *testing.T) {
			if got := corpusClosureRun(t, &table, c.path); got != c.want {
				t.Fatalf("change %q: got %q, want %q", c.path, got, c.want)
			}
		})
	}
}

// corpusClosureRunLocale runs the relevance script with LC_ALL=locale in its environment, over a table of exactly table.
func corpusClosureRunLocale(t *testing.T, locale, table string, paths ...string) string {
	t.Helper()
	dir := corpusQueueRepo(t, true)
	if err := os.WriteFile(filepath.Join(dir, "ci", "auth_corpus_closure.txt"), []byte(table), 0o644); err != nil {
		t.Fatal(err)
	}
	corpusQueueGit(t, dir, "add", "-A")
	corpusQueueGit(t, dir, "commit", "-q", "-m", "table")
	tip := corpusQueueGit(t, dir, "rev-parse", "HEAD")
	head := corpusQueueCommit(t, dir, paths...)
	cmd := exec.Command("bash", filepath.Join(dir, "ci", "auth_corpus_queue_relevance.sh"), tip, head)
	cmd.Dir = dir
	cmd.Env = []string{"PATH=/usr/bin:/bin:/usr/local/bin", "HOME=" + t.TempDir(), "LC_ALL=" + locale}
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("the script must always exit 0: %v", err)
	}
	return strings.TrimSpace(string(out))
}

// The row check is byte-exact ASCII whatever the caller's locale: a letter outside [A-Za-z] is never inside a range.
func TestAuthCorpusQueueClosureRowIsAsciiInEveryLocale(t *testing.T) {
	for _, locale := range []string{"C", "C.UTF-8", "en_US.UTF-8"} {
		for _, row := range []string{"internal/é", "internal/É", "é", "internal/aéb", "internal/а"} {
			t.Run(fmt.Sprintf("%s %q", locale, row), func(t *testing.T) {
				if got := corpusClosureRunLocale(t, locale, "internal/good\n"+row+"\n", "docs/a.md"); got != "corpus=true" {
					t.Fatalf("LC_ALL=%s row %q: got %q, want corpus=true (refused, fails closed)", locale, row, got)
				}
			})
		}
		t.Run(locale+" a well-formed table", func(t *testing.T) {
			if got := corpusClosureRunLocale(t, locale, "internal/good\n", "docs/a.md"); got != "corpus=false" {
				t.Fatalf("LC_ALL=%s: got %q, want corpus=false", locale, got)
			}
		})
	}
}

// The script pins the C locale itself, before its first pattern match or read.
func TestAuthCorpusQueueRelevanceScriptPinsTheCLocale(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), "ci", "auth_corpus_queue_relevance.sh"))
	if err != nil {
		t.Fatal(err)
	}
	pin := -1
	for i, line := range strings.Split(string(raw), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "export LC_ALL=C" {
			pin = i
			break
		}
		if trimmed == "" || strings.HasPrefix(trimmed, "#") || strings.HasPrefix(trimmed, "set -") {
			continue
		}
		if strings.Contains(trimmed, "[[") || strings.Contains(trimmed, "case ") || strings.Contains(trimmed, "read ") || strings.Contains(trimmed, "$(") {
			t.Fatalf("line %d reaches a pattern match, a read or a command before `export LC_ALL=C`: %s", i+1, line)
		}
	}
	if pin < 0 {
		t.Fatal("ci/auth_corpus_queue_relevance.sh never runs `export LC_ALL=C`")
	}
}

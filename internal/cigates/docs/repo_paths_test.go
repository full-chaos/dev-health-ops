package docs_test

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"
)

// A docs page that names a repository path must name a path git tracks, or carry pathRemovedNote on the same line.
// A "repo path" is a whole backticked token, a markdown link target, or a front matter list/value, that starts with a
// top-level directory git tracks files in (cmd/, internal/, src/, docs/, scripts/, .github/, ...). Tokens with
// globs, placeholders, spaces or a scheme (URLs) are not repo paths. A trailing :line or :line-line is dropped.

const pathRemovedNote = "removed with the Python api"

var (
	backtickToken = regexp.MustCompile("`([^`\n]+)`")
	linkTarget    = regexp.MustCompile(`\]\(([^)\s]+)\)`)
	pathChars     = regexp.MustCompile(`^[A-Za-z0-9_.@\-/]+$`)
	pythonTree    = regexp.MustCompile(`src/dev_health_ops(?:/[^\s` + "`" + `)'"]*)?`)
	lineSuffix    = regexp.MustCompile(`:\d+(?:-\d+)?$`)
	frontValue    = regexp.MustCompile(`^\s*(?:-\s+|[A-Za-z_][A-Za-z0-9_]*:\s+)["']?([^\s"']+)["']?\s*$`)
	// History records keep their text: a name that was true when the record was written (a planned package, a web
	// repository directory, a migration range) stays, and is listed here by page and name.
	historyNames = map[string][]string{
		"docs/contribute/architecture/auth-control-plane-adrs.md": {"internal/identity", "internal/keystore",
			"src/lib/admin/server", "src/lib/billing/actions"},
		"docs/contribute/architecture/auth-control-plane-decision-sheet.md": {"internal/identity", "internal/keystore",
			"src/lib/admin/server", "src/lib/billing/actions"},
		"docs/contribute/development/integration-suite-targets.md":           {"internal/testsupport/computeparity"},
		"docs/contribute/development/lane-isolation-kiac.md":                 {"deploy/local/kiac.sh", "deploy/local/trial-data.sh", "deploy/vendor"},
		"docs/contribute/architecture/go-api-wave-0-proof-infrastructure.md": {"cmd/go-api-routing"},
		"docs/contribute/architecture/auth-control-plane-database.md":        {"internal/storage/postgres/authschema/migrations/0001"},
	}
	// Port records list the Python modules by design: each row is a Python path and its Go replacement.
	portRecordPages = map[string]bool{
		"docs/go-migration-matrix.md":                          true,
		"docs/reference/runtime/python-go-live-path-ledger.md": true,
	}
)

// trackedFiles is `git ls-files -z` in root: the tracked paths that still exist on disk. A git failure fails the test:
// a guard that silently scanned nothing would pass.
func trackedFiles(t *testing.T, root string) []string {
	t.Helper()
	cmd := exec.Command("git", "-C", root, "ls-files", "-z")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git ls-files in %s: %v: %s", root, err, stderr.String())
	}
	var files []string
	for _, rel := range strings.Split(string(out), "\x00") {
		if rel == "" {
			continue
		}
		if _, err := os.Stat(filepath.Join(root, rel)); err != nil {
			continue
		}
		files = append(files, rel)
	}
	return files
}

// files is the tracked set with the planted edits applied.
func (d docTree) files() map[string]bool {
	out := map[string]bool{}
	for _, f := range d.tracked {
		out[f] = true
	}
	for rel, v := range d.over {
		if v == nil {
			delete(out, rel)
		} else {
			out[rel] = true
		}
	}
	return out
}

func (d docTree) markdownUnder(dir string) []string {
	var out []string
	for rel := range d.files() {
		if strings.HasPrefix(rel, dir+"/") && strings.HasSuffix(rel, ".md") {
			out = append(out, rel)
		}
	}
	sort.Strings(out)
	return out
}

func repoPathOf(token string, topDirs map[string]bool) (string, bool) {
	token = strings.TrimSpace(token)
	if i := strings.IndexByte(token, '#'); i >= 0 {
		token = token[:i]
	}
	token = lineSuffix.ReplaceAllString(token, "")
	token = strings.TrimRight(token, ".,;")
	if strings.HasSuffix(token, "...") || !pathChars.MatchString(token) {
		return "", false
	}
	top, _, ok := strings.Cut(token, "/")
	if !ok || !topDirs[top] {
		return "", false
	}
	return strings.TrimSuffix(token, "/"), true
}

func pathTokens(line string, front bool, topDirs map[string]bool) []string {
	var raw []string
	for _, m := range backtickToken.FindAllStringSubmatch(line, -1) {
		raw = append(raw, m[1])
	}
	for _, m := range linkTarget.FindAllStringSubmatch(line, -1) {
		raw = append(raw, m[1])
	}
	if front {
		if m := frontValue.FindStringSubmatch(line); m != nil {
			raw = append(raw, m[1])
		}
	}
	var out []string
	for _, r := range raw {
		if p, ok := repoPathOf(r, topDirs); ok {
			out = append(out, p)
		}
	}
	return out
}

// namesGoSymbolIn reports a Go qualified symbol such as internal/jobs/workgraph/linktier.Rank: dropping the trailing
// .Identifier parts leaves a tracked package directory.
func namesGoSymbolIn(ref string, dirs map[string]bool) bool {
	for {
		i := strings.LastIndexByte(ref, '.')
		if i < 0 || i < strings.LastIndexByte(ref, '/') {
			return false
		}
		ref = ref[:i]
		if dirs[ref] {
			return true
		}
	}
}

func docsRepoPathGuard(d docTree) []string {
	files := d.files()
	topDirs := map[string]bool{}
	dirs := map[string]bool{}
	for f := range files {
		parts := strings.Split(f, "/")
		if len(parts) > 1 {
			topDirs[parts[0]] = true
		}
		for i := 1; i < len(parts); i++ {
			dirs[strings.Join(parts[:i], "/")] = true
		}
	}
	var out []string
	for _, rel := range d.markdownUnder("docs") {
		if portRecordPages[rel] {
			continue
		}
		text, err := d.read(rel)
		if err != nil {
			out = append(out, fmt.Sprintf("%s: %v", rel, err))
			continue
		}
		front, fenced := false, false
		for i, line := range strings.Split(text, "\n") {
			if i == 0 && strings.TrimSpace(line) == "---" {
				front = true
				continue
			}
			if front && strings.TrimSpace(line) == "---" {
				front = false
				continue
			}
			if !front && strings.HasPrefix(strings.TrimSpace(line), "```") {
				fenced = !fenced
			}
			if strings.Contains(line, pathRemovedNote) {
				continue
			}
			refs := []string(nil)
			if !fenced {
				refs = pathTokens(line, front, topDirs)
			}
			// The retired Python tree is checked everywhere on the line, in code fences and with placeholders too.
			for _, m := range pythonTree.FindAllString(line, -1) {
				refs = append(refs, strings.TrimSuffix(strings.TrimRight(m, ".,;:"), "/"))
			}
			seen := map[string]bool{}
			for _, ref := range refs {
				if seen[ref] {
					continue
				}
				seen[ref] = true
				if slices.Contains(historyNames[rel], ref) || files[ref] || dirs[ref] || namesGoSymbolIn(ref, dirs) {
					continue
				}
				out = append(out, fmt.Sprintf("%s:%d names %s, which git does not track: name the Go owner, or write %q on that line when no Go owner exists", rel, i+1, ref, pathRemovedNote))
			}
		}
	}
	return out
}

func TestDocsNameNoUntrackedRepoPath(t *testing.T) {
	gone := "src/dev_health_ops/api/main.py"
	assertGuard(t, docsRepoPathGuard, []plantCase{
		{"a new page names a deleted Python module", func(t *testing.T, d docTree) docTree {
			return d.added("docs/reference/planted.md", "# Planted\n\nThe app is `"+gone+"`.\n")
		}, "docs/reference/planted.md:3 names " + gone},
		{"a front matter line names a deleted Python module", func(t *testing.T, d docTree) docTree {
			return d.added("docs/reference/planted.md", "---\nsource_of_truth:\n  - "+gone+"\n---\n# P\n")
		}, "docs/reference/planted.md:3 names " + gone},
		{"a Python path in a code fence", func(t *testing.T, d docTree) docTree {
			return d.added("docs/reference/planted.md", "```text\nsrc/dev_health_ops/providers/<provider>/\n```\n")
		}, "docs/reference/planted.md:2 names src/dev_health_ops/providers/<provider>"},
		{"a deleted Python package directory", func(t *testing.T, d docTree) docTree {
			return d.added("docs/reference/planted.md", "See `src/dev_health_ops/providers/`.\n")
		}, "src/dev_health_ops/providers"},
		{"a missing Go package", func(t *testing.T, d docTree) docTree {
			return d.added("docs/reference/planted.md", "Owner: `internal/nosuchpackage/run.go:12`.\n")
		}, "docs/reference/planted.md:1 names internal/nosuchpackage/run.go"},
		{"a missing cmd directory in a link", func(t *testing.T, d docTree) docTree {
			return d.added("docs/reference/planted.md", "See [it](cmd/nosuchcmd/main.go#L3).\n")
		}, "cmd/nosuchcmd/main.go"},
		{"a missing script", func(t *testing.T, d docTree) docTree {
			return d.added("docs/reference/planted.md", "Run `scripts/no_such_script.sh`.\n")
		}, "scripts/no_such_script.sh"},
		{"a tracked file removed from the tree", func(t *testing.T, d docTree) docTree {
			return d.removed("cmd/dho/main.go").added("docs/reference/planted.md", "`cmd/dho/main.go`\n")
		}, "cmd/dho/main.go"},
		{"a port record page does not exempt another page", func(t *testing.T, d docTree) docTree {
			return d.added("docs/reference/runtime/planted.md", "`"+gone+"`\n")
		}, "docs/reference/runtime/planted.md:1"},
		{"the note on another line does not exempt the line", func(t *testing.T, d docTree) docTree {
			return d.added("docs/reference/planted.md", pathRemovedNote+"\n`"+gone+"`\n")
		}, "docs/reference/planted.md:2"},
	})
}

func TestDocsRepoPathGuardAcceptsTrackedPathsAndTheNote(t *testing.T) {
	real := realTree(t)
	before := map[string]bool{}
	for _, v := range docsRepoPathGuard(real) {
		before[v] = true
	}
	for _, c := range []struct{ name, rel, page string }{
		{"a tracked file with a line number", "docs/reference/planted.md", "`cmd/dho/main.go:10-20`\n"},
		{"a Go symbol in a tracked package", "docs/reference/planted.md", "`internal/cigates/docs.Guard.Step`\n"},
		{"a tracked directory", "docs/reference/planted.md", "`internal/cigates/docs/`\n"},
		{"the note on the same line", "docs/reference/planted.md", "`src/dev_health_ops/api/main.py` (" + pathRemovedNote + ")\n"},
		{"a URL", "docs/reference/planted.md", "[x](https://example.com/internal/nope)\n"},
		{"a glob and a placeholder", "docs/reference/planted.md", "`internal/jobs/*/x.go` `internal/providersync/<provider>_route.go`\n"},
		{"a fenced block of untracked non-Python paths", "docs/reference/planted.md", "```\ninternal/nope/x.go\n```\n"},
		{"a port record page", "docs/go-migration-matrix.md", "`src/dev_health_ops/api/main.py`\n"},
	} {
		for _, v := range docsRepoPathGuard(real.added(c.rel, c.page)) {
			if !before[v] {
				t.Errorf("%s: the guard refused it: %s", c.name, v)
			}
		}
	}
}

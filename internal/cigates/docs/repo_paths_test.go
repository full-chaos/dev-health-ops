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

	"gopkg.in/yaml.v3"
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
	// History records keep their text: a name that was true when the record was written (a planned package, a web
	// repository directory, a migration range) stays, and is listed here by page and name.
	historyNames = map[string][]string{
		"docs/contribute/architecture/auth-control-plane-adrs.md": {"internal/identity", "internal/keystore",
			"src/lib/admin/server", "src/lib/billing/actions", "src/dev_health_ops/api", "ci/check_endpoint_profiles.py"},
		"docs/contribute/architecture/auth-control-plane-decision-sheet.md": {"internal/identity", "internal/keystore",
			"src/lib/admin/server", "src/lib/billing/actions", "src/dev_health_ops/api", "ci/check_endpoint_profiles.py"},
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

type pathRef struct {
	ref string
	end int
}

// lineRefs lists the repo paths named on one body line, with the offset where each token ends. The retired Python tree
// is listed wherever it appears, in code fences and with placeholders too; other tokens only outside fences.
func lineRefs(line string, fenced bool, topDirs map[string]bool) []pathRef {
	var out []pathRef
	if !fenced {
		for _, re := range []*regexp.Regexp{backtickToken, linkTarget} {
			for _, m := range re.FindAllStringSubmatchIndex(line, -1) {
				if p, ok := repoPathOf(line[m[2]:m[3]], topDirs); ok {
					out = append(out, pathRef{p, m[3]})
				}
			}
		}
	}
	for _, m := range pythonTree.FindAllStringIndex(line, -1) {
		out = append(out, pathRef{strings.TrimSuffix(lineSuffix.ReplaceAllString(strings.TrimRight(line[m[0]:m[1]], ".,;:"), ""), "/"), m[1]})
	}
	return out
}

// check reports every ref that git does not track. The removed note exempts only the one path it follows: the nearest
// path that ends before the note.
func checkRefs(rel string, lineNo int, text string, refs []pathRef, files, dirs map[string]bool) []string {
	exempt := map[string]bool{}
	for from := 0; ; {
		i := strings.Index(text[from:], pathRemovedNote)
		if i < 0 {
			break
		}
		n := from + i
		best := -1
		for k, r := range refs {
			if r.end <= n && (best < 0 || r.end >= refs[best].end) {
				best = k
			}
		}
		if best >= 0 {
			exempt[refs[best].ref] = true
		}
		from = n + len(pathRemovedNote)
	}
	var out []string
	seen := map[string]bool{}
	for _, r := range refs {
		if seen[r.ref] {
			continue
		}
		seen[r.ref] = true
		if exempt[r.ref] || slices.Contains(historyNames[rel], r.ref) || files[r.ref] || dirs[r.ref] || namesGoSymbolIn(r.ref, dirs) {
			continue
		}
		out = append(out, fmt.Sprintf("%s:%d names %s, which git does not track: name the Go owner, or write %q right after that path when no Go owner exists", rel, lineNo, r.ref, pathRemovedNote))
	}
	return out
}

func frontMatterScalars(n *yaml.Node, visit func(line int, value string)) {
	if n.Kind == yaml.ScalarNode {
		visit(n.Line, n.Value)
	}
	for _, c := range n.Content {
		frontMatterScalars(c, visit)
	}
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
		lines := strings.Split(text, "\n")
		body := 0
		if len(lines) > 0 && strings.TrimSpace(lines[0]) == "---" {
			for i := 1; i < len(lines); i++ {
				if strings.TrimSpace(lines[i]) == "---" {
					body = i + 1
					break
				}
			}
		}
		if body > 0 {
			var doc yaml.Node
			if err := yaml.Unmarshal([]byte(strings.Join(lines[1:body-1], "\n")), &doc); err != nil {
				out = append(out, fmt.Sprintf("%s: front matter: %v", rel, err))
			}
			// Every string in the front matter, in any YAML shape (block or flow list, mapping, scalar), is a candidate:
			// its first word is the path, and the rest is prose.
			frontMatterScalars(&doc, func(line int, value string) {
				var refs []pathRef
				if first := strings.Fields(value); len(first) > 0 {
					if p, ok := repoPathOf(first[0], topDirs); ok {
						refs = append(refs, pathRef{p, len(first[0])})
					}
				}
				for _, m := range pythonTree.FindAllStringIndex(value, -1) {
					refs = append(refs, pathRef{strings.TrimSuffix(lineSuffix.ReplaceAllString(strings.TrimRight(value[m[0]:m[1]], ".,;:"), ""), "/"), m[1]})
				}
				out = append(out, checkRefs(rel, line+1, value, refs, files, dirs)...)
			})
		}
		fenced := false
		for i := body; i < len(lines); i++ {
			line := lines[i]
			if strings.HasPrefix(strings.TrimSpace(line), "```") {
				fenced = !fenced
			}
			out = append(out, checkRefs(rel, i+1, line, lineRefs(line, fenced, topDirs), files, dirs)...)
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

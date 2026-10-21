package workflows_test

import (
	"bytes"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
)

// CHAOS-10177: Go port of tests/tooling/test_go_workflow_path_filters.py (CHAOS-4834, 6801). `go.yml`'s `paths` is the
// list GitHub reads to queue Go CI, so a fixture or Go input missing from it means a fixture-only change is judged
// irrelevant and a required gate is skipped. The oracle is the tree, never a pattern: every non-Go file under a
// testdata/ or fixtures/ directory, every file Go source NAMES (string literal, filepath.Join, //go:embed,
// os.ReadDir) must match the filter or be excused with a reason. A literal that names a
// real file but cannot be resolved fails loud (the property all earlier shape gaps shared).
// The tracked set comes from `git ls-files`; the git index is read through os so the test cache moves with it.

// fxDir is split so this file never holds a literal the fixture-reference scan would read as a reference.
const fxDir = "tests/fix" + "tures/"

var uncoveredFixtureDirectories = map[string]string{
	"tests/testops/fixtures":      "read by the Python testops suite, not by Go tests; covered by the Python workflow's own filters",
	"mkdocs.yml":                  "docs site config named by internal/cigates/workflowpaths, which pins that the docs workflows run for it; no Go test reads its content, and a docs-only change must not queue Go CI",
	"tools/codex-review/fixtures": "log excerpts read only by tools/codex-review/test-exec-classify.sh, a shell proof no workflow runs; no Go test reads them",
	"internal/providersync/JIRA_PROVIDER_GAP_MATRIX.md": "surfaced by the os.ReadDir(\".\") reader shape; the tests doing that filter to *.go, which **/*.go already covers, so a markdown file there cannot change what they assert",
	"internal/queryapi/server/README.md":                "surfaced by the os.ReadDir(\".\") reader shape in internal_auth_test.go's route-boundary scan, which skips every entry that is not a non-test .go file",
}

var fabricatedPathLiterals = map[string]string{
	"__init__.py":     "synthetic name in a fabricated provider file listing",
	"src/__init__.py": "same fixture, same fabricated layout",
}

var goDataSuffixes = []string{".json", ".py", ".sql", ".csv", ".tsv", ".txt", ".yaml", ".yml", ".golden"}

func hasDataSuffix(s string) bool {
	for _, suf := range goDataSuffixes {
		if strings.HasSuffix(s, suf) {
			return true
		}
	}
	return false
}

// githubGlobToRegexp mirrors GitHub's path-filter globs: `**/` is zero or more directories at any
// position, `**` spans separators, `*` does not, and `?[]!` are refused rather than read as literal text.
func githubGlobToRegexp(pattern string) (*regexp.Regexp, error) {
	if i := strings.IndexAny(pattern, "?[]!"); i >= 0 {
		return nil, fmt.Errorf("go.yml path filter %q uses %q, which this translator does not implement: a filter that matches nothing silently marks real changes irrelevant", pattern, pattern[i])
	}
	var b strings.Builder
	b.WriteString("(?s)^")
	for i := 0; i < len(pattern); {
		switch {
		case strings.HasPrefix(pattern[i:], "**/"):
			b.WriteString("(?:.*/)?")
			i += 3
		case strings.HasPrefix(pattern[i:], "**"):
			b.WriteString(".*")
			i += 2
		case pattern[i] == '*':
			b.WriteString("[^/]*")
			i++
		default:
			b.WriteString(regexp.QuoteMeta(pattern[i : i+1]))
			i++
		}
	}
	b.WriteString("$")
	return regexp.Compile(b.String())
}

func matchesAny(t *testing.T, p string, patterns []string) bool {
	t.Helper()
	for _, pat := range patterns {
		re, err := githubGlobToRegexp(pat)
		if err != nil {
			t.Fatal(err)
		}
		if re.MatchString(p) {
			return true
		}
	}
	return false
}

// ---- tracked files ---------------------------------------------------------------------------------------------

var (
	gitRunner  = runGitStripped
	trackedMu  sync.Mutex
	trackedMap = map[string][]string{}
)

// runGitStripped runs git without any GIT_* variable: an inherited GIT_DIR once made the oracle read a sibling
// worktree's index and shrink 401 fixtures to 309 with every test green.
func runGitStripped(dir string, args ...string) (stdout []byte, err error) {
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "GIT_") {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		return out.Bytes(), fmt.Errorf("%w: %s", err, errb.String())
	}
	return out.Bytes(), nil
}

func walkFiles(anchor string) []string {
	var out []string
	_ = filepath.WalkDir(anchor, func(p string, d fs.DirEntry, err error) error {
		if err == nil && d.Type().IsRegular() {
			rel, _ := filepath.Rel(anchor, p)
			out = append(out, filepath.ToSlash(rel))
		}
		return nil
	})
	sort.Strings(out)
	return out
}

// trackedPaths returns the repo-relative slash paths git tracks under anchor. The real repo root never falls back
// to a walk, and an empty or foreign index is an error: every oracle below would otherwise pass by examining
// nothing.
func trackedPaths(realRoot, anchor string) ([]string, error) {
	trackedMu.Lock()
	defer trackedMu.Unlock()
	if v, ok := trackedMap[anchor]; ok {
		return v, nil
	}
	isRoot := filepath.Clean(anchor) == filepath.Clean(realRoot)
	inside, err := gitRunner(anchor, "rev-parse", "--is-inside-work-tree")
	if err != nil || strings.TrimSpace(string(inside)) != "true" {
		if isRoot {
			return nil, fmt.Errorf("no git work tree at the repo root (%s): %v; refusing to fall back to a filesystem walk for the real tree, because that reinstates the hardcoded-exclusion bug", anchor, err)
		}
		trackedMap[anchor] = walkFiles(anchor)
		return trackedMap[anchor], nil
	}
	raw, err := gitRunner(anchor, "ls-files", "-z")
	if err != nil {
		return nil, err
	}
	var names []string
	for _, n := range strings.Split(string(raw), "\x00") {
		if n != "" {
			names = append(names, n)
		}
	}
	if len(names) == 0 {
		if isRoot {
			return nil, fmt.Errorf("git ls-files found no tracked files under the repo root (%s): every check would pass vacuously", anchor)
		}
		trackedMap[anchor] = walkFiles(anchor)
		return trackedMap[anchor], nil
	}
	var missing []string
	for _, n := range names {
		if _, err := os.Stat(filepath.Join(anchor, n)); err != nil {
			missing = append(missing, n)
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("git listed %d tracked path(s) under %s that do not exist there, e.g. %s: the index being read is not this checkout's, or this is a sparse checkout", len(missing), anchor, missing[0])
	}
	if idx, e := gitRunner(anchor, "rev-parse", "--git-path", "index"); e == nil {
		p := strings.TrimSpace(string(idx))
		if !filepath.IsAbs(p) {
			p = filepath.Join(anchor, p)
		}
		_, _ = os.ReadFile(p)
	}
	trackedMap[anchor] = names
	return names, nil
}

func resetTracked() {
	trackedMu.Lock()
	trackedMap = map[string][]string{}
	trackedMu.Unlock()
}

// ---- the resolver ----------------------------------------------------------------------------------------------

func looksLikeAPath(l string) bool {
	return !(strings.ContainsAny(l, " \t%") || strings.HasPrefix(l, "http://") || strings.HasPrefix(l, "https://"))
}

// fileIndex is the tracked set: by basename, and as a set. A file that exists on disk but is not tracked (a CI step's
// scratch output such as changed.txt) is never a path-filter input, so a literal naming one resolves to nothing.
type fileIndex struct {
	byBase map[string][]string
	set    map[string]bool
	list   []string
}

// underDir lists the tracked non-Go files at or below the repo-relative directory.
func (x *fileIndex) underDir(dir string) []string {
	var out []string
	prefix := strings.TrimSuffix(dir, "/") + "/"
	for _, p := range x.list {
		if strings.HasPrefix(p, prefix) && !strings.HasSuffix(p, ".go") {
			out = append(out, p)
		}
	}
	return out
}

func basenameIndex(tracked []string) *fileIndex {
	idx := &fileIndex{byBase: map[string][]string{}, set: map[string]bool{}, list: tracked}
	for _, p := range tracked {
		idx.byBase[path.Base(p)] = append(idx.byBase[path.Base(p)], p)
		idx.set[p] = true
	}
	return idx
}

func (x *fileIndex) tracked(anchor, abs string) bool {
	rel, err := filepath.Rel(anchor, filepath.Clean(abs))
	return err == nil && x.set[filepath.ToSlash(rel)]
}

func isFile(p string) bool {
	st, err := os.Stat(p)
	return err == nil && st.Mode().IsRegular()
}

func goSources(realRoot, anchor string) ([]string, error) {
	tracked, err := trackedPaths(realRoot, anchor)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, p := range tracked {
		if strings.HasSuffix(p, ".go") {
			out = append(out, p)
		}
	}
	return out, nil
}

func isExcused(excused map[string]string, p string) (string, bool) {
	for dir, reason := range excused {
		if p == dir || strings.HasPrefix(p, dir+"/") {
			return reason, true
		}
	}
	return "", false
}

func fixtureLikeFilesOnDisk(realRoot, anchor string) (map[string]bool, error) {
	tracked, err := trackedPaths(realRoot, anchor)
	if err != nil {
		return nil, err
	}
	found := map[string]bool{}
	for _, p := range tracked {
		if strings.HasSuffix(p, ".go") {
			continue
		}
		for _, seg := range strings.Split(p, "/") {
			if seg == "testdata" || seg == "fixtures" {
				found[p] = true
				break
			}
		}
	}
	return found, nil
}

var fixtureLiteral = regexp.MustCompile(`tests/fixtures/[A-Za-z0-9_./-]+\.(?:json|py|tsv|txt)`)

func goPaths(t *testing.T, root, event string) []string {
	t.Helper()
	doc := loadWf(t, root, "go.yml")
	var out []string
	for _, p := range asList(asMap(asMap(doc["on"])[event])["paths"]) {
		out = append(out, asString(p))
	}
	return out
}

// pathFilterViolations runs every real-tree check of the Python file against one root.
func pathFilterViolations(t *testing.T, root string, excused map[string]string) []string {
	t.Helper()
	resetTracked()
	realRoot := repoRoot(t)
	var out []string
	add := func(f string, a ...any) { out = append(out, fmt.Sprintf(f, a...)) }
	fail := func(err error) []string { return append(out, err.Error()) }

	fixtures, err := fixtureLikeFilesOnDisk(realRoot, root)
	if err != nil {
		return fail(err)
	}
	sources, err := goSources(realRoot, root)
	if err != nil {
		return fail(err)
	}
	tracked, _ := trackedPaths(realRoot, root)
	index := basenameIndex(tracked)
	named := map[string]bool{}
	for p := range fixtures {
		named[p] = true
	}
	var unres []unresolved
	var unresLoc []string
	fixtureRefs := map[string]bool{}
	for _, src := range sources {
		text := readFile(t, filepath.Join(root, src))
		if mayNamePaths(text) {
			pg, err := parseGoSource(text)
			if err != nil {
				if !strings.Contains(src, "testdata/") {
					add("%s does not parse as Go (%v): the resolver would read nothing from it", src, err)
				}
			} else {
				dir := filepath.Dir(filepath.Join(root, src))
				for p := range resolveParsed(pg, dir, root, index) {
					named[p] = true
				}
				for _, u := range unresolvableParsed(pg, dir, root, index) {
					unres = append(unres, u)
					unresLoc = append(unresLoc, fmt.Sprintf("%s:%d", src, u.line))
				}
			}
		}
		if strings.HasSuffix(src, "_test.go") {
			for _, m := range fixtureLiteral.FindAllString(text, -1) {
				fixtureRefs[m] = true
			}
		}
	}
	if len(named) == 0 {
		add("no fixture-like files found anywhere in the tree: the walk has broken and the guard would pass vacuously")
	}

	for _, event := range []string{"push", "pull_request"} {
		paths := goPaths(t, root, event)
		var unmatched []string
		for p := range named {
			if _, ex := isExcused(excused, p); !ex && !matchesAny(t, p, paths) {
				unmatched = append(unmatched, p)
			}
		}
		sort.Strings(unmatched)
		if len(unmatched) > 0 {
			add("%d of %d fixture-like file(s) match no %s path filter in go.yml and are not excused, so a PR changing only one is classified non-Go and the guard never runs: %v; add a pattern covering them to BOTH path lists in go.yml or excuse the directory with a reason, never individual files", len(unmatched), len(named), event, unmatched)
		}

		// an exclusion that stops being necessary must fail
		for _, dir := range sortedKeys(toAnyMap2(excused)) {
			var covered []string
			for p := range fixtures {
				if (p == dir || strings.HasPrefix(p, dir+"/")) && matchesAny(t, p, paths) {
					covered = append(covered, p)
				}
			}
			if len(covered) > 0 {
				sort.Strings(covered)
				add("%q is excused because %q, but %d of its files now MATCH the %s filters: the exclusion is stale, delete the entry (%v)", dir, excused[dir], len(covered), event, covered)
			}
		}

		// the fixture filter is a glob, not an enumeration
		var fixturePatterns []string
		for _, p := range paths {
			if strings.HasPrefix(p, "tests/fixtures/") {
				fixturePatterns = append(fixturePatterns, p)
			}
		}
		if len(fixturePatterns) != 1 || fixturePatterns[0] != "tests/fixtures/**" {
			add("go.yml %s.paths should cover fixtures with the single glob 'tests/fixtures/**', not by enumeration; found %v", event, fixturePatterns)
		}
	}

	for _, dir := range sortedKeys(toAnyMap2(excused)) {
		if _, err := os.Stat(filepath.Join(root, dir)); err != nil {
			add("%q is excused from fixture coverage but does not exist: delete the entry", dir)
		}
	}

	var missingRefs []string
	for ref := range fixtureRefs {
		if !fixtures[ref] {
			missingRefs = append(missingRefs, ref)
		}
	}
	sort.Strings(missingRefs)
	if len(missingRefs) > 0 {
		add("Go test(s) reference fixture paths that do not exist on disk: %v", missingRefs)
	}

	prPaths := goPaths(t, root, "pull_request")
	for i, u := range unres {
		var uncovered []string
		for _, c := range u.candidates {
			if _, ex := isExcused(excused, c); !matchesAny(t, c, prPaths) && !(u.call && ex) {
				uncovered = append(uncovered, c)
			}
		}
		if len(uncovered) > 0 && u.call {
			add("%s: %s names a directory with tracked files that match no pull_request path filter, so a change limited to one of them skips the Go tests that read the directory: %v", unresLoc[i], u.literal, uncovered)
			continue
		}
		if len(uncovered) > 0 {
			add("%s: %q names a real file the resolver could not locate, and at least one candidate matches no path filter: %v; teach resolveNamedFiles the shape or add a pattern covering every candidate, never an exception list", unresLoc[i], u.literal, uncovered)
		}
	}
	return out
}

// miniRepo writes a synthetic tree (not a git repo) and returns its root.
func miniRepo(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for name, content := range files {
		p := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

const miniGoYml = `on:
  push:
    paths:
      - '**/*.go'
      - 'tests/fixtures/**'
  pull_request:
    paths:
      - '**/*.go'
      - 'tests/fixtures/**'
`

func miniFiles(extra map[string]string, goYml string) map[string]string {
	files := map[string]string{
		".github/workflows/go.yml": goYml,
		fxDir + "a.json":           "{}",
		"internal/x/x_test.go":     "package x",
	}
	for k, v := range extra {
		files[k] = v
	}
	return files
}

func TestPathFiltersMiniRepoBaselineIsClean(t *testing.T) {
	root := miniRepo(t, miniFiles(nil, miniGoYml))
	expectNoViolations(t, pathFilterViolations(t, root, map[string]string{}))
}

func TestDocsRobotsNamedByAGoToolIsCoveredByAnExplicitFilterRow(t *testing.T) {
	files := miniFiles(map[string]string{"docs/robots.txt": "User-agent: *", "internal/y/y.go": "package y\nvar _ = filepath.Join(out, \"robots.txt\")"},
		strings.ReplaceAll(miniGoYml, "      - '**/*.go'\n", "      - '**/*.go'\n      - 'docs/robots.txt'\n"))
	expectNoViolations(t, pathFilterViolations(t, miniRepo(t, files), map[string]string{}))
}

func TestAnExcusedFileNamedByGoIsNotDemandedInGoYml(t *testing.T) {
	files := miniFiles(map[string]string{"site.yml": "a: 1", "internal/x/x_test.go": "package x\nvar _ = \"site.yml\""}, miniGoYml)
	root := miniRepo(t, files)
	expectNoViolations(t, pathFilterViolations(t, root, map[string]string{"site.yml": "docs-only config"}))
	expectViolation(t, pathFilterViolations(t, root, map[string]string{}), "site.yml", "match no")
	files["other.yml"] = "b: 1"
	files["internal/x/x_test.go"] = "package x\nvar _ = \"site.yml\"\nvar _ = \"other.yml\""
	root = miniRepo(t, files)
	expectViolation(t, pathFilterViolations(t, root, map[string]string{"site.yml": "docs-only config"}), "other.yml", "match no")
}

func TestGoWorkflowPathFiltersPlantedDefects(t *testing.T) {
	cases := []struct {
		name    string
		extra   map[string]string
		goYml   string
		excused map[string]string
		want    []string
	}{
		{"a testdata fixture matches no filter", map[string]string{"internal/y/testdata/data.json": "{}"}, miniGoYml, map[string]string{}, []string{"match no push path filter", "internal/y/testdata/data.json"}},
		{"a file named by a Go test through go:embed matches no filter", map[string]string{"cfg/embedded.yaml": "a: 1", "internal/z/z_test.go": "package z\n//go:embed ../../cfg/embedded.yaml\nvar _ string"}, miniGoYml, map[string]string{}, []string{"cfg/embedded.yaml", "match no"}},
		{"an excused directory is now covered", map[string]string{fxDir + "sub/b.json": "{}"}, miniGoYml, map[string]string{"tests/fixtures": "claimed uncovered"}, []string{"exclusion is stale"}},
		{"an excused directory no longer exists", nil, miniGoYml, map[string]string{"gone/fixtures": "claimed"}, []string{"does not exist", "gone/fixtures"}},
		{"a bare push trigger has no path list and matches nothing", nil, strings.Replace(miniGoYml, "  push:\n    paths:\n      - '**/*.go'\n      - 'tests/fixtures/**'\n", "  push:\n", 1), map[string]string{}, []string{"match no push path filter"}},
		{"a raw-string path to an uncovered file is neither resolved nor dropped silently", map[string]string{"a/data.json": "{}", "b/data.json": "{}", "internal/x/x_test.go": "package x\nvar _ = `data.json`"}, miniGoYml, map[string]string{"a": "x", "b": "x"}, []string{"could not locate", "data.json"}},
		{"a raw-string path resolved to a real file that matches no filter", map[string]string{"cfg/data.yaml": "a: 1", "internal/y/y_test.go": "package y\nvar _ = `../../cfg/data.yaml`"}, miniGoYml, map[string]string{}, []string{"cfg/data.yaml", "match no"}},
		{"a Join with a raw-string argument names an uncovered file", map[string]string{"cfg/data.yaml": "a: 1", "internal/y/y_test.go": "package y\nvar _ = filepath.Join(root, `cfg/data.yaml`)"}, miniGoYml, map[string]string{}, []string{"cfg/data.yaml", "match no"}},
		{"a Join with a computed argument under an uncovered directory is reported, never skipped", map[string]string{"cfg/a.json": "{}", "cfg/b.json": "{}", "internal/y/y_test.go": "package y\nvar _ = filepath.Join(root, \"cfg\", name)"}, miniGoYml, map[string]string{}, []string{"names a directory with tracked files that match no pull_request path filter", "cfg/a.json"}},
		{"a computed Join under a directory where only *.json is covered reports the uncovered .yaml", map[string]string{"cfg/a.json": "{}", "cfg/b.yaml": "x: 1", "internal/y/y_test.go": "package y\nvar _ = filepath.Join(root, \"cfg\", name)"}, strings.ReplaceAll(miniGoYml, "      - '**/*.go'\n", "      - '**/*.go'\n      - 'cfg/*.json'\n"), map[string]string{}, []string{"names a directory with tracked files that match no pull_request path filter", "cfg/b.yaml"}},
		{"the fixture filter becomes an enumeration", nil, strings.ReplaceAll(miniGoYml, "'tests/fixtures/**'", "'"+fxDir+"a.json'"), map[string]string{}, []string{"single glob 'tests/fixtures/**'"}},
		{"a Go test names a fixture that is gone", map[string]string{"internal/x/x_test.go": "package x // \"" + fxDir + "ghost.json\"\nvar _ = \"" + fxDir + "ghost.json\""}, miniGoYml, map[string]string{}, []string{"reference fixture paths that do not exist", fxDir + "ghost.json"}},
		{"a non-test Go file naming robots.txt while docs/robots.txt matches no filter is refused", map[string]string{"docs/robots.txt": "User-agent: *", "internal/y/y.go": "package y\nvar _ = filepath.Join(out, \"robots.txt\")"}, miniGoYml, map[string]string{}, []string{"robots.txt", "match no"}},
		{"an ambiguous literal naming an uncovered file is neither resolved nor dropped silently", map[string]string{"a/data.json": "{}", "b/data.json": "{}", "internal/x/x_test.go": "package x\nvar _ = fsys.Open(\"data.json\")"}, miniGoYml, map[string]string{"a": "x", "b": "x"}, []string{"could not locate", "data.json"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			root := miniRepo(t, miniFiles(c.extra, c.goYml))
			expectViolation(t, pathFilterViolations(t, root, c.excused), c.want...)
		})
	}
}

// the empty-tracked-list guard must still bite where it matters (the real repo root).
func TestEmptyTrackedListForTheRepoRootFailsLoudly(t *testing.T) {
	orig := gitRunner
	defer func() { gitRunner = orig; resetTracked() }()
	gitRunner = func(dir string, args ...string) ([]byte, error) {
		if args[0] == "rev-parse" {
			return []byte("true\n"), nil
		}
		return nil, nil
	}
	resetTracked()
	root := repoRoot(t)
	if _, err := trackedPaths(root, root); err == nil || !strings.Contains(err.Error(), "no tracked files under the repo root") {
		t.Fatalf("an empty tracked list for the repo root did not fail loudly: %v", err)
	}
}

// ---- helper tests: the glob translator and the resolver, one case per shape the oracle claims ---------------------

func TestGlobTranslatorMatchesGithubSemantics(t *testing.T) {
	cases := []struct {
		pattern, path string
		want          bool
	}{
		{"**/testdata/**", "testdata/a.json", true},
		{"**/testdata/**", "internal/x/testdata/a.json", true},
		{"**/testdata/**", "notestdata/a.json", false},
		{"docs/**/x.md", "docs/x.md", true},
		{"docs/**/x.md", "docs/a/b/x.md", true},
		{"**/*.go", "root.go", true},
		{"**/go.mod", "go.mod", true},
		{"**/go.mod", "mygo.mod", false},
		{"docs/*.md", "docs/a/b.md", false},
		{"docs/**", "docs/a/b.md", true},
		{"**/*.go", "internal/newline\ndirectory/module.go", true},
		{"**/*.go", "internal/\n/module.go", true},
	}
	for _, c := range cases {
		re, err := githubGlobToRegexp(c.pattern)
		if err != nil {
			t.Fatal(err)
		}
		if got := re.MatchString(c.path); got != c.want {
			t.Errorf("%q vs %q = %v, want %v", c.pattern, c.path, got, c.want)
		}
	}
}

// go.yml's real path lists queue Go CI for root-level Go inputs: `**/*.go` must match `root.go`, not only `dir/root.go`.
// docs/README.md is the negative control.
func TestRootLevelPathsMatchTheDoubleStarPrefix(t *testing.T) {
	root := repoRoot(t)
	for _, event := range []string{"push", "pull_request"} {
		paths := goPaths(t, root, event)
		for _, c := range []struct {
			path string
			want bool
		}{
			{"root.go", true}, {"go.mod", true}, {"go.sum", true}, {"internal/x/y.go", true},
			{"cmd/dev-health-worker/main.go", true}, {"testdata/a.json", true}, {"internal/x/testdata/a.json", true},
			{"docs/README.md", false},
		} {
			if got := matchesAny(t, c.path, paths); got != c.want {
				t.Errorf("go.yml %s paths match %q = %v, want %v", event, c.path, got, c.want)
			}
		}
	}
}

func TestGlobTranslatorRefusesWhatItCannotExpress(t *testing.T) {
	for _, unsupported := range []string{"a?b", "a[bc]d", "!negated"} {
		if _, err := githubGlobToRegexp(unsupported); err == nil {
			t.Errorf("%q was translated instead of refused: a filter that matches nothing reports full coverage", unsupported)
		}
	}
}

func resolveIn(t *testing.T, source, base, root string) map[string]bool {
	t.Helper()
	tracked := walkFiles(root)
	return resolveNamedFiles(source, base, root, basenameIndex(tracked))
}

func TestEveryCoveredReaderShapeIsFound(t *testing.T) {
	root := miniRepo(t, map[string]string{"corpus.json": "{}", "nested/embedded.json": "{}", "listed/scanned.sql": "SELECT 1"})
	shapes := map[string]string{
		"string literal":                 `raw, _ := os.ReadFile("corpus.json")`,
		"filepath.Join literal segments": `os.ReadFile(filepath.Join("nested", "embedded.json"))`,
		"go:embed directive":             "//go:embed nested/embedded.json",
		"go:embed glob":                  "//go:embed nested/*.json",
		"os.ReadDir literal directory":   `entries, _ := os.ReadDir("listed")`,
	}
	for name, src := range shapes {
		if len(resolveIn(t, src, root, root)) == 0 {
			t.Errorf("reader shape %q found nothing: the oracle's claim for this shape is wrong or it regressed", name)
		}
	}
}

func TestRepoRootRelativeLiteralIsFound(t *testing.T) {
	root := miniRepo(t, map[string]string{"src/contract.json": "{}", "internal/providersync/p.go": "package p"})
	got := resolveIn(t, `readSource(t, "src/contract.json")`, filepath.Join(root, "internal/providersync"), root)
	if !got["src/contract.json"] {
		t.Fatalf("a repo-root-relative literal was not resolved: %v", got)
	}
}

func TestPythonNamedByGoIsAnInput(t *testing.T) {
	root := miniRepo(t, map[string]string{"sink.py": "x = 1"})
	if got := resolveIn(t, `readSource(t, "sink.py")`, root, root); !got["sink.py"] {
		t.Fatalf("a Python file named by Go source was not treated as an input: %v", got)
	}
}

func TestVariableRootedFilepathJoinIsFound(t *testing.T) {
	root := miniRepo(t, map[string]string{"src/graphql/ops.json": "{}", "one.json": "{}", "cmd/api/main.go": "package main"})
	pkg := filepath.Join(root, "cmd/api")
	if got := resolveIn(t, `p := filepath.Join(repoRoot, "src", "graphql", "ops.json")`, pkg, root); !got["src/graphql/ops.json"] {
		t.Errorf("a variable-rooted multi-segment filepath.Join was not resolved against the repo root: %v", got)
	}
	if got := resolveIn(t, `p := filepath.Join(root, "one.json")`, pkg, root); !got["one.json"] {
		t.Errorf("a variable-rooted SINGLE-segment filepath.Join was not resolved: %v", got)
	}
}

func TestGoEmbedPrefixAndDirectoryForms(t *testing.T) {
	root := miniRepo(t, map[string]string{
		"cmd/pkg/data/.hidden.json": "{}", "cmd/pkg/data/_under.json": "{}",
		"cmd/pkg/data/plain.json": "{}", "cmd/pkg/data/sub/deep.json": "{}",
	})
	pkg := filepath.Join(root, "cmd/pkg")
	resolve := func(pattern string) []string {
		var out []string
		for p := range resolveIn(t, "//go:embed "+pattern, pkg, root) {
			out = append(out, strings.TrimPrefix(p, "cmd/pkg/"))
		}
		sort.Strings(out)
		return out
	}
	cases := []struct {
		pattern string
		want    []string
	}{
		{"all:data/.hidden.json", []string{"data/.hidden.json"}},
		{"all:data", []string{"data/.hidden.json", "data/_under.json", "data/plain.json", "data/sub/deep.json"}},
		{"data", []string{"data/plain.json", "data/sub/deep.json"}},
	}
	for _, c := range cases {
		if got := resolve(c.pattern); strings.Join(got, ",") != strings.Join(c.want, ",") {
			t.Errorf("//go:embed %s resolved %v, want %v", c.pattern, got, c.want)
		}
	}
}

func TestASixthUnknownShapeFailsLoudInsteadOfVanishing(t *testing.T) {
	root := miniRepo(t, map[string]string{"a/data.json": "{}", "b/data.json": "{}", "cmd/svc/s.go": "package s"})
	pkg := filepath.Join(root, "cmd/svc")
	source := "fsys := os.DirFS(repoRoot)\n f, _ := fsys.Open(\"data.json\")"
	index := basenameIndex(walkFiles(root))
	if got := resolveNamedFiles(source, pkg, root, index); len(got) != 0 {
		t.Fatalf("the resolver is not expected to understand os.DirFS; if it does, rewrite this premise around a shape that is unknown: %v", got)
	}
	reported := unresolvableInSource(source, pkg, root, index)
	if len(reported) != 1 || reported[0].literal != "data.json" || strings.Join(reported[0].candidates, ",") != "a/data.json,b/data.json" {
		t.Fatalf("a literal naming a real file was neither resolved nor reported (or lost a candidate): %+v", reported)
	}
}

func TestAnAbsoluteLiteralIsJudgedLikeAnyOther(t *testing.T) {
	root := miniRepo(t, map[string]string{"a/secrets.yaml": "x: 1", "b/secrets.yaml": "x: 1", "cmd/svc/s.go": "package s"})
	pkg := filepath.Join(root, "cmd/svc")
	index := basenameIndex(walkFiles(root))
	abs := unresolvableInSource(`readConfig("/app/config/secrets.yaml")`, pkg, root, index)
	rel := unresolvableInSource(`readConfig("config/secrets.yaml")`, pkg, root, index)
	if len(abs) != 1 || len(rel) != 1 || strings.Join(abs[0].candidates, ",") != strings.Join(rel[0].candidates, ",") {
		t.Fatalf("a leading slash bought an exemption: absolute=%+v relative=%+v", abs, rel)
	}
}

func TestTheRuntimeBuiltPathLimitIsReal(t *testing.T) {
	root := miniRepo(t, map[string]string{"runtime.json": "{}"})
	built := "name := \"runtime\"\nraw, _ := os.ReadFile(fmt.Sprintf(\"%s.json\", name))\n"
	if got := resolveIn(t, built, root, root); got["runtime.json"] {
		t.Fatal("the oracle resolved a runtime-built path: an improvement, but correct the stated residual risk in the same change")
	}
	if got := resolveIn(t, `raw, _ := os.ReadFile("runtime.json")`, root, root); !got["runtime.json"] {
		t.Fatal("the literal form is not found either, so the limit test above proves nothing")
	}
}

// A gitignored directory (a dependency cache holding third-party fixtures/ and testdata/) is never collected.
func TestAGitignoredDirectoryIsNotCollectedAsARepoFixture(t *testing.T) {
	root := miniRepo(t, map[string]string{
		".gitignore":                        ".uv-cache/\n",
		fxDir + "real.json":                 "{}",
		".uv-cache/pkg/fixtures/probe.json": "{}",
	})
	for _, args := range [][]string{{"init", "-q"}, {"add", "-A"}, {"-c", "user.email=a@b", "-c", "user.name=t", "commit", "-q", "-m", "x", "--no-gpg-sign"}} {
		if _, err := runGitStripped(root, args...); err != nil {
			t.Skipf("git unavailable for the control: %v", err)
		}
	}
	if _, err := runGitStripped(root, "check-ignore", ".uv-cache/pkg/fixtures/probe.json"); err != nil {
		t.Fatalf("control: the probe is not gitignored, so this test no longer exercises the ignored path: %v", err)
	}
	resetTracked()
	defer resetTracked()
	got, err := fixtureLikeFilesOnDisk(repoRoot(t), root)
	if err != nil {
		t.Fatal(err)
	}
	if !got[fxDir+"real.json"] {
		t.Fatalf("control: the collector is not live: %v", got)
	}
	if got[".uv-cache/pkg/fixtures/probe.json"] {
		t.Fatal("a gitignored dependency-cache file was collected as a repo fixture")
	}
}

// A CI step's scratch output (an untracked changed.txt) named by a tracked Go test is not a path-filter input: path
// filters match paths in a commit. Hosted CI went red on exactly this.
func TestAnUntrackedScratchFileIsNotAnInput(t *testing.T) {
	root := miniRepo(t, map[string]string{"internal/x/x_test.go": "package x\nvar _ = \"changed.txt\"\n"})
	for _, args := range [][]string{{"init", "-q"}, {"add", "-A"}, {"-c", "user.email=a@b", "-c", "user.name=t", "commit", "-q", "-m", "x", "--no-gpg-sign"}} {
		if _, err := runGitStripped(root, args...); err != nil {
			t.Skipf("git unavailable for the control: %v", err)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "changed.txt"), []byte("scratch"), 0o644); err != nil {
		t.Fatal(err)
	}
	resetTracked()
	defer resetTracked()
	tracked, err := trackedPaths(repoRoot(t), root)
	if err != nil {
		t.Fatal(err)
	}
	index := basenameIndex(tracked)
	src := readFile(t, filepath.Join(root, "internal/x/x_test.go"))
	if got := resolveNamedFiles(src, filepath.Join(root, "internal/x"), root, index); got["changed.txt"] {
		t.Fatalf("an untracked scratch file was resolved as an input: %v", got)
	}
	if !index.tracked(root, filepath.Join(root, "internal/x/x_test.go")) || index.tracked(root, filepath.Join(root, "changed.txt")) {
		t.Fatal("control: the tracked set is wrong")
	}
}

// Prose in a Go comment (a backticked file name) is not a path the code reads; a literal after a `//` inside a string is.
func TestGoCommentsAreNotPathReferences(t *testing.T) {
	root := miniRepo(t, map[string]string{"cfg/prose.json": "{}", "cfg/code.json": "{}", "internal/x/x.go": "package x"})
	pkg := filepath.Join(root, "internal/x")
	src := "// see `cfg/prose.json` and \"cfg/prose.json\"\nvar _ = \"http://h/p\" // `cfg/prose.json`\nvar _ = `../../cfg/code.json`\n"
	got := resolveIn(t, src, pkg, root)
	if got["cfg/prose.json"] || !got["cfg/code.json"] {
		t.Fatalf("comments must not count and code must: %v", got)
	}
}

// A computed Join argument under a directory the filters DO cover is proved covered and is not reported.
func TestComputedJoinUnderACoveredDirectoryIsNotReported(t *testing.T) {
	root := miniRepo(t, miniFiles(map[string]string{"internal/y/y_test.go": "package y\nvar _ = filepath.Join(root, \"tests\", \"fixtures\", name)"}, miniGoYml))
	expectNoViolations(t, pathFilterViolations(t, root, map[string]string{}))
}

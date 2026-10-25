package workflowpaths_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
)

// ci/codeql_docs_only.sh is the one docs-only definition (CodeQL on pull requests, go.yml, docker-images, live-e2e). A docs path that a Go test reads is not docs-only: the base commit's go.yml on.push.paths lists the Go test
// inputs, and the script refuses any changed path matching one of them.

func classifierScript(t *testing.T) string {
	return filepath.Join(repoRoot(t), "ci", "codeql_docs_only.sh")
}

func runClassifier(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("bash", append([]string{classifierScript(t)}, args...)...)
	cmd.Dir = dir
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("classifier %v failed: %v", args, err)
	}
	return strings.TrimSpace(string(out))
}

// childCommit builds a commit on parent that adds one file, with plumbing only (no checkout), and returns its sha.
func (r *gitRepo) childCommit(parent, path string) string {
	r.t.Helper()
	index := filepath.Join(r.t.TempDir(), "index")
	env := append(os.Environ(), "GIT_INDEX_FILE="+index)
	run := func(stdin string, args ...string) string {
		cmd := exec.Command("git", append([]string{"-c", "user.name=t", "-c", "user.email=t@example.com", "-c", "commit.gpgsign=false"}, args...)...)
		cmd.Dir = r.dir
		cmd.Env = env
		cmd.Stdin = strings.NewReader(stdin)
		out, err := cmd.CombinedOutput()
		if err != nil {
			r.t.Fatalf("git %v: %v\n%s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	blob := run("x\n", "hash-object", "-w", "--stdin")
	run("", "read-tree", parent)
	run("", "update-index", "--add", "--cacheinfo", "100644,"+blob+","+path)
	tree := run("", "write-tree")
	return run("", "commit-tree", tree, "-p", parent, "-m", "c")
}

func realGoYml(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), ".github", "workflows", "go.yml"))
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// The script's own parse of go.yml must equal the YAML parse, entry for entry: a glob the awk reader dropped would let a
// Go-read docs path through as docs-only.
func TestCodeQLDocsOnlyClassifierParsesTheRealGoYmlPushPaths(t *testing.T) {
	r := newGitRepo(t)
	base := r.commit(map[string]string{"seed.txt": "s", ".github/workflows/go.yml": realGoYml(t)})
	got := strings.Split(runClassifier(t, r.dir, "--go-inputs", base), "\n")
	want := stringList(t, triggerMap(t, loadWorkflowMap(t, "go.yml"), "push")["paths"])
	if len(want) == 0 {
		t.Fatal("go.yml on.push.paths is empty: nothing to compare")
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("codeql_docs_only.sh --go-inputs parsed %d globs, the YAML parse has %d; they differ:\n got %q\nwant %q", len(got), len(want), got, want)
	}
}

// goInputSample turns a go.yml glob into one concrete path it matches, shaped as a docs path where the glob allows it.
func goInputSample(glob string) string {
	s := strings.ReplaceAll(glob, "**/", "")
	s = strings.ReplaceAll(s, "**", "z/a.md")
	s = strings.ReplaceAll(s, "*", "a")
	if strings.HasSuffix(s, "/") {
		s += "a.md"
	}
	return s
}

// Every real go.yml push glob, as a changed path, is not docs-only. The control proves the verdict came from the Go-input
// arm: the same sample under a base go.yml without that list is docs-only whenever the path is docs-shaped.
func TestCodeQLDocsOnlyClassifierRefusesEveryGoInput(t *testing.T) {
	r := newGitRepo(t)
	realBase := r.commit(map[string]string{"seed.txt": "s", ".github/workflows/go.yml": realGoYml(t)})
	controlBase := r.commit(map[string]string{".github/workflows/go.yml": "on:\n  push:\n    paths:\n      - '**/*.go'\n"})
	globs := stringList(t, triggerMap(t, loadWorkflowMap(t, "go.yml"), "push")["paths"])
	var mu sync.Mutex
	docsShaped := 0
	t.Run("globs", func(t *testing.T) {
		for _, glob := range globs {
			t.Run(glob, func(t *testing.T) {
				t.Parallel()
				sample := goInputSample(glob)
				rr := &gitRepo{t: t, dir: r.dir}
				if got := runClassifier(t, r.dir, realBase, rr.childCommit(realBase, sample)); got != "false" {
					t.Errorf("the sample %q is a Go test input but classified %q", sample, got)
				}
				if runClassifier(t, r.dir, controlBase, rr.childCommit(controlBase, sample)) == "true" {
					mu.Lock()
					docsShaped++
					mu.Unlock()
				}
			})
		}
	})
	// The real list holds Go-read docs paths (ci/**/*.md, internal/**/*.md, deploy/**, docs/go-migration-matrix.md,
	// docs-data/...): fewer docs-shaped samples than this means the sample builder no longer exercises the arm.
	if docsShaped < 20 {
		t.Errorf("only %d of %d samples are docs-shaped under the control go.yml; the test no longer proves the Go-input arm", docsShaped, len(globs))
	}
}

// The bash glob translator mirrors githubGlobToRegexp (internal/cigates/workflows): `**/` is zero or more directories,
// `**` spans separators, `*` does not, and every other byte is literal.
func TestCodeQLDocsOnlyClassifierGlobSemantics(t *testing.T) {
	cases := []struct {
		glob, path string
		goInput    bool
	}{
		{"docs/**/x.md", "docs/x.md", true},
		{"docs/**/x.md", "docs/a/b/x.md", true},
		{"docs/**/x.md", "docs/a/y.md", false},
		{"**/testdata/**", "docs/testdata/a.md", true},
		{"**/testdata/**", "docs/notestdata/a.md", false},
		{"docs/*.md", "docs/b.md", true},
		{"docs/*.md", "docs/a/b.md", false},
		{"docs/a.md", "docs/aXmd", false},
		{"docs/a+b.md", "docs/a+b.md", true},
		{"docs/a+b.md", "docs/aab.md", false},
		{"docs/(x).md", "docs/(x).md", true},
		{"docs/**", "docs/deep/er/a.png", true},
		{"README.md", "sub/README.md", false},
		{"**/README.md", "sub/README.md", true},
	}
	for _, c := range cases {
		r := newGitRepo(t)
		base := r.commit(map[string]string{".github/workflows/go.yml": "on:\n  push:\n    paths:\n      - '" + c.glob + "'\n"})
		want := "true"
		if c.goInput {
			want = "false"
		}
		if got := runClassifier(t, r.dir, base, r.childCommit(base, c.path)); got != want {
			t.Errorf("glob %q vs %q: classified %q, want %q (Go input = %v)", c.glob, c.path, got, want, c.goInput)
		}
	}
}

// A base go.yml the script cannot read or parse fails the verdict closed.
func TestCodeQLDocsOnlyClassifierFailsClosedOnTheBaseGoYml(t *testing.T) {
	for name, goYml := range map[string]string{
		"no go.yml at the base":         "",
		"an unquoted entry":             "on:\n  push:\n    paths:\n      - docs/x/**\n",
		"a double-quoted entry":         "on:\n  push:\n    paths:\n      - \"docs/x/**\"\n",
		"a glob with a refused byte":    "on:\n  push:\n    paths:\n      - 'docs/?.md'\n",
		"a negated glob":                "on:\n  push:\n    paths:\n      - '!docs/x.md'\n",
		"no push paths at all":          "on:\n  push:\n    branches: [main]\n  pull_request:\n    paths:\n      - 'docs/**'\n",
		"an empty push paths list":      "on:\n  push:\n    paths:\n  pull_request:\n",
		"a deeper-indented stray line":  "on:\n  push:\n    paths:\n      - 'x/**'\n        - 'y/**'\n",
		"a trailing comment on a entry": "on:\n  push:\n    paths:\n      - 'x/**' # note\n",
	} {
		r := newGitRepo(t)
		files := map[string]string{"seed.txt": "s"}
		if goYml != "" {
			files[".github/workflows/go.yml"] = goYml
		}
		base := r.commit(files)
		if got := runClassifier(t, r.dir, base, r.childCommit(base, "docs/a.md")); got != "false" {
			t.Errorf("%s: classified %q, want false", name, got)
		}
	}
	r := newGitRepo(t)
	base := r.commit(map[string]string{".github/workflows/go.yml": "on:\n  push:\n    paths:\n      - 'x/**'\n"})
	if got := runClassifier(t, r.dir, base, r.childCommit(base, "docs/a.md")); got != "true" {
		t.Errorf("control: a parseable go.yml and a docs path = %q, want true", got)
	}
}

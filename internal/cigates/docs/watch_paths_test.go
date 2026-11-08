package docs_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The production docs Worker (dev-health-docs) is built and deployed by Cloudflare Workers Builds on a push to main. Its
// watch paths are set in the Cloudflare dashboard, not in this repository, so nothing else stops a docs build input from
// falling outside them. This test pins the watch paths here and fails when a file the docs build reads is not covered.
//
// docsWatchPaths MUST be the same list as the Workers Builds trigger "Deploy default branch" (Build watch paths, include,
// no excludes). Change the list here and in Cloudflare in the same step. A pattern ending in /* covers the whole directory.
var docsWatchPaths = []string{
	"docs/*",
	"docs-data/*",
	"mkdocs.yml",
	"requirements-docs.txt",
	"wrangler.jsonc",
	"Makefile",
	"go.mod",
	"go.sum",
	"cmd/cigate/*",
	"internal/cigates/docsbuild/*",
	"internal/cigates/docsprepare/*",
	"internal/cigates/docstools/*",
	"internal/httpguard/*",
	"internal/jobs/workgraph/units/*",
	"internal/platform/errortext/*",
	"internal/platform/logging/*",
	"internal/platform/secrets/*",
	"internal/pythonparity/*",
}

// docsBuildGoRoots are the Go packages the docs build runs: docs-build, docs-prepare and the docs verbs of cigate. The
// cigate main package dispatches to them, so its own directory is an input too, but not the other verbs it links.
var docsBuildGoRoots = []string{"./internal/cigates/docsbuild", "./internal/cigates/docsprepare", "./internal/cigates/docstools"}

func watchCovers(pattern, path string) bool {
	if dir, ok := strings.CutSuffix(pattern, "/*"); ok {
		return strings.HasPrefix(path, dir+"/")
	}
	return pattern == path
}

func watched(path string) bool {
	for _, p := range docsWatchPaths {
		if watchCovers(p, path) {
			return true
		}
	}
	return false
}

// docsBuildInputs returns the repository files and directories (a directory ends in /) the docs build reads.
func docsBuildInputs(t *testing.T, root string) []string {
	t.Helper()
	read := func(name string) string {
		b, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	inputs := []string{"mkdocs.yml", "requirements-docs.txt", "wrangler.jsonc", "Makefile", "go.mod", "go.sum", "cmd/cigate/main.go"}

	docsDir := regexp.MustCompile(`(?m)^docs_dir:\s*(\S+)\s*$`).FindStringSubmatch(read("mkdocs.yml"))
	if docsDir == nil {
		t.Fatal("mkdocs.yml has no docs_dir")
	}
	inputs = append(inputs, docsDir[1]+"/x")

	// docs-build reads the redirect manifest and the search acceptance file by these paths.
	src := read("internal/cigates/docsbuild/docsbuild.go")
	for _, m := range regexp.MustCompile(`"(docs-data/[^"]+)"`).FindAllStringSubmatch(src, -1) {
		inputs = append(inputs, m[1])
	}

	args := append([]string{"list", "-mod=readonly", "-deps", "-f", `{{if not .Standard}}{{with .Module}}{{if .Main}}{{$.Dir}}{{end}}{{end}}{{end}}`}, docsBuildGoRoots...)
	cmd := exec.Command("go", args...)
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	for _, dir := range strings.Fields(string(out)) {
		rel, err := filepath.Rel(root, dir)
		if err != nil {
			t.Fatal(err)
		}
		inputs = append(inputs, filepath.ToSlash(rel)+"/x")
	}
	return inputs
}

func TestDocsBuildInputsAreWatched(t *testing.T) {
	root := realTree(t).root
	for _, in := range docsBuildInputs(t, root) {
		if !watched(in) {
			t.Errorf("docs build input %q is outside docsWatchPaths: a change to it does not rebuild production docs; add it here and to the Workers Builds watch paths", strings.TrimSuffix(in, "/x"))
		}
	}
}

func TestDocsWatchPathsAreNotStale(t *testing.T) {
	root := realTree(t).root
	for _, p := range docsWatchPaths {
		path := strings.TrimSuffix(p, "/*")
		info, err := os.Stat(filepath.Join(root, path))
		if err != nil {
			t.Errorf("watch path %q names nothing in the repository: %v", p, err)
			continue
		}
		if info.IsDir() != strings.HasSuffix(p, "/*") {
			t.Errorf("watch path %q: /* belongs on a directory and only on a directory", p)
		}
	}
}

func TestWatchCovers(t *testing.T) {
	if !watchCovers("docs/*", "docs/a/b.md") || watchCovers("docs/*", "docs-data/a") || watchCovers("mkdocs.yml", "mkdocs.yml.fixture") {
		t.Fatal("watchCovers matches the wrong paths")
	}
}

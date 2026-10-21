package docstools_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var docsChecksPythonFiles = []string{
	"scripts/check_built_site_links.py",
	"scripts/check_docs_candidate_search.py",
	"tests/docs/test_built_site_links.py",
}

// The three docs checks ported to Go are not run from Python anywhere: the scripts are gone and no workflow, make
// target or build script names them.
func TestDocsChecksRunNoPython(t *testing.T) {
	root := driftRepoRoot(t)
	for _, rel := range docsChecksPythonFiles {
		if _, err := os.Stat(filepath.Join(root, rel)); !os.IsNotExist(err) {
			t.Errorf("%s still exists (stat err: %v); the check is `cigate docs`", rel, err)
		}
	}
	for _, rel := range []string{"Makefile", ".gitlab-ci.yml", "scripts/build_docs_cloudflare.py", ".github/workflows/docs-guards.yml", ".github/workflows/docs-cloudflare.yml"} {
		raw, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			t.Fatal(err)
		}
		for number, line := range strings.Split(string(raw), "\n") {
			for _, py := range docsChecksPythonFiles[:2] {
				if strings.Contains(line, py) || strings.Contains(line, strings.TrimPrefix(py, "scripts/")) {
					t.Errorf("%s:%d runs %s: %s", rel, number+1, py, strings.TrimSpace(line))
				}
			}
		}
	}
}

// docs-guards.yml runs each Go verb on the built site, behind the Go toolchain step.
func TestDocsGuardsWorkflowRunsTheGoVerbs(t *testing.T) {
	root := driftRepoRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, ".github/workflows/docs-guards.yml"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	for _, want := range []string{
		"actions/setup-go@",
		"cigate docs check-built-site-links \\\n            --site-dir .build/docs",
		"cigate docs check-docs-search \\\n            --site-dir .build/docs",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("docs-guards.yml does not hold %q", want)
		}
	}
	cf, err := os.ReadFile(filepath.Join(root, ".github/workflows/docs-cloudflare.yml"))
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(string(cf), "actions/setup-go@"); got < 2 {
		t.Errorf("docs-cloudflare.yml has %d Go toolchain steps, want at least 2 (preview and production builds run the Go verbs)", got)
	}
}

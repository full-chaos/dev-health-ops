package docstools_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const navWithPage = "nav:\n  - Reference:\n    - reference/limits-and-compatibility/provider-limits.md\n"

// providerLimitsTree is a synthetic tree (never the real mkdocs.yml: a Go test that read it would need a go.yml
// filter, and go.yml must not list docs paths). The real tree is checked by docs-drift.yml.
func providerLimitsTree(t *testing.T, nav string, page bool) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "mkdocs.yml"), []byte(nav), 0o644); err != nil {
		t.Fatal(err)
	}
	if page {
		path := filepath.Join(dir, "docs", "reference", "limits-and-compatibility", "provider-limits.md")
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("# Provider limits\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestCheckProviderLimitsPassesAWiredPage(t *testing.T) {
	t.Chdir(providerLimitsTree(t, navWithPage, true))
	code, stdout, stderr := runDocs("check-provider-limits")
	if code != 0 || stderr != "" || stdout != "Provider limits page check passed\n" {
		t.Fatalf("rc %d stdout %q stderr %q", code, stdout, stderr)
	}
}

func TestCheckProviderLimitsFiresOnPlantedDefects(t *testing.T) {
	for name, c := range map[string]struct {
		nav  string
		page bool
		want string
	}{
		"page removed":          {navWithPage, false, "missing canonical provider limits page"},
		"page dropped from nav": {"nav:\n  - Reference:\n    - reference/other.md\n", true, "not wired into the mkdocs.yml nav"},
		"no mkdocs.yml":         {"", true, "not wired into the mkdocs.yml nav"},
	} {
		t.Run(name, func(t *testing.T) {
			dir := providerLimitsTree(t, c.nav, c.page)
			if c.nav == "" {
				os.Remove(filepath.Join(dir, "mkdocs.yml"))
			}
			t.Chdir(dir)
			code, stdout, stderr := runDocs("check-provider-limits")
			if code != 1 || stdout != "" || !strings.Contains(stderr, c.want) {
				t.Fatalf("rc %d stdout %q stderr %q, want rc 1, empty stdout, stderr naming %q", code, stdout, stderr, c.want)
			}
		})
	}
}

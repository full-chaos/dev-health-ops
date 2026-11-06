package docstools

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var excludedFromTheSite = []string{
	"go-migration-matrix.md",
}

func repoRootForDocs(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"mkdocs.yml", "docs", "docs-data"} {
		if _, err := os.Stat(filepath.Join(root, name)); err != nil {
			t.Skip("the repository docs are not here:", err)
		}
	}
	return root
}

func copyFile(t *testing.T, src, dst string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		t.Fatal(err)
	}
	in, err := os.Open(src)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	if _, err := io.Copy(out, in); err != nil {
		t.Fatal(err)
	}
}

// copyOfDocs returns a work dir holding a copy of the real tree, with mkdocs.yml text edited by edit.
func copyOfDocs(t *testing.T, root string, edit func(string) string) string {
	t.Helper()
	work, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{"docs", "docs-data"} {
		base := filepath.Join(root, dir)
		_ = filepath.Walk(base, func(path string, info os.FileInfo, err error) error {
			if err != nil || !info.Mode().IsRegular() {
				return nil
			}
			rel, _ := filepath.Rel(root, path)
			copyFile(t, path, filepath.Join(work, rel))
			return nil
		})
	}
	raw, err := os.ReadFile(filepath.Join(root, "mkdocs.yml"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(work, "mkdocs.yml"), []byte(edit(string(raw))), 0o644); err != nil {
		t.Fatal(err)
	}
	return work
}

func validatePublicationIn(t *testing.T, work string) (int, string) {
	t.Helper()
	t.Chdir(work)
	var stdout, stderr bytes.Buffer
	code := Run([]string{"validate-publication"}, &stdout, &stderr)
	return code, stdout.String() + stderr.String()
}

func withExclude(entries ...string) func(string) string {
	return func(s string) string {
		a := strings.Index(s, "exclude_docs: |\n")
		if a < 0 {
			return s
		}
		b := a + len("exclude_docs: |\n")
		for strings.HasPrefix(s[b:], "  ") {
			b += strings.Index(s[b:], "\n") + 1
		}
		var block string
		for _, e := range entries {
			block += "  " + e + "\n"
		}
		return s[:a+len("exclude_docs: |\n")] + block + s[b:]
	}
}

const strayPage = "zz-stray-page.md"

func addStrayPage(t *testing.T, work string) {
	t.Helper()
	copyFile(t, filepath.Join(work, "docs", "index.md"), filepath.Join(work, "docs", strayPage))
}

func TestExcludedPageNeedsNoNavigationRow(t *testing.T) {
	root := repoRootForDocs(t)
	work := copyOfDocs(t, root, withExclude(append([]string{strayPage}, excludedFromTheSite...)...))
	addStrayPage(t, work)
	if code, out := validatePublicationIn(t, work); code != 0 {
		t.Fatalf("a page named in exclude_docs was refused: exit %d: %s", code, out)
	}
}

func TestPageOutsideNavigationAndExcludeDocsStillFails(t *testing.T) {
	root := repoRootForDocs(t)
	work := copyOfDocs(t, root, withExclude(excludedFromTheSite...))
	addStrayPage(t, work)
	code, out := validatePublicationIn(t, work)
	if code == 0 || !strings.Contains(out, "unclassified Markdown outside navigation") || !strings.Contains(out, strayPage) {
		t.Fatalf("a stray page outside nav and exclude_docs was not refused: exit %d: %s", code, out)
	}
}

func TestExcludeDocsIsAnExactPathNotAPrefix(t *testing.T) {
	root := repoRootForDocs(t)
	work := copyOfDocs(t, root, withExclude(append([]string{"zz-stray"}, excludedFromTheSite...)...))
	addStrayPage(t, work)
	code, out := validatePublicationIn(t, work)
	if code == 0 || !strings.Contains(out, strayPage) {
		t.Fatalf("a prefix of a page path hid the page: exit %d: %s", code, out)
	}
}

func TestExcludeDocsHoldsExactlyTheApprovedPaths(t *testing.T) {
	root := repoRootForDocs(t)
	raw, err := os.ReadFile(filepath.Join(root, "mkdocs.yml"))
	if err != nil {
		t.Fatal(err)
	}
	config, err := dcPubLoadYAML(string(raw))
	if err != nil {
		t.Fatal(err)
	}
	v := dcPubOr(dcPubGetStr(config, "exclude_docs"), nil)
	if v == nil {
		t.Fatal("mkdocs.yml has no exclude_docs")
	}
	list, err := pyStr(v)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, line := range strings.Split(list, "\n") {
		if line = strings.TrimSpace(line); line != "" {
			got = append(got, line)
		}
	}
	if strings.Join(got, "\n") != strings.Join(excludedFromTheSite, "\n") {
		t.Fatalf("exclude_docs = %q, want exactly %q: a page hidden from the site needs a ruling", got, excludedFromTheSite)
	}
}

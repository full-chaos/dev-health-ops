package docstools_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const candFactsPageRel = "docs/reference/taxonomies/investment.md"

func candFactsPage(t *testing.T) []byte {
	t.Helper()
	page, err := os.ReadFile(filepath.Join("..", "..", "..", filepath.FromSlash(candFactsPageRel)))
	if err != nil {
		t.Fatal(err)
	}
	return page
}

func candFactsTree(t *testing.T, page []byte) string {
	t.Helper()
	root := t.TempDir()
	dst := filepath.Join(root, filepath.FromSlash(candFactsPageRel))
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dst, page, 0o644); err != nil {
		t.Fatal(err)
	}
	return root
}

func wantCandFactsPass(t *testing.T) {
	t.Helper()
	code, stdout, stderr := runDocs("check-candidate-facts")
	if code != 0 || !strings.HasPrefix(stdout, "Canonical fact drift check passed") || stderr != "" {
		t.Errorf("exit %d, stdout %q, stderr %q; want a pass", code, stdout, stderr)
	}
}

// The Python took its repo root from its own file path, so a run from any directory inside the repo found the page.
func TestCandidateFactsFindsTheRepoRootFromANestedDirectory(t *testing.T) {
	root := candFactsTree(t, candFactsPage(t))
	nested := filepath.Join(root, "ci", "deep")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Chdir(nested)
	wantCandFactsPass(t)
}

// A symlink that lives outside the tree has a logical parent chain that never reaches the repo; the physical one does.
func TestCandidateFactsFindsTheRepoRootFromASymlinkedDirectory(t *testing.T) {
	root := candFactsTree(t, candFactsPage(t))
	nested := filepath.Join(root, "ci", "deep")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(nested, link); err != nil {
		t.Fatal(err)
	}
	t.Chdir(link)
	wantCandFactsPass(t)
}

// Run from a subdirectory of this repo, the repo's own page is the one read.
func TestCandidateFactsFromARepoSubdirectoryReadsTheRepoPage(t *testing.T) {
	wantCandFactsPass(t)
}

// The nearest page wins: a nested directory holding its own page is read, not the page above it (chosen behaviour,
// where the Python's fixed root would have read the outer one).
func TestCandidateFactsNearestPageWins(t *testing.T) {
	root := candFactsTree(t, candFactsPage(t))
	inner := filepath.Join(root, "ci", "deep")
	if err := os.MkdirAll(filepath.Join(inner, filepath.FromSlash(filepath.Dir(candFactsPageRel))), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inner, filepath.FromSlash(candFactsPageRel)), []byte("`risk`\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Chdir(inner)
	code, stdout, _ := runDocs("check-candidate-facts")
	if code != 1 || !strings.Contains(stdout, "missing canonical taxonomy keys") {
		t.Errorf("exit %d, stdout %q; want the inner page's missing-keys error", code, stdout)
	}
}

// Known difference from the Python (stricter, named): outside any tree holding the page nothing is found.
func TestCandidateFactsOutsideAnyTreeStillRefuses(t *testing.T) {
	t.Chdir(t.TempDir())
	code, stdout, stderr := runDocs("check-candidate-facts")
	if code != 1 || stdout != "ERROR: canonical taxonomy source or page is missing\n" || stderr != "" {
		t.Errorf("exit %d, stdout %q, stderr %q; want the missing-page refusal", code, stdout, stderr)
	}
}

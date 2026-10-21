package recordedfiles

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// trackedFiles is `git ls-files -z -- <pathspecs>` in root (staged files count): the repository-relative,
// slash-separated paths git tracks. A git failure fails the test: a guard that silently scanned nothing would pass.
func trackedFiles(t *testing.T, root string, pathspecs ...string) map[string]bool {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", root, "ls-files", "-z", "--"}, pathspecs...)...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git ls-files in %s: %v: %s", root, err, stderr.String())
	}
	files := map[string]bool{}
	for _, rel := range strings.Split(string(out), "\x00") {
		if rel != "" {
			files[rel] = true
		}
	}
	return files
}

// untrackedRows is every manifest row whose file git does not track: a file that is on disk but ignored (or never
// added) passes TestEveryFileATestReadsIsInItsManifest here and is missing from a clean checkout.
func untrackedRows(t *testing.T, repo string) []string {
	t.Helper()
	roots, err := Roots(repo)
	if err != nil {
		t.Fatal(err)
	}
	if len(roots) == 0 {
		t.Fatal("no manifest roots: the walk is broken")
	}
	tracked := trackedFiles(t, repo, roots...)
	var out []string
	for _, root := range roots {
		manifest, err := Read(repo, root)
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range manifest.Rows {
			if file := root + "/" + row.Path; !tracked[file] {
				out = append(out, file+" is a row of "+ManifestPath(root)+" but git does not track it (ignored or never added): a clean checkout has no such file")
			}
		}
	}
	return out
}

func TestEveryManifestRowIsATrackedFile(t *testing.T) {
	repo, err := RepoRoot(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, problem := range untrackedRows(t, repo) {
		t.Error(problem)
	}
}

func TestAnIgnoredFileWithAManifestRowIsRefused(t *testing.T) {
	repo := t.TempDir()
	git := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", repo}, args...)...)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	git("init", "-q")
	write(t, repo, "go.mod", "module planted\n")
	write(t, repo, ".gitignore", "**/site**\n")
	write(t, repo, "a/testdata/in/pages/index.html", "kept\n")
	write(t, repo, "a/testdata/in/site/index.html", "ignored\n")
	rows := []Row{}
	for _, rel := range []string{"in/pages/index.html", "in/site/index.html"} {
		digest, err := Digest(repo, "a/testdata/"+rel)
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, Row{Digest: digest, Kind: HandWritten, Path: rel})
	}
	if err := Write(repo, Manifest{Root: "a/testdata", Rows: rows}); err != nil {
		t.Fatal(err)
	}
	git("add", "-A")
	got := untrackedRows(t, repo)
	if len(got) != 1 || !strings.HasPrefix(got[0], "a/testdata/in/site/index.html is a row of a/testdata.manifest.tsv") {
		t.Errorf("problems %q; want exactly the ignored in/site file", got)
	}
	if _, err := os.Stat(filepath.Join(repo, "a/testdata/in/site/index.html")); err != nil {
		t.Fatal("the planted ignored file must be on disk, as it was on the lane host", err)
	}
}

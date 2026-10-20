package nopython_test

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// trackedFiles is `git ls-files -z -- <pathspecs>` in root: the repository-relative, slash-separated paths git tracks
// that still exist on disk. Untracked files (caches, scratch) are never listed. A git failure fails the test: a guard
// that silently scanned nothing would pass.
func trackedFiles(t *testing.T, root string, pathspecs ...string) []string {
	t.Helper()
	args := append([]string{"-C", root, "ls-files", "-z", "--"}, pathspecs...)
	cmd := exec.Command("git", args...)
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
			continue // tracked but deleted in the work tree: nothing to scan
		}
		files = append(files, rel)
	}
	return files
}

// plantedRepo is a temporary git repository holding the given files; the ones in track are staged (tracked), the rest
// are left untracked.
func plantedRepo(t *testing.T, files map[string]string, track ...string) string {
	t.Helper()
	dir := t.TempDir()
	run := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	run("init", "-q")
	for rel, body := range files {
		path := filepath.Join(dir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if len(track) > 0 {
		run(append([]string{"add", "--"}, track...)...)
	}
	return dir
}

func TestTrackedFilesListsOnlyTrackedFiles(t *testing.T) {
	dir := plantedRepo(t, map[string]string{"a.sh": "x\n", ".uv-cache/b.sh": "y\n", "c/d.py": "z\n"}, "a.sh", "c/d.py")
	if got := strings.Join(trackedFiles(t, dir, "*.sh", "*.py"), ","); got != "a.sh,c/d.py" {
		t.Fatalf("trackedFiles = %q, want only the tracked a.sh and c/d.py", got)
	}
}

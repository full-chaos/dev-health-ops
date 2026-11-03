// Package bigboydir locates the bigboy compose tree for tests that pin it. The tree lives in the private
// deploy repo (docker/bigboy); a test run points BIGBOY_DIR at it. Without BIGBOY_DIR the in-repo ci/bigboy is
// used, and a run with neither skips instead of failing.
package bigboydir

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

const (
	// Env names the variable that points at the bigboy tree.
	Env     = "BIGBOY_DIR"
	marker  = "compose.bigboy.yml"
	inRepo  = "ci/bigboy"
	skipMsg = "bigboy tree absent: set " + Env + " to the deploy repo's docker/bigboy (looked in %s)"
)

// Lookup returns the bigboy tree and whether it holds compose.bigboy.yml (a marker that cannot be read counts as held). It never skips, for a gate that
// has other inputs to check when the tree is absent.
func Lookup(repoRoot string) (string, bool) {
	dir := os.Getenv(Env)
	switch {
	case dir == "":
		dir = filepath.Join(repoRoot, filepath.FromSlash(inRepo))
	case !filepath.IsAbs(dir):
		// go test runs in the package directory; a relative value is read from the repository root
		dir = filepath.Join(repoRoot, dir)
	}
	_, err := os.Stat(filepath.Join(dir, marker))
	// only a missing marker means "absent": an unreadable tree reports present, so the guards read it and fail
	return dir, err == nil || !errors.Is(err, fs.ErrNotExist)
}

// InRepo reports whether the selected tree is the in-repo ci/bigboy (BIGBOY_DIR unset, or naming it).
func InRepo(repoRoot string) bool {
	dir, _ := Lookup(repoRoot)
	a, errA := filepath.EvalSymlinks(dir)
	b, errB := filepath.EvalSymlinks(filepath.Join(repoRoot, filepath.FromSlash(inRepo)))
	return errA == nil && errB == nil && a == b
}

// Dir returns the bigboy tree: $BIGBOY_DIR when set, else <repoRoot>/ci/bigboy. It skips the test when the
// directory has no compose.bigboy.yml.
func Dir(t testing.TB, repoRoot string) string {
	t.Helper()
	dir, ok := Lookup(repoRoot)
	if !ok {
		t.Skipf(skipMsg, dir)
	}
	return dir
}

// Path joins elem under Dir.
func Path(t testing.TB, repoRoot string, elem ...string) string {
	t.Helper()
	return filepath.Join(append([]string{Dir(t, repoRoot)}, elem...)...)
}

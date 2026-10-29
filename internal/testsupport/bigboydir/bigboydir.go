// Package bigboydir locates the bigboy compose tree for tests that pin it. The tree lives in the private
// deploy repo (docker/bigboy); a test run points BIGBOY_DIR at it. Without BIGBOY_DIR the in-repo ci/bigboy is
// used, and a run with neither skips instead of failing.
package bigboydir

import (
	"os"
	"path/filepath"
	"testing"
)

const (
	// Env names the variable that points at the bigboy tree.
	Env     = "BIGBOY_DIR"
	marker  = "compose.bigboy.yml"
	inRepo  = "ci/bigboy"
	skipMsg = "bigboy tree absent: set " + Env + " to the deploy repo's docker/bigboy"
)

// Lookup returns the bigboy tree and whether it holds compose.bigboy.yml. It never skips, for a gate that
// has other inputs to check when the tree is absent.
func Lookup(repoRoot string) (string, bool) {
	dir := os.Getenv(Env)
	if dir == "" {
		dir = filepath.Join(repoRoot, filepath.FromSlash(inRepo))
	}
	_, err := os.Stat(filepath.Join(dir, marker))
	return dir, err == nil
}

// Dir returns the bigboy tree: $BIGBOY_DIR when set, else <repoRoot>/ci/bigboy. It skips the test when the
// directory has no compose.bigboy.yml.
func Dir(t testing.TB, repoRoot string) string {
	t.Helper()
	dir, ok := Lookup(repoRoot)
	if !ok {
		t.Skip(skipMsg)
	}
	return dir
}

// Path joins elem under Dir.
func Path(t testing.TB, repoRoot string, elem ...string) string {
	t.Helper()
	return filepath.Join(append([]string{Dir(t, repoRoot)}, elem...)...)
}

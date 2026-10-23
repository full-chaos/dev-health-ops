package checkgo_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// The discovery scenarios run a ci script against a scratch copy of the tracked tree. The copy is built once per process
// (copying ~20k files costs seconds) and removed by TestMain with the shim directory. This file is specific to this package:
// ci_script_helpers_test.go is a byte-identical helper copy shared with other packages and stays as it is.
var (
	discoveryTreeOnce sync.Once
	discoveryTreeDir  string
	discoveryTreeErr  error
	// discoveryTreeMu serializes the scenarios that edit the copy; each restores what it changed before the next.
	discoveryTreeMu sync.Mutex
)

func ensureDiscoveryTree() (string, error) {
	discoveryTreeOnce.Do(func() {
		discoveryTreeDir = filepath.Join(noPythonDir, "discovery-tree")
		discoveryTreeErr = cloneTreeInto(repoRootNoT(), discoveryTreeDir)
	})
	return discoveryTreeDir, discoveryTreeErr
}

// cloneTreeInto copies every tracked file of root into a fresh git repository at dst (index only, no commit).
func cloneTreeInto(root, dst string) error {
	listed := cmdSpec{dir: root, env: gitEnv}.run("git", "ls-files", "-z", "--cached")
	if listed.code != 0 {
		return fmt.Errorf("git ls-files: %s", listed.stderr)
	}
	copied := 0
	for _, rel := range strings.Split(listed.stdout, "\x00") {
		if rel == "" {
			continue
		}
		info, err := os.Lstat(filepath.Join(root, rel))
		if err != nil || !info.Mode().IsRegular() {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			return err
		}
		out := filepath.Join(dst, rel)
		if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(out, raw, info.Mode().Perm()); err != nil {
			return err
		}
		copied++
	}
	if copied < 1000 {
		return fmt.Errorf("cloned only %d files: the tracked-tree copy measured nothing", copied)
	}
	r := newGitRepo(dst)
	r.git("add", "-A")
	return r.err
}

package checkgoshards_test

import (
	"os"
	"path/filepath"
	"testing"
)

// Verbatim copy of a helper that lives in a sibling package; this package needs it and cannot import another package's _test files.

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above the test directory")
		}
		dir = parent
	}
}

// failureTail is the end of a failed script run for an error message: stderr first and alone, then stdout. It matches
// the helper of internal/cigates/checkgo and lives outside ci_script_helpers_test.go, whose copies must stay byte-identical.
func failureTail(res cmdResult) string {
	return "stderr: " + tail(res.stderr, 1500) + "\nstdout: " + tail(res.stdout, 500)
}

// deadlineEnvNames are the job-deadline variables a stand-in run pins to unset; the same list lives in internal/cigates/checkgo.
var deadlineEnvNames = []string{"GITHUB_ACTIONS", "CHECK_GO_JOB_CAP_MIN", "CHECK_GO_JOB_START", "CHECK_GO_NOW_EPOCH", "CHECK_GO_NOW_EPOCH_FILE"}

func tail(s string, n int) string {
	if len(s) > n {
		return s[len(s)-n:]
	}
	return s
}

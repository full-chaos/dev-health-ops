package cigates_test

import (
	"bufio"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// ci/kept_python_tests.txt is the only list of Python tests hosted CI runs.
// A tests/authclient/test_*.py file missing from it never runs (CHAOS-10188).
func TestEveryAuthclientPythonTestIsKept(t *testing.T) {
	root := keptRepoRoot(t)

	listed := map[string]bool{}
	f, err := os.Open(filepath.Join(root, "ci", "kept_python_tests.txt"))
	if err != nil {
		t.Fatalf("read kept list: %v", err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line != "" && !strings.HasPrefix(line, "#") {
			listed[line] = true
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan kept list: %v", err)
	}

	entries, err := os.ReadDir(filepath.Join(root, "tests", "authclient"))
	if err != nil {
		t.Fatalf("read tests/authclient: %v", err)
	}
	var missing []string
	for _, e := range entries {
		n := e.Name()
		if !e.IsDir() && strings.HasPrefix(n, "test_") && strings.HasSuffix(n, ".py") {
			if p := "tests/authclient/" + n; !listed[p] {
				missing = append(missing, p)
			}
		}
	}
	sort.Strings(missing)
	for _, p := range missing {
		t.Errorf("%s is not in ci/kept_python_tests.txt, so hosted CI never runs it", p)
	}
}

func keptRepoRoot(t *testing.T) string {
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
			t.Fatal("go.mod not found above test dir")
		}
		dir = parent
	}
}

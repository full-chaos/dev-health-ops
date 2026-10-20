package nopython_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// testsPyAllowed is the closed list of Python files that may stay under tests/, as the path under tests/ without the
// ".py" suffix. Stems, not file names: a literal file name would make the go.yml path-filter guard treat each file as
// a path a Go test reads, and these files are not inputs of any Go test.
// Anything under tests/tooling/ and tests/docs/ is allowed as a whole.
var testsPyAllowed = map[string]bool{
	"__init__":                             true,
	"conftest":                             true,
	"_env_isolation":                       true,
	"test_go_api_routing_digest_checker":   true,
	"test_go_api_routing_operator_text":    true,
	"scripts/test_strip_agent_attribution": true,
}

func testsTreeRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := moduleroot.Caller(0)
	if !ok {
		t.Fatal("cannot locate test file")
	}
	return filepath.Join(filepath.Dir(file), "..", "..", "..")
}

func testsTreePythonFiles(t *testing.T, root string) []string {
	t.Helper()
	var found []string
	err := filepath.WalkDir(filepath.Join(root, "tests"), func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(p, ".py") {
			return nil
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		found = append(found, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return found
}

func testsTreeStray(files []string) []string {
	var stray []string
	for _, f := range files {
		if testsPyAllowed[strings.TrimSuffix(strings.TrimPrefix(f, "tests/"), ".py")] || strings.HasPrefix(f, "tests/tooling/") || strings.HasPrefix(f, "tests/docs/") {
			continue
		}
		stray = append(stray, f)
	}
	return stray
}

func TestNoPythonUnderTests(t *testing.T) {
	root := testsTreeRoot(t)
	if _, err := os.Stat(filepath.Join(root, "tests")); err != nil {
		t.Fatal(err)
	}
	if stray := testsTreeStray(testsTreePythonFiles(t, root)); len(stray) > 0 {
		t.Fatalf("Python is dead; %d .py file(s) under tests/ outside the allow list, first: %s", len(stray), stray[0])
	}
}

func TestNoPythonUnderTestsFiresOnPlantedFile(t *testing.T) {
	stray := testsTreeStray([]string{"tests/tooling/x.py", "tests/test_planted.py"})
	if len(stray) != 1 || stray[0] != "tests/test_planted.py" {
		t.Fatalf("guard did not name the planted file: %v", stray)
	}
}

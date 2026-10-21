package nopython_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ci/run_tests.sh keeps only the live-e2e tier: it is off the shell-python
// pending list, none of its code lines names python/python3/uv/pytest, and a
// planted pytest line in it is caught by the same match the shell scan uses.
func TestRunTestsShellNamesNoPythonAndAPlantedPytestLineIsCaught(t *testing.T) {
	const rel = "ci/run_tests.sh"
	if owner, ok := shellPythonPending[rel]; ok {
		t.Fatalf("%s is still on the shell-python pending list (%s)", rel, owner)
	}
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), rel))
	if err != nil {
		t.Fatal(err)
	}
	for number, line := range strings.Split(string(raw), "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "#") && scriptPython.MatchString(line) {
			t.Errorf("%s:%d names python/python3/uv/pytest: %s", rel, number+1, strings.TrimSpace(line))
		}
	}
	for _, planted := range []string{"  run_cmd pytest tests -v", "  uv run pytest", `  "${ROOT}/.venv/bin/python" -m pytest`} {
		if !scriptPython.MatchString(planted) {
			t.Errorf("planted line %q in %s is not caught by the shell scan", planted, rel)
		}
	}
}

package bigboyrender_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var portedScripts = []string{
	"render-dho-api-ch-users.py",
	"bigboy-grants-check-compare.py",
	"cut-contents-pg-api-posture.py",
	"cut-contents-pg-domain-posture.py",
	"pg_posture_expect.py",
	"bump-swap-rewrite-values.py",
}

var portedCallers = []string{
	"check-dho-api-ch-user.sh",
	"bigboy-grants-check.sh",
	"cut-contents.sh",
	"bump-swap.sh",
}

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

func TestPortedPythonScriptsAreDeleted(t *testing.T) {
	root := repoRoot(t)
	for _, name := range portedScripts {
		path := filepath.Join(root, "ci", "bigboy", name)
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Errorf("ci/bigboy/%s still exists (stat err: %v); its Go port is cigate bigboy", name, err)
		}
	}
}

func TestCallersRunNoPython(t *testing.T) {
	root := repoRoot(t)
	for _, name := range portedCallers {
		raw, err := os.ReadFile(filepath.Join(root, "ci", "bigboy", name))
		if err != nil {
			t.Fatal(err)
		}
		for number, line := range strings.Split(string(raw), "\n") {
			if strings.HasPrefix(strings.TrimSpace(line), "#") {
				continue
			}
			for _, script := range portedScripts {
				if strings.Contains(line, script) {
					t.Errorf("ci/bigboy/%s:%d names a ported Python script: %s", name, number+1, strings.TrimSpace(line))
				}
			}
		}
	}
}

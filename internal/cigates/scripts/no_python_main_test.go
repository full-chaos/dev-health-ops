package scripts_test

import (
	"os"
	"path/filepath"
	"testing"
)

// No test of this package may run Python: the process PATH starts with shims for python, python3, uv and pip that exit 127,
// so any ci script a test launches that reaches for them fails loudly instead of using the host's interpreter.
// noPythonDir is the shim directory; every child environment that sets its own PATH puts it first (withShims).
var noPythonDir string

func withShims(rest string) string { return noPythonDir + string(os.PathListSeparator) + rest }

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "no-python-shims-")
	if err != nil {
		panic(err)
	}
	for _, name := range []string{"python", "python3", "uv", "pip", "pip3"} {
		body := "#!/bin/sh\necho 'python-forbidden-in-ci-tests: '\"$0\" >&2\nexit 127\n"
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o755); err != nil {
			panic(err)
		}
	}
	noPythonDir = dir
	os.Setenv("CIGATES_NO_PYTHON_DIR", dir)
	os.Setenv("PATH", withShims(os.Getenv("PATH")))
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// A python call through the package's own runner with only the fixed system PATH must still meet the shims: exit 127.
func TestPythonIsRefusedThroughTheRunners(t *testing.T) {
	t.Parallel()
	for _, prog := range []string{"python3 -c 1", "python -c 1", "uv --version", "pip --version"} {
		if r := (cmdSpec{env: []string{basePath}}).run("bash", "-c", prog); r.code != 127 {
			t.Errorf("cmdSpec ran %q with exit %d, want 127", prog, r.code)
		}
		if r := (freeSpec{env: []string{basePath}}).run("bash", "-c", prog); r.code != 127 {
			t.Errorf("freeSpec ran %q with exit %d, want 127", prog, r.code)
		}
	}
}

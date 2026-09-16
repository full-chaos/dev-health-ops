// Package pyoracle resolves the interpreter every live-Python oracle test in
// this repository compares Go against.
//
// It exists because two policies grew independently: chschema resolved
// DEV_HEALTH_PYTHON, then the repo's checked-out .venv, then PATH; every
// other oracle honoured a differently-named PYTHON variable and fell back
// straight to a bare "python3" on PATH, skipping the venv entirely. Setting
// one variable fixed some oracles and left the rest pointed at whatever
// system interpreter happened to be first on PATH -- which reports a missing
// dependency as if it were a Go/Python divergence, because the failure text
// never says which interpreter ran.
//
// One policy, one primary variable: DEV_HEALTH_PYTHON, then the deprecated
// PYTHON alias, then the repo's checked-out .venv, then PATH.
package pyoracle

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// Interpreter resolves the interpreter a live-Python oracle should run
// against, given the repository root (the directory a caller would also use
// to build PYTHONPATH). It reports which rule matched, so callers can make
// the resolution visible even on a passing run.
func Interpreter(root string) (path string, rule string, err error) {
	if override := os.Getenv("DEV_HEALTH_PYTHON"); override != "" {
		return override, "DEV_HEALTH_PYTHON override", nil
	}
	if alias := os.Getenv("PYTHON"); alias != "" {
		return alias, "PYTHON override (deprecated alias of DEV_HEALTH_PYTHON)", nil
	}
	venv := filepath.Join(root, ".venv", "bin", "python")
	if info, statErr := os.Stat(venv); statErr == nil && !info.IsDir() {
		return venv, "repo .venv at " + venv, nil
	}
	found, lookErr := exec.LookPath("python3")
	if lookErr != nil {
		return "", "", fmt.Errorf(
			"no Python to run the live oracle: %s does not exist, "+
				"DEV_HEALTH_PYTHON and PYTHON are both unset, and python3 is not on PATH: %w",
			venv, lookErr)
	}
	return found, "python3 on PATH", nil
}

// Resolve is the standard entry point for a live-Python oracle test: it
// resolves the interpreter under root, logs the resolved path and which rule
// matched (so a wrong-but-passing run is never silent), and fails the test
// naming the interpreter search when none can be found.
func Resolve(t *testing.T, root string) string {
	t.Helper()
	python, rule, err := Interpreter(root)
	if err != nil {
		t.Fatalf("pyoracle: %v", err)
	}
	t.Logf("pyoracle: resolved interpreter %s (%s)", python, rule)
	return python
}

// RunError wraps an error from executing a resolved interpreter so the
// message names the interpreter path. Without it, a missing dependency or
// import failure reads exactly like an oracle divergence, because both
// print as a bare test failure with no mention of which Python answered.
func RunError(python string, err error, output []byte) error {
	if err == nil {
		return nil
	}
	if len(output) > 0 {
		return fmt.Errorf("interpreter %s: %w: %s", python, err, output)
	}
	return fmt.Errorf("interpreter %s: %w", python, err)
}

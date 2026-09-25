package pyoracle

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestInterpreterOverrideWins asserts DEV_HEALTH_PYTHON beats every other
// rule, including an existing repo .venv.
func TestInterpreterOverrideWins(t *testing.T) {
	root := t.TempDir()
	venv := filepath.Join(root, ".venv", "bin", "python")
	if err := os.MkdirAll(filepath.Dir(venv), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(venv, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DEV_HEALTH_PYTHON", "/override/python")
	t.Setenv("PYTHON", "")

	path, rule, err := Interpreter(root)
	if err != nil {
		t.Fatalf("Interpreter: %v", err)
	}
	if path != "/override/python" {
		t.Fatalf("path = %q, want the DEV_HEALTH_PYTHON override", path)
	}
	if !strings.Contains(rule, "DEV_HEALTH_PYTHON") {
		t.Fatalf("rule = %q, want it to name DEV_HEALTH_PYTHON", rule)
	}
}

// TestInterpreterPythonAliasWinsOverVenv asserts the deprecated PYTHON alias
// still beats the repo .venv when DEV_HEALTH_PYTHON is unset -- it is a
// fallback for the override rule, not for the venv rule.
func TestInterpreterPythonAliasWinsOverVenv(t *testing.T) {
	root := t.TempDir()
	venv := filepath.Join(root, ".venv", "bin", "python")
	if err := os.MkdirAll(filepath.Dir(venv), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(venv, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DEV_HEALTH_PYTHON", "")
	t.Setenv("PYTHON", "/alias/python")

	path, rule, err := Interpreter(root)
	if err != nil {
		t.Fatalf("Interpreter: %v", err)
	}
	if path != "/alias/python" {
		t.Fatalf("path = %q, want the PYTHON alias override", path)
	}
	if !strings.Contains(rule, "PYTHON") {
		t.Fatalf("rule = %q, want it to name the PYTHON alias", rule)
	}
}

// TestInterpreterFindsVenv asserts the repo .venv is used when neither env
// var is set.
func TestInterpreterFindsVenv(t *testing.T) {
	root := t.TempDir()
	venv := filepath.Join(root, ".venv", "bin", "python")
	if err := os.MkdirAll(filepath.Dir(venv), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(venv, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DEV_HEALTH_PYTHON", "")
	t.Setenv("PYTHON", "")

	path, rule, err := Interpreter(root)
	if err != nil {
		t.Fatalf("Interpreter: %v", err)
	}
	if path != venv {
		t.Fatalf("path = %q, want the repo venv %q", path, venv)
	}
	if !strings.Contains(rule, "venv") {
		t.Fatalf("rule = %q, want it to name the venv", rule)
	}
}

// TestInterpreterFallsBackToPath asserts PATH's python3 is used only when
// neither env var is set and no repo .venv exists.
func TestInterpreterFallsBackToPath(t *testing.T) {
	root := t.TempDir() // no .venv under here
	t.Setenv("DEV_HEALTH_PYTHON", "")
	t.Setenv("PYTHON", "")

	path, rule, err := Interpreter(root)
	if err != nil {
		t.Skipf("python3 not on PATH in this environment: %v", err)
	}
	if path == "" {
		t.Fatal("path is empty despite no error")
	}
	if !strings.Contains(rule, "PATH") {
		t.Fatalf("rule = %q, want it to name PATH", rule)
	}
}

// TestRunErrorNamesInterpreter asserts a run failure's message names the
// interpreter path, so a missing dependency cannot be misread as an oracle
// divergence.
func TestRunErrorNamesInterpreter(t *testing.T) {
	underlying := &fakeExitError{msg: "exit status 1"}
	wrapped := RunError("/some/venv/bin/python", underlying, []byte("ModuleNotFoundError: No module named 'httpx'"))
	if wrapped == nil {
		t.Fatal("RunError returned nil for a non-nil error")
	}
	if !strings.Contains(wrapped.Error(), "/some/venv/bin/python") {
		t.Fatalf("error %q does not name the interpreter path", wrapped.Error())
	}
	if !strings.Contains(wrapped.Error(), "ModuleNotFoundError") {
		t.Fatalf("error %q dropped the underlying output", wrapped.Error())
	}
}

func TestRunErrorNilPassthrough(t *testing.T) {
	if err := RunError("/some/python", nil, nil); err != nil {
		t.Fatalf("RunError(nil) = %v, want nil", err)
	}
}

type fakeExitError struct{ msg string }

func (e *fakeExitError) Error() string { return e.msg }

// fakeInterpreter writes an executable that answers the version probe with
// output (or exits non-zero when output is empty).
func fakeInterpreter(t *testing.T, output string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "python")
	script := "#!/bin/sh\nexit 1\n"
	if output != "" {
		script = "#!/bin/sh\necho '" + output + "'\n"
	}
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestDeployedInterpreterError pins the version gate at its boundary: the
// deployed release and newer pass; the release before it, an older major, an
// unparsable answer and an interpreter that cannot run are all refused.
func TestDeployedInterpreterError(t *testing.T) {
	cases := []struct {
		name    string
		output  string
		wantErr bool
	}{
		{"deployed release", "3.14", false},
		{"newer minor", "3.15", false},
		{"newer major", "4.0", false},
		{"one minor before", "3.13", true},
		{"hosted runner release", "3.12", true},
		{"older major", "2.7", true},
		{"unparsable", "not-a-version", true},
		{"cannot run", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := DeployedInterpreterError(fakeInterpreter(t, tc.output))
			if (err != nil) != tc.wantErr {
				t.Fatalf("output %q: err = %v, wantErr %v", tc.output, err, tc.wantErr)
			}
			if err != nil && !strings.Contains(err.Error(), "python") {
				t.Fatalf("error %q does not name the interpreter", err)
			}
		})
	}
}

func TestDeployedInterpreterErrorMissingBinary(t *testing.T) {
	if err := DeployedInterpreterError(filepath.Join(t.TempDir(), "no-such-python")); err == nil {
		t.Fatal("a missing interpreter must be refused")
	}
}

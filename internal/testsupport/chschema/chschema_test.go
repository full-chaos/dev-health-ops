package chschema

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// writeFakeInterpreter creates an executable file that is NOT reachable through
// PATH, and returns its absolute path.
func writeFakeInterpreter(t *testing.T, directory, name string) string {
	t.Helper()
	path := filepath.Join(directory, name)
	if err := os.WriteFile(path, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestPythonCommandUsesTheResolvedPathNotABareName pins the defect that made
// every chschema-based integration test fail on Ubuntu.
//
// exec.CommandContext resolves a bare name via LookPath against the PARENT
// process's PATH at construction time, so command.Env cannot influence it. The
// old code resolved a good absolute path in pythonBinary and then threw it away
// in favour of the literal "python", which does not exist on a host shipping
// only python3.
func TestPythonCommandUsesTheResolvedPathNotABareName(t *testing.T) {
	directory := t.TempDir()
	// A name that cannot plausibly be on any host's PATH, so the control below
	// is decisive rather than dependent on whether this machine has `python`.
	const name = "chschema-fake-python"
	fake := writeFakeInterpreter(t, directory, name)

	// PATH deliberately excludes `directory`.
	t.Setenv("PATH", filepath.Join(directory, "nowhere"))

	// CONTROL, and it is load-bearing: if the bare name were resolvable here,
	// the assertion below would pass for the wrong reason and prove nothing.
	// This is the same discipline as the CHSCHEMA_APPLIED marker in Apply --
	// verify the situation you think you are in before believing the result.
	if bare := exec.CommandContext(context.Background(), name, "-c", ""); bare.Err == nil {
		t.Fatalf("control failed: %q resolved through PATH, so this test cannot "+
			"demonstrate that passing the resolved path matters", name)
	}

	command := pythonCommand(context.Background(), fake, "http://dsn", directory)
	if command.Err != nil {
		t.Fatalf("pythonCommand failed for an absolute path %q: %v", fake, command.Err)
	}
	if command.Path != fake {
		t.Errorf("command.Path = %q, want the resolved path %q", command.Path, fake)
	}
}

// TestDevHealthPythonOverrideAcceptsAnyInterpreterName covers the second half
// of the defect: pythonBinary honours DEV_HEALTH_PYTHON, but the basename
// switch then rejected anything not named exactly `python` or `python3` with
// "unsupported Python executable". An override the code accepts and then
// refuses is worse than one it does not offer at all.
func TestDevHealthPythonOverrideAcceptsAnyInterpreterName(t *testing.T) {
	directory := t.TempDir()
	for _, name := range []string{"python3.12", "uv-python", "python.sh"} {
		t.Run(name, func(t *testing.T) {
			fake := writeFakeInterpreter(t, directory, name)
			t.Setenv("DEV_HEALTH_PYTHON", fake)

			resolved, err := pythonBinary(directory)
			if err != nil {
				t.Fatalf("pythonBinary rejected the override: %v", err)
			}
			if resolved != fake {
				t.Fatalf("pythonBinary = %q, want the override %q", resolved, fake)
			}
			command := pythonCommand(context.Background(), resolved, "http://dsn", directory)
			if command.Err != nil {
				t.Fatalf("pythonCommand rejected interpreter %q: %v", name, command.Err)
			}
			if command.Path != fake {
				t.Errorf("command.Path = %q, want %q", command.Path, fake)
			}
		})
	}
}

// TestPythonCommandDoesNotPutDotOnTheChildPath guards the one behaviour change
// beyond the fix itself. The PATH entry is kept because it remains correct for
// whatever the migration runner shells out to, but filepath.Dir("python3.12")
// is ".", and prepending the CWD to a child's PATH is a footgun that the old
// code could not hit only because the switch rejected bare-name overrides.
func TestPythonCommandDoesNotPutDotOnTheChildPath(t *testing.T) {
	command := pythonCommand(context.Background(), "python3.12", "http://dsn", t.TempDir())
	for _, entry := range command.Env {
		if entry == "PATH=."+string(os.PathListSeparator)+os.Getenv("PATH") {
			t.Fatalf("child PATH begins with the working directory: %q", entry)
		}
	}
}

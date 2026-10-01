// Package rotguard runs the generator of a frozen fixture as a Python program
// through a golden, so that a "rot guard" (a test that asserts the checked-in
// fixture is still what the Python producer renders) no longer needs a Python
// interpreter on the machine that runs it.
//
// A rot guard has always had two halves: a producer (a script of tests/fixtures
// that renders the fixture from the real Python) and a comparison (what the
// script renders against the checked-in file). Only the producer needs Python.
// Here the producer's stdout is recorded once, on the build that still carries
// the Python sources (the goldenrecord verb), and frozen in the package's
// testdata. A frozen run compares the recorded stdout with the checked-in
// fixture: the guard still fails when the fixture is edited without recording
// the producer again, and a changed generator is another request, so it cannot
// reuse a stale recording.
package rotguard

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// PythonBuild is the commit whose Python sources the rot guards were recorded on:
// the last build that still carried them.
const PythonBuild = "a4847c5e93607451a0c987b314d37e02fc43ce85"

// Generator is one script of the repository, run as a program with arguments.
type Generator struct {
	// Name identifies the run in the golden.
	Name string
	// Path is the script's place under the repository root.
	Path string
	// Args are the command-line arguments the script is given.
	Args []string
}

// Spec is the golden spec of a package's rot guards: path is the golden file
// (relative to the package directory), digest the SHA-256 the test pins (empty
// only while recording), and test the test function recorded.
func Spec(path, digest, pkg, test string) venueoracle.GoldenSpec {
	return venueoracle.GoldenSpec{
		Path:        path,
		PythonBuild: PythonBuild,
		SHA256:      digest,
		Recipe: "git worktree add --detach $DIR " + PythonBuild + " (with its .venv: uv sync --frozen --no-install-project); then from the repository root: " +
			"go run ./internal/testsupport/venueoracle/goldenrecord -pkg " + pkg + " -test '" + test + "' -python-root $DIR",
	}
}

// Run executes each generator as the pinned build runs it and returns its
// stdout, in order. A generator that exits non-zero fails the test. root is the
// repository root of the running test.
func Run(t *testing.T, spec venueoracle.GoldenSpec, root string, generators ...Generator) []string {
	t.Helper()
	programs := make([]programoracle.Program, len(generators))
	for index, generator := range generators {
		source, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(generator.Path)))
		if err != nil {
			t.Fatalf("rotguard: generator %s: %v", generator.Path, err)
		}
		script := programoracle.Script(generator.Name, generator.Path, string(source), nil)
		script.Text = "import sys\nsys.argv = [" + pythonList(append([]string{generator.Path}, generator.Args...)) + "]\n" + script.Text
		programs[index] = script
	}
	answers := programoracle.Run(t, spec, root, programs)
	out := make([]string, len(answers))
	for index, answer := range answers {
		if answer.ExitCode != 0 {
			t.Fatalf("rotguard: generator %s exited %d (stdout %q)", generators[index].Path, answer.ExitCode, answer.Stdout)
		}
		out[index] = answer.Stdout
	}
	return out
}

func pythonList(items []string) string {
	quoted := make([]string, len(items))
	for index, item := range items {
		quoted[index] = strconv.Quote(item)
	}
	return strings.Join(quoted, ", ")
}

// RepositoryRoot is the repository root for a test whose package directory is
// `depth` levels below it (internal/jobs/metrics/numerical is 4).
func RepositoryRoot(t *testing.T, depth int) string {
	t.Helper()
	parts := make([]string, depth)
	for index := range parts {
		parts[index] = ".."
	}
	root, err := filepath.Abs(filepath.Join(parts...))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatal(fmt.Errorf("rotguard: %s is not the repository root: %w", root, err))
	}
	return root
}

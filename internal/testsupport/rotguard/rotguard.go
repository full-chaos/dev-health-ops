// Package rotguard replays the frozen stdout of the generator of a fixture, so
// that a "rot guard" (a test that asserts the checked-in fixture is still what
// the Python producer rendered) needs no Python interpreter.
//
// A rot guard has two halves: a producer (a script that rendered the fixture
// from the real Python) and a comparison (what the script rendered against the
// checked-in file). The producer's stdout was recorded once, on the build that
// still carried the Python sources, and is frozen in the package's testdata.
// The test compares the frozen stdout with the checked-in fixture: the guard
// still fails when the fixture is edited.
package rotguard

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// PythonBuild is the commit whose Python sources the rot guards were recorded on:
// the last build that still carried them.
const PythonBuild = "a4847c5e93607451a0c987b314d37e02fc43ce85"

// Generator is one frozen run of a generator script.
type Generator struct {
	// Name identifies the run in the golden.
	Name string
}

// Spec is the golden spec of a package's rot guards: path is the golden file
// (relative to the package directory) and digest the SHA-256 the test pins.
func Spec(path, digest string) venueoracle.GoldenSpec {
	return venueoracle.GoldenSpec{
		Path:        path,
		PythonBuild: PythonBuild,
		SHA256:      digest,
	}
}

// Run returns the frozen stdout of each generator, in order. A generator that
// exited non-zero when it was recorded fails the test.
func Run(t *testing.T, spec venueoracle.GoldenSpec, generators ...Generator) []string {
	t.Helper()
	programs := make([]programoracle.Program, len(generators))
	for index, generator := range generators {
		programs[index] = programoracle.Program{Name: generator.Name}
	}
	answers := programoracle.Run(t, spec, programs)
	out := make([]string, len(answers))
	for index, answer := range answers {
		if answer.ExitCode != 0 {
			t.Fatalf("rotguard: generator %s exited %d (stdout %q)", generators[index].Name, answer.ExitCode, answer.Stdout)
		}
		out[index] = answer.Stdout
	}
	return out
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

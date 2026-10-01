package emailvalidator_test

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

// emailGoldens is the set of this package's frozen Python answers: each
// oracle program was executed once on Build, and its answer is frozen under
// testdata/golden. The producer is
// pydantic's validate_email and the distributions under it, not a source file
// of this repository, so Identity names them.
// A golden recorded by another producer is refused.
var emailGoldens = programoracle.Set{
	Package:       "./internal/pythonparity/emailvalidator/",
	Build:         "a4847c5e93607451a0c987b314d37e02fc43ce85",
	Identity:      "python 3.14.7\nunicodedata 16.0.0\nemail-validator 2.3.0\nidna 3.20\npydantic 2.13.5\npydantic-core 2.46.5",
	Distributions: []string{"email-validator", "idna", "pydantic", "pydantic-core"},
	// The goldenrecord verb writes each digest when it promotes a recording; a
	// new golden starts as "PIN:" + its file name without ".json".
	Pins: map[string]string{
		"validate-email.golden.json": "c89010e1cde5a318a0defbf604fd5399a211d1458524ac8896f59b82ac667053",
	},
}

// frozenPython returns the stdout of each program, in order, from the golden
// of the running test. No Python runs. A golden that is missing, edited,
// recorded for another program or input, or recorded by another producer
// fails the test; so does a program that exited non-zero when it was recorded.
func frozenPython(t *testing.T, golden string, programs ...programoracle.Program) []string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate the test source")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	return emailGoldens.Outputs(t, root, golden, programs...)
}

package billing

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

// billingGoldens is the set of this package's frozen Python answers: each
// oracle program was executed once on Build, and its answer is frozen under
// testdata/golden. The producers are the billing request models and helpers
// of that build and the distributions under them (FastAPI's request
// validation, pydantic, the Stripe SDK's signature check), so Identity names
// those distributions. A golden recorded by another producer is refused.
var billingGoldens = programoracle.Set{
	Package:       "./internal/api/billing/",
	Build:         "a4847c5e93607451a0c987b314d37e02fc43ce85",
	Identity:      "python 3.14.7\nunicodedata 16.0.0\nfastapi 0.136.3\nhttpx 0.28.1\npydantic 2.13.5\npydantic-core 2.46.5\nstarlette 1.7.0\nstripe 15.6.1",
	Distributions: []string{"fastapi", "httpx", "pydantic", "pydantic-core", "starlette", "stripe"},
	// The goldenrecord verb writes each digest when it promotes a recording; a
	// new golden starts as "PIN:" + its file name without ".json".
	Pins: map[string]string{
		"billing-bodies.golden.json":   "94ad7eaea03cec3c7f9497325aaecea62daa19f81fa99f613df3c4573c26089e",
		"billing-helpers.golden.json":  "ae573750cabe34d47e98268bc379b56854c106c72baabb509e7a548e1793c8c7",
		"stripe-signature.golden.json": "8d3bfdd91583c3374e8a6a4e45272272427a789fe6cbd581f897e10aea68daf6",
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
	return billingGoldens.Outputs(t, root, golden, programs...)
}

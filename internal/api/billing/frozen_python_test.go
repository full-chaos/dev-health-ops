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
		"billing-bodies.golden.json":   "06792c4c527015100942788b97b8b158af574c2c373385269151c661f57b83d4",
		"billing-helpers.golden.json":  "cfacd91e3e9112b20ed4e94d5fb58c9a1b0b11609831e52b00017d29d1a52ea6",
		"stripe-signature.golden.json": "f541fcbfb12cae5491e5a7ef03bbad6dba15cb44c09b874a2e115389003df38e",
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

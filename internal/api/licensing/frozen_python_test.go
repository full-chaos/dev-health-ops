package licensing

import (
	"path/filepath"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

// licensingGoldens is the set of this package's frozen Python answers: each
// oracle program was executed once on Build, and its answer is frozen under
// testdata/golden. The producers are the licensing sources of that build and
// the distributions under them (the signature library, the payload model, the
// database layer of the two stored-row oracles), so Identity names those
// distributions. A golden recorded by another producer is refused.
var licensingGoldens = programoracle.Set{
	Package:       "./internal/api/licensing/",
	Build:         "a4847c5e93607451a0c987b314d37e02fc43ce85",
	Identity:      "python 3.14.7\nunicodedata 16.0.0\naiosqlite 0.22.1\npydantic 2.13.5\npydantic-core 2.46.5\npynacl 1.6.2\nsqlalchemy 2.0.54",
	Distributions: []string{"aiosqlite", "pydantic", "pydantic-core", "pynacl", "sqlalchemy"},
	// The goldenrecord verb writes each digest when it promotes a recording; a
	// new golden starts as "PIN:" + its file name without ".json".
	Pins: map[string]string{
		"b64decode.golden.json":            "ec9a87b95fddd902cb8b6802a28eb871a7225bfde5d155ef1aa21e460cb3f6d1",
		"big-duration-license.golden.json": "53934760650929148fc7cf8513ef280ac23e7cae7cf9ac72612188cb4db93312",
		"org-feature.golden.json":          "41cac2835fc8c9c20e39c81321ebdcb8d83aeaa083f20783e0b0384a5cec37ba",
		"sign-license.golden.json":         "d10a1aa7bb67cdf71ecf95f3ecb9d2ccf677d7a9c22096211317c204f5178cbb",
		"tier-limits.golden.json":          "3042497569e646a6aaae28badc61fc6a4743f055779a9eb786f3732f646211ce",
		"tier-registry.golden.json":        "7ce637acde684f7fcd3a14324c69a01edd70dc1134c0889e38693443f8ff8c42",
	},
}

// frozenPython returns the stdout of each program, in order, from the golden
// of the running test. No Python runs. A golden that is missing, edited,
// recorded for another program or input, or recorded by another producer
// fails the test; so does a program that exited non-zero when it was recorded.
func frozenPython(t *testing.T, golden string, programs ...programoracle.Program) []string {
	t.Helper()
	_, file, _, ok := moduleroot.Caller(0)
	if !ok {
		t.Fatal("cannot locate the test source")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	return licensingGoldens.Outputs(t, root, golden, programs...)
}

// textDigest is how a golden holds a signed license: the SHA-256 of its text
// and the length of the text in bytes. A license is a signed token, and no
// golden holds a token.
type textDigest struct {
	SHA256 string `json:"sha256"`
	Length int    `json:"length"`
}

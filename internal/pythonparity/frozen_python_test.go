package pythonparity_test

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

// parityPythonBuild is the build whose interpreter answered the frozen oracles
// of this package: each program was executed there once.
const parityPythonBuild = "a4847c5e93607451a0c987b314d37e02fc43ce85"

// parityProducerIdentity is the producer those answers came from. For most
// oracles of this package the producer is the interpreter itself (str.lower,
// repr, difflib, urllib, the UTF-8 codec), not a source file of this
// repository, so the build above does not identify it: this does. A golden
// recorded by another interpreter or another Unicode data version is refused.
const parityProducerIdentity = "python 3.14.7\nunicodedata 16.0.0"

// parityGoldenPins pins the SHA-256 of every golden under testdata/golden. The
// goldenrecord verb writes each digest when it promotes a recording; a new
// golden starts as "PIN:" + its file name without ".json".
var parityGoldenPins = map[string]string{
	"casing-multi-rune.golden.json":       "85a8b45c880693343ba0de32bb04a4e78420bb192ff77d083c00a19ecfbdd2c7",
	"casing-sigma-distance.golden.json":   "9d83a8b00b9515df08d78fcf1537ea03c1aad822a6f4efdf2e7b1f24ca1a8566",
	"casing-sigma-properties.golden.json": "1b13bcc8b298812a8c87d0510f9292c11d5db19c5500b3e6469455d269ea27e5",
	"errorsanitize.golden.json":           "e9e8bf2d88d8ccd4fc3990bafd6fddb02d3f135788a3bccb474375195ec25c37",
	"fnmatch.golden.json":                 "951fe8a5fbcec332b52a939946d80df50d2ca0b31c268d9f1b0beddfd0c0a16d",
	"isoformat.golden.json":               "8d605cc0e9d71c1178fdf900dfa20317516a85026387c2866f777d71c90d5a83",
	"seqratio.golden.json":                "f10b09a91df58bc94cfd7bd27c0516aa764649bf9a34d73c86c4ffcccc63409f",
	"strrepr.golden.json":                 "7f2e7ea3c9b8998110abee0a4b30c5c352a9780d0618c43c274c04d1d10f38e6",
	"urlsplit.golden.json":                "86a8ebed0469a0d4f987816dd4b118aed74de156ecd7848eb7e37e87f0db3ecb",
	"utf8replace.golden.json":             "fd67060a2f796214171aedcbba1278fbb20014c887e467ddd24cd1e963f6e676",
}

// parityGoldens is the set of this package's frozen Python answers.
var parityGoldens = programoracle.Set{
	Package:  "./internal/pythonparity/",
	Build:    parityPythonBuild,
	Identity: parityProducerIdentity,
	Pins:     parityGoldenPins,
}

// frozenPython returns the stdout of each program, in order, from the golden
// of the running test. No Python runs: the answers were executed once on
// parityPythonBuild and are frozen in testdata/golden. A golden that is
// missing, edited, recorded for another program or input, or recorded by
// another producer than parityProducerIdentity fails the test; so does a
// program that exited non-zero when it was recorded.
//
// The caller compares by value and sends no JSON number through float64.
func frozenPython(t *testing.T, golden string, programs ...programoracle.Program) []string {
	t.Helper()
	return parityGoldens.Outputs(t, frozenRepositoryRoot(t), golden, programs...)
}

// frozenRepositoryRoot is the repository root, from this file's own location.
func frozenRepositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate the test source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

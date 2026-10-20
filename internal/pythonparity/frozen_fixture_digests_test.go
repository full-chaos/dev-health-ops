package pythonparity_test

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

// frozenFixture is a fixture that holds Python's answers for a golden test of
// this package, and the generator script that wrote it when the script is still in the tree (the
// tests/fixtures generators were deleted with the Python tree: those goldens are frozen).
type frozenFixture struct {
	fixture, fixtureSHA256     string
	generator, generatorSHA256 string
}

// frozenFixtures pins the eight fixtures the rot guards of this package
// protect. A rot guard runs its generator on live Python and compares the
// output with the fixture, so it needs an interpreter and goes away with it.
// These digests need none: a fixture or a generator that changes without its
// pin fails here, with or without Python. A fixture is written by its
// generator, never edited; when it is regenerated on purpose, re-read the
// port against it and pin the new digests.
var frozenFixtures = []frozenFixture{
	{
		fixture:       "tests/fixtures/clickhouse_string_decode_python_golden.json",
		fixtureSHA256: "b2318e73c5f6944fcc62509caa4fdd6df7841a9e2863ead2ca5c719af18a3bdb",
	},
	{
		fixture:       "tests/fixtures/evidence_json_edge_shapes_python_golden.json",
		fixtureSHA256: "3cc25d26b5fa7b72a030b591bc968c66875fdd52d2d59cd6f14727193cd2a005",
	},
	{
		fixture:         "internal/pythonparity/testdata/float_text_golden.json",
		fixtureSHA256:   "ab525f0193314e087252f94db9893deb3b817b3a3e5dff87fe57f2002439308f",
		generator:       "internal/pythonparity/testdata/generate_float_text_golden.py",
		generatorSHA256: "d2f76eb64bac0c3b86f2e87f5334106699fcf7c72e5ff8c1e333e32323cd13ba",
	},
	{
		fixture:       "tests/fixtures/python_json_python_golden.json",
		fixtureSHA256: "fec52cbb12a42fca582e0c8740859faf2745a410ecf7a0832ab4081d984c09f6",
	},
	{
		fixture:       "tests/fixtures/python_json_insertion_order_python_golden.json",
		fixtureSHA256: "123979d3908b6e700a0d9bbab536e715f1dcd831f6a8923f43eca687710d6f65",
	},
	{
		fixture:       "tests/fixtures/evidence_json_repr_band_python_golden.json",
		fixtureSHA256: "f638228f74815574fea389dfa0eb5f65df6f171b80ca246ef902509e393e8510",
	},
	{
		fixture:       "tests/fixtures/python_sum_python_golden.json",
		fixtureSHA256: "487527f4236e2b8b5d09526879d53bdfabcc8ae2ba156d2edfc0306bc3fdf499",
	},
	{
		fixture:       "tests/fixtures/python_whitespace_python_golden.json",
		fixtureSHA256: "9e697cf389945c93e446bbd255043ba74c35ba6c7ed330f513ac882d795d565b",
	},
}

// TestFrozenFixturesHoldTheirPinnedBytes fails when one of the frozen fixtures
// or its generator is missing or holds other bytes than the pinned ones.
func TestFrozenFixturesHoldTheirPinnedBytes(t *testing.T) {
	root := frozenRepositoryRoot(t)
	if len(frozenFixtures) == 0 {
		t.Fatal("no frozen fixture is pinned: the test would measure nothing")
	}
	for _, pinned := range frozenFixtures {
		files := []struct{ path, want, kind string }{{pinned.fixture, pinned.fixtureSHA256, "fixture"}}
		if pinned.generator != "" {
			files = append(files, struct{ path, want, kind string }{pinned.generator, pinned.generatorSHA256, "generator"})
		}
		for _, file := range files {
			raw, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(file.path)))
			if err != nil {
				t.Errorf("frozen %s %s: %v", file.kind, file.path, err)
				continue
			}
			sum := sha256.Sum256(raw)
			if got := hex.EncodeToString(sum[:]); got != file.want {
				t.Errorf("frozen %s %s has sha256 %s, the test pins %s: a fixture is written by its generator, never edited; "+
					"if it was regenerated on purpose, re-read the port against it and pin the new digest", file.kind, file.path, got, file.want)
			}
		}
	}
}

package checkgo_test

import (
	"path/filepath"
	"testing"
)

// goldenscan/treegate is one CPU-bound regexp scan of the whole tree. Run beside the heavy packages that a heaviest-first leg
// starts together, it took more than the 10 minute go test timeout; alone it takes a few minutes. It must run alone in its leg.
func TestTreegateRunsAloneInExactlyOneRaceLeg(t *testing.T) {
	root := repoRoot(t)
	solo := filepath.Join(root, "ci", "go_race_solo.tsv")
	weights := mergedWeights(t, root)
	mod, pkgs := raceModulePackages(t, root)
	target := mod + "/internal/testsupport/goldenscan/treegate"
	if !contains(pkgs, target) {
		t.Fatalf("%s is not a package of the module", target)
	}
	for count := 2; count <= 4; count++ {
		var shared, alone []string
		for k := 1; k <= count; k++ {
			shared = append(shared, shardAwk(t, root, mod, pkgs, k, count, weights, solo, "")...)
			alone = append(alone, shardAwk(t, root, mod, pkgs, k, count, weights, solo, "solo")...)
		}
		if contains(shared, target) {
			t.Fatalf("count %d: %s is in a shared split and starts beside the heavy packages of its leg: list it in ci/go_race_solo.tsv", count, target)
		}
		n := 0
		for _, p := range alone {
			if p == target {
				n++
			}
		}
		if n != 1 {
			t.Fatalf("count %d: %s appears %d times in the solo parts %v, want exactly once", count, target, n, alone)
		}
	}
}

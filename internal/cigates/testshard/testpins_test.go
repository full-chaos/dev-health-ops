package testshard_test

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// CHAOS-13422 pinned the heaviest test packages (ci/go_test_pins.tsv), each on its own shard, before the longest-processing-time
// split. CHAOS-13520 splits those packages by test name (ci/go_test_splits.tsv) instead, so their parts spread over the legs and
// a pin is left only for a whole package. A pin must name a real package of the module, and the pinned packages land on
// distinct shards while there are no more pins than shards.

func pinRows(t *testing.T) []string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(repoRoot(t), "ci", "go_test_pins.tsv"))
	if err != nil {
		t.Fatalf("ci/go_test_pins.tsv: %v", err)
	}
	var keys []string
	for _, l := range strings.Split(string(b), "\n") {
		if strings.TrimSpace(l) == "" || strings.HasPrefix(l, "#") {
			continue
		}
		k, reason, ok := strings.Cut(l, "\t")
		if !ok || strings.TrimSpace(reason) == "" {
			t.Fatalf("pin row is not <package><TAB><reason>: %q", l)
		}
		keys = append(keys, k)
	}
	return keys
}

func TestThePinnedHeavyPackagesLandOnDistinctShards(t *testing.T) {
	t.Parallel()
	mod, pkgs := goList(t)
	known := map[string]bool{}
	for _, p := range pkgs {
		known[keyOf(mod, p)] = true
	}
	pins := pinRows(t)
	for _, k := range pins {
		if !known[k] {
			t.Errorf("stale pin: %s is not a package of the module", k)
		}
	}
	if len(pins) == 0 || len(pins) > shardCount {
		return
	}
	shardOf := map[string]int{}
	for k := 1; k <= shardCount; k++ {
		for _, p := range shardPlan(t, k, shardCount) {
			shardOf[keyOf(mod, p)] = k
		}
	}
	used := map[int]string{}
	for _, k := range pins {
		s := shardOf[k]
		if s == 0 {
			t.Errorf("pinned package %s is in no shard", k)
			continue
		}
		if other, dup := used[s]; dup {
			t.Errorf("pinned packages %s and %s share test shard %d/%d", other, k, s, shardCount)
		}
		used[s] = k
	}
}

func testWeights(t *testing.T) map[string]int {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(repoRoot(t), "ci", "go_test_weights.tsv"))
	if err != nil {
		t.Fatalf("ci/go_test_weights.tsv: %v", err)
	}
	w := map[string]int{}
	for _, l := range strings.Split(string(b), "\n") {
		if strings.TrimSpace(l) == "" || strings.HasPrefix(l, "#") {
			continue
		}
		k, v, ok := strings.Cut(l, "\t")
		if !ok {
			continue
		}
		n, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil {
			t.Fatalf("weight row %q: %v", l, err)
		}
		w[k] = n
	}
	return w
}

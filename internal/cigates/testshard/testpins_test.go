package testshard_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// CHAOS-13422: the five heaviest test packages (each runs 330-720 s alone) must not share a shard, because two of them side by
// side make a shard's wall exceed the sum of their weights' share. ci/go_test_pins.tsv lists them; ci/go_race_shard.awk
// (-v pin=) places the pinned packages first, each on the then-lightest shard, before the longest-processing-time split.

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
	if len(pins) != shardCount {
		t.Fatalf("ci/go_test_pins.tsv holds %d rows, want one per test shard (%d)", len(pins), shardCount)
	}
	for _, k := range pins {
		if !known[k] {
			t.Errorf("stale pin: %s is not a package of the module", k)
		}
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

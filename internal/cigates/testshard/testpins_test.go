package testshard_test

import (
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// CHAOS-13422: the five heaviest test packages (median 517-792 s in the test leg) must not share a shard, because two of them side by
// side make a shard's wall exceed the sum of their weights' share. ci/go_test_pins.tsv lists them; ci/go_race_shard.awk
// (-v pin=) places the pinned packages first, each on the then-lightest shard, before the longest-processing-time split.
// An apart package is pinned after them because its measured time beside one named package is far above its median.
// The serial package (check_go.sh SERIAL_HEAVY_TEST_PACKAGE) is pinned too: it runs alone after the shard's other packages, so
// its whole wall adds to the shard; it goes last, onto the then-lightest shard.

const serialPinKey = "internal/gqlgenguard"

// apartPins: pinned package -> the package it must not share a test shard with. internal/cigates/chartgates has a 392 s median on
// main and ran 674 s and 925 s on PR #5995's shard beside internal/cigates/integrationshards.
var apartPins = map[string]string{
	"internal/cigates/chartgates": "internal/cigates/integrationshards",
}

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
	var pins []string
	serialPinned := false
	apartPinned := map[string]bool{}
	for _, k := range pinRows(t) {
		if k == serialPinKey {
			serialPinned = true
			continue
		}
		if _, ok := apartPins[k]; ok {
			apartPinned[k] = true
			continue
		}
		pins = append(pins, k)
	}
	for k := range apartPins {
		if !apartPinned[k] {
			t.Errorf("ci/go_test_pins.tsv does not pin the apart package %s", k)
		}
	}
	if !serialPinned {
		t.Errorf("ci/go_test_pins.tsv does not pin the serial package %s", serialPinKey)
	}
	if len(pins) != shardCount {
		t.Fatalf("ci/go_test_pins.tsv holds %d heavy rows, want one per test shard (%d)", len(pins), shardCount)
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
	w := testWeights(t)
	// The heavy pins are the shardCount heaviest packages of ci/go_test_weights.tsv (the serial package aside), so the
	// pin list follows the measured medians when they are refreshed.
	var ranked []string
	for k := range w {
		if k != serialPinKey {
			ranked = append(ranked, k)
		}
	}
	sort.Slice(ranked, func(i, j int) bool {
		if w[ranked[i]] != w[ranked[j]] {
			return w[ranked[i]] > w[ranked[j]]
		}
		return ranked[i] < ranked[j]
	})
	top := map[string]bool{}
	for _, k := range ranked[:shardCount] {
		top[k] = true
	}
	for _, k := range pins {
		if !top[k] {
			t.Errorf("pinned package %s (weight %d) is not one of the %d heaviest in ci/go_test_weights.tsv: %v", k, w[k], shardCount, ranked[:shardCount])
		}
	}
	for k, other := range apartPins {
		if apartPinned[k] && shardOf[k] == shardOf[other] {
			t.Errorf("apart package %s shares test shard %d/%d with %s", k, shardOf[k], shardCount, other)
		}
	}
	pinLoad := map[int]int{}
	for _, k := range pins {
		pinLoad[shardOf[k]] += w[k]
	}
	for k := range apartPinned {
		pinLoad[shardOf[k]] += w[k]
	}
	lightest := 1
	for s := 2; s <= shardCount; s++ {
		if pinLoad[s] < pinLoad[lightest] {
			lightest = s
		}
	}
	if serialPinned && shardOf[serialPinKey] != lightest {
		t.Errorf("serial package %s is on shard %d, want shard %d, the lightest after the other pins (pin loads %v)",
			serialPinKey, shardOf[serialPinKey], lightest, pinLoad)
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

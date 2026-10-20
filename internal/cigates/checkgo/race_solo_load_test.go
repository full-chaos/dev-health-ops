package checkgo_test

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// ci/go_race_shard.awk places each solo package first, so its weight pre-loads the shard it runs in and the other packages
// fill the lighter shards. The solo package still runs last in its leg; only the accounting changes.

const soloLoadMod = "example.test/m"

var soloLoadWeights = map[string]int{"s": 150, "a": 100, "b": 100, "c": 100, "d": 100, "e": 100, "f": 100}

func soloLoadFixture(t *testing.T) (root, weights, solo string, pkgs []string) {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	var w strings.Builder
	for _, k := range []string{"s", "a", "b", "c", "d", "e", "f"} {
		fmt.Fprintf(&w, "%s\t%d\n", k, soloLoadWeights[k])
		pkgs = append(pkgs, soloLoadMod+"/"+k)
	}
	weights = filepath.Join(dir, "weights.tsv")
	solo = filepath.Join(dir, "solo.tsv")
	if err := os.WriteFile(weights, []byte(w.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(solo, []byte("s\tstand-in\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return root, weights, solo, pkgs
}

func soloLoadSpread(loads []int, heaviestShared int) (int, bool) {
	lo, hi := loads[0], loads[0]
	for _, l := range loads {
		lo, hi = min(lo, l), max(hi, l)
	}
	return hi - lo, hi-lo <= heaviestShared
}

func TestSoloWeightCountsInItsShardLoad(t *testing.T) {
	root, weights, solo, pkgs := soloLoadFixture(t)
	const count = 3
	loads := make([]int, count)
	var seen []string
	soloShard := 0
	for k := 1; k <= count; k++ {
		shared := shardAwk(t, root, soloLoadMod, pkgs, k, count, weights, solo, "")
		alone := shardAwk(t, root, soloLoadMod, pkgs, k, count, weights, solo, "solo")
		for _, p := range append(slices.Clone(shared), alone...) {
			loads[k-1] += soloLoadWeights[strings.TrimPrefix(p, soloLoadMod+"/")]
			seen = append(seen, p)
		}
		if len(alone) > 0 {
			if !slices.Equal(alone, []string{soloLoadMod + "/s"}) {
				t.Fatalf("shard %d solo part %v, want only the solo package", k, alone)
			}
			soloShard = k
		}
	}
	slices.Sort(seen)
	want := slices.Clone(pkgs)
	slices.Sort(want)
	if !slices.Equal(seen, want) {
		t.Fatalf("shared and solo parts of all shards are not an exactly-once partition: %v want %v", seen, want)
	}
	if soloShard != 1 {
		t.Fatalf("the solo package ran in shard %d, want shard 1 (lightest, lowest index, when every shard is empty)", soloShard)
	}
	if spread, ok := soloLoadSpread(loads, 100); !ok {
		t.Fatalf("shard loads %v (solo weight included) differ by %d, more than the heaviest shared package (100): the solo weight is not counted in the split", loads, spread)
	}
}

// Planted: the awk with the passes swapped (split the others first, hand the solo to the lightest shard last) must break the
// spread rule on the same fixture, so the guard cannot pass when the solo load is ignored.
func TestSoloLoadIgnoredTripsTheSpreadRule(t *testing.T) {
	root, weights, solo, pkgs := soloLoadFixture(t)
	src, err := os.ReadFile(filepath.Join(root, "ci", "go_race_shard.awk"))
	if err != nil {
		t.Fatal(err)
	}
	const placedFirst = "if (soloPkg[k] != (pass == 1)) continue"
	if !strings.Contains(string(src), placedFirst) {
		t.Fatalf("ci/go_race_shard.awk no longer holds %q: update this planted copy", placedFirst)
	}
	planted := filepath.Join(t.TempDir(), "solo_last.awk")
	if err := os.WriteFile(planted, []byte(strings.Replace(string(src), placedFirst, "if (soloPkg[k] != (pass == 2)) continue", 1)), 0o644); err != nil {
		t.Fatal(err)
	}
	const count = 3
	loads := make([]int, count)
	for k := 1; k <= count; k++ {
		for _, part := range []string{"", "solo"} {
			args := []string{"-v", "mod=" + soloLoadMod, "-v", fmt.Sprint("shard=", k), "-v", fmt.Sprint("count=", count), "-v", "weights=" + weights, "-v", "solo=" + solo, "-v", "part=" + part, "-f", planted}
			out, errs, err := soloExec(t, root, nil, strings.Join(pkgs, "\n")+"\n", "awk", args...)
			if err != nil {
				t.Fatalf("awk: %v\n%s", err, errs)
			}
			for _, p := range strings.Fields(out) {
				loads[k-1] += soloLoadWeights[strings.TrimPrefix(p, soloLoadMod+"/")]
			}
		}
	}
	if spread, ok := soloLoadSpread(loads, 100); ok {
		t.Fatalf("loads %v with the solo placed last pass the spread rule (spread %d): the guard cannot detect an ignored solo load", loads, spread)
	}
}

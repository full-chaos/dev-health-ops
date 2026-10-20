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

// go test starts packages in the order it is given, so a shard lists its packages heaviest first.
var orderWeights = map[string]int{"a": 5, "b": 90, "c": 20, "d": 90, "e": 1, "f": 60}

func orderFixture(t *testing.T) (root, weights string, pkgs []string) {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	var w strings.Builder
	for _, k := range []string{"a", "b", "c", "d", "e", "f"} {
		fmt.Fprintf(&w, "%s\t%d\n", k, orderWeights[k])
		pkgs = append(pkgs, soloLoadMod+"/"+k)
	}
	weights = filepath.Join(t.TempDir(), "weights.tsv")
	if err := os.WriteFile(weights, []byte(w.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	return root, weights, pkgs
}

func orderHeavyFirst(t *testing.T, awkFile, weights string, pkgs []string, count int) (ok bool, detail string) {
	t.Helper()
	root, _ := filepath.Abs(filepath.Join("..", "..", ".."))
	var all []string
	for k := 1; k <= count; k++ {
		args := []string{"-v", "mod=" + soloLoadMod, "-v", fmt.Sprint("shard=", k), "-v", fmt.Sprint("count=", count), "-v", "weights=" + weights, "-f", awkFile}
		out, errs, err := soloExec(t, root, nil, strings.Join(pkgs, "\n")+"\n", "awk", args...)
		if err != nil {
			t.Fatalf("awk: %v\n%s", err, errs)
		}
		got := strings.Fields(out)
		all = append(all, got...)
		prev := 1 << 30
		for _, p := range got {
			w := orderWeights[strings.TrimPrefix(p, soloLoadMod+"/")]
			if w > prev {
				return false, fmt.Sprintf("shard %d/%d lists %v: a package of weight %d follows a lighter one", k, count, got, w)
			}
			prev = w
		}
	}
	slices.Sort(all)
	want := slices.Clone(pkgs)
	slices.Sort(want)
	if !slices.Equal(all, want) {
		return false, fmt.Sprintf("the shards %v are not an exactly-once partition of %v", all, want)
	}
	return true, ""
}

func TestRaceShardListsHeaviestFirst(t *testing.T) {
	root, weights, pkgs := orderFixture(t)
	for count := 1; count <= 4; count++ {
		if ok, detail := orderHeavyFirst(t, filepath.Join(root, "ci", "go_race_shard.awk"), weights, pkgs, count); !ok {
			t.Fatalf("count %d: %s", count, detail)
		}
	}
}

// Planted: the awk that prints in input order must fail the heaviest-first rule on the same fixture.
func TestRaceShardInputOrderTripsHeaviestFirst(t *testing.T) {
	root, weights, pkgs := orderFixture(t)
	src, err := os.ReadFile(filepath.Join(root, "ci", "go_race_shard.awk"))
	if err != nil {
		t.Fatal(err)
	}
	const sorted = "    k = order[i]\n    if ((k in keep)"
	if !strings.Contains(string(src), sorted) {
		t.Fatalf("ci/go_race_shard.awk no longer holds the print loop %q: update this planted copy", sorted)
	}
	planted := filepath.Join(t.TempDir(), "input_order.awk")
	if err := os.WriteFile(planted, []byte(strings.Replace(string(src), sorted, "    k = i\n    if ((k in keep)", 1)), 0o644); err != nil {
		t.Fatal(err)
	}
	if ok, _ := orderHeavyFirst(t, planted, weights, pkgs, 1); ok {
		t.Fatal("the awk that prints in input order passes the heaviest-first rule: the guard cannot detect it")
	}
}

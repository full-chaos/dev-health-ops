package testshard_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// The go-quality legs on ubuntu-26.04 (CAL-2604): 4 test shards and 6 race legs. The pins hold the COUNTS and the row
// partition of those counts; they assert no seconds (D6418).

const wantTestShards, wantRaceLegs = 4, 6

func legCounts(t *testing.T) (tests, races int, names []string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(repoRoot(t), "ci", "go_quality_legs.json"))
	if err != nil {
		t.Fatal(err)
	}
	var legs []struct{ Name, Args string }
	if err := json.Unmarshal(data, &legs); err != nil {
		t.Fatalf("ci/go_quality_legs.json: %v", err)
	}
	for _, l := range legs {
		names = append(names, l.Name)
		var k, n int
		if c, _ := fmt.Sscanf(l.Args, "test %d %d", &k, &n); c == 2 {
			tests++
			if n != wantTestShards || l.Name != fmt.Sprintf("test-%d-of-%d", k, n) {
				t.Errorf("leg %q args %q: want test K %d named test-K-of-%d", l.Name, l.Args, wantTestShards, wantTestShards)
			}
		}
		if c, _ := fmt.Sscanf(l.Args, "race %d %d", &k, &n); c == 2 {
			races++
			if n != wantRaceLegs || l.Name != fmt.Sprintf("race-%d-of-%d", k, n) {
				t.Errorf("leg %q args %q: want race K %d named race-K-of-%d", l.Name, l.Args, wantRaceLegs, wantRaceLegs)
			}
		}
	}
	return tests, races, names
}

func TestTheLegListHasFourTestShardsAndSixRaceLegs(t *testing.T) {
	tests, races, names := legCounts(t)
	if tests != wantTestShards || races != wantRaceLegs {
		t.Errorf("legs %v: %d test shard(s) and %d race leg(s), want %d and %d", names, tests, races, wantTestShards, wantRaceLegs)
	}
}

// planKeys runs a shard plan verb and returns its "module<TAB>package<TAB>key" lines.
func planKeys(t *testing.T, verb string, shard, count int) []string {
	t.Helper()
	out, errs, err := checkGo(t, nil, verb, fmt.Sprint(shard), fmt.Sprint(count))
	if err != nil {
		t.Fatalf("%s %d %d: %v\n%s", verb, shard, count, err, errs)
	}
	var rows []string
	for _, l := range strings.Split(out, "\n") {
		if strings.Count(l, "\t") == 2 {
			rows = append(rows, l)
		}
	}
	return rows
}

// partitionDefects: the shards of one verb at one count hold every row of the whole plan once and none twice.
func partitionDefects(t *testing.T, verb string, count int) []string {
	t.Helper()
	whole := planKeys(t, verb, 1, 1)
	// internal/providersync is in EVERY race leg by design (one test-name slice each): it is not a partitioned row.
	onEveryLeg := func(r string) bool {
		return verb == "race-shard-plan" && strings.Contains(r, "/internal/providersync\t")
	}
	if len(whole) < 100 {
		t.Fatalf("%s 1 1: only %d rows", verb, len(whole))
	}
	seen := map[string]int{}
	for k := 1; k <= count; k++ {
		rows := planKeys(t, verb, k, count)
		if len(rows) == 0 {
			return []string{fmt.Sprintf("%s %d %d is empty", verb, k, count)}
		}
		for _, r := range rows {
			if !onEveryLeg(r) {
				seen[r]++
			}
		}
	}
	var bad []string
	for _, r := range whole {
		if onEveryLeg(r) {
			continue
		}
		if seen[r] != 1 {
			bad = append(bad, fmt.Sprintf("%s: row %q in %d of the %d shards, want 1", verb, r, seen[r], count))
		}
		delete(seen, r)
	}
	for r := range seen {
		bad = append(bad, fmt.Sprintf("%s: row %q is in a shard but not in the whole plan", verb, r))
	}
	sort.Strings(bad)
	return bad
}

func TestTheTestShardsAtTheLegCountHoldEveryRowOnce(t *testing.T) {
	t.Parallel()
	tests, _, _ := legCounts(t)
	for _, d := range partitionDefects(t, "test-shard-plan", tests) {
		t.Error(d)
	}
}

// Every race package is in exactly one race leg, except internal/providersync, whose tests are split by name so that each providersync
// test runs in exactly one leg.
func TestTheRaceLegsAtTheLegCountHoldEveryPackageOnceExceptProvidersync(t *testing.T) {
	t.Parallel()
	_, races, _ := legCounts(t)
	for _, d := range partitionDefects(t, "race-shard-plan", races) {
		t.Error(d)
	}
}

// Each providersync race test name is in exactly one of the race legs, and no leg's slice is empty, at the shipped leg count.
func TestEveryProvidersyncRaceTestIsInExactlyOneRaceLegAtTheLegCount(t *testing.T) {
	t.Parallel()
	_, races, _ := legCounts(t)
	weights, awk := providersyncPaths(t)
	names := providersyncNames(t)
	seen := map[string]int{}
	for k := 1; k <= races; k++ {
		part, err := providersyncShard(t, awk, weights, names, k, races)
		if err != nil {
			t.Fatalf("providersync slice %d/%d: %v", k, races, err)
		}
		if len(part) == 0 {
			t.Errorf("providersync race slice %d/%d is empty", k, races)
		}
		for _, n := range part {
			seen[n]++
		}
	}
	for _, n := range names {
		if seen[n] != 1 {
			t.Errorf("providersync test %s is in %d of the %d race legs, want 1", n, seen[n], races)
		}
	}
}

// The prose that counts the legs (the suite catalog trigger cell, the go-quality.yml pool comment) must say what
// ci/go_quality_legs.json holds, so a change of the leg list cannot leave the docs behind.

func poolLegNames(t *testing.T) []string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(repoRoot(t), "ci", "go_quality_legs.json"))
	if err != nil {
		t.Fatal(err)
	}
	var legs []struct {
		Name string
		Pool bool
	}
	if err := json.Unmarshal(data, &legs); err != nil {
		t.Fatalf("ci/go_quality_legs.json: %v", err)
	}
	var pool []string
	for _, l := range legs {
		if l.Pool {
			pool = append(pool, l.Name)
		}
	}
	return pool
}

func TestTheSuiteCatalogLegRowStatesTheLegCounts(t *testing.T) {
	tests, races, _ := legCounts(t)
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), "ci", "suites.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.HasPrefix(line, "go-quality-ci-leg\t") {
			continue
		}
		want := fmt.Sprintf("static, test x%d, race x%d legs", tests, races)
		if !strings.Contains(line, want) {
			t.Errorf("ci/suites.tsv go-quality-ci-leg trigger cell must say %q (the leg list has %d test and %d race legs)", want, tests, races)
		}
		return
	}
	t.Fatal("ci/suites.tsv has no go-quality-ci-leg row")
}

func TestTheWorkflowPoolCommentNamesEveryPoolLeg(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), ".github", "workflows", "go-quality.yml"))
	if err != nil {
		t.Fatal(err)
	}
	pool := poolLegNames(t)
	if len(pool) == 0 {
		t.Fatal("ci/go_quality_legs.json flags no pool leg")
	}
	var short []string
	for _, n := range pool {
		short = append(short, strings.TrimSuffix(n, fmt.Sprintf("-of-%d", wantRaceLegs)))
	}
	want := fmt.Sprintf("the race legs flagged `pool` (%s)", strings.Join(short, ", "))
	if !strings.Contains(string(raw), want) {
		t.Errorf(".github/workflows/go-quality.yml pool comment must say %q (the legs flagged pool in ci/go_quality_legs.json)", want)
	}
}

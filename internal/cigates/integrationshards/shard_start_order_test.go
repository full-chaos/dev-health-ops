//go:build !race

package integrationshards_test

import (
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

const fixturescliPackage = "internal/fixturescli"

func packagesShardsOfThePlan(t *testing.T) map[int][]string {
	t.Helper()
	stdout, _ := realPlan(t)
	byShard := map[int][]string{}
	for _, l := range strings.Split(stdout, "\n") {
		m := shardRow.FindStringSubmatch(l)
		if m == nil || splitPackages[m[2]] {
			continue
		}
		s, err := strconv.Atoi(m[1])
		if err != nil {
			t.Fatal(err)
		}
		byShard[s] = append(byShard[s], m[2])
	}
	return byShard
}

func TestTheLongestPackageHoldsAShardOfItsOwn(t *testing.T) {
	t.Parallel()
	byShard := packagesShardsOfThePlan(t)
	var holders []int
	for s, pkgs := range byShard {
		if containsStr(pkgs, fixturescliPackage) {
			holders = append(holders, s)
		}
	}
	if len(holders) != 1 || len(byShard[holders[0]]) != 1 {
		t.Fatalf("%s runs 1,150 s next to three other packages unless it holds a shard row of its own: %v", fixturescliPackage, byShard)
	}
}

func TestAPackagesShardStartsItsLongestPackageFirstAndTheOrderIsStable(t *testing.T) {
	t.Parallel()
	_, weights := manifestRows(t)
	checked := 0
	for shard, pkgs := range packagesShardsOfThePlan(t) {
		if len(pkgs) < 2 {
			continue
		}
		want := append([]string(nil), pkgs...)
		sort.SliceStable(want, func(i, j int) bool {
			if weights[want[i]] != weights[want[j]] {
				return weights[want[i]] > weights[want[j]]
			}
			return want[i] < want[j]
		})
		first := runCheckGoSharding(t, shardEnv{}, "integration-shard", "packages", strconv.Itoa(shard), "--dry-run")
		second := runCheckGoSharding(t, shardEnv{}, "integration-shard", "packages", strconv.Itoa(shard), "--dry-run")
		if first.code != 0 || second.code != 0 {
			t.Fatalf("shard %d dry run: exit %d / %d: %s%s", shard, first.code, second.code, first.stderr, second.stderr)
		}
		if first.stdout != second.stdout {
			t.Errorf("shard %d: two dry runs of the same input print different output", shard)
		}
		got := lineValues(first.stdout, "  SHARD-RUN ")
		if strings.Join(got, "\n") != strings.Join(want, "\n") {
			t.Errorf("shard %d starts %v, want weight descending then name ascending: %v", shard, got, want)
		}
		checked++
	}
	if checked < 2 {
		t.Fatalf("only %d packages shard held two or more packages: the order check measured nothing", checked)
	}
}

func TestTheSortDoesNotChangeTheLocaleOfTheGoTestChild(t *testing.T) {
	t.Parallel()
	_, weights := manifestRows(t)
	realGo, err := exec.LookPath("go")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	record := filepath.Join(dir, "child-locale")
	stub := "#!/usr/bin/env bash\nif [ \"${1:-}\" = test ]; then\n  printf '%s\\n' \"${LC_ALL-unset}\" >> " + record + "\n  printf '%s\\n' \"$*\" >> " + record + ".args\n  exit 0\nfi\nexec " + realGo + " \"$@\"\n"
	if err := os.WriteFile(filepath.Join(dir, "go"), []byte(stub), 0o755); err != nil {
		t.Fatal(err)
	}
	shard := 0
	var pkgs []string
	for s, p := range packagesShardsOfThePlan(t) {
		if len(p) >= 2 && (shard == 0 || s < shard) {
			shard, pkgs = s, p
		}
	}
	if shard == 0 {
		t.Fatal("no packages shard held two packages")
	}
	const callerLocale = "en_US.utf8"
	r := runCheckGoSharding(t, shardEnv{extra: []string{"LC_ALL=" + callerLocale, "PATH=" + dir + string(os.PathListSeparator) + os.Getenv("PATH")}}, "integration-shard", "packages", strconv.Itoa(shard))
	if r.code != 0 {
		t.Fatalf("shard %d with a stub go: exit %d: %s%s", shard, r.code, r.stdout, r.stderr)
	}
	got := fileLines(dir, "child-locale")
	if len(got) != 1 || got[0] != callerLocale {
		t.Errorf("the go test child saw LC_ALL %v, want the caller's %q", got, callerLocale)
	}
	want := append([]string(nil), pkgs...)
	sort.SliceStable(want, func(i, j int) bool {
		if weights[want[i]] != weights[want[j]] {
			return weights[want[i]] > weights[want[j]]
		}
		return want[i] < want[j]
	})
	if order := lineValues(r.stdout, "  SHARD-RUN "); strings.Join(order, "\n") != strings.Join(want, "\n") {
		t.Errorf("under the caller locale %q the shard starts %v, want byte order %v", callerLocale, order, want)
	}
}

func TestTheGoTestGroupsOfAShardKeepTheLongestFirstOrder(t *testing.T) {
	t.Parallel()
	_, weights := manifestRows(t)
	byWeight := func(a, b string) bool {
		if weights[a] != weights[b] {
			return weights[a] > weights[b]
		}
		return a < b
	}
	checked := 0
	for shard, pkgs := range packagesShardsOfThePlan(t) {
		if len(pkgs) < 2 {
			continue
		}
		r := runCheckGoSharding(t, shardEnv{}, "integration-shard", "packages", strconv.Itoa(shard), "--dry-run")
		if r.code != 0 {
			t.Fatalf("shard %d dry run: exit %d: %s%s", shard, r.code, r.stdout, r.stderr)
		}
		longest := append([]string(nil), pkgs...)
		sort.Slice(longest, func(i, j int) bool { return byWeight(longest[i], longest[j]) })
		groups := lineValues(r.stdout, "  SHARD-GROUP . ")
		if len(groups) == 0 {
			t.Fatalf("shard %d prints no SHARD-GROUP line", shard)
		}
		for i, g := range groups {
			keys := strings.Fields(g)[1:]
			if !sort.SliceIsSorted(keys, func(a, b int) bool { return byWeight(keys[a], keys[b]) }) {
				t.Errorf("shard %d go test group %d runs %v, want weight descending then name ascending", shard, i+1, keys)
			}
		}
		if first := strings.Fields(groups[0])[1]; first != longest[0] {
			t.Errorf("shard %d: go test group 1 starts %s, want the longest package %s", shard, first, longest[0])
		}
		checked++
	}
	if checked < 2 {
		t.Fatalf("only %d packages shard held two or more packages: the group order check measured nothing", checked)
	}
}

func TestTheShardPathStartsLongestFirstAndTheUnshardedVerbStillSorts(t *testing.T) {
	t.Parallel()
	_, weights := manifestRows(t)
	root, err := filepath.EvalSymlinks(rootOf(t))
	if err != nil {
		t.Fatal(err)
	}
	fakeGo := recordingGo(t)
	run := func(args ...string) []goTestCall {
		record := t.TempDir()
		r := runCheckGoSharding(t, shardEnv{extra: []string{
			"PATH=" + fakeGo + string(os.PathListSeparator) + os.Getenv("PATH"), "GOTEST_RECORD_DIR=" + record,
		}}, args...)
		if r.code != 0 {
			t.Fatalf("%v: exit %d: %s%s", args, r.code, r.stdout, r.stderr)
		}
		return recordedCalls(t, record, root)
	}
	keyOf := func(c goTestCall, p string) string { return path.Join(c.dir, strings.TrimPrefix(p, "./")) }

	shard, shardPkgs := 0, []string(nil)
	for s, p := range packagesShardsOfThePlan(t) {
		if len(p) >= 2 && (shard == 0 || s < shard) {
			shard, shardPkgs = s, p
		}
	}
	if shard == 0 {
		t.Fatal("no packages shard held two packages")
	}
	longest := shardPkgs[0]
	for _, p := range shardPkgs {
		if weights[p] > weights[longest] || (weights[p] == weights[longest] && p < longest) {
			longest = p
		}
	}
	shardCalls := run("integration-shard", "packages", strconv.Itoa(shard))
	started, headSeen := 0, false
	for _, c := range shardCalls {
		for i, p := range c.pkgs {
			k := keyOf(c, p)
			started++
			if i > 0 {
				prev := keyOf(c, c.pkgs[i-1])
				if weights[prev] < weights[k] || (weights[prev] == weights[k] && prev > k) {
					t.Errorf("shard %d go test starts %s before %s: not weight descending, then name ascending", shard, prev, k)
				}
			}
			headSeen = headSeen || (i == 0 && k == longest)
		}
	}
	if started != len(shardPkgs) || !headSeen {
		t.Errorf("shard %d: the go test calls start %d package(s), first of some call is the longest %s: %v; want %d", shard, started, longest, headSeen, len(shardPkgs))
	}

	whole := run("integration")
	sortedCalls := 0
	for _, c := range whole {
		keys := make([]string, 0, len(c.pkgs))
		for _, p := range c.pkgs {
			keys = append(keys, keyOf(c, p))
		}
		if len(keys) > 1 {
			sortedCalls++
		}
		if !sort.StringsAreSorted(keys) {
			t.Errorf("the unsharded integration verb starts %v: want byte order", keys)
		}
	}
	if sortedCalls == 0 {
		t.Fatal("the unsharded integration verb made no multi-package go test call: the order check measured nothing")
	}
}

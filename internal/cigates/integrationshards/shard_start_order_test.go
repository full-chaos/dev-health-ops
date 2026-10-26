//go:build !race

package integrationshards_test

import (
	"os"
	"os/exec"
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

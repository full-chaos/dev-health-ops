package testshard_test

import (
	"sort"
	"strings"
	"testing"
)

// CHAOS-13520: a package of ci/go_test_splits.tsv runs as PARTS parts of its test names across the go-quality test legs. Every
// name `go test -list` of the current tree reports for it (Test*, Example*, Fuzz*: what -run can select) must run in exactly one
// leg (`test-shard-split-list`, the names the leg's own runner selects), no other name may run, and the package may sit in at most
// PARTS legs. A new test needs no table edit: it joins a part.

func goTestListNames(t *testing.T, pkg string) map[string]bool {
	t.Helper()
	out, errs, err := run(t, []string{"GOWORK=off"}, "go", "test", "-mod=readonly", "-trimpath", "-list", ".*", pkg)
	if err != nil {
		t.Fatalf("go test -list %s: %v\n%s", pkg, err, errs)
	}
	names := map[string]bool{}
	for _, l := range strings.Split(out, "\n") {
		if strings.HasPrefix(l, "Test") || strings.HasPrefix(l, "Example") || strings.HasPrefix(l, "Fuzz") {
			names[strings.TrimSpace(l)] = true
		}
	}
	if len(names) == 0 {
		t.Fatalf("go test -list %s reports no name: a vacuous oracle", pkg)
	}
	return names
}

func TestEverySplitTestNameRunsInExactlyOneTestLeg(t *testing.T) {
	t.Parallel()
	mod, _ := goList(t)
	splits := splitRows(t)
	if len(splits) == 0 {
		t.Skip("ci/go_test_splits.tsv splits no package")
	}
	rows := splitWeightRows(t)
	runs := map[string][]string{}
	legs := map[string][]int{}
	// One all-legs run gives every leg's names (TestTheAllLegsFormsPrintEachLegsOwnOutput holds it equal to each leg's own run).
	_, parts := allLegs(t, nil, "test-shard-split-list", shardCount)
	for k := 1; k <= shardCount; k++ {
		seen := map[string]bool{}
		for _, l := range strings.Split(parts[k], "\n") {
			rest, ok := strings.CutPrefix(l, "TEST-SPLIT-RUN ")
			if !ok {
				continue
			}
			key, name, ok := strings.Cut(rest, " ")
			if !ok {
				t.Errorf("leg %d: malformed line %q", k, l)
				continue
			}
			runs[key] = append(runs[key], name)
			if !seen[key] {
				seen[key] = true
				legs[key] = append(legs[key], k)
			}
		}
	}
	if t.Failed() {
		return
	}
	for key, parts := range splits {
		t.Run(key, func(t *testing.T) {
			t.Parallel()
			want := goTestListNames(t, mod+"/"+key)
			got := map[string]int{}
			for _, n := range runs[key] {
				got[n]++
			}
			var missing, twice, extra []string
			for n := range want {
				if got[n] == 0 {
					missing = append(missing, n)
				}
			}
			for n, c := range got {
				if c > 1 {
					twice = append(twice, n)
				}
				if !want[n] {
					extra = append(extra, n)
				}
			}
			sort.Strings(missing)
			sort.Strings(twice)
			sort.Strings(extra)
			if len(missing) > 0 || len(twice) > 0 || len(extra) > 0 {
				t.Errorf("in no leg (%d): %v; in two legs (%d): %v; not listed by go test -list (%d): %v",
					len(missing), missing[:min(5, len(missing))], len(twice), twice[:min(5, len(twice))], len(extra), extra[:min(5, len(extra))])
			}
			if n := len(legs[key]); n == 0 || n > parts {
				t.Errorf("%s runs in %d test leg(s) %v, want 1..%d (its parts)", key, n, legs[key], parts)
			}
			// CHAOS-13668: every row names a listed test, and the leg's cut (ci/go_race_shard.awk names mode) is the Go mirror's.
			var stale []string
			for n := range rows[key] {
				if !want[n] {
					stale = append(stale, n)
				}
			}
			sort.Strings(stale)
			if len(stale) > 0 {
				t.Errorf("ci/go_test_split_weights.tsv rows of %s name %d test(s) go test -list no longer reports: %v", key, len(stale), stale)
				return
			}
			var names []string
			for n := range want {
				names = append(names, n)
			}
			sort.Strings(names)
			cut, err := awkCut(t, realShardAwk(t), key, parts, map[string]map[string]int{key: rows[key]}, names)
			if err != nil {
				t.Fatalf("the cut of %s: %v", key, err)
			}
			mirror := cutMirror(rows[key], parts, names)
			for _, n := range names {
				if cut[n] != mirror[n] {
					t.Errorf("%s %s: the leg's cut gives part %d, its Go mirror part %d", key, n, cut[n], mirror[n])
				}
			}
		})
	}
}

// A split package is never pinned: a pin places a whole package, and its parts already spread over the legs.
func TestNoSplitPackageIsPinned(t *testing.T) {
	t.Parallel()
	splits := splitRows(t)
	for _, k := range pinRows(t) {
		if _, ok := splits[k]; ok {
			t.Errorf("ci/go_test_pins.tsv pins %s, which ci/go_test_splits.tsv splits", k)
		}
	}
}

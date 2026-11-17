//go:build !race

package integrationshards_test

import (
	"fmt"
	"sort"
	"strings"
	"testing"
)

// nameClashAllowList holds reviewed pairs ("<pkgA> <pkgB> <name>", packages sorted) whose shared name cannot collide
// under go test -run. Every entry needs a one-line reason.
var nameClashAllowList = map[string]string{}

// TestNoTopLevelIntegrationTestNameIsSharedByTwoPackages fails on a top-level test name that two integration packages
// share when the planner can turn the share into a collision, whatever today's packing. The rule, from the planner:
//   - a package may be cut across shards at test level only if its manifest has a split-by-test row (a weight alone
//     never splits a package: the planner places it whole);
//   - the isolated and name-sharded packages (splitPackages) run alone in their go test, so they share no -run;
//   - every other package may share a go test group (one -run regex over several packages) with any other;
//   - the name is a collision when one package selects it (integration-only name, or any name of a package with a
//     tag-only hook), the other package declares it, and at least one of the two can be split: the -run of one call
//     can then match the copy that belongs to another shard. Two whole packages run each copy once: not flagged.
func TestNoTopLevelIntegrationTestNameIsSharedByTwoPackages(t *testing.T) {
	t.Parallel()
	facts := integrationTaggedFacts(t)
	splitRows := manifestSplitKeys(t, rootOf(t))
	canSplit := func(k string) bool { return splitRows[k] }
	selected := map[string]map[string]bool{}
	declared := map[string]map[string]bool{}
	for k, tagged := range facts.tagged {
		if splitPackages[k] {
			continue
		}
		declared[k] = map[string]bool{}
		selected[k] = map[string]bool{}
		for n := range tagged {
			declared[k][n], selected[k][n] = true, true
		}
		for n := range facts.untagged[k] {
			declared[k][n] = true
			if facts.hookOf(k) != "" {
				selected[k][n] = true
			}
		}
	}
	pkgs := make([]string, 0, len(declared))
	for k := range declared {
		pkgs = append(pkgs, k)
	}
	sort.Strings(pkgs)
	if len(pkgs) < 2 {
		t.Fatal("fewer than two integration packages: the census measures nothing")
	}
	used := map[string]bool{}
	for _, a := range pkgs {
		for n := range selected[a] {
			for _, b := range pkgs {
				if a == b || !declared[b][n] || !(canSplit(a) || canSplit(b)) || (selected[b][n] && b < a) {
					continue
				}
				lo, hi := a, b
				if hi < lo {
					lo, hi = hi, lo
				}
				key := fmt.Sprintf("%s %s %s", lo, hi, n)
				if _, ok := nameClashAllowList[key]; ok {
					used[key] = true
					continue
				}
				t.Errorf("top-level test name %s is declared by both %s and %s (one of them can be split across shards, so a shared -run may select it in the copy of another shard); rename one copy or add %q to nameClashAllowList with a reason", n, a, b, key)
			}
		}
	}
	for key, why := range nameClashAllowList {
		if strings.TrimSpace(why) == "" {
			t.Errorf("nameClashAllowList entry %q has no reason", key)
		}
		if !used[key] {
			t.Errorf("nameClashAllowList entry %q matches no clash: drop it", key)
		}
	}
}

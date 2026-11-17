//go:build !race

package integrationshards_test

import (
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

var (
	plannedSplitRow = regexp.MustCompile(`(?m)^integration split package (\S+): `)
	ownShardKeyDecl = regexp.MustCompile(`(?m)^(?:PROVIDER|DAILY|WORKERSERVICE|QAPISERVER|REMAINING)_INTEGRATION_PACKAGE_KEY="([^"]+)"$`)
	isolatedKeyDecl = regexp.MustCompile(`(?m)^INTEGRATION_ISOLATED_PACKAGE_KEYS=" \$\{[A-Z_]+:-([^}]*)\} "$`)
)

// plannerSplitAndAlone reads the two sets from the planner, never from this package's own copy of its rules: the packages
// it cuts at test level are the `integration split package` lines of its plan (its own manifest parser), and the
// packages that run alone in their go test are the own-shard and isolated keys that ci/check_go.sh declares.
func plannerSplitAndAlone(t *testing.T) (split, alone map[string]bool) {
	t.Helper()
	stdout, _ := realPlan(t)
	split, alone = map[string]bool{}, map[string]bool{}
	for _, m := range plannedSplitRow.FindAllStringSubmatch(stdout, -1) {
		split[m[1]] = true
	}
	script := readText(t, filepath.Join(rootOf(t), "ci", "check_go.sh"))
	for _, m := range ownShardKeyDecl.FindAllStringSubmatch(script, -1) {
		alone[m[1]] = true
	}
	iso := isolatedKeyDecl.FindStringSubmatch(script)
	if iso == nil || len(alone) != 5 {
		t.Fatalf("planner facts not found: %d own-shard keys, isolated declaration %v", len(alone), iso != nil)
	}
	for _, k := range strings.Fields(iso[1]) {
		alone[k] = true
	}
	return split, alone
}

// nameClashAllowList holds reviewed pairs ("<pkgA> <pkgB> <name>", packages sorted) whose shared name cannot collide
// under go test -run. Every entry needs a one-line reason.
var nameClashAllowList = map[string]string{}

// TestNoTopLevelIntegrationTestNameIsSharedByTwoPackages fails on a top-level test name that two integration packages
// share when the planner can turn the share into a collision, whatever today's packing. The rule, from the planner:
//   - a package may be cut across shards at test level only if the planner's own plan splits it (an `integration split
//     package` line: its manifest parser decides, a weight alone never splits a package);
//   - the isolated and own-test-shard packages that ci/check_go.sh declares run alone in their go test, so they share
//     no -run;
//   - every other package may share a go test group (one -run regex over several packages) with any other;
//   - the name is a collision when one package selects it (integration-only name, or any name of a package with a
//     tag-only hook), the other package declares it, and at least one of the two can be split: the -run of one call
//     can then match the copy that belongs to another shard. Two whole packages run each copy once: not flagged.
func TestNoTopLevelIntegrationTestNameIsSharedByTwoPackages(t *testing.T) {
	t.Parallel()
	facts := integrationTaggedFacts(t)
	splitKeys, aloneKeys := plannerSplitAndAlone(t)
	canSplit := func(k string) bool { return splitKeys[k] }
	selected := map[string]map[string]bool{}
	declared := map[string]map[string]bool{}
	for k, tagged := range facts.tagged {
		if aloneKeys[k] {
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

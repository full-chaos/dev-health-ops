package venuechanged

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

const modulePrefix = "github.com/full-chaos/dev-health-ops/"

// tsvRows reads a tab separated file, skipping blank lines.
func tsvRows(t *testing.T, path string) [][]string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var rows [][]string
	for _, line := range strings.Split(string(data), "\n") {
		if strings.TrimSpace(line) != "" {
			rows = append(rows, strings.Split(line, "\t"))
		}
	}
	return rows
}

// producerImports returns the in-module packages the _test.go files of a venue package import
// and compare against: everything under internal/ except the test support, the platform
// plumbing and the package's own tree.
func producerImports(t *testing.T, root, pkg string) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(root, pkg, "*_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, file := range files {
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}
		for _, spec := range parsed.Imports {
			path, _ := strconv.Unquote(spec.Path.Value)
			rel, ok := strings.CutPrefix(path, modulePrefix)
			if !ok || !strings.HasPrefix(rel, "internal/") ||
				strings.HasPrefix(rel, "internal/testsupport") || strings.HasPrefix(rel, "internal/platform") ||
				rel == pkg || strings.HasPrefix(rel, pkg+"/") {
				continue
			}
			seen[rel] = true
		}
	}
	var out []string
	for rel := range seen {
		out = append(out, rel)
	}
	sort.Strings(out)
	return out
}

// TestEveryProducerAVenueTestImportsIsDeclaredAsATrigger closes the gap that let CHAOS-9578
// through: a pull request that changed a producer package a venue test compares selected
// nothing, because only the test's own package selects it. For every package with a `run`
// row, each producer its tests import must be equal to or under a path declared in
// ci/venue_oracle_triggers.d/, or be listed in ci/venue_oracle_trigger_gaps.tsv (the
// known gaps, a ratchet: a new gap fails, and a gap that got covered must leave the file).
func TestEveryProducerAVenueTestImportsIsDeclaredAsATrigger(t *testing.T) {
	root := repoRoot(t)
	packages := map[string]bool{}
	tests := map[string]int{}
	registry, _ := filepath.Glob(filepath.Join(root, "ci", "venue_oracle_registry.d", "*.tsv"))
	for _, file := range registry {
		for _, row := range tsvRows(t, file) {
			if len(row) == 3 && row[2] == "run" {
				packages[row[0]] = true
				tests[row[0]]++
			}
		}
	}
	if len(packages) == 0 {
		t.Fatal("no run rows found in ci/venue_oracle_registry.d/")
	}

	declared := map[string][]string{}
	for pkg := range packages {
		file := filepath.Join(root, "ci", "venue_oracle_triggers.d", strings.ReplaceAll(pkg, "/", "__")+".tsv")
		if _, err := os.Stat(file); err != nil {
			continue
		}
		for _, row := range tsvRows(t, file) {
			declared[pkg] = append(declared[pkg], row[2])
		}
	}

	known := map[string]bool{}
	for _, row := range tsvRows(t, filepath.Join(root, "ci", "venue_oracle_trigger_gaps.tsv")) {
		known[row[0]+"\t"+row[1]] = true
	}

	found := map[string]bool{}
	covered := 0
	var pkgs []string
	for pkg := range packages {
		pkgs = append(pkgs, pkg)
	}
	sort.Strings(pkgs)
	for _, pkg := range pkgs {
		for _, imp := range producerImports(t, root, pkg) {
			ok := false
			for _, path := range declared[pkg] {
				if imp == path || strings.HasPrefix(imp, path+"/") {
					ok = true
					break
				}
			}
			if ok {
				covered++
				continue
			}
			found[pkg+"\t"+imp] = true
		}
	}
	for gap := range found {
		if !known[gap] {
			t.Errorf("venue package imports a producer no trigger row declares, so a change there selects no oracle on a pull request: %q (add a row to ci/venue_oracle_triggers.d/, see its README)", gap)
		}
	}
	for gap := range known {
		if !found[gap] {
			t.Errorf("ci/venue_oracle_trigger_gaps.tsv lists %q, which is now declared or no longer imported: remove the line", gap)
		}
	}
	t.Logf("%d run package(s), %d run test(s), %d producer import(s) declared, %d known gap(s)", len(packages), sumInts(tests), covered, len(known))
}

func sumInts(m map[string]int) int {
	total := 0
	for _, n := range m {
		total += n
	}
	return total
}

// TestTheBillingLedgerOracleIsSelectedByItsProducers pins the CHAOS-9578 case: the entitlement keys
// #4032 changed live in internal/api/licensing, and a pull request that changes them selects the ledger oracle.
func TestTheBillingLedgerOracleIsSelectedByItsProducers(t *testing.T) {
	root := repoRoot(t)
	for _, path := range []string{"internal/api/licensing/registry.go", "internal/api/billing/entitlements.go"} {
		rows, err, stderr := selected(t, root, "\n", path)
		if err != nil {
			t.Fatalf("%s: %v\n%s", path, err, stderr)
		}
		want := "internal/apiservice/billingvenue\tTestVenueOracleBillingLedger"
		found := false
		for _, row := range rows {
			if row == want {
				found = true
			}
		}
		if !found {
			t.Errorf("%s selects %v, want it to include %q", path, rows, want)
		}
	}
}

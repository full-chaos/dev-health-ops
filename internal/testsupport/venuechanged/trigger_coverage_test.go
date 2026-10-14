package venuechanged

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path"
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

// declInfo is what one top-level declaration of a package's test files uses: the in-module
// producer packages it names through an import selector, and the identifiers it mentions
// (resolved later against the other declarations of the package).
type declInfo struct {
	imports map[string]bool
	refs    map[string]bool
	// always marks code that runs in every test binary run whatever test is selected:
	// init, TestMain and package-level variables with an initializer.
	always bool
}

// packageName returns the declared name of the package in dir, or the last path element
// when the directory holds no Go file. An import used without an alias is selected by it.
func packageName(root, rel string) string {
	files, _ := filepath.Glob(filepath.Join(root, rel, "*.go"))
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, parser.PackageClauseOnly)
		if err == nil {
			return parsed.Name.Name
		}
	}
	return path.Base(rel)
}

// isProducer reports whether an import path is a producer package of a venue package:
// in-module, under internal/, not the test plumbing and not the package's own tree.
func isProducer(imp, pkg string) (string, bool) {
	rel, ok := strings.CutPrefix(imp, modulePrefix)
	if !ok || !strings.HasPrefix(rel, "internal/") ||
		strings.HasPrefix(rel, "internal/testsupport") ||
		rel == pkg || strings.HasPrefix(rel, pkg+"/") {
		return "", false
	}
	return rel, true
}

// scanPackage reads the _test.go files of pkg and returns, per top-level declaration, the
// producers it uses. Declarations are keyed by name; a method is keyed ".Name" (all methods
// of that name merge), and the blank and dot names merge too. The second result is the set
// of producers imported with the blank identifier: they are linked into the test binary,
// so every test of the package depends on them. Declarations marked always (init, TestMain,
// initialized package variables) run whatever test is selected, so every test depends on them.
func scanPackage(t *testing.T, root, pkg string) (map[string]*declInfo, map[string]bool) {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(root, pkg, "*_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	decls := map[string]*declInfo{}
	blank := map[string]bool{}
	get := func(name string) *declInfo {
		if decls[name] == nil {
			decls[name] = &declInfo{imports: map[string]bool{}, refs: map[string]bool{}}
		}
		return decls[name]
	}
	for _, file := range files {
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}
		named := map[string]string{}
		var dot []string
		for _, spec := range parsed.Imports {
			imp, _ := strconv.Unquote(spec.Path.Value)
			rel, ok := isProducer(imp, pkg)
			if !ok {
				continue
			}
			switch {
			case spec.Name == nil:
				named[packageName(root, rel)] = rel
			case spec.Name.Name == "_":
				blank[rel] = true
			case spec.Name.Name == ".":
				dot = append(dot, rel)
			default:
				named[spec.Name.Name] = rel
			}
		}
		for _, decl := range parsed.Decls {
			var names []string
			always := false
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if d.Recv != nil {
					names = []string{"." + d.Name.Name}
				} else {
					names = []string{d.Name.Name}
					always = d.Name.Name == "init" || d.Name.Name == "TestMain"
				}
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					switch s := spec.(type) {
					case *ast.TypeSpec:
						names = append(names, s.Name.Name)
					case *ast.ValueSpec:
						for _, n := range s.Names {
							names = append(names, n.Name)
						}
						if d.Tok == token.VAR && len(s.Values) > 0 {
							always = true
						}
					}
				}
			}
			if len(names) == 0 {
				continue
			}
			info := &declInfo{imports: map[string]bool{}, refs: map[string]bool{}}
			for _, rel := range dot {
				info.imports[rel] = true
			}
			ast.Inspect(decl, func(node ast.Node) bool {
				switch n := node.(type) {
				case *ast.SelectorExpr:
					info.refs["."+n.Sel.Name] = true
					if x, ok := n.X.(*ast.Ident); ok {
						if rel, ok := named[x.Name]; ok {
							info.imports[rel] = true
						}
					}
				case *ast.Ident:
					info.refs[n.Name] = true
				}
				return true
			})
			for _, name := range names {
				merged := get(name)
				merged.always = merged.always || always
				for rel := range info.imports {
					merged.imports[rel] = true
				}
				for ref := range info.refs {
					merged.refs[ref] = true
				}
			}
		}
	}
	return decls, blank
}

// testProducers returns the producer packages one venue test depends on: those named by the
// test function and, through the identifiers it mentions, by every declaration of the
// package's test files it reaches (helpers, fixtures, types and methods), plus the producers
// the package imports for side effects and those the always-running declarations reach.
func testProducers(decls map[string]*declInfo, blank map[string]bool, test string) []string {
	seen := map[string]bool{}
	out := map[string]bool{}
	for rel := range blank {
		out[rel] = true
	}
	queue := []string{test}
	for name, info := range decls {
		if info.always {
			queue = append(queue, name)
		}
	}
	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]
		if seen[name] {
			continue
		}
		seen[name] = true
		info := decls[name]
		if info == nil {
			continue
		}
		for rel := range info.imports {
			out[rel] = true
		}
		for ref := range info.refs {
			queue = append(queue, ref)
		}
	}
	var list []string
	for rel := range out {
		list = append(list, rel)
	}
	sort.Strings(list)
	return list
}

// undeclaredProducers is the guard's core: for every `run` test of every venue package,
// each producer the test reaches must be equal to or under a path declared for THAT test
// (declared[package][test]). It returns the missing "package\ttest\tproducer" keys and the
// number of producer imports that are declared. Selection on a pull request is per test, so
// a row for one test of a package does not cover another test of it.
func undeclaredProducers(t *testing.T, root string, tests map[string][]string, declared map[string]map[string][]string) (map[string]bool, int) {
	t.Helper()
	found := map[string]bool{}
	covered := 0
	for pkg, names := range tests {
		decls, blank := scanPackage(t, root, pkg)
		for _, test := range names {
			if decls[test] == nil {
				t.Errorf("%s: registry run row %s is declared in no _test.go file of the package", pkg, test)
				continue
			}
			for _, imp := range testProducers(decls, blank, test) {
				ok := false
				for _, p := range declared[pkg][test] {
					if imp == p || strings.HasPrefix(imp, p+"/") {
						ok = true
						break
					}
				}
				if ok {
					covered++
					continue
				}
				found[pkg+"\t"+test+"\t"+imp] = true
			}
		}
	}
	return found, covered
}

// TestEveryProducerAVenueTestImportsIsDeclaredAsATrigger closes the gap that let CHAOS-9578
// through: a pull request that changed a producer package a venue test compares selected
// nothing, because only the test's own package selects it. For every `run` test, each
// producer that test reaches must be equal to or under a path declared FOR THAT TEST in
// ci/venue_oracle_triggers.d/ (CHAOS-9614: the pull request job selects per test, so a row
// for another test of the package does not count), or be listed in
// ci/venue_oracle_trigger_gaps.tsv (the known gaps, a ratchet: a new gap fails, and a gap that
// got covered must leave the file).
func TestEveryProducerAVenueTestImportsIsDeclaredAsATrigger(t *testing.T) {
	root := repoRoot(t)
	tests := map[string][]string{}
	total := 0
	registry, _ := filepath.Glob(filepath.Join(root, "ci", "venue_oracle_registry.d", "*.tsv"))
	for _, file := range registry {
		for _, row := range tsvRows(t, file) {
			if len(row) == 3 && row[2] == "run" {
				tests[row[0]] = append(tests[row[0]], row[1])
				total++
			}
		}
	}
	if len(tests) == 0 {
		t.Fatal("no run rows found in ci/venue_oracle_registry.d/")
	}

	declared := map[string]map[string][]string{}
	for pkg := range tests {
		file := filepath.Join(root, "ci", "venue_oracle_triggers.d", strings.ReplaceAll(pkg, "/", "__")+".tsv")
		if _, err := os.Stat(file); err != nil {
			continue
		}
		declared[pkg] = map[string][]string{}
		for _, row := range tsvRows(t, file) {
			declared[pkg][row[1]] = append(declared[pkg][row[1]], row[2])
		}
	}

	known := map[string]bool{}
	for _, row := range tsvRows(t, filepath.Join(root, "ci", "venue_oracle_trigger_gaps.tsv")) {
		if len(row) != 3 {
			t.Fatalf("ci/venue_oracle_trigger_gaps.tsv row %q: want <package dir> TAB <Test function> TAB <producer package>", strings.Join(row, "\t"))
		}
		known[strings.Join(row, "\t")] = true
	}

	found, covered := undeclaredProducers(t, root, tests, declared)
	for gap := range found {
		if !known[gap] {
			t.Errorf("a venue test uses a producer no trigger row declares for it, so a change there does not select that test on a pull request: %q (add a row to ci/venue_oracle_triggers.d/, see its README)", gap)
		}
	}
	for gap := range known {
		if !found[gap] {
			t.Errorf("ci/venue_oracle_trigger_gaps.tsv lists %q, which is now declared or no longer used by that test: remove the line", gap)
		}
	}
	t.Logf("%d run package(s), %d run test(s), %d producer use(s) declared, %d known gap(s)", len(tests), total, covered, len(known))
}

// writeFixture writes files (path relative to root -> content) under root.
func writeFixture(t *testing.T, root string, files map[string]string) {
	t.Helper()
	for rel, content := range files {
		full := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// TestATriggerRowForOneTestDoesNotCoverAnotherTestOfThePackage pins CHAOS-9614 (the case
// codex round 2 of #4107 reproduced): the package has two tests, only the first one uses the
// licensing producer, and the only row is for the second test. The old per-package check saw a
// row for the package and passed; selection is per test, so the first test is not selected by a
// licensing change and the guard must name it.
func TestATriggerRowForOneTestDoesNotCoverAnotherTestOfThePackage(t *testing.T) {
	root := t.TempDir()
	const venue = "internal/venue"
	writeFixture(t, root, map[string]string{
		venue + "/ledger_test.go": `package venue

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
)

func TestLedger(t *testing.T) { _ = licensing.Keys() }
`,
		venue + "/refund_test.go": `package venue

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/billing"
)

func TestRefundCreate(t *testing.T) { helper(); _ = billing.Refund }

func helper() {}
`,
	})
	tests := map[string][]string{venue: {"TestLedger", "TestRefundCreate"}}

	rowForRefundOnly := map[string]map[string][]string{venue: {
		"TestRefundCreate": {"internal/api/billing", "internal/api/licensing"},
	}}
	found, _ := undeclaredProducers(t, root, tests, rowForRefundOnly)
	want := venue + "\tTestLedger\tinternal/api/licensing"
	if len(found) != 1 || !found[want] {
		t.Fatalf("row for TestRefundCreate only: found %v, want exactly %q", found, want)
	}

	rowsForBoth := map[string]map[string][]string{venue: {
		"TestLedger":       {"internal/api/licensing"},
		"TestRefundCreate": {"internal/api/billing"},
	}}
	if found, covered := undeclaredProducers(t, root, tests, rowsForBoth); len(found) != 0 || covered != 2 {
		t.Fatalf("a row per test: found %v covered %d, want none and 2", found, covered)
	}
}

// TestAProducerReachedThroughAHelperBelongsToTheTestThatCallsIt pins the attribution: a
// producer imported by a helper file counts for the tests that call the helper (directly,
// through another helper or through a method), and a blank import counts for every test.
func TestAProducerReachedThroughAHelperBelongsToTheTestThatCallsIt(t *testing.T) {
	root := t.TempDir()
	const venue = "internal/venue"
	writeFixture(t, root, map[string]string{
		"internal/api/policy/policy.go": "package policy\n",
		venue + "/a_test.go": `package venue

import "testing"

func TestViaHelper(t *testing.T) { outer() }

func TestViaMethod(t *testing.T) { harness{}.Run() }

func TestViaVariable(t *testing.T) { _ = fixture }

func TestPlain(t *testing.T) {}
`,
		venue + "/helpers_test.go": `package venue

import (
	pol "github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/orgs"
	_ "github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/venue/sub"
)

func outer() { inner() }

func inner() { _ = pol.Check }

type harness struct{}

func (harness) Run() { _ = orgs.New }

var fixture = clickhouse.New(sub.X)
`,
	})
	tests := map[string][]string{venue: {"TestViaHelper", "TestViaMethod", "TestViaVariable", "TestPlain"}}
	found, _ := undeclaredProducers(t, root, tests, nil)
	var got []string
	for key := range found {
		got = append(got, key)
	}
	sort.Strings(got)
	want := []string{
		venue + "\tTestPlain\tinternal/platform/config",
		venue + "\tTestViaHelper\tinternal/api/policy",
		venue + "\tTestViaHelper\tinternal/platform/config",
		venue + "\tTestViaMethod\tinternal/api/orgs",
		venue + "\tTestViaMethod\tinternal/platform/config",
		venue + "\tTestViaVariable\tinternal/platform/config",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("producers by test:\n got %q\nwant %q", got, want)
	}
}

// TestCodeThatRunsInEveryTestBinaryBelongsToEveryTest pins the always-running roots: init,
// TestMain and a package variable with an initializer run whatever test is selected, so the
// producers they reach count for every test; a variable without an initializer does not run
// anything and a function nobody calls reaches no test.
func TestCodeThatRunsInEveryTestBinaryBelongsToEveryTest(t *testing.T) {
	root := t.TempDir()
	const venue = "internal/venue"
	writeFixture(t, root, map[string]string{
		venue + "/a_test.go": `package venue

import (
	"os"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/audit"
	"github.com/full-chaos/dev-health-ops/internal/api/billing"
	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/api/orgs"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

func TestMain(m *testing.M) { _ = licensing.Keys(); os.Exit(m.Run()) }

func init() { _ = billing.Refund }

var registered = register(orgs.New)

var declared policy.Rule

func register(f func()) int { return 0 }

func unused() { _ = audit.Log }

func TestOne(t *testing.T) {}
`,
	})
	found, _ := undeclaredProducers(t, root, map[string][]string{venue: {"TestOne"}}, nil)
	var got []string
	for key := range found {
		got = append(got, key)
	}
	sort.Strings(got)
	want := []string{
		venue + "\tTestOne\tinternal/api/billing",
		venue + "\tTestOne\tinternal/api/licensing",
		venue + "\tTestOne\tinternal/api/orgs",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("producers of TestOne:\n got %q\nwant %q", got, want)
	}
}

// TestTheBillingLedgerOracleIsSelectedByItsProducers pins the CHAOS-9578 case: the entitlement keys
// #4032 changed live in internal/api/licensing, and a pull request that changes them selects the ledger oracle.
func TestTheBillingLedgerOracleIsSelectedByItsProducers(t *testing.T) {
	root := repoRoot(t)
	for _, path := range []string{"internal/api/licensing/registry.go", "internal/api/billing/entitlements.go", "internal/platform/config/config.go", "internal/platform/secrets/value.go"} {
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

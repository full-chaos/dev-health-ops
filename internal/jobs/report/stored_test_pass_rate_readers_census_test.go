package report

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// The census of the readers of the stored TestOps test pass rate.
//
// testops_test_metrics_daily.pass_rate divides by every case of the day, skipped ones too, and holds 0 on a day where
// every case was skipped. No Go source of this repository reads it. A pass rate is read from the counts, passed over
// passed plus failed (report chart rule, benchmarking definition, query-api TEST_PASS_RATE). There is no reader
// allow-list: a reader is never listed.
//
// The census reads every non-test Go source under internal/ and cmd/. A mention is a compile-time string (a literal,
// a concatenation of literals and string constants, a decoded struct tag value) that spells pass_rate. Each mention
// must be one of:
//   - a reviewed metric-name site below (the bare string "pass_rate", counted per file);
//   - the daily writer's column list: a PrepareBatch INSERT INTO testops_test_metrics_daily with a column list only.
//
// Any other mention fails, whatever its syntax: SQL that names the column, a column name passed to a helper or a
// field, a struct tag, a new use of the metric name. A new metric-name use is added to the list after review.
// A compile-time string that selects every column of testops_test_metrics_daily (SELECT *, t.*, COLUMNS(...)) reads
// the stored rate without naming it: it must be a reviewed wildcard site whose outer query names its columns.
var reviewedPassRateNameSites = map[string]int{
	"internal/jobs/metrics/daily/benchmarking/clickhouse.go": 1, // metricDefinitions key
	"internal/jobs/metrics/daily/benchmarking/run.go":        1, // DefaultBenchmarkMetrics name
	"internal/jobs/report/dedup.go":                          1, // chartRules key
	"internal/jobs/report/render.go":                         1, // sectionMetrics "quality" name
}

var reviewedTestMetricsWildcardSites = map[string]int{
	"internal/queryapi/aianalytics/ai_opportunities.go": 1, // newest-row subquery; the outer query names flake and case counts
	"internal/queryapi/testopswindow/testopswindow.go":  1, // newest-row subquery; the outer query names the case counts
}

var (
	passRateWord        = regexp.MustCompile(`\bpass_rate\b`)
	testopsTestWriter   = regexp.MustCompile(`^\s*INSERT\s+INTO\s+testops_test_metrics_daily\s*\(\s*\w+(\s*,\s*\w+)*\s*\)\s*$`)
	wildcardProjection  = regexp.MustCompile(`(?is)\bselect\s+(distinct\s+)?([\w.]+\s*\.\s*)?\*|\.\s*\*|\bcolumns\s*\(`)
	structTagValuePairs = regexp.MustCompile(`(\w+):("(?:[^"\\]|\\.)*")`)
)

func repositoryRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("no go.mod above the test directory")
		}
		dir = parent
	}
}

type censusFile struct {
	relative string
	fileSet  *token.FileSet
	parsed   *ast.File
}

// packageStringConstants maps each constant name of a package to the expressions it is declared with (a name can be
// declared in more than one scope; each declaration is tried).
func packageStringConstants(files []censusFile) map[string][]ast.Expr {
	constants := map[string][]ast.Expr{}
	for _, file := range files {
		ast.Inspect(file.parsed, func(node ast.Node) bool {
			declaration, ok := node.(*ast.GenDecl)
			if !ok || declaration.Tok != token.CONST {
				return true
			}
			for _, spec := range declaration.Specs {
				value, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for index, name := range value.Names {
					if index < len(value.Values) {
						constants[name.Name] = append(constants[name.Name], value.Values[index])
					}
				}
			}
			return true
		})
	}
	return constants
}

// foldString returns every value a compile-time string expression can take: a string literal, a parenthesized
// expression, a string constant of the package, or a concatenation of those. ok is false for anything else.
func foldString(expression ast.Expr, constants map[string][]ast.Expr, depth int) (values []string, ok bool) {
	if depth > 16 {
		return nil, false
	}
	switch node := expression.(type) {
	case *ast.BasicLit:
		if node.Kind != token.STRING {
			return nil, false
		}
		value, err := strconv.Unquote(node.Value)
		if err != nil {
			return nil, false
		}
		return []string{value}, true
	case *ast.ParenExpr:
		return foldString(node.X, constants, depth+1)
	case *ast.Ident:
		for _, declared := range constants[node.Name] {
			if folded, ok := foldString(declared, constants, depth+1); ok {
				values = append(values, folded...)
			}
		}
		return values, len(values) > 0
	case *ast.BinaryExpr:
		if node.Op != token.ADD {
			return nil, false
		}
		left, leftOK := foldString(node.X, constants, depth+1)
		right, rightOK := foldString(node.Y, constants, depth+1)
		if !leftOK || !rightOK {
			return nil, false
		}
		for _, l := range left {
			for _, r := range right {
				values = append(values, l+r)
			}
		}
		return values, true
	}
	return nil, false
}

// spellings returns a compile-time string and, when it holds struct tag pairs, each decoded tag value.
func spellings(value string) []string {
	out := []string{value}
	for _, pair := range structTagValuePairs.FindAllStringSubmatch(value, -1) {
		if decoded, err := strconv.Unquote(pair[2]); err == nil {
			out = append(out, decoded)
		}
	}
	return out
}

func spellsPassRate(value string) bool {
	for _, spelling := range spellings(value) {
		if passRateWord.MatchString(spelling) {
			return true
		}
	}
	return false
}

func readsEveryTestMetricsColumn(value string) bool {
	return strings.Contains(value, "testops_test_metrics_daily") && wildcardProjection.MatchString(value)
}

func TestNoGoSourceReadsTheStoredTestPassRateCensus(t *testing.T) {
	root := repositoryRoot(t)
	packages := map[string][]censusFile{}
	files := 0
	for _, top := range []string{"internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, top), func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			relative, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			fileSet := token.NewFileSet()
			parsed, err := parser.ParseFile(fileSet, path, nil, parser.SkipObjectResolution)
			if err != nil {
				return err
			}
			files++
			directory := filepath.Dir(path)
			packages[directory] = append(packages[directory], censusFile{filepath.ToSlash(relative), fileSet, parsed})
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", top, err)
		}
	}

	nameSites, wildcardSites := map[string]int{}, map[string]int{}
	writers := 0
	for _, packageFiles := range packages {
		constants := packageStringConstants(packageFiles)
		for _, file := range packageFiles {
			// The operands of a concatenation are read as the concatenation, once, at its outermost node.
			operands := map[ast.Expr]bool{}
			writerArguments := map[ast.Expr]bool{}
			ast.Inspect(file.parsed, func(node ast.Node) bool {
				switch typed := node.(type) {
				case *ast.BinaryExpr:
					if typed.Op == token.ADD {
						if _, ok := foldString(typed, constants, 0); ok {
							operands[typed.X], operands[typed.Y] = true, true
						}
					}
				case *ast.CallExpr:
					if selector, ok := typed.Fun.(*ast.SelectorExpr); ok && selector.Sel.Name == "PrepareBatch" && len(typed.Args) >= 2 {
						writerArguments[typed.Args[1]] = true
					}
				}
				return true
			})
			ast.Inspect(file.parsed, func(node ast.Node) bool {
				expression, ok := node.(ast.Expr)
				if !ok || operands[expression] {
					return true
				}
				switch expression.(type) {
				case *ast.BasicLit, *ast.BinaryExpr:
				default:
					return true
				}
				values, ok := foldString(expression, constants, 0)
				if !ok {
					return true
				}
				line := file.fileSet.Position(expression.Pos()).Line
				for _, value := range values {
					if readsEveryTestMetricsColumn(value) {
						wildcardSites[file.relative]++
					}
					if !spellsPassRate(value) {
						continue
					}
					switch {
					case value == "pass_rate":
						nameSites[file.relative]++
					case writerArguments[expression] && testopsTestWriter.MatchString(value):
						writers++
					default:
						t.Errorf("%s:%d spells pass_rate (%q): the stored testops_test_metrics_daily.pass_rate is not read; read passed_count and failed_count, passed / (passed + failed), NULL when both are 0",
							file.relative, line, strings.Join(strings.Fields(value), " "))
					}
				}
				return true
			})
		}
	}
	compareSites(t, "metric-name use of \"pass_rate\"", nameSites, reviewedPassRateNameSites)
	compareSites(t, "select of every testops_test_metrics_daily column", wildcardSites, reviewedTestMetricsWildcardSites)
	// A census that read nothing measured nothing: it must see the tree and the daily writer.
	if files < 1000 || writers != 1 {
		t.Fatalf("the census read %d Go file(s) and %d daily writer column list(s), want >= 1000 and 1: it did not measure", files, writers)
	}
}

// compareSites fails on a site count that differs from the reviewed list, in either direction: a new site is
// reviewed before it is listed, and a site that went away leaves the list.
func compareSites(t *testing.T, what string, found, reviewed map[string]int) {
	t.Helper()
	keys := map[string]bool{}
	for key := range found {
		keys[key] = true
	}
	for key := range reviewed {
		keys[key] = true
	}
	sorted := make([]string, 0, len(keys))
	for key := range keys {
		sorted = append(sorted, key)
	}
	sort.Strings(sorted)
	for _, key := range sorted {
		if found[key] != reviewed[key] {
			t.Errorf("%s: %s holds %d, the reviewed list %d. A read of the stored pass_rate is never listed: read the counts. A reviewed non-read is listed with its role",
				what, key, found[key], reviewed[key])
		}
	}
}

// The report registry names pass_rate on testops_test_metrics_daily. A registry metric is charted from the column of
// its own name unless it has a chart rule, so the registry entry must go through the rule.
func TestTheRegistryPassRateChartsThroughTheCountRuleCensus(t *testing.T) {
	found := 0
	for name, definition := range supportedMetrics {
		if definition.SourceTable != "testops_test_metrics_daily" || !passRateWord.MatchString(name) {
			continue
		}
		found++
		if withChartRule(definition).rule == "" {
			t.Errorf("registry metric %s on testops_test_metrics_daily has no chart rule: its chart reads the stored pass_rate", name)
		}
	}
	if found == 0 {
		t.Fatal("no registry metric names pass_rate on testops_test_metrics_daily: the census did not measure")
	}
}

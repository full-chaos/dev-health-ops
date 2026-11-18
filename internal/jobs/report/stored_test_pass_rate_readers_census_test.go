package report

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// The census of the readers of the stored TestOps test pass rate.
//
// testops_test_metrics_daily.pass_rate divides by every case of the day, skipped ones too, and holds 0 on a day where
// every case was skipped. No Go source of this repository reads it. A pass rate is read from the counts, passed over
// passed plus failed (report chart rule, benchmarking definition, query-api TEST_PASS_RATE). The allow-list of readers
// is empty; a new reader fails here.
//
// A Go string literal that names pass_rate is one of:
//   - the metric name: the bare literal "pass_rate" as a map key or an unkeyed element of a composite literal;
//   - the daily writer's INSERT into testops_test_metrics_daily;
//   - a read: anything else (SQL that names the column, a column name passed to a helper or a field, a struct tag).
var storedTestPassRateReaders = map[string]bool{}

var passRateWord = regexp.MustCompile(`\bpass_rate\b`)

const testopsTestWriterInsert = "INSERT INTO testops_test_metrics_daily"

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

func TestNoGoSourceReadsTheStoredTestPassRateCensus(t *testing.T) {
	root := repositoryRoot(t)
	files, names, writers := 0, 0, 0
	for _, top := range []string{"internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, top), func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			source, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			files++
			if !passRateWord.Match(source) {
				return nil
			}
			relative, _ := filepath.Rel(root, path)
			relative = filepath.ToSlash(relative)
			fileSet := token.NewFileSet()
			parsed, err := parser.ParseFile(fileSet, path, source, parser.SkipObjectResolution)
			if err != nil {
				return err
			}
			nameSites := map[*ast.BasicLit]bool{}
			ast.Inspect(parsed, func(node ast.Node) bool {
				composite, ok := node.(*ast.CompositeLit)
				if !ok {
					return true
				}
				for _, element := range composite.Elts {
					if pair, ok := element.(*ast.KeyValueExpr); ok {
						if literal, ok := pair.Key.(*ast.BasicLit); ok {
							nameSites[literal] = true
						}
						continue
					}
					if literal, ok := element.(*ast.BasicLit); ok {
						nameSites[literal] = true
					}
				}
				return true
			})
			ast.Inspect(parsed, func(node ast.Node) bool {
				literal, ok := node.(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					return true
				}
				value, err := strconv.Unquote(literal.Value)
				if err != nil || !passRateWord.MatchString(value) {
					return true
				}
				switch {
				case value == "pass_rate" && nameSites[literal]:
					names++
				case strings.Contains(value, testopsTestWriterInsert):
					writers++
				case storedTestPassRateReaders[relative]:
				default:
					t.Errorf("%s:%d reads the stored testops_test_metrics_daily.pass_rate (%q): read passed_count and failed_count, passed / (passed + failed), NULL when both are 0",
						relative, fileSet.Position(literal.Pos()).Line, strings.Join(strings.Fields(value), " "))
				}
				return true
			})
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", top, err)
		}
	}
	// A census that read nothing measured nothing: it must see the tree, the writer and the metric-name sites
	// (benchmarking definition key and run list, report quality section).
	if files < 1000 || writers < 1 || names < 3 {
		t.Fatalf("the census read %d Go file(s), %d writer insert(s), %d metric-name site(s): it did not measure", files, writers, names)
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

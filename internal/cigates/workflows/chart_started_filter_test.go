package workflows_test

import (
	"fmt"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// CHAOS-11418: chart-started selects a pull request by its input closure, not by "any code change". The closure is the chart
// (deploy/helm/**: templates, values with the ClickHouse image pin, every test file compiled with the started test), go.mod and
// go.sum (toolchain and yaml.v3) and test.yml itself. That closure is only complete while the started test file imports
// nothing from this module: these guards pin both halves, and that the merge queue always runs the job.

const (
	startedWorkflow = ".github/workflows/test.yml"
	startedTestPath = "deploy/helm/dev-health/clickhouse_started_test.go"
)

var wantChartFilter = []string{".github/workflows/test.yml", "deploy/helm/**", "go.mod", "go.sum"}

// importPathIsStdlib reports whether an import path is the standard library: its first element has no dot.
func importPathIsStdlib(p string) bool { return !strings.Contains(strings.SplitN(p, "/", 2)[0], ".") }

func chartStartedFilterViolations(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	add := func(f string, a ...any) { out = append(out, fmt.Sprintf(f, a...)) }

	var wf workflowFile
	if err := yaml.Unmarshal([]byte(readFile(t, filepath.Join(root, startedWorkflow))), &wf); err != nil {
		t.Fatalf("parsing %s: %v", startedWorkflow, err)
	}
	patterns := codePatterns(t, wf)
	got := append([]string{}, patterns...)
	sort.Strings(got)
	want := append([]string{}, wantChartFilter...)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		add("test.yml's `code` filter is %v, want exactly the chart-started input closure %v", got, want)
	}

	for _, c := range []struct {
		path     string
		selected bool
	}{
		{"deploy/helm/dev-health/templates/clickhouse.yaml", true},
		{"deploy/helm/dev-health/values.yaml", true},
		{startedTestPath, true},
		{".github/workflows/test.yml", true},
		{"go.mod", true},
		{"go.sum", true},
		{"internal/cigates/workflows/chart_started_filter_test.go", false},
		{"ci/check_go.sh", false},
		{"cmd/cigate/main.go", false},
		{"docs/index.md", false},
	} {
		if isCovered(c.path, patterns) != c.selected {
			add("%s: selected=%v by the filter, want %v", c.path, !c.selected, c.selected)
		}
	}

	cond := ""
	if j := job(t, wf, "chart-started"); j.If != nil {
		cond = normalize(*j.If)
	}
	if !strings.Contains(cond, "github.event_name == 'merge_group'") {
		add("chart-started `if` is %q: it must always run in the merge queue (github.event_name == 'merge_group')", cond)
	}

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, filepath.Join(root, startedTestPath), nil, parser.ImportsOnly)
	if err != nil {
		return append(out, fmt.Sprintf("%s: %v", startedTestPath, err))
	}
	for _, im := range f.Imports {
		p, _ := strconv.Unquote(im.Path.Value)
		if !importPathIsStdlib(p) && p != "gopkg.in/yaml.v3" {
			add("%s imports %s: the file is outside the standard library and yaml.v3, so the chart-started filter in test.yml no longer covers its inputs (add the import closure or move the import)", startedTestPath, p)
		}
	}
	return out
}

func TestChartStartedFilterIsItsInputClosure(t *testing.T) {
	expectNoViolations(t, chartStartedFilterViolations(t, repoRoot(t)))
}

func TestChartStartedFilterPlantedDefects(t *testing.T) {
	root := repoRoot(t)
	files := []string{startedWorkflow, startedTestPath}
	for _, c := range []struct {
		name string
		muts []mutation
		want []string
	}{
		{"the chart leaves the filter", []mutation{{startedWorkflow, "              - 'deploy/helm/**'\n", ""}}, []string{"input closure", "templates/clickhouse.yaml"}},
		{"the workflow file leaves the filter", []mutation{{startedWorkflow, "              - '.github/workflows/test.yml'\n", ""}}, []string{"input closure", ".github/workflows/test.yml"}},
		{"the filter widens to every Go file", []mutation{{startedWorkflow, "              - 'go.sum'\n", "              - 'go.sum'\n              - '**/*.go'\n"}}, []string{"input closure"}},
		{"the filter widens to internal", []mutation{{startedWorkflow, "              - 'go.sum'\n", "              - 'go.sum'\n              - 'internal/**'\n"}}, []string{"input closure"}},
		{"the merge queue loses the job", []mutation{{startedWorkflow, "if: github.event_name == 'merge_group' || github.event_name == 'workflow_dispatch' || needs.changes.outputs.code == 'true'", "if: needs.changes.outputs.code == 'true'"}}, []string{"merge_group"}},
		{"the started test gains a module import", []mutation{{startedTestPath, "\t\"gopkg.in/yaml.v3\"\n", "\t\"gopkg.in/yaml.v3\"\n\t\"github.com/full-chaos/dev-health-ops/internal/platform/logging\"\n"}}, []string{startedTestPath, "internal/platform/logging"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			tree := plantTree(t, root, files, c.muts...)
			expectViolation(t, chartStartedFilterViolations(t, tree), c.want...)
		})
	}
}

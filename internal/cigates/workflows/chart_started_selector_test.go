package workflows_test

import (
	"fmt"
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// The chart-started job runs one Go test by name. `go test -run` that matches nothing prints "no tests to run" and
// exits 0, so a rename or a typo would leave the required `test` check green with the chart never started. These
// guards read the job's step and the package it names and prove the selector matches exactly one real test.

const (
	chartStartedWorkflow = ".github/workflows/test.yml"
	chartStartedJob      = "chart-started"
	chartStartedPackage  = "deploy/helm/dev-health"
	chartStartedTestFile = "deploy/helm/dev-health/clickhouse_started_test.go"
)

// chartRunLine matches one whole shell line that is a `go test` run of one named test in one package: a comment, an
// echo, a pipeline, `|| true` or a trailing command does not match.
var chartRunLine = regexp.MustCompile(`^\s*go test\b.*\s-run\s+'([^']*)'\s+\./(\S+)\s*$`)

var chartTagsFlag = regexp.MustCompile(`\s-tags[= ]'?([^\s']+)'?`)

// chartAllowedFlag lists the only flags the started-chart command may carry. Any other flag can change what `go test`
// does without changing the selector or the package: `-list` prints names and runs nothing, `-skip` drops the test,
// `-bench` or `-fuzz` run something else.
var chartAllowedFlag = regexp.MustCompile(`^-(mod=readonly|trimpath|count=1|v|race|json|timeout=[0-9]+[smh]|tags=[A-Za-z0-9_,]+)$`)

// chartUnexpectedFlags returns the flag-like tokens of a `go test ... -run '<re>' ./<pkg>` line outside the allowlist.
func chartUnexpectedFlags(line string) []string {
	tokens := strings.Fields(strings.TrimPrefix(strings.TrimSpace(line), "go test"))
	var bad []string
	for i := 0; i < len(tokens); i++ {
		tok := tokens[i]
		if tok == "-run" {
			i++
			continue
		}
		if strings.HasPrefix(tok, "-") && !chartAllowedFlag.MatchString(tok) {
			bad = append(bad, tok)
		}
	}
	return bad
}

type chartStartedWorkflowFile struct {
	Env  map[string]string `yaml:"env"`
	Jobs map[string]*struct {
		ContinueOnError any               `yaml:"continue-on-error"`
		Env             map[string]string `yaml:"env"`
		Steps           []struct {
			Run             string            `yaml:"run"`
			If              *string           `yaml:"if"`
			ContinueOnError any               `yaml:"continue-on-error"`
			Env             map[string]string `yaml:"env"`
		} `yaml:"steps"`
	} `yaml:"jobs"`
}

func chartIgnoresFailure(v any) bool { return v != nil && fmt.Sprint(v) != "false" }

// chartStartedViolations returns one line per defect found in the chart-started job of the workflow at
// root/chartStartedWorkflow, resolving the package it tests under root with the build tags the step passes.
func chartStartedViolations(t *testing.T, root string) []string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, chartStartedWorkflow))
	if err != nil {
		t.Fatal(err)
	}
	var wf chartStartedWorkflowFile
	if err := yaml.Unmarshal(raw, &wf); err != nil {
		t.Fatalf("parsing %s: %v", chartStartedWorkflow, err)
	}
	j, ok := wf.Jobs[chartStartedJob]
	if !ok || j == nil {
		return []string{"job " + chartStartedJob + " not found"}
	}
	var out []string
	if _, ok := wf.Env["GOFLAGS"]; ok {
		out = append(out, "the workflow sets GOFLAGS: it can add test flags the command line does not show")
	}
	if _, ok := j.Env["GOFLAGS"]; ok {
		out = append(out, "job "+chartStartedJob+" sets GOFLAGS: it can add test flags the command line does not show")
	}
	if chartIgnoresFailure(j.ContinueOnError) {
		out = append(out, "job "+chartStartedJob+" sets continue-on-error: its failure would not fail the check")
	}
	found := false
	var selector, pkg, tags string
	for _, s := range j.Steps {
		for _, line := range strings.Split(s.Run, "\n") {
			m := chartRunLine.FindStringSubmatch(line)
			if m == nil || found {
				continue
			}
			found = true
			selector, pkg = m[1], m[2]
			if bad := chartUnexpectedFlags(line); len(bad) != 0 {
				out = append(out, fmt.Sprintf("the started-chart command carries flags outside the allowlist %v: they can change what go test runs (-list runs nothing, -skip drops the test)", bad))
			}
			if tm := chartTagsFlag.FindStringSubmatch(line); tm != nil {
				tags = tm[1]
			}
			if _, ok := s.Env["GOFLAGS"]; ok {
				out = append(out, "the started-chart step sets GOFLAGS: it can add test flags the command line does not show")
			}
			if s.Env["CHART_STARTED_TEST_REQUIRED"] != "1" {
				out = append(out, `the started-chart step does not set CHART_STARTED_TEST_REQUIRED: "1" (a missing helm or docker would skip, not fail)`)
			}
			if s.If != nil {
				out = append(out, "the started-chart step has an if: condition ("+*s.If+"): it can be skipped")
			}
			if chartIgnoresFailure(s.ContinueOnError) {
				out = append(out, "the started-chart step sets continue-on-error: its failure would not fail the job")
			}
		}
	}
	if !found {
		return append(out, "job "+chartStartedJob+" has no line that is exactly `go test ... -run '<selector>' ./<package>` (a comment, an echo, `|| true` or a pipeline does not count)")
	}
	if !strings.HasPrefix(selector, "^") || !strings.HasSuffix(selector, "$") {
		out = append(out, fmt.Sprintf("selector %q is not anchored with ^...$: it can match more or fewer tests than the one it names", selector))
	}
	re, err := regexp.Compile(selector)
	if err != nil {
		return append(out, fmt.Sprintf("selector %q does not compile: %v", selector, err))
	}
	dir := filepath.Join(root, filepath.FromSlash(pkg))
	files, err := filepath.Glob(filepath.Join(dir, "*_test.go"))
	if err != nil || len(files) == 0 {
		return append(out, fmt.Sprintf("package ./%s has no *_test.go files under %s", pkg, root))
	}
	ctxt := build.Default
	ctxt.GOOS = "linux"
	ctxt.BuildTags = nil
	if tags != "" {
		ctxt.BuildTags = strings.Split(tags, ",")
	}
	var matched []string
	fset := token.NewFileSet()
	for _, f := range files {
		if included, err := ctxt.MatchFile(dir, filepath.Base(f)); err != nil || !included {
			continue
		}
		parsed, err := parser.ParseFile(fset, f, nil, 0)
		if err != nil {
			out = append(out, fmt.Sprintf("parse %s: %v", f, err))
			continue
		}
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if ok && fn.Recv == nil && strings.HasPrefix(fn.Name.Name, "Test") && re.MatchString(fn.Name.Name) {
				if !chartIsTestSignature(fn) {
					out = append(out, fmt.Sprintf("%s matches the selector but is not `func %s(t *testing.T)`: go test rejects it with a wrong-signature error", fn.Name.Name, fn.Name.Name))
					continue
				}
				matched = append(matched, fn.Name.Name)
			}
		}
	}
	if len(matched) != 1 {
		out = append(out, fmt.Sprintf("selector %q matches %d tests in ./%s (%v); want exactly 1: with 0 `go test` prints \"no tests to run\" and exits 0", selector, len(matched), pkg, matched))
	}
	return out
}

func TestChartStartedSelectorNamesExactlyOneRealTest(t *testing.T) {
	if got := chartStartedViolations(t, repoRoot(t)); len(got) != 0 {
		t.Fatalf("chart-started selector guard: %s", strings.Join(got, "; "))
	}
}

func TestChartStartedSelectorGuardFiresPerPlantedDefect(t *testing.T) {
	root := repoRoot(t)
	read := func(rel string) string {
		raw, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	workflow, testFile := read(chartStartedWorkflow), read(chartStartedTestFile)
	const name = "TestClickHouseConfigdStartsAndAnswersOnThePodAddress"
	for _, tc := range []struct {
		label        string
		workflowEdit [2]string
		testFileEdit [2]string
		want         string
	}{
		{label: "selector typo", workflowEdit: [2]string{"^" + name + "$", "^" + name + "s$"}, want: "matches 0 tests"},
		{label: "test renamed", testFileEdit: [2]string{"func " + name + "(", "func TestClickHouseConfigdStarts("}, want: "matches 0 tests"},
		{label: "selector unanchored", workflowEdit: [2]string{"'^" + name + "$'", "'ClickHouseConfigdStarts'"}, want: "not anchored"},
		{label: "selector widened to two tests", workflowEdit: [2]string{"'^" + name + "$'", "'^TestClickHouseConfigd.*$'"}, want: "matches 2 tests"},
		{label: "required env dropped", workflowEdit: [2]string{`CHART_STARTED_TEST_REQUIRED: "1"`, `CHART_STARTED_TEST_OPTIONAL: "1"`}, want: "CHART_STARTED_TEST_REQUIRED"},
		{label: "package path wrong", workflowEdit: [2]string{"./deploy/helm/dev-health", "./deploy/helm/missing"}, want: "no *_test.go files"},
		{label: "step removed", workflowEdit: [2]string{"go test -mod=readonly", "echo skipped -mod=readonly"}, want: "has no line that is exactly"},
		{label: "command commented out", workflowEdit: [2]string{"go test -mod=readonly", "# go test -mod=readonly"}, want: "has no line that is exactly"},
		{label: "failure swallowed by || true", workflowEdit: [2]string{"./deploy/helm/dev-health\n", "./deploy/helm/dev-health || true\n"}, want: "has no line that is exactly"},
		{label: "step gated by an if", workflowEdit: [2]string{"      - name: Run the started-chart test (ClickHouse config.d)\n", "      - name: Run the started-chart test (ClickHouse config.d)\n        if: github.event_name == 'never'\n"}, want: "can be skipped"},
		{label: "step failure ignored", workflowEdit: [2]string{"      - name: Run the started-chart test (ClickHouse config.d)\n", "      - name: Run the started-chart test (ClickHouse config.d)\n        continue-on-error: true\n"}, want: "the started-chart step sets continue-on-error"},
		{label: "job failure ignored", workflowEdit: [2]string{"  chart-started:\n    name: chart-started\n", "  chart-started:\n    name: chart-started\n    continue-on-error: true\n"}, want: "job chart-started sets continue-on-error"},
		{label: "matching test excluded by a build constraint", testFileEdit: [2]string{"package devhealth_test", "//go:build ignore\n\npackage devhealth_test"}, want: "matches 0 tests"},
		{label: "matching test needs a tag the step does not pass", testFileEdit: [2]string{"package devhealth_test", "//go:build integration\n\npackage devhealth_test"}, want: "matches 0 tests"},
		{label: "-list added: names printed, nothing run", workflowEdit: [2]string{"-count=1 -v -run", "-count=1 -v -list . -run"}, want: "flags outside the allowlist"},
		{label: "-skip added: the test is dropped", workflowEdit: [2]string{"-count=1 -v -run", "-count=1 -v -skip Starts -run"}, want: "flags outside the allowlist"},
		{label: "-bench added", workflowEdit: [2]string{"-count=1 -v -run", "-count=1 -v -bench=. -run"}, want: "flags outside the allowlist"},
		{label: "GOFLAGS injects a skip", workflowEdit: [2]string{"          CHART_STARTED_TEST_REQUIRED: \"1\"\n", "          CHART_STARTED_TEST_REQUIRED: \"1\"\n          GOFLAGS: -skip=Starts\n"}, want: "sets GOFLAGS"},
		{label: "matching function has the wrong signature", testFileEdit: [2]string{"func TestClickHouseConfigdStartsAndAnswersOnThePodAddress(t *testing.T) {", "func TestClickHouseConfigdStartsAndAnswersOnThePodAddress() {"}, want: "is not `func"},
	} {
		t.Run(tc.label, func(t *testing.T) {
			tree := t.TempDir()
			put := func(rel, body string) {
				p := filepath.Join(tree, filepath.FromSlash(rel))
				if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			wf, tf := workflow, testFile
			if e := tc.workflowEdit; e[0] != "" {
				if !strings.Contains(wf, e[0]) {
					t.Fatalf("the workflow no longer contains %q: the plant measures nothing", e[0])
				}
				wf = strings.Replace(wf, e[0], e[1], 1)
			}
			if e := tc.testFileEdit; e[0] != "" {
				if !strings.Contains(tf, e[0]) {
					t.Fatalf("the test file no longer contains %q: the plant measures nothing", e[0])
				}
				tf = strings.Replace(tf, e[0], e[1], 1)
			}
			put(chartStartedWorkflow, wf)
			put(chartStartedTestFile, tf)
			if tc.label == "selector widened to two tests" {
				put(chartStartedPackage+"/clickhouse_configd_render_test.go", "package helm_test\n\nfunc TestClickHouseConfigdRendersOne(t *testing.T) {}\n")
			}
			got := strings.Join(chartStartedViolations(t, tree), "; ")
			if !strings.Contains(got, tc.want) {
				t.Fatalf("planted %q: violations %q do not name %q", tc.label, got, tc.want)
			}
		})
	}
}

// chartIsTestSignature reports whether fn has the exact signature go test requires: one *testing.T parameter, no
// results, no type parameters.
func chartIsTestSignature(fn *ast.FuncDecl) bool {
	if fn.Type.TypeParams != nil && len(fn.Type.TypeParams.List) != 0 {
		return false
	}
	if fn.Type.Results != nil && len(fn.Type.Results.List) != 0 {
		return false
	}
	params := fn.Type.Params.List
	if len(params) != 1 || len(params[0].Names) > 1 {
		return false
	}
	star, ok := params[0].Type.(*ast.StarExpr)
	if !ok {
		return false
	}
	sel, ok := star.X.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	return ok && pkg.Name == "testing" && sel.Sel.Name == "T"
}

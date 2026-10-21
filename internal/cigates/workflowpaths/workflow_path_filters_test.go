package workflowpaths_test

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func repoRoot(t *testing.T) string {
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
			t.Fatal("go.mod not found above the test directory")
		}
		dir = parent
	}
}

const docsSiteConfig = "mkdocs.yml"

var docsOnlyPatterns = []string{"docs/**", "docs-data/**", "**/*.md", ".github/docs-legacy/**", docsSiteConfig}

func loadWorkflowMap(t *testing.T, name string) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), ".github", "workflows", name))
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	return doc
}

func triggerMap(t *testing.T, wf map[string]any, event string) map[string]any {
	t.Helper()
	on, ok := wf["on"].(map[string]any)
	if !ok {
		t.Fatal("workflow has no `on` map")
	}
	if on[event] == nil {
		if _, present := on[event]; present {
			return map[string]any{}
		}
		return nil
	}
	m, _ := on[event].(map[string]any)
	if m == nil {
		return map[string]any{}
	}
	return m
}

func stringList(t *testing.T, v any) []string {
	t.Helper()
	items, _ := v.([]any)
	out := make([]string, 0, len(items))
	for _, it := range items {
		out = append(out, it.(string))
	}
	return out
}

func requireNoPathFilter(t *testing.T, wf map[string]any, event string) {
	t.Helper()
	trig := triggerMap(t, wf, event)
	if trig == nil {
		t.Errorf("trigger %s is missing", event)
		return
	}
	for _, key := range []string{"paths", "paths-ignore"} {
		if _, bad := trig[key]; bad {
			t.Errorf("%s trigger must not carry %s: a path filter there would skip main, merge_group or the schedule", event, key)
		}
	}
}

func TestWorkflowPathFiltersCodeQLPullRequestIgnoresDocsOnly(t *testing.T) {
	wf := loadWorkflowMap(t, "codeql-analysis.yml")
	pr := triggerMap(t, wf, "pull_request")
	if pr == nil {
		t.Fatal("codeql-analysis.yml no longer runs on pull_request")
	}
	if _, bad := pr["paths"]; bad {
		t.Error("pull_request must use paths-ignore, not paths: an allowlist silently skips new code directories")
	}
	got := stringList(t, pr["paths-ignore"])
	if !reflect.DeepEqual(got, docsOnlyPatterns) {
		t.Errorf("pull_request.paths-ignore = %v, want %v", got, docsOnlyPatterns)
	}
}

func TestWorkflowPathFiltersCodeQLOtherTriggersUnfiltered(t *testing.T) {
	wf := loadWorkflowMap(t, "codeql-analysis.yml")
	for _, event := range []string{"push", "merge_group", "schedule"} {
		requireNoPathFilter(t, wf, event)
	}
	on := wf["on"].(map[string]any)
	if on["schedule"] == nil {
		t.Error("the weekly CodeQL schedule trigger was removed")
	}
}

// codeqlMatrixProblem is why a CodeQL language matrix is wrong, or "": the matrix is Go only. Any change of the
// configuration set is paired with the admin step on main's code-scanning analyses, because the code_scanning
// ruleset blocks every pull request while main holds a configuration the pull requests no longer upload.
func codeqlMatrixProblem(languages []string) string {
	if reflect.DeepEqual(languages, []string{"go"}) {
		return ""
	}
	return fmt.Sprintf("CodeQL language matrix = %v, want [go]: Python is out of the CodeQL scan, and a changed configuration set needs the admin step on main's code-scanning analyses, or the code_scanning ruleset blocks every pull request", languages)
}

func TestWorkflowPathFiltersCodeQLMatrixIsGoOnly(t *testing.T) {
	wf := loadWorkflowMap(t, "codeql-analysis.yml")
	job := wf["jobs"].(map[string]any)["analyze"].(map[string]any)
	matrix := job["strategy"].(map[string]any)["matrix"].(map[string]any)
	if problem := codeqlMatrixProblem(stringList(t, matrix["language"])); problem != "" {
		t.Error(problem)
	}
}

func TestWorkflowPathFiltersCodeQLMatrixPlantedLanguages(t *testing.T) {
	for _, planted := range [][]string{{"python", "go"}, {"go", "python"}, {"python"}, {}, {"go", "javascript"}} {
		if codeqlMatrixProblem(planted) == "" {
			t.Errorf("planted CodeQL matrix %v passed the Go-only rule", planted)
		}
	}
}

// docsWorkflowPathProblems is every way the paths of a docs workflow's events break the docs-only contract.
func docsWorkflowPathProblems(t *testing.T, file string, wf map[string]any, events []string) []string {
	t.Helper()
	var problems []string
	for _, event := range events {
		trig := triggerMap(t, wf, event)
		if trig == nil {
			problems = append(problems, file+": no "+event+" trigger")
			continue
		}
		paths := stringList(t, trig["paths"])
		if len(paths) == 0 {
			problems = append(problems, file+": "+event+" has no paths filter")
		}
		has := map[string]bool{}
		for _, p := range paths {
			has[p] = true
			if p == "**" || p == "*" {
				problems = append(problems, file+": "+event+" paths contains catch-all \""+p+"\"")
			}
		}
		for _, p := range paths {
			for _, bad := range []string{"src/", "internal/", "cmd/", "deploy/", "tests/", "web/"} {
				if strings.HasPrefix(p, bad) {
					problems = append(problems, file+": "+event+" paths contains code path \""+p+"\"")
				}
			}
		}
		for _, want := range []string{"docs/**", docsSiteConfig, "docs-data/**"} {
			if !has[want] {
				problems = append(problems, file+": "+event+" paths is missing "+want)
			}
		}
		if !has[".github/workflows/"+file] {
			problems = append(problems, file+": "+event+" paths must include the workflow file itself")
		}
	}
	return problems
}

func TestWorkflowPathFiltersDocsWorkflowsRunOnlyForDocsPaths(t *testing.T) {
	cases := []struct {
		file   string
		events []string
	}{
		{"docs-guards.yml", []string{"pull_request", "push"}},
		{"docs-cloudflare.yml", []string{"pull_request"}},
	}
	for _, c := range cases {
		for _, problem := range docsWorkflowPathProblems(t, c.file, loadWorkflowMap(t, c.file), c.events) {
			t.Error(problem)
		}
	}
}

// Planted: a code path added to docs-guards.yml must keep the docs-only contract red.
func TestDocsGuardsCodePathStaysRed(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), ".github", "workflows", "docs-guards.yml"))
	if err != nil {
		t.Fatal(err)
	}
	const anchor = "      - 'docs-data/**'\n"
	if strings.Count(string(raw), anchor) != 2 {
		t.Fatalf("docs-guards.yml no longer lists docs-data/** once per event; the plant cannot land")
	}
	planted := strings.ReplaceAll(string(raw), anchor, anchor+"      - 'internal/cigates/docstools/**'\n")
	var wf map[string]any
	if err := yaml.Unmarshal([]byte(planted), &wf); err != nil {
		t.Fatal(err)
	}
	problems := docsWorkflowPathProblems(t, "docs-guards.yml", wf, []string{"pull_request", "push"})
	if len(problems) != 2 || !strings.Contains(strings.Join(problems, "\n"), `contains code path "internal/cigates/docstools/**"`) {
		t.Fatalf("planted code path not caught for both events: %q", problems)
	}
}

var docsDriftPaths = []string{
	"docs/**",
	"mkdocs.yml",
	"docs/reference/limits-and-compatibility/provider-limits.md",
	"internal/jobs/workgraph/units/taxonomy.go",
	"deploy/go-workers/deployment.json",
	"contracts/jobs/v1/registry.json",
	"contracts/jobs/v1/migration-state.json",
	"internal/cigates/docstools/**",
	"cmd/cigate/**",
	"go.mod",
	"go.sum",
	".github/workflows/docs-drift.yml",
}

// docsDriftPathProblems compares each event's path list of a docs-drift workflow with the pinned list.
func docsDriftPathProblems(t *testing.T, wf map[string]any) []string {
	t.Helper()
	var problems []string
	for _, event := range []string{"push", "pull_request"} {
		trig := triggerMap(t, wf, event)
		if trig == nil {
			problems = append(problems, "docs-drift.yml: no "+event+" trigger")
			continue
		}
		if got := stringList(t, trig["paths"]); !reflect.DeepEqual(got, docsDriftPaths) {
			problems = append(problems, fmt.Sprintf("docs-drift.yml %s paths = %q, want %q", event, got, docsDriftPaths))
		}
	}
	return problems
}

// docs-drift.yml carries the docs drift checks and the provider-limits nav check with the paths their inputs live
// in (mkdocs.yml and the page are docs paths go.yml must not list); its list is pinned.
func TestDocsDriftWorkflowPathsArePinned(t *testing.T) {
	for _, problem := range docsDriftPathProblems(t, loadWorkflowMap(t, "docs-drift.yml")) {
		t.Error(problem)
	}
}

// Planted: dropping the mkdocs.yml row from docs-drift.yml must fail the pin for both events.
func TestDocsDriftPinFailsWithoutMkdocs(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), ".github", "workflows", "docs-drift.yml"))
	if err != nil {
		t.Fatal(err)
	}
	const row = "      - 'mkdocs.yml'\n"
	if strings.Count(string(raw), row) != 2 {
		t.Fatal("docs-drift.yml no longer lists mkdocs.yml once per event; the plant cannot land")
	}
	var wf map[string]any
	if err := yaml.Unmarshal([]byte(strings.ReplaceAll(string(raw), row, "")), &wf); err != nil {
		t.Fatal(err)
	}
	if problems := docsDriftPathProblems(t, wf); len(problems) != 2 {
		t.Fatalf("pin passed a docs-drift.yml without mkdocs.yml: %q", problems)
	}
}

func TestWorkflowPathFiltersSecurityScanGatesByJobNotWorkflow(t *testing.T) {
	wf := loadWorkflowMap(t, "security-scan.yml")
	for _, event := range []string{"push", "pull_request", "merge_group"} {
		requireNoPathFilter(t, wf, event)
	}
	jobs := wf["jobs"].(map[string]any)
	for name, want := range map[string]string{"semgrep": "needs.changes.outputs.code == 'true'"} {
		cond, _ := jobs[name].(map[string]any)["if"].(string)
		if !strings.Contains(cond, want) {
			t.Errorf("job %s no longer gates on %q: %q", name, want, cond)
		}
	}
	gl := jobs["gitleaks"].(map[string]any)
	if _, gated := gl["needs"]; gated {
		t.Error("gitleaks must stay ungated: a secret can sit in a doc")
	}
	if cond, _ := gl["if"].(string); strings.Contains(cond, "needs.changes") {
		t.Errorf("gitleaks must not depend on the changes job: %q", cond)
	}
}

func TestWorkflowPathFiltersSecurityScanCodeFilterExcludesDocsSiteConfig(t *testing.T) {
	wf := loadWorkflowMap(t, "security-scan.yml")
	steps := wf["jobs"].(map[string]any)["changes"].(map[string]any)["steps"].([]any)
	var filters string
	for _, st := range steps {
		with, _ := st.(map[string]any)["with"].(map[string]any)
		if f, ok := with["filters"].(string); ok {
			filters = f
		}
	}
	var parsed map[string][]string
	if err := yaml.Unmarshal([]byte(filters), &parsed); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, p := range parsed["code"] {
		if p == "!"+docsSiteConfig {
			found = true
		}
	}
	if !found {
		t.Errorf("the code filter does not exclude %s: %v", docsSiteConfig, parsed["code"])
	}
}

func TestWorkflowPathFiltersGoWorkflowDoesNotTriggerOnDocsSiteConfig(t *testing.T) {
	wf := loadWorkflowMap(t, "go.yml")
	for _, event := range []string{"push", "pull_request"} {
		for _, p := range stringList(t, triggerMap(t, wf, event)["paths"]) {
			if p == docsSiteConfig {
				t.Errorf("go.yml %s paths lists %s: a docs-only change would queue Go CI", event, docsSiteConfig)
			}
		}
	}
}

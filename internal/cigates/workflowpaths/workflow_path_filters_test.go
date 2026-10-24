package workflowpaths_test

import (
	"fmt"
	"os"
	"os/exec"
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

func codeqlJob(t *testing.T, name string) map[string]any {
	t.Helper()
	jobs, _ := loadWorkflowMap(t, "codeql-analysis.yml")["jobs"].(map[string]any)
	job, _ := jobs[name].(map[string]any)
	if job == nil {
		t.Fatalf("codeql-analysis.yml has no job %q", name)
	}
	return job
}

func TestWorkflowPathFiltersCodeQLPullRequestRunsEveryChange(t *testing.T) {
	wf := loadWorkflowMap(t, "codeql-analysis.yml")
	pr := triggerMap(t, wf, "pull_request")
	if pr == nil {
		t.Fatal("codeql-analysis.yml no longer runs on pull_request")
	}
	for _, key := range []string{"paths", "paths-ignore"} {
		if _, bad := pr[key]; bad {
			t.Errorf("pull_request must not carry %s: a skipped workflow uploads no CodeQL result and the code_scanning rule then blocks a docs-only pull request", key)
		}
	}
}

func TestWorkflowPathFiltersCodeQLDocsOnlyTwinAndGate(t *testing.T) {
	analyze := codeqlJob(t, "analyze")
	cond, _ := analyze["if"].(string)
	for _, want := range []string{"always()", "needs.changes.outputs.docs_only != 'true'"} {
		if !strings.Contains(cond, want) {
			t.Errorf("analyze.if lacks %q (fail closed: the real analysis runs unless docs_only is exactly true, even when `changes` failed): %s", want, cond)
		}
	}
	if strings.Contains(cond, "docs_only == 'false'") {
		t.Errorf("analyze.if must not require docs_only == 'false': an empty output must still analyze: %s", cond)
	}
	twin := codeqlJob(t, "analyze-docs-only")
	twinCond, _ := twin["if"].(string)
	for _, want := range []string{"github.event_name == 'pull_request' && needs.changes.result == 'success'", "needs.changes.outputs.docs_only == 'true'"} {
		if !strings.Contains(twinCond, want) {
			t.Errorf("analyze-docs-only.if lacks %q: %s", want, twinCond)
		}
	}
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), ".github", "workflows", "codeql-analysis.yml"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	if !strings.Contains(text, `"name":"CodeQL"`) {
		t.Error("the empty result must carry the CodeQL tool name the real Go leg uploads")
	}
	if strings.Count(text, `category: "/language:go"`) != 1 || !strings.Contains(text, `category: "/language:${{matrix.language}}"`) {
		t.Error("the empty result must upload under the category /language:go, the real leg's category for the go matrix entry")
	}
	for _, step := range twin["steps"].([]any) {
		name, _ := step.(map[string]any)["name"].(string)
		if !strings.Contains(name, "no code changed") {
			t.Errorf("analyze-docs-only step %q must say in its name that no code changed", name)
		}
	}
	changes := codeqlJob(t, "changes")
	for _, step := range changes["steps"].([]any) {
		m := step.(map[string]any)
		if with, ok := m["with"].(map[string]any); ok && strings.Contains(fmt.Sprint(with["ref"]), "base.sha") {
			return
		}
	}
	t.Error("changes must check the classifier script out from the pull request base commit, so a pull request cannot change the script that judges it")
}

func runDocsOnlyClassifier(t *testing.T, input string) string {
	t.Helper()
	cmd := exec.Command("bash", filepath.Join(repoRoot(t), "ci", "codeql_docs_only.sh"))
	cmd.Stdin = strings.NewReader(input)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("classifier failed: %v", err)
	}
	return strings.TrimSpace(string(out))
}

func TestCodeQLDocsOnlyClassifier(t *testing.T) {
	many := "3001\n" + strings.Repeat("docs/a.md\n", 3001)
	cases := []struct {
		name  string
		input string
		want  string
	}{
		{"markdown at the root (the 10950 shape)", "1\nci/bigboy/README.md\n", "true"},
		{"every docs pattern", "5\ndocs/a/b.png\ndocs-data/x.json\nREADME.md\n.github/docs-legacy/x.yml\nmkdocs.yml\n", "true"},
		{"a docs rename inside docs", "1\ndocs/new.md\tdocs/old.md\n", "true"},
		{"mixed docs and code runs the real scan", "2\ndocs/a.md\ninternal/x.go\n", "false"},
		{"a rename from code into docs: the source is code", "1\ndocs/x.md\tinternal/x.go\n", "false"},
		{"a rename from docs into code", "1\ninternal/x.go\tdocs/x.md\n", "false"},
		{"a workflow file is code", "2\ndocs/a.md\n.github/workflows/codeql-analysis.yml\n", "false"},
		{"the classifier script is code", "1\nci/codeql_docs_only.sh\n", "false"},
		{"nested mkdocs.yml is not the root file", "1\nsub/mkdocs.yml\n", "false"},
		{"a docs lookalike directory", "1\ndocsx/a.txt\n", "false"},
		{"truncation: fewer rows than changed_files", "3\ndocs/a.md\ndocs/b.md\n", "false"},
		{"truncation: more rows than changed_files", "1\ndocs/a.md\ndocs/b.md\n", "false"},
		{"over the 3000 file API cap", many, "false"},
		{"exactly at the cap needs matching rows", "3000\n" + strings.Repeat("docs/a.md\n", 3000), "true"},
		{"no files", "0\n", "false"},
		{"count missing", "docs/a.md\n", "false"},
		{"count not a number", "x\ndocs/a.md\n", "false"},
		{"count with leading sign", "+1\ndocs/a.md\n", "false"},
		{"empty input", "", "false"},
		{"a blank row", "2\ndocs/a.md\n\n", "false"},
		{"no trailing newline", "1\ndocs/a.md", "true"},
	}
	for _, c := range cases {
		if got := runDocsOnlyClassifier(t, c.input); got != c.want {
			t.Errorf("%s: classifier = %q, want %q", c.name, got, c.want)
		}
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

var deletedPythonManifests = []string{"uv.lock", "pyproject.toml", "requirements.txt", "pytest.ini"}

func deletedManifestPathProblems(t *testing.T, wf map[string]any) []string {
	t.Helper()
	var problems []string
	for _, event := range []string{"push", "pull_request"} {
		trig := triggerMap(t, wf, event)
		for _, key := range []string{"paths", "paths-ignore"} {
			for _, p := range stringList(t, trig[key]) {
				for _, name := range deletedPythonManifests {
					if strings.TrimPrefix(p, "/") == name {
						problems = append(problems, fmt.Sprintf("%s %s lists deleted Python manifest %s", event, key, name))
					}
				}
			}
		}
	}
	return problems
}

func TestWorkflowPathFiltersGoWorkflowNamesNoDeletedPythonManifest(t *testing.T) {
	if problems := deletedManifestPathProblems(t, loadWorkflowMap(t, "go.yml")); len(problems) != 0 {
		t.Errorf("go.yml path filters name deleted Python manifests: %v", problems)
	}
}

func TestWorkflowPathFiltersDeletedPythonManifestPlants(t *testing.T) {
	for _, name := range deletedPythonManifests {
		for _, event := range []string{"push", "pull_request"} {
			for _, key := range []string{"paths", "paths-ignore"} {
				wf := map[string]any{"on": map[string]any{
					event: map[string]any{key: []any{"internal/**", name}},
				}}
				if len(deletedManifestPathProblems(t, wf)) != 1 {
					t.Errorf("planted %s in %s %s was not refused", name, event, key)
				}
			}
		}
	}
	clean := map[string]any{"on": map[string]any{"push": map[string]any{"paths": []any{"internal/**", "go.mod"}}}}
	if got := deletedManifestPathProblems(t, clean); len(got) != 0 {
		t.Errorf("clean filter refused: %v", got)
	}
}

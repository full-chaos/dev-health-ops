package workflowpaths_test

import (
	"encoding/json"
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
	for _, want := range []string{"!cancelled()", "needs.changes.outputs.docs_only != 'true'"} {
		if !strings.Contains(cond, want) {
			t.Errorf("analyze.if lacks %q (fail closed: the real analysis runs unless docs_only is exactly true, even when `changes` failed): %s", want, cond)
		}
	}
	if strings.Contains(cond, "always()") {
		t.Errorf("analyze.if must use !cancelled(), not always(), so a cancelled run does not start the analysis: %s", cond)
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
	matrixJob := codeqlJob(t, "analyze")["strategy"].(map[string]any)["matrix"].(map[string]any)
	languages := stringList(t, matrixJob["language"])
	if len(languages) != 1 || !strings.Contains(text, `category: "/language:`+languages[0]+`"`) {
		t.Errorf("the empty result must upload one category per matrix language, same list: matrix %v, text lacks the category of the only language", languages)
	}
	for _, step := range twin["steps"].([]any) {
		name, _ := step.(map[string]any)["name"].(string)
		if !strings.Contains(name, "no code changed") {
			t.Errorf("analyze-docs-only step %q must say in its name that no code changed", name)
		}
	}
	if perms, _ := twin["permissions"].(map[string]any); perms["security-events"] != "write" {
		t.Errorf("analyze-docs-only needs security-events: write to upload, got %v", twin["permissions"])
	}
	var upload map[string]any
	var sarifRun string
	for _, step := range twin["steps"].([]any) {
		m := step.(map[string]any)
		if uses, _ := m["uses"].(string); strings.HasPrefix(uses, "github/codeql-action/upload-sarif@") {
			upload, _ = m["with"].(map[string]any)
		}
		if run, _ := m["run"].(string); strings.Contains(run, "empty.sarif") {
			sarifRun = run
		}
	}
	if upload == nil || upload["sarif_file"] != "empty.sarif" {
		t.Errorf("analyze-docs-only must upload empty.sarif with github/codeql-action/upload-sarif, got %v", upload)
	}
	start, end := strings.Index(sarifRun, "'{"), strings.LastIndex(sarifRun, "}'")
	if start < 0 || end < start {
		t.Fatalf("no SARIF literal in the write step: %q", sarifRun)
	}
	var sarif struct {
		Version string `json:"version"`
		Runs    []struct {
			Tool struct {
				Driver struct{ Name string } `json:"driver"`
			} `json:"tool"`
			Results []any `json:"results"`
		} `json:"runs"`
	}
	if err := json.Unmarshal([]byte(sarifRun[start+1:end+1]), &sarif); err != nil {
		t.Fatalf("the empty SARIF is not valid JSON: %v", err)
	}
	if sarif.Version != "2.1.0" || len(sarif.Runs) != 1 || sarif.Runs[0].Tool.Driver.Name != "CodeQL" || len(sarif.Runs[0].Results) != 0 {
		t.Errorf("the empty SARIF must be version 2.1.0 with one CodeQL run and no results: %+v", sarif)
	}
	changes := codeqlJob(t, "changes")
	var classify map[string]any
	for _, step := range changes["steps"].([]any) {
		if m := step.(map[string]any); m["id"] == "classify" {
			classify, _ = m["env"].(map[string]any)
		}
	}
	if classify["BASE_SHA"] != "${{ github.event.pull_request.base.sha }}" || classify["HEAD_SHA"] != "${{ github.event.pull_request.head.sha }}" {
		t.Errorf("the classifier must be given the event's own base and head commits, got %v", classify)
	}
	for _, step := range changes["steps"].([]any) {
		m := step.(map[string]any)
		if with, ok := m["with"].(map[string]any); ok && strings.Contains(fmt.Sprint(with["ref"]), "base.sha") {
			return
		}
	}
	t.Error("changes must check the classifier script out from the pull request base commit, so a pull request cannot change the script that judges it")
}

type gitRepo struct {
	t   *testing.T
	dir string
}

func newGitRepo(t *testing.T) *gitRepo {
	t.Helper()
	r := &gitRepo{t: t, dir: t.TempDir()}
	r.git("init", "-q", "-b", "main")
	return r
}

func (r *gitRepo) git(args ...string) string {
	r.t.Helper()
	cmd := exec.Command("git", append([]string{"-c", "user.name=t", "-c", "user.email=t@example.com", "-c", "commit.gpgsign=false"}, args...)...)
	cmd.Dir = r.dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		r.t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

// commit writes the files (path -> content; "" deletes), commits and returns the sha.
func (r *gitRepo) commit(files map[string]string) string {
	r.t.Helper()
	for p, content := range files {
		full := filepath.Join(r.dir, p)
		if content == "" {
			_ = os.Remove(full)
			continue
		}
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			r.t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			r.t.Fatal(err)
		}
	}
	r.git("add", "-A")
	r.git("commit", "-q", "-m", "c", "--allow-empty")
	return r.git("rev-parse", "HEAD")
}

func (r *gitRepo) classify(base, head string) string {
	r.t.Helper()
	cmd := exec.Command("bash", filepath.Join(repoRoot(r.t), "ci", "codeql_docs_only.sh"), base, head)
	cmd.Dir = r.dir
	out, err := cmd.Output()
	if err != nil {
		r.t.Fatalf("classifier failed: %v", err)
	}
	return strings.TrimSpace(string(out))
}

// seedGoYml is the smallest base go.yml the classifier parses: its on.push.paths are the Go test inputs that are never
// docs-only. ci/bigboy/** stands in for the real list's Go-read docs paths.
const seedGoYml = "name: Go\non:\n  push:\n    branches: [main]\n    paths:\n      # a comment line is skipped\n      - '**/*.go'\n      - 'ci/bigboy/**'\n      - 'docs/go-*.md'\n  pull_request:\n    paths:\n      - 'docs/**'\n"

func seedFiles() map[string]string {
	return map[string]string{"seed.txt": "s", ".github/workflows/go.yml": seedGoYml}
}

func TestCodeQLDocsOnlyClassifier(t *testing.T) {
	cases := []struct {
		name  string
		setup map[string]string
		pr    map[string]string
		want  string
	}{
		{"markdown in a directory (the 10950 shape)", map[string]string{"ci/other/README.md": "a"}, map[string]string{"ci/other/README.md": "b"}, "true"},
		{"a docs path the base go.yml lists as a Go input (the 10950 file: ci/bigboy/**)", map[string]string{"ci/bigboy/README.md": "a"}, map[string]string{"ci/bigboy/README.md": "b"}, "false"},
		{"a docs/ file a single-star Go input glob matches", nil, map[string]string{"docs/go-migration-matrix.md": "x"}, "false"},
		{"a single star does not cross a directory", nil, map[string]string{"docs/go-x/a.md": "x"}, "true"},
		{"pull_request paths are not read (push paths are the Go inputs)", nil, map[string]string{"docs/a.md": "x"}, "true"},
		{"every docs pattern", nil, map[string]string{"docs/a/b.png": "x", "docs-data/x.json": "x", "README.md": "x", ".github/docs-legacy/x.yml": "x", "mkdocs.yml": "x"}, "true"},
		{"mixed docs and code runs the real scan", nil, map[string]string{"docs/a.md": "x", "internal/x.go": "x"}, "false"},
		{"a code file renamed into docs: the source is code", map[string]string{"internal/x.go": "package x\n// enough content to be detected as a rename\n"}, map[string]string{"internal/x.go": "", "docs/x.md": "package x\n// enough content to be detected as a rename\n"}, "false"},
		{"a workflow file is code", nil, map[string]string{"docs/a.md": "x", ".github/workflows/codeql-analysis.yml": "x"}, "false"},
		{"the classifier script is code", nil, map[string]string{"ci/codeql_docs_only.sh": "x"}, "false"},
		{"nested mkdocs.yml is not the root file", nil, map[string]string{"sub/" + docsSiteConfig: "x"}, "false"},
		{"a docs lookalike directory", nil, map[string]string{"docsx/a.txt": "x"}, "false"},
		{"an empty diff", nil, map[string]string{}, "false"},
		{"Go source under docs/ is still Go", nil, map[string]string{"docs/pwn.go": "package pwn\n"}, "false"},
		{"a Go module file under docs/", nil, map[string]string{"docs/go.mod": "module x\n"}, "false"},
		{"a nested Go work file", nil, map[string]string{"docs/sub/go.work": "go 1.22\n"}, "false"},
		{"one path holding a newline is one path", nil, map[string]string{docsSiteConfig + "\ndocs/x.txt": "x"}, "false"},
		{"a Go test file named like markdown", nil, map[string]string{"docs/a.md.go": "x"}, "false"},
	}
	for _, c := range cases {
		r := newGitRepo(t)
		base := r.commit(seedFiles())
		if c.setup != nil {
			base = r.commit(c.setup)
		}
		head := r.commit(c.pr)
		if got := r.classify(base, head); got != c.want {
			t.Errorf("%s: classifier = %q, want %q", c.name, got, c.want)
		}
	}
}

// A push after a run started: head A is docs-only, head B adds code. The verdict is a function of the commit the
// run was given, so the run for B can never read A's file list.
func TestCodeQLDocsOnlyClassifierIsPinnedToTheEventHead(t *testing.T) {
	r := newGitRepo(t)
	base := r.commit(seedFiles())
	headA := r.commit(map[string]string{"docs/a.md": "a"})
	headB := r.commit(map[string]string{"internal/x.go": "package x\n"})
	if got := r.classify(base, headA); got != "true" {
		t.Errorf("head A (docs only) = %q, want true", got)
	}
	if got := r.classify(base, headB); got != "false" {
		t.Errorf("head B (docs then code) = %q, want false: the run for B must not inherit A's verdict", got)
	}
}

// A git that prints a docs path and then fails must not read as docs-only.
func TestCodeQLDocsOnlyClassifierFailsClosedWhenGitFailsAfterOutput(t *testing.T) {
	r := newGitRepo(t)
	base := r.commit(seedFiles())
	head := r.commit(map[string]string{"docs/a.md": "a"})
	bin := t.TempDir()
	fake := "#!/bin/sh\nprintf 'docs/a.md\\0'\nexit 1\n"
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte(fake), 0o755); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("bash", filepath.Join(repoRoot(t), "ci", "codeql_docs_only.sh"), base, head)
	cmd.Dir = r.dir
	cmd.Env = append(os.Environ(), "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("classifier failed: %v", err)
	}
	if got := strings.TrimSpace(string(out)); got != "false" {
		t.Errorf("git failing after printing a docs path = %q, want false", got)
	}
	cmd = exec.Command("bash", filepath.Join(repoRoot(t), "ci", "codeql_docs_only.sh"), base, head)
	cmd.Dir = r.dir
	if out, _ := cmd.Output(); strings.TrimSpace(string(out)) != "true" {
		t.Errorf("control: the real git on the same input = %q, want true", out)
	}
}

func TestCodeQLDocsOnlyClassifierFailsClosedOnBadInput(t *testing.T) {
	r := newGitRepo(t)
	base := r.commit(seedFiles())
	head := r.commit(map[string]string{"docs/a.md": "a"})
	for name, args := range map[string][]string{
		"no arguments":        {},
		"one argument":        {base},
		"three arguments":     {base, head, head},
		"a short sha":         {base[:12], head},
		"a ref name":          {"main", head},
		"an option":           {"--output=/tmp/x", head},
		"an unknown commit":   {strings.Repeat("0", 40), head},
		"upper case hex":      {strings.ToUpper(base), head},
		"a sha plus a suffix": {base + "x", head},
	} {
		cmd := exec.Command("bash", append([]string{filepath.Join(repoRoot(t), "ci", "codeql_docs_only.sh")}, args...)...)
		cmd.Dir = r.dir
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("%s: classifier failed: %v", name, err)
		}
		if got := strings.TrimSpace(string(out)); got != "false" {
			t.Errorf("%s: classifier = %q, want false", name, got)
		}
	}
	if got := r.classify(base, head); got != "true" {
		t.Errorf("control: valid docs-only input = %q, want true", got)
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

// A pull request gets its preview from docs-guards.yml (job docs-preview); a second pull_request build here would build the
// site twice per revision.
func TestDocsCloudflareWorkflowHasNoPullRequestTrigger(t *testing.T) {
	if triggerMap(t, loadWorkflowMap(t, "docs-cloudflare.yml"), "pull_request") != nil {
		t.Error("docs-cloudflare.yml must not run on pull_request: docs-guards.yml builds and checks the site once per revision")
	}
}

func TestWorkflowPathFiltersDocsWorkflowsRunOnlyForDocsPaths(t *testing.T) {
	cases := []struct {
		file   string
		events []string
	}{
		{"docs-guards.yml", []string{"pull_request", "push"}},
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

package workflowpaths_test

import (
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

func TestWorkflowPathFiltersCodeQLMatrixKeepsEveryMainConfiguration(t *testing.T) {
	wf := loadWorkflowMap(t, "codeql-analysis.yml")
	job := wf["jobs"].(map[string]any)["analyze"].(map[string]any)
	matrix := job["strategy"].(map[string]any)["matrix"].(map[string]any)
	got := stringList(t, matrix["language"])
	if !reflect.DeepEqual(got, []string{"python", "go"}) {
		t.Errorf("CodeQL language matrix = %v, want [python go]: dropping a configuration that main has makes the code_scanning ruleset block every pull request until its stale analyses are deleted", got)
	}
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
		wf := loadWorkflowMap(t, c.file)
		for _, event := range c.events {
			trig := triggerMap(t, wf, event)
			if trig == nil {
				t.Errorf("%s: no %s trigger", c.file, event)
				continue
			}
			paths := stringList(t, trig["paths"])
			if len(paths) == 0 {
				t.Errorf("%s: %s has no paths filter", c.file, event)
			}
			has := map[string]bool{}
			for _, p := range paths {
				has[p] = true
				if p == "**" || p == "*" {
					t.Errorf("%s: %s paths contains catch-all %q", c.file, event, p)
				}
			}
			for _, p := range paths {
				for _, bad := range []string{"src/", "internal/", "cmd/", "deploy/", "tests/", "web/"} {
					if strings.HasPrefix(p, bad) {
						t.Errorf("%s: %s paths contains code path %q", c.file, event, p)
					}
				}
			}
			for _, want := range []string{"docs/**", docsSiteConfig, "docs-data/**"} {
				if !has[want] {
					t.Errorf("%s: %s paths is missing %s", c.file, event, want)
				}
			}
			if !has[".github/workflows/"+c.file] {
				t.Errorf("%s: %s paths must include the workflow file itself", c.file, event)
			}
		}
	}
}

func TestWorkflowPathFiltersSecurityScanGatesByJobNotWorkflow(t *testing.T) {
	wf := loadWorkflowMap(t, "security-scan.yml")
	for _, event := range []string{"push", "pull_request", "merge_group"} {
		requireNoPathFilter(t, wf, event)
	}
	jobs := wf["jobs"].(map[string]any)
	for name, want := range map[string]string{"pip-audit": "needs.changes.outputs.deps == 'true'", "semgrep": "needs.changes.outputs.code == 'true'"} {
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

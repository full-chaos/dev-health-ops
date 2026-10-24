package workflows_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// auth-changes' relevance step (acp 5a): a docs-only pull request or merge queue entry is not relevant; a merge queue
// entry otherwise goes through ci/auth_queue_relevance.sh (whole group, fail closed); a push to main is relevant, forced.

func authRelevanceStep(t *testing.T, root string) yamlMap {
	t.Helper()
	step, n := stepNamed(asMap(jobsOf(loadWf(t, root, "go-auth-service.yml"))["auth-changes"]), "Does the change touch the auth service tree?")
	if n != 1 {
		t.Fatalf("auth-changes has %d relevance steps, want 1", n)
	}
	return step
}

func TestAuthRelevanceStepWiring(t *testing.T) {
	root := repoRoot(t)
	step := authRelevanceStep(t, root)
	env := asMap(step["env"])
	for k, want := range map[string]string{
		"EVENT":       "${{ github.event_name }}",
		"MG_BASE_REF": "${{ github.event.merge_group.base_ref }}",
		"MG_HEAD_SHA": "${{ github.event.merge_group.head_sha }}",
		"DOCS_ONLY":   "${{ steps.docs.outputs.docs_only }}",
	} {
		if got := asString(env[k]); got != want {
			t.Errorf("relevance step env %s = %q, want %q", k, got, want)
		}
	}
	run := asString(step["run"])
	for _, want := range []string{
		`bash ci/auth_queue_relevance.sh "${tip}" "${MG_HEAD_SHA}" > relevance.txt || echo "relevant=true" > relevance.txt`,
		`git fetch --no-tags origin "+${MG_BASE_REF}:refs/remotes/origin/auth-relevance-base"`,
	} {
		if !strings.Contains(run, want) {
			t.Errorf("relevance step does not contain %q", want)
		}
	}
	if strings.Contains(run, "merge_group.base_sha") || strings.Contains(fmt.Sprint(env), "base_sha") {
		t.Error("the relevance step must not judge from merge_group.base_sha (the parent entry): that is one PR, not the group")
	}
}

// runAuthRelevanceStep executes the step text in a repo with no origin, so every merge_group fetch fails.
func runAuthRelevanceStep(t *testing.T, env map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	if out, err := exec.Command("git", "-C", dir, "init", "-q").CombinedOutput(); err != nil {
		t.Fatalf("git init: %v\n%s", err, out)
	}
	output := filepath.Join(t.TempDir(), "output")
	cmd := exec.Command("bash", "-e", "-c", asString(authRelevanceStep(t, repoRoot(t))["run"]))
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GITHUB_OUTPUT="+output)
	for _, k := range []string{"EVENT", "BASE_SHA", "HEAD_SHA", "MG_BASE_REF", "MG_HEAD_SHA", "DOCS_ONLY"} {
		cmd.Env = append(cmd.Env, k+"="+env[k])
	}
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("the relevance step failed: %v\n%s", err, out)
	}
	written, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(written))
}

func TestAuthRelevanceStepBehaviour(t *testing.T) {
	head := strings.Repeat("b", 40)
	for _, c := range []struct {
		name string
		env  map[string]string
		want string
	}{
		{"a push to main is relevant, forced", map[string]string{"EVENT": "push", "DOCS_ONLY": "false"}, "relevant=true"},
		{"a push ignores a docs-only verdict", map[string]string{"EVENT": "push", "DOCS_ONLY": "true"}, "relevant=true"},
		{"a docs-only pull request is not relevant", map[string]string{"EVENT": "pull_request", "DOCS_ONLY": "true"}, "relevant=false"},
		{"a docs-only merge queue entry is not relevant", map[string]string{"EVENT": "merge_group", "DOCS_ONLY": "true", "MG_BASE_REF": "refs/heads/main", "MG_HEAD_SHA": head}, "relevant=false"},
		{"a merge queue entry whose target branch cannot be fetched runs the gates", map[string]string{"EVENT": "merge_group", "DOCS_ONLY": "false", "MG_BASE_REF": "refs/heads/main", "MG_HEAD_SHA": head}, "relevant=true"},
		{"a merge queue entry with a non-branch base ref runs the gates", map[string]string{"EVENT": "merge_group", "DOCS_ONLY": "false", "MG_BASE_REF": "main", "MG_HEAD_SHA": head}, "relevant=true"},
		{"a manual run is relevant", map[string]string{"EVENT": "workflow_dispatch", "DOCS_ONLY": "false"}, "relevant=true"},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := runAuthRelevanceStep(t, c.env); got != c.want {
				t.Errorf("GITHUB_OUTPUT %q, want %q", got, c.want)
			}
		})
	}
}

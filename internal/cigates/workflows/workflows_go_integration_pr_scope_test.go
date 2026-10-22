package workflows_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// CHAOS-10700: the integration shards run on the main push; a pull request runs them only for a trigger path or the
// ci:integration label. These pins keep the skip wired into the plan job without renaming a pinned job.
func TestIntegrationShardsSkipOnPullRequestWiring(t *testing.T) {
	root := rootOf(t)
	raw, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "go.yml"))
	if err != nil {
		t.Fatal(err)
	}
	doc := loadWf(t, root, "go.yml")
	step, count := stepNamed(pinJob(doc, "go-storage-integration-plan"), "Select the integration packages the change can affect")
	if count != 1 {
		t.Fatal("the plan job lost its selection step")
	}
	run := asString(step["run"])
	for _, want := range []string{
		"bash ci/integration_pr_scope.sh",
		`printf "NONE\n"`,
		"plan-verdict --event",
	} {
		if !strings.Contains(run, want) {
			t.Errorf("selection step does not contain %q", want)
		}
	}
	if strings.Index(run, "integration_pr_scope.sh") > strings.Index(run, "integration-coverage") {
		t.Error("the scope decision must come before the planner work")
	}
	env, _ := step["env"].(yamlMap)
	if !strings.Contains(asString(env["PR_LABELS_JSON"]), "pull_request.labels") {
		t.Error("the selection step does not pass the pull request labels")
	}
	if step["if"] != "github.event_name == 'pull_request'" {
		t.Errorf("selection step if %v: the main push must never be judged", step["if"])
	}
	text := string(raw)
	if strings.Count(text, "- 'ci/integration_pr_scope.sh'") != 2 {
		t.Error("ci/integration_pr_scope.sh must be in both the push and pull_request path filters")
	}
	if strings.Contains(text, "types: [opened, synchronize, reopened, ready_for_review, labeled") {
		t.Error("a labeled trigger reruns the whole workflow on every label change; the label applies at the next push")
	}
	if !strings.Contains(text, "cancel-in-progress: ${{ github.event_name == 'pull_request' || (github.event_name == 'push' && github.ref != 'refs/heads/main') }}") {
		t.Error("a main push run must never be cancelled in progress: every merge sha needs its own integration verdict")
	}
}

// Executes the real selection step text with a stub changed-file producer and a stub check_go.sh that fails, so any
// path that reaches the planner shows as FULL and a skip shows as NONE.
func TestIntegrationShardsSelectionStepBehaviour(t *testing.T) {
	root := rootOf(t)
	doc := loadWf(t, root, "go.yml")
	step, count := stepNamed(pinJob(doc, "go-storage-integration-plan"), "Select the integration packages the change can affect")
	if count != 1 {
		t.Fatal("the plan job lost its selection step")
	}
	scope, err := os.ReadFile(filepath.Join(root, "ci", "integration_pr_scope.sh"))
	if err != nil {
		t.Fatal(err)
	}
	rows := []struct{ name, list, labelsJSON, want string }{
		{"leaf go file is NONE", "internal/foo/bar.go\\0", "[]", "NONE"},
		{"migration reaches the planner", "internal/pgmigrate/sql/1.sql\\0", "[]", "FULL"},
		{"label reaches the planner", "internal/foo/bar.go\\0", `["ci:integration"]`, "FULL"},
		{"malformed labels fall through", "internal/foo/bar.go\\0", "{bad", "FULL"},
		{"unterminated list falls through", "docs/a.md\\0internal/chmigrate/x.go", "[]", "FULL"},
	}
	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			dir := t.TempDir()
			ci := filepath.Join(dir, "ci")
			if err := os.MkdirAll(ci, 0o755); err != nil {
				t.Fatal(err)
			}
			files := map[string]string{
				"integration_pr_scope.sh": string(scope),
				"go_relevant_diff.sh":     "printf '" + r.list + "'\n",
				"check_go.sh":             "exit 1\n",
			}
			for n, c := range files {
				if err := os.WriteFile(filepath.Join(ci, n), []byte(c), 0o755); err != nil {
					t.Fatal(err)
				}
			}
			tmp := t.TempDir()
			cmd := exec.Command("bash", "-c", asString(step["run"]))
			cmd.Dir = dir
			cmd.Env = append(os.Environ(), "RUNNER_TEMP="+tmp, "GITHUB_ENV="+filepath.Join(tmp, "env"), "GITHUB_WORKSPACE="+dir,
				"BASE_SHA=0000", "EVENT_NAME=pull_request", "PR_LABELS_JSON="+r.labelsJSON)
			out, _ := cmd.CombinedOutput()
			got, err := os.ReadFile(filepath.Join(tmp, "affected.txt"))
			if err != nil {
				t.Fatalf("no affected.txt: %v\n%s", err, out)
			}
			if !strings.HasPrefix(string(got), r.want) {
				t.Fatalf("affected.txt %q, want prefix %q\n%s", got, r.want, out)
			}
		})
	}
}

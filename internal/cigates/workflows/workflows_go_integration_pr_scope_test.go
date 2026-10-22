package workflows_test

import (
	"os"
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

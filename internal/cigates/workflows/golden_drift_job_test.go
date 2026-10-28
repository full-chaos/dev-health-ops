package workflows_test

import (
	"strings"
	"testing"
)

// The golden-drift check is ADVISORY: a pull_request-only job that go-queue-gate does not wait for, so it is never a
// required status and never runs in the merge queue.
func TestGoldenDriftJobIsAdvisoryAndPullRequestOnly(t *testing.T) {
	root := rootOf(t)
	doc := loadWf(t, root, "go.yml")
	job := pinJob(doc, "go-golden-drift")
	if job == nil {
		t.Fatal("go.yml lost the go-golden-drift job")
	}
	cond := asString(job["if"])
	if !strings.Contains(cond, "github.event_name == 'pull_request'") {
		t.Errorf("job if %q must be pull_request only", cond)
	}
	if !strings.Contains(cond, "github.event.pull_request.head.repo.full_name == github.repository") {
		t.Errorf("job if %q must keep the same-repository predicate (a fork pull request has no registry login secret)", cond)
	}
	if strings.Contains(cond, "merge_group") {
		t.Errorf("job if %q must not name merge_group", cond)
	}
	if strings.Contains(cond, "ci:full") {
		t.Errorf("job if %q must not name ci:full (the ci:full-gated job set is pinned); the select step reads the labels", cond)
	}
	for _, need := range pinNeeds(pinJob(doc, "go-queue-gate")) {
		if need == "go-golden-drift" {
			t.Error("go-queue-gate must not wait for go-golden-drift: the job is advisory")
		}
	}
	sel, n := stepNamed(job, "Select the golden tests the change can reach")
	if n != 1 {
		t.Fatal("job lost its selection step")
	}
	run := asString(sel["run"])
	for _, want := range []string{"ci/go_relevant_diff.sh", "ci/check_golden_drift.sh select", "ci:integration", "ci:full"} {
		if !strings.Contains(run, want) {
			t.Errorf("selection step lacks %q", want)
		}
	}
	if strings.Contains(run, "| tee") {
		t.Error("the select step must not pipe the selector into tee: write the rows to a file, then cat it, so the selector's status decides the step")
	}
	for _, line := range strings.Split(run, "\n") {
		if strings.Contains(line, "check_golden_drift.sh select") && strings.Contains(line, "||") {
			t.Errorf("the select call must not swallow its status: %q", line)
		}
	}
	for _, name := range []string{"Select the golden tests the change can reach", "Run the selected frozen-Python golden tests"} {
		st, _ := stepNamed(job, name)
		if strings.Contains(asString(st["run"]), "go test") && !strings.Contains(asString(st["run"]), "-count=1") {
			t.Errorf("step %q runs go test without -count=1", name)
		}
	}
	runStep, n := stepNamed(job, "Run the selected frozen-Python golden tests")
	if n != 1 || !strings.Contains(asString(runStep["run"]), "ci/check_golden_drift.sh run") {
		t.Fatal("job lost its run step")
	}
	if !strings.Contains(asString(runStep["if"]), "steps.select.outputs.rows") {
		t.Errorf("run step if %q must skip when no row is selected", runStep["if"])
	}
}

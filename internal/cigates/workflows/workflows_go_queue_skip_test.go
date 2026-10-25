package workflows_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const queueSkipStep = "Skip the integration shards on a main push that the merge queue already tested"

func TestMainPushShardSkipIsKeyedOnAGreenQueueRun(t *testing.T) {
	root := rootOf(t)
	doc := loadWf(t, root, "go.yml")
	job := pinJob(doc, "go-storage-integration-plan")
	step, count := stepNamed(job, queueSkipStep)
	if count != 1 {
		t.Fatalf("the plan job has %d steps named %q, want 1", count, queueSkipStep)
	}
	condition := asString(step["if"])
	for _, want := range []string{"github.event_name == 'push'", "vars.MERGE_QUEUE == 'enabled'"} {
		if !strings.Contains(condition, want) {
			t.Errorf("skip step if %q lacks %q", condition, want)
		}
	}
	run := asString(step["run"])
	if !strings.Contains(run, `if bash ci/queue_tested_sha.sh "${GITHUB_REPOSITORY}" "${GITHUB_SHA}"; then`) {
		t.Error("the skip must depend on ci/queue_tested_sha.sh for the pushed sha")
	}
	if strings.Index(run, "queue_tested_sha.sh") > strings.Index(run, "NONE") {
		t.Error("NONE must be written only after the queue check")
	}
	env, _ := step["env"].(yamlMap)
	if asString(env["GH_TOKEN"]) != "${{ github.token }}" {
		t.Error("the skip step needs the workflow token to read the merge_group runs")
	}
	permissions, _ := job["permissions"].(yamlMap)
	if asString(permissions["actions"]) != "read" {
		t.Error("the plan job needs actions: read to list the merge_group runs")
	}
	var names []string
	for _, s := range stepsOf(job) {
		names = append(names, asString(s["name"]))
	}
	skip, plan := -1, -1
	for index, name := range names {
		switch name {
		case queueSkipStep:
			skip = index
		case "Validate and emit deterministic integration shards":
			plan = index
		}
	}
	if skip < 0 || plan < 0 || skip > plan {
		t.Errorf("the skip step must run before the plan step: skip=%d plan=%d", skip, plan)
	}
}

func queueRunsJSON(sha string, runs ...[2]string) string {
	var entries []string
	for _, run := range runs {
		entries = append(entries, fmt.Sprintf(`{"id":%s,"head_sha":"%s","conclusion":"%s"}`, run[0], sha, run[1]))
	}
	return `{"workflow_runs":[` + strings.Join(entries, ",") + `]}`
}

func queueJobsJSON(conclusions ...string) string {
	entries := []string{
		`{"name":"go-storage-integration-plan","conclusion":"success"}`,
		`{"name":"go-storage-integration","conclusion":"success"}`,
		`{"name":"go-quality","conclusion":"failure"}`,
	}
	for index, conclusion := range conclusions {
		entries = append(entries, fmt.Sprintf(`{"name":"go-storage-integration-shard-packages-%d","conclusion":"%s"}`, index+1, conclusion))
	}
	return `{"jobs":[` + strings.Join(entries, ",") + `]}`
}

const queueGhStub = `#!/usr/bin/env bash
set -u
filter=""; url=""; prev=""
for a in "$@"; do
  [ "$prev" = "--jq" ] && filter=$a
  case "$a" in repos/*) url=$a;; esac
  prev=$a
done
echo "$url" >> "$STUB_DIR/calls"
case "$url" in
  *workflows/go.yml/runs*) [ -f "$STUB_DIR/fail-runs" ] && exit 1; file="$STUB_DIR/runs.json" ;;
  */actions/runs/*/jobs*) id=${url#*actions/runs/}; id=${id%%/*}; file="$STUB_DIR/jobs-$id.json"; [ -f "$file" ] || exit 1 ;;
  *) exit 1 ;;
esac
jq -r "$filter" "$file"
`

// Runs the real ci/queue_tested_sha.sh against a stub gh that applies the real jq filters to canned replies.
func TestQueueTestedShaAnswersOnlyForAGreenMergeGroupRunWithGreenShards(t *testing.T) {
	if _, err := exec.LookPath("jq"); err != nil {
		t.Fatalf("jq is required by this test and is not on PATH: %v", err)
	}
	root := rootOf(t)
	script := filepath.Join(root, "ci", "queue_tested_sha.sh")
	const sha = "0123456789abcdef0123456789abcdef01234567"

	cases := []struct {
		name     string
		runs     string
		jobs     map[string]string
		failRuns bool
		want     bool
	}{
		{"green run, every shard green", queueRunsJSON(sha, [2]string{"11", "success"}),
			map[string]string{"11": queueJobsJSON("success", "success", "success")}, false, true},
		{"green run, one shard skipped", queueRunsJSON(sha, [2]string{"11", "success"}),
			map[string]string{"11": queueJobsJSON("success", "skipped", "success")}, false, false},
		{"green run, one shard cancelled", queueRunsJSON(sha, [2]string{"11", "success"}),
			map[string]string{"11": queueJobsJSON("success", "cancelled")}, false, false},
		{"green run, one shard unfinished", queueRunsJSON(sha, [2]string{"11", "success"}),
			map[string]string{"11": queueJobsJSON("success", "null")}, false, false},
		{"green run, one shard failed", queueRunsJSON(sha, [2]string{"11", "success"}),
			map[string]string{"11": queueJobsJSON("failure", "success")}, false, false},
		{"green run, no shard job at all", queueRunsJSON(sha, [2]string{"11", "success"}),
			map[string]string{"11": queueJobsJSON()}, false, false},
		{"the run itself failed", queueRunsJSON(sha, [2]string{"11", "failure"}),
			map[string]string{"11": queueJobsJSON("success")}, false, false},
		{"no run for the sha", queueRunsJSON(sha), nil, false, false},
		{"the runs call fails", "", nil, true, false},
		{"the jobs call fails", queueRunsJSON(sha, [2]string{"11", "success"}), nil, false, false},
		{"a second green run has the green shards", queueRunsJSON(sha, [2]string{"11", "success"}, [2]string{"12", "success"}),
			map[string]string{"11": queueJobsJSON(), "12": queueJobsJSON("success")}, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			write := func(name, content string) {
				if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o755); err != nil {
					t.Fatal(err)
				}
			}
			write("gh", queueGhStub)
			write("runs.json", tc.runs)
			for id, body := range tc.jobs {
				write("jobs-"+id+".json", body)
			}
			if tc.failRuns {
				write("fail-runs", "")
			}
			command := exec.Command("bash", script, "full-chaos/dev-health-ops", sha)
			command.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "STUB_DIR="+dir)
			err := command.Run()
			if got := err == nil; got != tc.want {
				t.Fatalf("exit ok = %v, want %v (err %v)", got, tc.want, err)
			}
			calls, _ := os.ReadFile(filepath.Join(dir, "calls"))
			for _, want := range []string{"event=merge_group", "head_sha=" + sha, "status=completed", "workflows/go.yml/runs"} {
				if !strings.Contains(string(calls), want) {
					t.Errorf("gh was called with %q, which lacks %q", calls, want)
				}
			}
		})
	}
}

// A failing shard first, then more output than a pipe holds: a script that feeds a pipeline into grep -q dies of
// SIGPIPE, and under pipefail with a leading "!" that read as a green answer.
func TestQueueTestedShaStaysRedWhenAFailedShardComesFirstInALongAnswer(t *testing.T) {
	if _, err := exec.LookPath("jq"); err != nil {
		t.Fatalf("jq is required by this test and is not on PATH: %v", err)
	}
	root := rootOf(t)
	script := filepath.Join(root, "ci", "queue_tested_sha.sh")
	const sha = "0123456789abcdef0123456789abcdef01234567"
	conclusions := make([]string, 200000)
	for index := range conclusions {
		conclusions[index] = "success"
	}
	conclusions[0] = "failure"
	dir := t.TempDir()
	for name, content := range map[string]string{
		"gh":           queueGhStub,
		"runs.json":    queueRunsJSON(sha, [2]string{"11", "success"}),
		"jobs-11.json": queueJobsJSON(conclusions...),
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	const attempts = 10
	for attempt := 1; attempt <= attempts; attempt++ {
		command := exec.Command("bash", script, "full-chaos/dev-health-ops", sha)
		command.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "STUB_DIR="+dir)
		if err := command.Run(); err == nil {
			t.Fatalf("attempt %d of %d: exit 0 (a green answer) although the first shard failed", attempt, attempts)
		}
	}
}

// A green merge_group run proves the main-push shards only if the merge_group run planned the same shard set. The plan
// reads nothing but INTEGRATION_AFFECTED_FILE, and only a pull request (selection), a queue-tested push (skip) or a
// docs-only change sets it. The docs-only case cannot fake a proof: its merge_group run has no shard job, and
// ci/queue_tested_sha.sh accepts only a run in which at least one shard ran and all succeeded (a docs-only push, for
// its part, matches no go.yml push path: ci/codeql_docs_only.sh refuses every Go input).
func TestMergeGroupPlansTheSameIntegrationShardsAsAMainPush(t *testing.T) {
	root := rootOf(t)
	raw, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "go.yml"))
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(string(raw), `echo "INTEGRATION_AFFECTED_FILE=${out}" >> "${GITHUB_ENV}"`); got != 3 {
		t.Errorf("INTEGRATION_AFFECTED_FILE is set in %d places, want 3 (pull request selection, queue-tested push skip, docs-only skip)", got)
	}
	job := pinJob(loadWf(t, root, "go.yml"), "go-storage-integration-plan")
	for _, step := range stepsOf(job) {
		name := asString(step["name"])
		if !strings.Contains(asString(step["run"]), "INTEGRATION_AFFECTED_FILE") {
			continue
		}
		condition := asString(step["if"])
		switch name {
		case "Select the integration packages the change can affect":
			if condition != "github.event_name == 'pull_request' && steps.docs.outputs.docs_only != 'true'" {
				t.Errorf("selection step if %q: a merge_group run must plan the full set", condition)
			}
		case "Skip the integration shards for a docs-only change":
			if condition != "steps.docs.outputs.docs_only == 'true'" {
				t.Errorf("docs-only skip step if %q: only the docs-only verdict may skip", condition)
			}
		case queueSkipStep:
			if !strings.Contains(condition, "github.event_name == 'push'") {
				t.Errorf("skip step if %q: only a main push may skip", condition)
			}
		default:
			t.Errorf("step %q sets INTEGRATION_AFFECTED_FILE", name)
		}
	}
	if step, _ := stepNamed(job, "Validate and emit deterministic integration shards"); step["if"] != nil {
		t.Errorf("the plan step must not depend on the event: if %v", step["if"])
	}
	planIf := asString(job["if"])
	if strings.Contains(planIf, "merge_group") || strings.Contains(planIf, "'push'") {
		t.Errorf("the plan job if %q must not tell push and merge_group apart", planIf)
	}
	shard := pinJob(loadWf(t, root, "go.yml"), "go-storage-integration-shard")
	if strings.Contains(asString(shard["if"]), "event_name") {
		t.Errorf("the shard job if %q must not depend on the event", shard["if"])
	}

	plan := func(event string) string {
		out := filepath.Join(t.TempDir(), "output")
		command := exec.Command("bash", "ci/check_go.sh", "integration-shard-plan")
		command.Dir = root
		var env []string
		for _, entry := range os.Environ() {
			if !strings.HasPrefix(entry, "INTEGRATION_AFFECTED_FILE=") && !strings.HasPrefix(entry, "GITHUB_EVENT_NAME=") {
				env = append(env, entry)
			}
		}
		command.Env = append(env, "GITHUB_EVENT_NAME="+event, "GITHUB_OUTPUT="+out)
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("integration-shard-plan for %s: %v\n%s", event, err, output)
		}
		written, err := os.ReadFile(out)
		if err != nil {
			t.Fatal(err)
		}
		return string(written)
	}
	push, queue := plan("push"), plan("merge_group")
	if push != queue {
		t.Errorf("the plan differs between a main push and a merge_group run:\npush: %s\nmerge_group: %s", push, queue)
	}
	if !strings.Contains(push, "legs=") || strings.Contains(push, "legs=0") {
		t.Errorf("the unfiltered plan has no legs: %s", push)
	}
}

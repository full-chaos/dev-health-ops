package workflows_test

import (
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

// Runs the real ci/queue_tested_sha.sh against a stub gh.
func TestQueueTestedShaAnswersOnlyForAGreenMergeGroupRun(t *testing.T) {
	root := rootOf(t)
	script := filepath.Join(root, "ci", "queue_tested_sha.sh")
	const sha = "0123456789abcdef0123456789abcdef01234567"

	cases := []struct {
		name string
		gh   string
		want bool
	}{
		{"a green merge_group run for the sha", `echo 1`, true},
		{"no run for the sha", `echo 0`, false},
		{"the API call fails", `exit 1`, false},
		{"the API answers with nothing", `true`, false},
		{"the API answers with text", `echo "none"`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			argsFile := filepath.Join(dir, "args")
			stub := "#!/usr/bin/env bash\necho \"$*\" > '" + argsFile + "'\n" + tc.gh + "\n"
			if err := os.WriteFile(filepath.Join(dir, "gh"), []byte(stub), 0o755); err != nil {
				t.Fatal(err)
			}
			command := exec.Command("bash", script, "full-chaos/dev-health-ops", sha)
			command.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"))
			err := command.Run()
			if got := err == nil; got != tc.want {
				t.Fatalf("exit ok = %v, want %v (err %v)", got, tc.want, err)
			}
			args, _ := os.ReadFile(argsFile)
			for _, want := range []string{"event=merge_group", "head_sha=" + sha, "status=completed", "workflows/go.yml/runs"} {
				if !strings.Contains(string(args), want) {
					t.Errorf("gh was called with %q, which lacks %q", args, want)
				}
			}
			if !strings.Contains(string(args), `.conclusion == "success"`) {
				t.Errorf("the jq filter must require conclusion success: %q", args)
			}
		})
	}
}

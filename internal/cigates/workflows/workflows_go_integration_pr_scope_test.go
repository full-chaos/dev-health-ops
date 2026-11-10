package workflows_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The integration shards run on the main push and, CHAOS-13075, on a ready pull request through the planner; a draft runs
// them only with the ci:integration or ci:full label, no pull request skips them for the merge queue, and a docs-only change
// skips them everywhere. These pins keep the skip and its reason wired into the plan job without
// renaming a pinned job.
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
	if asString(env["PR_DRAFT"]) != "${{ github.event.pull_request.draft }}" {
		t.Errorf("the selection step must pass the draft flag, got %q", asString(env["PR_DRAFT"]))
	}
	if _, ok := env["MERGE_QUEUE"]; ok || strings.Contains(run, "MERGE_QUEUE") {
		t.Error("the pull request selection must not read MERGE_QUEUE: it said enabled while main had no merge queue (CHAOS-13075)")
	}
	plan := pinJob(doc, "go-storage-integration-plan")
	outs, _ := plan["outputs"].(yamlMap)
	if asString(outs["skip_reason"]) != "${{ steps.skip.outputs.reason }}" {
		t.Errorf("the plan job must output skip_reason from its skip step, got %q", asString(outs["skip_reason"]))
	}
	agg := pinJob(doc, "go-storage-integration")
	aggEnv, _ := agg["env"].(yamlMap)
	if asString(aggEnv["SKIP_REASON"]) != "${{ needs.go-storage-integration-plan.outputs.skip_reason }}" {
		t.Errorf("the aggregate must read the plan's skip_reason, got %q", asString(aggEnv["SKIP_REASON"]))
	}
	for _, w := range []string{`"INTEGRATION_SKIP_REASON=docs"`, `"INTEGRATION_SKIP_REASON=queue"`, `"INTEGRATION_SKIP_REASON=${skip}"`} {
		if strings.Count(string(raw), w) != 1 {
			t.Errorf("go.yml must write %s exactly once", w)
		}
	}
	if step["if"] != "github.event_name == 'pull_request' && steps.docs.outputs.docs_only != 'true'" {
		t.Errorf("selection step if %v: the main push must never be judged, and a docs-only change keeps the docs step's NONE", step["if"])
	}
	docs, count := stepNamed(pinJob(doc, "go-storage-integration-plan"), "Skip the integration shards for a docs-only change")
	if count != 1 || docs["if"] != "steps.docs.outputs.docs_only == 'true'" || !strings.Contains(asString(docs["run"]), "printf 'NONE\\n'") {
		t.Errorf("the plan job must turn a docs-only verdict into a NONE selection, got %v", docs)
	}
	text := string(raw)
	if strings.Count(text, "- 'ci/integration_pr_scope.sh'") != 1 {
		t.Error("ci/integration_pr_scope.sh must be in the push path filter (pull_request has none)")
	}
	if strings.Contains(text, "types: [opened, synchronize, reopened, ready_for_review, labeled") {
		t.Error("a labeled trigger reruns the whole workflow on every label change; the label applies at the next push")
	}
	if !strings.Contains(text, "cancel-in-progress: ${{ (github.event_name == 'pull_request' && github.event.action != 'ready_for_review') || (github.event_name == 'push' && github.ref != 'refs/heads/main') }}") {
		t.Error("a main push run must never be cancelled in progress: every merge sha needs its own integration verdict")
	}
}

// Executes the real selection step text with a stub changed-file producer and a stub check_go.sh that fails, so any
// path that reaches the planner shows as FULL and a skip shows as NONE. With planner set, stub go and git binaries
// build a planner that prints that verdict, so the planner's own NONE shows its reason.
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
	rows := []struct{ name, list, labelsJSON, draft, queue, planner, want, reason string }{
		{"draft leaf go file is NONE", "internal/foo/bar.go\\0", "[]", "true", "", "", "NONE", "draft"},
		{"draft auth path is NONE", "internal/auth/authapi/oauth.go\\0", "[]", "true", "", "", "NONE", "draft"},
		{"ready leaf go file reaches the planner", "internal/foo/bar.go\\0", "[]", "false", "", "", "FULL", ""},
		{"ready pull request with the queue variable on reaches the planner", "internal/foo/bar.go\\0", "[]", "false", "enabled", "", "FULL", ""},
		{"draft ci:full reaches the planner", "internal/foo/bar.go\\0", `["ci:full"]`, "true", "", "", "FULL", ""},
		{"draft label reaches the planner", "internal/foo/bar.go\\0", `["ci:integration"]`, "true", "", "", "FULL", ""},
		{"malformed labels fall through", "internal/foo/bar.go\\0", "{bad", "true", "", "", "FULL", ""},
		{"bad draft flag falls through", "internal/foo/bar.go\\0", "[]", "", "", "", "FULL", ""},
		{"unterminated list falls through", "docs/a.md\\0internal/chmigrate/x.go", "[]", "true", "", "", "FULL", ""},
		{"planner NONE names the planner", "internal/foo/bar.go\\0", "[]", "false", "", "NONE", "NONE", "planner"},
		{"planner selection has no reason", "internal/foo/bar.go\\0", "[]", "false", "", "AFFECTED 1\\ninternal/x", "AFFECTED 1", ""},
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
			bin := filepath.Join(dir, "stubbin")
			path := os.Getenv("PATH")
			if r.planner != "" {
				files["check_go.sh"] = "printf '  RUN  internal/x\\n'\n"
				stubs := map[string]string{
					"go":  "#!/usr/bin/env bash\nwhile [ $# -gt 0 ]; do if [ \"$1\" = -o ]; then printf '#!/usr/bin/env bash\\nprintf \"" + r.planner + "\\\\n\"\\n' > \"$2\"; chmod +x \"$2\"; fi; shift; done\nexit 0\n",
					"git": "#!/usr/bin/env bash\n[ \"$1\" = merge-base ] && echo 0000\nexit 0\n",
				}
				if err := os.MkdirAll(bin, 0o755); err != nil {
					t.Fatal(err)
				}
				for n, c := range stubs {
					if err := os.WriteFile(filepath.Join(bin, n), []byte(c), 0o755); err != nil {
						t.Fatal(err)
					}
				}
				path = bin + string(os.PathListSeparator) + path
			}
			for n, c := range files {
				if err := os.WriteFile(filepath.Join(ci, n), []byte(c), 0o755); err != nil {
					t.Fatal(err)
				}
			}
			tmp := t.TempDir()
			cmd := exec.Command("bash", "-c", asString(step["run"]))
			cmd.Dir = dir
			cmd.Env = append(os.Environ(), "PATH="+path, "RUNNER_TEMP="+tmp, "GITHUB_ENV="+filepath.Join(tmp, "env"), "GITHUB_WORKSPACE="+dir,
				"BASE_SHA=0000", "EVENT_NAME=pull_request", "PR_LABELS_JSON="+r.labelsJSON, "PR_DRAFT="+r.draft, "MERGE_QUEUE="+r.queue)
			out, _ := cmd.CombinedOutput()
			got, err := os.ReadFile(filepath.Join(tmp, "affected.txt"))
			if err != nil {
				t.Fatalf("no affected.txt: %v\n%s", err, out)
			}
			if !strings.HasPrefix(string(got), r.want) {
				t.Fatalf("affected.txt %q, want prefix %q\n%s", got, r.want, out)
			}
			envFile, err := os.ReadFile(filepath.Join(tmp, "env"))
			if err != nil {
				t.Fatal(err)
			}
			var reasons []string
			for _, l := range strings.Split(string(envFile), "\n") {
				if v, ok := strings.CutPrefix(l, "INTEGRATION_SKIP_REASON="); ok {
					reasons = append(reasons, v)
				}
			}
			if strings.Join(reasons, ",") != r.reason {
				t.Fatalf("INTEGRATION_SKIP_REASON %q, want %q\n%s", reasons, r.reason, out)
			}
		})
	}
}

// CHAOS-13075: executes the real aggregate verdict step. A zero-leg plan passes only for docs, queue and planner; a draft
// skip, a missing or any other reason ran nothing and must fail with INTEGRATION NOT RUN.
func TestIntegrationAggregateRefusesASkipThatRanNothing(t *testing.T) {
	root := rootOf(t)
	doc := loadWf(t, root, "go.yml")
	step, count := stepNamed(pinJob(doc, "go-storage-integration"), "Require the plan and every shard to have passed")
	if count != 1 {
		t.Fatal("the aggregate lost its verdict step")
	}
	rows := []struct {
		name, plan, shard, legs, reason string
		code                            int
		want                            string
	}{
		{"docs-only zero passes", "success", "skipped", "0", "docs", 0, "plan selected 0 integration shards; none ran (reason: docs)"},
		{"queue zero passes", "success", "skipped", "0", "queue", 0, "plan selected 0 integration shards; none ran (reason: queue)"},
		{"planner zero passes", "success", "skipped", "0", "planner", 0, "plan selected 0 integration shards; none ran (reason: planner)"},
		{"draft zero fails", "success", "skipped", "0", "draft", 1, "INTEGRATION NOT RUN: draft"},
		{"missing reason fails", "success", "skipped", "0", "", 1, "INTEGRATION NOT RUN: plan selected 0 shards with reason ''"},
		{"unknown reason fails", "success", "skipped", "0", "label", 1, "INTEGRATION NOT RUN: plan selected 0 shards with reason 'label'"},
		{"zero legs with a matrix success and a draft reason fails", "success", "success", "0", "draft", 1, "INTEGRATION NOT RUN: draft"},
		{"zero legs with a matrix success and an unknown reason fails", "success", "success", "0", "label", 1, "INTEGRATION NOT RUN: plan selected 0 shards with reason 'label'"},
		{"zero legs with a matrix success and no reason fails", "success", "success", "0", "", 1, "INTEGRATION NOT RUN: plan selected 0 shards with reason ''"},
		{"zero legs with a matrix success and an allowed reason fails", "success", "success", "0", "docs", 1, "INTEGRATION NOT RUN: plan selected 0 shards but the shard result is 'success'"},
		{"shards passed", "success", "success", "30", "", 0, "all deterministic Go integration shards passed"},
		{"shards skipped with legs fails", "success", "skipped", "30", "docs", 1, "shard result: skipped"},
		{"red shard fails", "success", "failure", "30", "", 1, "shard result: failure"},
		{"red plan fails", "failure", "skipped", "0", "docs", 1, "integration plan result: failure"},
	}
	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			cmd := exec.Command("bash", "-c", asString(step["run"]))
			cmd.Dir = t.TempDir()
			cmd.Env = append(os.Environ(), "PLAN_RESULT="+r.plan, "SHARD_RESULT="+r.shard, "PLAN_LEGS="+r.legs, "SKIP_REASON="+r.reason)
			out, err := cmd.CombinedOutput()
			code := 0
			if ee, ok := err.(*exec.ExitError); ok {
				code = ee.ExitCode()
			} else if err != nil {
				t.Fatal(err)
			}
			if code != r.code || !strings.Contains(string(out), r.want) {
				t.Fatalf("exit %d want %d; output %q, want it to contain %q", code, r.code, out, r.want)
			}
		})
	}
}

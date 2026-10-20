package workflows_test

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// CHAOS-10174 PR A: Go port of tests/tooling/test_aggregate_gate_results.py (CHAOS-3482, 3513, 3514, 3948, 4843, 6930).
// The first half drives the REAL ci/aggregate_gate_results.sh through its result matrix; the second half parses the
// workflows and pins that each gated job's `if:` says what the script models for its policy.

const (
	pass = true
	fail = false
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

type verdict struct {
	code   int
	stdout string
	stderr string
}

// runGate runs the checked-in script with ONLY the given environment: an inherited GATED_JOB_* or GATE_* variable
// would be a second source of truth.
func runGate(t *testing.T, env map[string]string) verdict {
	t.Helper()
	cmd := exec.Command("bash", filepath.Join(repoRoot(t), "ci", "aggregate_gate_results.sh"))
	cmd.Env = []string{"PATH=/usr/bin:/bin:/usr/local/bin"}
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	code := 0
	if err := cmd.Run(); err != nil {
		ee, ok := err.(*exec.ExitError)
		if !ok {
			t.Fatalf("running the gate script: %v", err)
		}
		code = ee.ExitCode()
	}
	return verdict{code: code, stdout: out.String(), stderr: errb.String()}
}

func assertVerdict(t *testing.T, gate string, v verdict, want bool, context string) {
	t.Helper()
	if want {
		if v.code != 0 || !strings.Contains(v.stdout, gate+" gate passed") || strings.Contains(v.stderr, gate+" gate failed") {
			t.Fatalf("expected %s gate to PASS for %s; code=%d stdout=%q stderr=%q", gate, context, v.code, v.stdout, v.stderr)
		}
		return
	}
	if v.code != 1 || !strings.Contains(v.stderr, gate+" gate failed") || strings.Contains(v.stdout, gate+" gate passed") {
		t.Fatalf("expected %s gate to FAIL (exit 1) for %s; code=%d stdout=%q stderr=%q", gate, context, v.code, v.stdout, v.stderr)
	}
}

// twoJobRow: (event, changes result, changes code, job-1 result, job-2 result, verdict).
type twoJobRow struct {
	id, event, changes, code, job1, job2 string
	want                                 bool
}

// legacyMatrix is the CHAOS-3482 table: a path-filtered job and a merge-time-only job.
var legacyMatrix = []twoJobRow{
	{"changes-failure", "pull_request", "failure", "", "skipped", "skipped", fail},
	{"changes-cancelled-jobs-skipped", "pull_request", "cancelled", "", "skipped", "skipped", fail},
	{"changes-cancelled-jobs-cancelled", "pull_request", "cancelled", "", "cancelled", "cancelled", fail},
	{"changes-failure-with-published-code", "pull_request", "failure", "false", "skipped", "skipped", fail},
	{"changes-cancelled-with-published-code", "push", "cancelled", "false", "skipped", "skipped", fail},
	{"changes-skipped", "pull_request", "skipped", "", "skipped", "skipped", fail},
	{"everything-empty", "pull_request", "", "", "", "", fail},
	{"changes-unknown-word", "pull_request", "neutral", "", "skipped", "skipped", fail},
	{"normal-pr", "pull_request", "success", "true", "success", "skipped", pass},
	{"docs-only-pr", "pull_request", "success", "false", "skipped", "skipped", pass},
	{"docs-only-push", "push", "success", "false", "skipped", "skipped", pass},
	{"main-push", "push", "success", "true", "success", "success", pass},
	{"merge-queue", "merge_group", "success", "false", "success", "success", pass},
	{"dispatch", "workflow_dispatch", "success", "", "success", "skipped", pass},
	{"matrix-skipped-but-selected", "pull_request", "success", "true", "skipped", "skipped", fail},
	{"merge-queue-matrix-skipped", "merge_group", "success", "false", "skipped", "success", fail},
	{"merge-queue-coverage-skipped", "merge_group", "success", "true", "success", "skipped", fail},
	{"merge-queue-coverage-skipped-code-false", "merge_group", "success", "false", "success", "skipped", fail},
	{"push-code-empty-coverage-skipped", "push", "success", "", "success", "skipped", fail},
	{"dispatch-matrix-skipped", "workflow_dispatch", "success", "", "skipped", "skipped", fail},
	{"dispatch-matrix-skipped-filter-false", "workflow_dispatch", "success", "false", "skipped", "skipped", fail},
	{"pr-code-empty", "pull_request", "success", "", "skipped", "skipped", fail},
	{"pr-code-not-a-boolean", "pull_request", "success", "neutral", "skipped", "skipped", fail},
	{"push-code-empty", "push", "success", "", "skipped", "skipped", fail},
	{"pr-code-empty-but-matrix-ran", "pull_request", "success", "", "success", "skipped", pass},
	{"merge-queue-code-empty", "merge_group", "success", "", "success", "success", pass},
	{"push-coverage-skipped-but-selected", "push", "success", "true", "success", "skipped", fail},
	{"matrix-failure", "pull_request", "success", "true", "failure", "skipped", fail},
	{"matrix-cancelled", "pull_request", "success", "true", "cancelled", "skipped", fail},
	{"coverage-failure", "push", "success", "true", "success", "failure", fail},
	{"coverage-cancelled", "push", "success", "true", "success", "cancelled", fail},
	{"matrix-unknown-word", "pull_request", "success", "true", "neutral", "skipped", fail},
}

func TestGateVerdictLegacyMatrix(t *testing.T) {
	for _, r := range legacyMatrix {
		t.Run(r.id, func(t *testing.T) {
			v := runGate(t, map[string]string{
				"GATE_NAME": "test", "EVENT_NAME": r.event, "CHANGES_RESULT": r.changes, "CHANGES_CODE": r.code,
				"GATED_JOB_1": "test-matrix|path-filtered|" + r.job1,
				"GATED_JOB_2": "coverage|merge-time-only|" + r.job2,
			})
			assertVerdict(t, "test", v, r.want, fmt.Sprintf("%s/%s/code=%q/%s/%s", r.event, r.changes, r.code, r.job1, r.job2))
		})
	}
}

// currentMatrix is the shape test.yml ships (docs-tests unconditional, tooling-tests path-filtered):
// job1 = docs-tests, job2 = tooling-tests.
var currentMatrix = []twoJobRow{
	{"normal-pr", "pull_request", "success", "true", "success", "success", pass},
	{"docs-only-pr-tooling-path-skipped", "pull_request", "success", "false", "success", "skipped", pass},
	{"docs-only-push-tooling-path-skipped", "push", "success", "false", "success", "skipped", pass},
	{"selected-then-skipped", "pull_request", "success", "true", "success", "skipped", fail},
	{"selected-then-cancelled", "pull_request", "success", "true", "success", "cancelled", fail},
	{"selected-then-failed", "pull_request", "success", "true", "success", "failure", fail},
	{"path-skipped-but-docs-failed", "pull_request", "success", "false", "failure", "skipped", fail},
	{"docs-skipped-is-never-legitimate", "pull_request", "success", "false", "skipped", "skipped", fail},
	{"docs-cancelled-is-unmeasured", "pull_request", "success", "false", "cancelled", "skipped", fail},
	{"path-skipped-then-failed", "pull_request", "success", "false", "success", "failure", fail},
	{"undecided-code-cannot-explain-a-skip", "pull_request", "success", "", "success", "skipped", fail},
	{"garbage-code-cannot-explain-a-skip", "push", "success", "neutral", "success", "skipped", fail},
	{"undecided-code-but-tooling-ran", "pull_request", "success", "", "success", "success", pass},
	{"merge-queue-tooling-skipped", "merge_group", "success", "false", "success", "skipped", fail},
	{"merge-queue-tooling-ran", "merge_group", "success", "false", "success", "success", pass},
	{"dispatch-tooling-skipped", "workflow_dispatch", "success", "", "success", "skipped", fail},
	{"dispatch-tooling-skipped-filter-false", "workflow_dispatch", "success", "false", "success", "skipped", fail},
	{"dispatch-ran", "workflow_dispatch", "success", "", "success", "success", pass},
	{"changes-failed-everything-skipped", "pull_request", "failure", "", "skipped", "skipped", fail},
	{"changes-cancelled-with-published-code", "pull_request", "cancelled", "false", "success", "skipped", fail},
	{"changes-skipped", "pull_request", "skipped", "", "success", "success", fail},
	{"tooling-unknown-word", "pull_request", "success", "true", "success", "neutral", fail},
}

func TestGateVerdictCurrentTestWorkflowShape(t *testing.T) {
	for _, r := range currentMatrix {
		t.Run(r.id, func(t *testing.T) {
			v := runGate(t, map[string]string{
				"GATE_NAME": "test", "EVENT_NAME": r.event, "CHANGES_RESULT": r.changes, "CHANGES_CODE": r.code,
				"GATED_JOB_1": "docs-tests|unconditional|" + r.job1,
				"GATED_JOB_2": "tooling-tests|path-filtered|" + r.job2,
			})
			assertVerdict(t, "test", v, r.want, fmt.Sprintf("%s/%s/code=%q/%s/%s", r.id, r.changes, r.code, r.job1, r.job2))
		})
	}
}

func TestFailureOutputNamesTheUpstreamJobThatCausedIt(t *testing.T) {
	v := runGate(t, map[string]string{
		"GATE_NAME": "test", "EVENT_NAME": "pull_request", "CHANGES_RESULT": "failure", "CHANGES_CODE": "",
		"GATED_JOB_1": "test-matrix|path-filtered|skipped", "GATED_JOB_2": "coverage|merge-time-only|skipped",
	})
	if v.code != 1 || !strings.Contains(v.stderr, "changes reported 'failure'") {
		t.Fatalf("the reason must blame `changes`; code=%d stderr=%q", v.code, v.stderr)
	}
}

func TestLegitimateSkipsAreReportedWithTheirReason(t *testing.T) {
	v := runGate(t, map[string]string{
		"GATE_NAME": "test", "EVENT_NAME": "pull_request", "CHANGES_RESULT": "success", "CHANGES_CODE": "false",
		"GATED_JOB_1": "test-matrix|path-filtered|skipped", "GATED_JOB_2": "coverage|merge-time-only|skipped",
	})
	if v.code != 0 || !strings.Contains(v.stdout, "test-matrix skipped legitimately") || !strings.Contains(v.stdout, "coverage skipped legitimately") {
		t.Fatalf("a legitimate skip must be reported with its reason; code=%d stdout=%q", v.code, v.stdout)
	}
}

// singleJobMatrix is the CHAOS-3513 table (lint, typecheck): (event, changes, code, job, verdict).
var singleJobMatrix = []struct {
	id, event, changes, code, job string
	want                          bool
}{
	{"changes-failure", "pull_request", "failure", "", "skipped", fail},
	{"changes-cancelled", "pull_request", "cancelled", "", "skipped", fail},
	{"changes-skipped", "pull_request", "skipped", "", "skipped", fail},
	{"changes-empty", "pull_request", "", "", "", fail},
	{"merge-queue-skipped", "merge_group", "success", "false", "skipped", fail},
	{"merge-queue-code-empty-skipped", "merge_group", "success", "", "skipped", fail},
	{"merge-queue-ran", "merge_group", "success", "false", "success", pass},
	{"docs-only-pr", "pull_request", "success", "false", "skipped", pass},
	{"docs-only-push", "push", "success", "false", "skipped", pass},
	{"normal-pr", "pull_request", "success", "true", "success", pass},
	{"push-to-main", "push", "success", "true", "success", pass},
	{"dispatch-ran", "workflow_dispatch", "success", "", "success", pass},
	{"dispatch-skipped", "workflow_dispatch", "success", "", "skipped", fail},
	{"dispatch-skipped-filter-false", "workflow_dispatch", "success", "false", "skipped", fail},
	{"pr-code-empty-skipped", "pull_request", "success", "", "skipped", fail},
	{"pr-code-garbage-skipped", "pull_request", "success", "neutral", "skipped", fail},
	{"pr-code-empty-but-ran", "pull_request", "success", "", "success", pass},
	{"job-failure", "pull_request", "success", "true", "failure", fail},
	{"job-cancelled", "pull_request", "success", "true", "cancelled", fail},
	{"job-unknown-word", "pull_request", "success", "true", "neutral", fail},
}

func TestSingleJobGateVerdict(t *testing.T) {
	for _, g := range []struct{ gate, job string }{{"lint", "lint-job"}, {"typecheck", "typecheck-mypy"}} {
		for _, r := range singleJobMatrix {
			t.Run(g.gate+"/"+r.id, func(t *testing.T) {
				v := runGate(t, map[string]string{
					"GATE_NAME": g.gate, "EVENT_NAME": r.event, "CHANGES_RESULT": r.changes, "CHANGES_CODE": r.code,
					"GATED_JOB_1": g.job + "|path-filtered|" + r.job,
				})
				assertVerdict(t, g.gate, v, r.want, r.id)
			})
		}
	}
}

func TestAGateGivenNothingToJudgeFails(t *testing.T) {
	v := runGate(t, map[string]string{"GATE_NAME": "lint", "EVENT_NAME": "pull_request", "CHANGES_RESULT": "success", "CHANGES_CODE": "true"})
	if v.code != 1 || !strings.Contains(v.stderr, "asked to judge nothing") {
		t.Fatalf("code=%d stderr=%q", v.code, v.stderr)
	}
}

func TestAGapInTheJobNumberingFailsTheGate(t *testing.T) {
	for _, c := range []struct {
		id               string
		env              map[string]string
		judged, supplied int
	}{
		{"gap-hides-a-failure", map[string]string{"GATED_JOB_1": "a|path-filtered|success", "GATED_JOB_3": "b|path-filtered|failure"}, 1, 2},
		{"empty-middle-entry-hides-a-skip", map[string]string{"GATED_JOB_1": "a|path-filtered|success", "GATED_JOB_2": "", "GATED_JOB_3": "b|path-filtered|skipped"}, 1, 2},
	} {
		t.Run(c.id, func(t *testing.T) {
			env := map[string]string{"GATE_NAME": "test", "EVENT_NAME": "pull_request", "CHANGES_RESULT": "success", "CHANGES_CODE": "true"}
			for k, v := range c.env {
				env[k] = v
			}
			v := runGate(t, env)
			if v.code != 1 || !strings.Contains(v.stderr, fmt.Sprintf("judged %d of %d", c.judged, c.supplied)) {
				t.Fatalf("code=%d stderr=%q", v.code, v.stderr)
			}
		})
	}
}

func TestAnUnrecognizedPolicyFailsTheGate(t *testing.T) {
	v := runGate(t, map[string]string{
		"GATE_NAME": "lint", "EVENT_NAME": "pull_request", "CHANGES_RESULT": "success", "CHANGES_CODE": "false",
		"GATED_JOB_1": "lint-job|invented-policy|skipped",
	})
	if v.code != 1 || !strings.Contains(v.stderr, "not recognized") {
		t.Fatalf("code=%d stderr=%q", v.code, v.stderr)
	}
}

func selectorless(t *testing.T, result, event, policy, hasSelector string, extra map[string]string) verdict {
	env := map[string]string{
		"GATE_NAME": "ask-dev-acceptance", "EVENT_NAME": event, "GATE_HAS_SELECTOR": hasSelector,
		"GATED_JOB_1": "acceptance-live|" + policy + "|" + result,
	}
	for k, v := range extra {
		env[k] = v
	}
	return runGate(t, env)
}

func TestSelectorlessGateVerdict(t *testing.T) {
	for _, c := range []struct {
		id, result, event string
		want              bool
	}{
		{"nightly-ran-and-passed", "success", "schedule", pass},
		{"dispatch-ran-and-passed", "success", "workflow_dispatch", pass},
		{"nightly-booted-nothing", "skipped", "schedule", fail},
		{"dispatch-booted-nothing", "skipped", "workflow_dispatch", fail},
		{"stack-or-corpus-failed", "failure", "schedule", fail},
		{"cancelled-is-unmeasured", "cancelled", "schedule", fail},
		{"empty-result-is-not-evidence", "", "schedule", fail},
		{"unknown-result-is-not-evidence", "neutral", "schedule", fail},
	} {
		t.Run(c.id, func(t *testing.T) {
			v := selectorless(t, c.result, c.event, "unconditional", "false", nil)
			assertVerdict(t, "ask-dev-acceptance", v, c.want, c.id)
			if !c.want {
				reason := "acceptance-live reported '" + c.result + "'"
				if c.result == "" {
					reason = "acceptance-live reported '<empty>'"
				}
				if c.result == "skipped" {
					reason = "was skipped, but its job condition selected it to run"
				}
				if !strings.Contains(v.stderr, reason) {
					t.Fatalf("failed for the wrong reason; want %q in %q", reason, v.stderr)
				}
			}
		})
	}
}

func TestASelectorlessGateMayNotAlsoReportASelector(t *testing.T) {
	v := selectorless(t, "success", "schedule", "unconditional", "false", map[string]string{"CHANGES_RESULT": "success", "CHANGES_CODE": "true"})
	if v.code != 1 || !strings.Contains(v.stderr, "cannot both have no selector job and report one") {
		t.Fatalf("code=%d stderr=%q", v.code, v.stderr)
	}
}

func TestAnUnrecognizedPolicyIsRefusedWhateverTheResult(t *testing.T) {
	for _, policy := range []string{"totally-bogus", "uncondit1onal", ""} {
		for _, result := range []string{"success", "skipped", "failure"} {
			t.Run(policy+"/"+result, func(t *testing.T) {
				v := selectorless(t, result, "schedule", policy, "false", nil)
				if v.code != 1 || !strings.Contains(v.stderr, "is not recognized") {
					t.Fatalf("code=%d stderr=%q", v.code, v.stderr)
				}
			})
		}
	}
}

func TestTheUnconditionalPolicyArmIsLoadBearing(t *testing.T) {
	v := selectorless(t, "skipped", "schedule", "unconditional", "false", nil)
	if v.code != 1 || !strings.Contains(v.stderr, "was skipped, but its job condition selected it to run") || strings.Contains(v.stderr, "is not recognized") {
		t.Fatalf("code=%d stderr=%q", v.code, v.stderr)
	}
}

func TestTypecheckMypyUnconditionalArmFailsOnALiteralSkip(t *testing.T) {
	base := map[string]string{"GATE_NAME": "typecheck", "EVENT_NAME": "push", "GATE_HAS_SELECTOR": "false"}
	with := func(extra map[string]string) verdict {
		env := map[string]string{}
		for k, v := range base {
			env[k] = v
		}
		for k, v := range extra {
			env[k] = v
		}
		return runGate(t, env)
	}
	v := with(map[string]string{"GATED_JOB_1": "typecheck-mypy|unconditional|skipped"})
	if v.code != 1 || !strings.Contains(v.stderr, "was skipped, but its job condition selected it to run") {
		t.Fatalf("code=%d stderr=%q", v.code, v.stderr)
	}
	v = with(map[string]string{"GATE_HAS_SELECTOR": "true", "CHANGES_RESULT": "success", "CHANGES_CODE": "false", "GATED_JOB_1": "typecheck-mypy|path-filtered|skipped"})
	if v.code != 0 || !strings.Contains(v.stdout, "typecheck gate passed") {
		t.Fatalf("negative control: code=%d stdout=%q", v.code, v.stdout)
	}
	v = with(map[string]string{"GATED_JOB_1": "typecheck-mypy|unconditional|success"})
	if v.code != 0 || !strings.Contains(v.stdout, "typecheck gate passed") {
		t.Fatalf("success leg: code=%d stdout=%q", v.code, v.stdout)
	}
}

func TestAFilterPolicyWithoutASelectorIsRefused(t *testing.T) {
	v := selectorless(t, "skipped", "schedule", "path-filtered", "false", nil)
	if v.code != 1 || !strings.Contains(v.stderr, "no filter result to decide it with") {
		t.Fatalf("code=%d stderr=%q", v.code, v.stderr)
	}
}

func TestANonLiteralSelectorDeclarationIsRefused(t *testing.T) {
	for _, value := range []string{"", "yes", "FALSE", "1"} {
		t.Run("value="+value, func(t *testing.T) {
			v := selectorless(t, "success", "schedule", "unconditional", value, nil)
			if v.code != 1 || !strings.Contains(v.stderr, "must be literally 'true' or 'false'") {
				t.Fatalf("code=%d stderr=%q", v.code, v.stderr)
			}
		})
	}
}

func TestTheDefaultSelectorDeclarationIsUnchangedForExistingGates(t *testing.T) {
	v := runGate(t, map[string]string{
		"GATE_NAME": "lint", "EVENT_NAME": "pull_request", "CHANGES_RESULT": "cancelled", "CHANGES_CODE": "",
		"GATED_JOB_1": "lint-job|path-filtered|skipped",
	})
	if v.code != 1 || !strings.Contains(v.stderr, "the job that decides what must run did not complete") {
		t.Fatalf("code=%d stderr=%q", v.code, v.stderr)
	}
}

// --- workflow contract ------------------------------------------------------

type workflowFile struct {
	Jobs map[string]*workflowJob `yaml:"jobs"`
}

type workflowJob struct {
	Needs   yaml.Node         `yaml:"needs"`
	If      *string           `yaml:"if"`
	Outputs map[string]string `yaml:"outputs"`
	Steps   []workflowStep    `yaml:"steps"`
}

type workflowStep struct {
	ID   string            `yaml:"id"`
	Name string            `yaml:"name"`
	Uses string            `yaml:"uses"`
	If   *string           `yaml:"if"`
	Run  string            `yaml:"run"`
	Env  map[string]string `yaml:"env"`
	With map[string]string `yaml:"with"`
}

func loadWorkflow(t *testing.T, rel string) workflowFile {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), rel))
	if err != nil {
		t.Fatal(err)
	}
	var wf workflowFile
	if err := yaml.Unmarshal(raw, &wf); err != nil {
		t.Fatalf("parsing %s: %v", rel, err)
	}
	return wf
}

func job(t *testing.T, wf workflowFile, name string) *workflowJob {
	t.Helper()
	j, ok := wf.Jobs[name]
	if !ok || j == nil {
		t.Fatalf("job %q not found", name)
	}
	return j
}

func normalize(s string) string { return strings.Join(strings.Fields(s), " ") }

func needsOf(t *testing.T, j *workflowJob) []string {
	t.Helper()
	var out []string
	switch j.Needs.Kind {
	case yaml.SequenceNode:
		if err := j.Needs.Decode(&out); err != nil {
			t.Fatal(err)
		}
	case yaml.ScalarNode:
		out = []string{j.Needs.Value}
	}
	return out
}

const (
	pathFilteredIf = "github.event_name == 'merge_group' || github.event_name == 'workflow_dispatch' || needs.changes.outputs.code == 'true'"
	mergeTimeIf    = "github.event_name != 'pull_request' && (github.event_name == 'merge_group' || needs.changes.outputs.code == 'true')"
)

type gateSpec struct {
	path, gate, step string
	gated            [][2]string
	hasSelector      bool
}

// The policies the workflows actually pass to the script; docs-tests stays unconditional (CHAOS-3514), tooling-tests
// is path-filtered (CHAOS-10174 PR A).
var gates = []gateSpec{{
	path: ".github/workflows/test.yml", gate: "test", step: "Aggregate test results",
	gated:       [][2]string{{"docs-tests", "unconditional"}, {"tooling-tests", "path-filtered"}},
	hasSelector: true,
}}

func TestGateJobConsumesChangesAndCallsTheSharedScript(t *testing.T) {
	for _, g := range gates {
		t.Run(g.gate, func(t *testing.T) {
			wf := loadWorkflow(t, g.path)
			j := job(t, wf, g.gate)
			want := []string{}
			if g.hasSelector {
				want = append(want, "changes")
			}
			for _, p := range g.gated {
				want = append(want, p[0])
			}
			if got := needsOf(t, j); strings.Join(got, ",") != strings.Join(want, ",") {
				t.Fatalf("%s needs = %v, want %v", g.gate, got, want)
			}
			if j.If == nil || normalize(*j.If) != "always()" {
				t.Fatalf("%s must run under `always()`, got %v", g.gate, j.If)
			}
			var step *workflowStep
			for i := range j.Steps {
				if j.Steps[i].Name == g.step {
					step = &j.Steps[i]
				}
			}
			if step == nil {
				t.Fatalf("step %q not found", g.step)
			}
			wantEnv := map[string]string{"GATE_NAME": g.gate, "EVENT_NAME": "${{ github.event_name }}"}
			if g.hasSelector {
				wantEnv["CHANGES_RESULT"] = "${{ needs.changes.result }}"
				wantEnv["CHANGES_CODE"] = "${{ needs.changes.outputs.code }}"
			} else {
				wantEnv["GATE_HAS_SELECTOR"] = "false"
			}
			for i, p := range g.gated {
				wantEnv[fmt.Sprintf("GATED_JOB_%d", i+1)] = fmt.Sprintf("%s|%s|${{ needs.%s.result }}", p[0], p[1], p[0])
			}
			if fmt.Sprint(step.Env) != fmt.Sprint(wantEnv) {
				t.Fatalf("gate step env = %v, want %v", step.Env, wantEnv)
			}
			if !strings.Contains(normalize(step.Run), "ci/aggregate_gate_results.sh") {
				t.Fatalf("gate step does not run the shared script: %q", step.Run)
			}
			checkout := false
			for _, s := range j.Steps {
				checkout = checkout || strings.HasPrefix(s.Uses, "actions/checkout@")
			}
			if !checkout {
				t.Fatal("the gate job must check the repository out to reach the script")
			}
		})
	}
}

func TestGatedJobsCarryTheConditionTheirPolicyModels(t *testing.T) {
	expected := map[string]string{"path-filtered": pathFilteredIf, "merge-time-only": mergeTimeIf}
	for _, g := range gates {
		wf := loadWorkflow(t, g.path)
		for _, p := range g.gated {
			j := job(t, wf, p[0])
			if p[1] == "unconditional" {
				if j.If != nil {
					t.Fatalf("%s is registered 'unconditional' with %s but carries an `if:`", p[0], g.gate)
				}
				continue
			}
			if j.If == nil || normalize(*j.If) != expected[p[1]] {
				t.Fatalf("%s:%s no longer matches the '%s' policy that %s passes to the script; got %v", g.path, p[0], p[1], g.gate, j.If)
			}
		}
	}
}

func filterStep(t *testing.T, wf workflowFile) workflowStep {
	t.Helper()
	for _, s := range job(t, wf, "changes").Steps {
		if s.ID == "filter" {
			return s
		}
	}
	t.Fatal("changes job has no `filter` step")
	return workflowStep{}
}

func codePatterns(t *testing.T, wf workflowFile) []string {
	t.Helper()
	var filters map[string][]string
	if err := yaml.Unmarshal([]byte(filterStep(t, wf).With["filters"]), &filters); err != nil {
		t.Fatal(err)
	}
	return filters["code"]
}

func isCovered(path string, patterns []string) bool {
	for _, p := range patterns {
		if p == path {
			return true
		}
		if strings.HasSuffix(p, "/**") && strings.HasPrefix(path, strings.TrimSuffix(p, "**")) {
			return true
		}
	}
	return false
}

func TestPathFilterIsNotRunOnManualDispatch(t *testing.T) {
	for _, g := range gates {
		wf := loadWorkflow(t, g.path)
		s := filterStep(t, wf)
		if s.If == nil || normalize(*s.If) != "github.event_name != 'workflow_dispatch'" {
			t.Fatalf("%s: the filter step must not run on workflow_dispatch; got %v", g.path, s.If)
		}
	}
}

func TestChangesJobPublishesTheCodeOutput(t *testing.T) {
	for _, g := range gates {
		wf := loadWorkflow(t, g.path)
		if got := job(t, wf, "changes").Outputs["code"]; got != "${{ steps.filter.outputs.code }}" {
			t.Fatalf("%s: changes.outputs.code = %q", g.path, got)
		}
	}
}

func TestAWorkflowGatesOnChangesToItself(t *testing.T) {
	for _, g := range gates {
		if patterns := codePatterns(t, loadWorkflow(t, g.path)); !isCovered(g.path, patterns) {
			t.Fatalf("%s is not selected by its own `code` filter %v: a PR editing only it would skip %s's gated jobs", g.path, patterns, g.gate)
		}
	}
}

var pipInstall = regexp.MustCompile(`pip install -r\s+(\S+)`)
var uvSync = regexp.MustCompile(`\buv sync\b`)

func TestPathsFilterCoversEveryFileTheGatedJobsInstall(t *testing.T) {
	for _, g := range gates {
		wf := loadWorkflow(t, g.path)
		patterns := codePatterns(t, wf)
		installed := map[string]bool{}
		for _, p := range g.gated {
			for _, s := range job(t, wf, p[0]).Steps {
				for _, m := range pipInstall.FindAllStringSubmatch(s.Run, -1) {
					installed[m[1]] = true
				}
				if uvSync.MatchString(s.Run) {
					installed["pyproject.toml"], installed["uv.lock"] = true, true
				}
			}
		}
		if len(installed) == 0 {
			t.Fatalf("no dependency manifest install found in %s: the guard measured nothing", g.path)
		}
		var uncovered []string
		for name := range installed {
			if !isCovered(name, patterns) {
				uncovered = append(uncovered, name)
			}
		}
		sort.Strings(uncovered)
		if len(uncovered) > 0 {
			t.Fatalf("%s's gated jobs install %v but its `code` filter does not select them; patterns: %v", g.path, uncovered, patterns)
		}
	}
}

func TestPathsFilterCoversLefthookYml(t *testing.T) {
	if patterns := codePatterns(t, loadWorkflow(t, ".github/workflows/test.yml")); !isCovered("lefthook.yml", patterns) {
		t.Fatal("test.yml's path filter has no lefthook.yml entry: a lefthook.yml-only change would skip the guards")
	}
}

func trackedFiles(t *testing.T) []string {
	t.Helper()
	cmd := exec.Command("git", "ls-files")
	cmd.Dir = repoRoot(t)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git ls-files: %v", err)
	}
	var files []string
	for _, l := range strings.Split(string(out), "\n") {
		if l != "" {
			files = append(files, l)
		}
	}
	return files
}

// CHAOS-6930: a PR that only touches a directory the tooling suite reads must run it. The directories are derived from
// the tracked fixtures, not listed.
func TestPathsFilterSelectsEveryDirectoryTheToolingSuiteReads(t *testing.T) {
	patterns := codePatterns(t, loadWorkflow(t, ".github/workflows/test.yml"))
	fixtureDirs := map[string]string{}
	for _, f := range trackedFiles(t) {
		parts := strings.Split(f, "/")
		if len(parts) < 2 || strings.HasSuffix(f, ".go") {
			continue
		}
		for _, seg := range parts[:len(parts)-1] {
			if seg == "fixtures" || seg == "testdata" {
				if _, ok := fixtureDirs[parts[0]]; !ok {
					fixtureDirs[parts[0]] = f
				}
				break
			}
		}
	}
	if len(fixtureDirs) < 3 {
		t.Fatalf("only %d directories hold fixtures: the derivation measured nothing", len(fixtureDirs))
	}
	var uncovered []string
	for dir, example := range fixtureDirs {
		if !isCovered(example, patterns) {
			uncovered = append(uncovered, dir+" (e.g. "+example+")")
		}
	}
	sort.Strings(uncovered)
	if len(uncovered) > 0 {
		t.Fatalf("the `code` filter of test.yml does not select %v; patterns: %v", uncovered, patterns)
	}
	for _, named := range []string{"tools/x", "ci/x", "tests/tooling/x", ".github/workflows/x"} {
		if !isCovered(named, patterns) {
			t.Fatalf("the `code` filter does not select %s", named)
		}
	}
}

// CHAOS-3514 docs guards.
func TestDocsTestsJobCannotBeDeselected(t *testing.T) {
	if job(t, loadWorkflow(t, ".github/workflows/test.yml"), "docs-tests").If != nil {
		t.Fatal("docs-tests carries an `if:` but is registered `unconditional` with the aggregator")
	}
}

func docsRuns(t *testing.T) string {
	var runs []string
	for _, s := range job(t, loadWorkflow(t, ".github/workflows/test.yml"), "docs-tests").Steps {
		runs = append(runs, s.Run)
	}
	return strings.Join(runs, " ")
}

func TestDocsTestsJobRunsTheWholeDocsSuite(t *testing.T) {
	runs := docsRuns(t)
	if !strings.Contains(runs, "pytest tests/docs") {
		t.Fatalf("docs-tests no longer invokes `pytest tests/docs`; ran %q", runs)
	}
	if regexp.MustCompile(`pytest tests/docs/\S`).MatchString(runs) {
		t.Fatalf("docs-tests runs only part of tests/docs: %q", runs)
	}
}

func TestDocsTestsInstallsWhatTheDocsSuiteImports(t *testing.T) {
	runs := docsRuns(t)
	if !strings.Contains(runs, "uv sync --frozen --all-extras --dev") {
		t.Fatal("docs-tests must install the dev environment from uv.lock")
	}
	if !strings.Contains(runs, "requirements-docs.txt") {
		t.Fatal("docs-tests must install requirements-docs.txt")
	}
}

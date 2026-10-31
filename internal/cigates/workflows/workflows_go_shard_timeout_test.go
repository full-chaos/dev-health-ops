package workflows_test

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// CHAOS-11331: a shard that ends at its time limit must read as a timeout, never as a test failure and never as a
// fail-fast cancel. These cells pin the naming step, the gate note and the measured bound of the shard timeout.

const (
	timeoutNamingStepID = "name_red"
	timeoutOutputName   = "timed_out"
	gateNoteEnvPrefix   = "GATE_TIMEOUT_NOTE_"
	// measuredShardWallSecs is the longest packages-shard job wall in go.yml runs 41193580536 and 41195412958 (shard 6,
	// 1056 s), after the shard weights of #5369. The timeout bound rule of go.yml is the measured maximum x2.
	measuredShardWallSecs = 1056
)

type namingLeg struct {
	conclusion, id, name, started, completed, log string
}

// runNamingTimeouts runs the real naming step with a fake gh that serves the given legs and their logs.
func runNamingTimeouts(t *testing.T, script string, capMin int, legs []namingLeg) (int, string, string) {
	t.Helper()
	dir := t.TempDir()
	var listing strings.Builder
	for _, l := range legs {
		fmt.Fprintf(&listing, "%s\t%s\t%s\thttps://example.test/job/%s\t%s\t%s\n", l.conclusion, l.id, l.name, l.id, l.started, l.completed)
		if err := os.WriteFile(filepath.Join(dir, "log-"+l.id+".txt"), []byte(l.log), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "listing.tsv"), []byte(listing.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	stub := fmt.Sprintf(`#!/usr/bin/env bash
D=%q
case "$*" in
  *"/jobs/"*"/logs"*) all="$*"; id="${all##*/jobs/}"; id="${id%%%%/*}"; cat "$D/log-${id}.txt"; exit 0 ;;
  *"/jobs?"*) cat "$D/listing.tsv"; exit 0 ;;
esac
exit 1
`, dir)
	if err := os.WriteFile(filepath.Join(dir, "gh"), []byte(stub), 0o755); err != nil {
		t.Fatal(err)
	}
	summary := filepath.Join(dir, "summary.md")
	output := filepath.Join(dir, "github_output")
	code, so, se := bashRun(t, script, []string{
		"GH_TOKEN=x", "REPO=o/r", "RUN_ID=1", "GITHUB_STEP_SUMMARY=" + summary, "GITHUB_OUTPUT=" + output,
		fmt.Sprintf("SHARD_TIMEOUT_MIN=%d", capMin), "PATH=" + dir + ":" + withShims("/usr/bin:/bin"),
	})
	b, _ := os.ReadFile(summary)
	o, _ := os.ReadFile(output)
	return code, so + string(b) + "\nGITHUB_OUTPUT:\n" + string(o), se
}

const (
	failLog     = "2026-10-25T10:00:01Z --- FAIL: TestRole (0.1s)\nFAIL\tdev/pkg\t0.2s\n"
	deadlineLog = "2026-10-25T10:21:30Z panic: test timed out after 21m30s\n\trunning tests:\n\t\tTestSlow (21m0s)\n"
)

func timeoutNamingStep(t *testing.T) string {
	t.Helper()
	return stepRun(t, loadWf(t, rootOf(t), "go.yml"), aggregateJobName, namingStepName)
}

func TestNamingStepLabelsAShardCancelledAtItsTimeLimitAsATimeout(t *testing.T) {
	script := timeoutNamingStep(t)
	legs := []namingLeg{
		{"failure", "777", "go-storage-integration-shard-packages-7", "2026-10-25T03:30:00Z", "2026-10-25T03:40:00Z", failLog},
		{"cancelled", "778", "go-storage-integration-shard-packages-8", "2026-10-25T03:29:27Z", "2026-10-25T03:54:47Z", ""},
		{"cancelled", "779", "go-storage-integration-shard-packages-9", "2026-10-25T03:30:00Z", "2026-10-25T03:35:00Z", ""},
	}
	code, out, _ := runNamingTimeouts(t, script, 25, legs)
	if code != 0 {
		t.Fatalf("the naming step must exit 0, got %d", code)
	}
	re := regexp.MustCompile(`(?m)^- go-storage-integration-shard-packages-8: JOB TIMEOUT .*not a test failure.*https://example.test/job/778`)
	if !re.MatchString(out) {
		t.Errorf("want a JOB TIMEOUT line for packages-8; out=%q", out)
	}
	if strings.Contains(out, "packages-9: JOB TIMEOUT") || strings.Contains(out, "packages-7: JOB TIMEOUT") {
		t.Errorf("a fail-fast cancel and a real failure must not read as timeouts; out=%q", out)
	}
	if !strings.Contains(out, "1 cancelled by fail-fast") {
		t.Errorf("only the short cancel is a fail-fast cancel; out=%q", out)
	}
	if !strings.Contains(out, "TestRole") {
		t.Errorf("the real failure must still show its test name; out=%q", out)
	}
	if !strings.Contains(out, "\ntimed_out=go-storage-integration-shard-packages-8\n") {
		t.Errorf("GITHUB_OUTPUT must carry timed_out=<name>; out=%q", out)
	}

	// The bound: a wall of the cap minus 30 s is a timeout, one second less is not.
	edge := func(completed string) string {
		_, o, _ := runNamingTimeouts(t, script, 25, []namingLeg{
			{"cancelled", "801", "go-storage-integration-shard-packages-6", "2026-10-25T03:00:00Z", completed, ""}})
		return o
	}
	if o := edge("2026-10-25T03:24:30Z"); !strings.Contains(o, "\ntimed_out=go-storage-integration-shard-packages-6\n") {
		t.Errorf("a wall of cap-30 s must be a timeout; out=%q", o)
	}
	if o := edge("2026-10-25T03:24:29Z"); strings.Contains(o, "JOB TIMEOUT") || !strings.Contains(o, "\ntimed_out=\n") {
		t.Errorf("a wall of cap-31 s must not be a timeout and timed_out must be empty; out=%q", o)
	}
}

func TestNamingStepLabelsAGoTestDeadlinePanicAsATimeout(t *testing.T) {
	script := timeoutNamingStep(t)
	legs := []namingLeg{
		{"failure", "777", "go-storage-integration-shard-packages-7", "2026-10-25T03:30:00Z", "2026-10-25T03:52:00Z", deadlineLog},
		{"failure", "778", "go-storage-integration-shard-packages-8", "2026-10-25T03:30:00Z", "2026-10-25T03:40:00Z", failLog},
	}
	_, out, _ := runNamingTimeouts(t, script, 25, legs)
	if !regexp.MustCompile(`(?m)^- go-storage-integration-shard-packages-7: JOB TIMEOUT`).MatchString(out) {
		t.Errorf("a go test deadline panic must read as a JOB TIMEOUT; out=%q", out)
	}
	if strings.Contains(out, "packages-8: JOB TIMEOUT") {
		t.Errorf("a plain test failure must not read as a timeout; out=%q", out)
	}
	if !strings.Contains(out, "\ntimed_out=go-storage-integration-shard-packages-7\n") {
		t.Errorf("GITHUB_OUTPUT must carry timed_out=<name>; out=%q", out)
	}
}

func gateEnv(result string, extra map[string]string) map[string]string {
	env := map[string]string{
		"GATE_NAME": "go-queue-gate", "EVENT_NAME": "merge_group", "GATE_HAS_SELECTOR": "true",
		"CHANGES_RESULT": "success", "CHANGES_CODE": "true", "HEAVY_ON_PR": "false",
		"GATED_JOB_1": "go-storage-integration|unconditional|" + result,
	}
	for k, v := range extra {
		env[k] = v
	}
	return env
}

func TestQueueGateNamesAShardTimeoutAndStaysRed(t *testing.T) {
	note := map[string]string{gateNoteEnvPrefix + "1": "go-storage-integration|go-storage-integration-shard-packages-8"}
	for _, res := range []string{"failure", "cancelled"} {
		v := runGate(t, gateEnv(res, note))
		assertVerdict(t, "go-queue-gate", v, false, "a timed out shard with result "+res)
		if !strings.Contains(v.stderr, "JOB TIMEOUT, not a test failure") || !strings.Contains(v.stderr, "go-storage-integration-shard-packages-8") {
			t.Errorf("result %s: stderr must name the timeout and the shard; stderr=%q", res, v.stderr)
		}
	}
	// No note: the line is the old one, with no timeout words.
	v := runGate(t, gateEnv("failure", map[string]string{gateNoteEnvPrefix + "1": "go-storage-integration|"}))
	assertVerdict(t, "go-queue-gate", v, false, "a failure with an empty note")
	if strings.Contains(v.stderr, "JOB TIMEOUT") || !strings.Contains(v.stderr, "go-storage-integration reported 'failure'") {
		t.Errorf("an empty note must keep the plain failure line; stderr=%q", v.stderr)
	}
	// A note for another job is not used, and a green job never prints one.
	v = runGate(t, gateEnv("failure", map[string]string{gateNoteEnvPrefix + "1": "go-container-smoke|x"}))
	if strings.Contains(v.stderr, "JOB TIMEOUT") {
		t.Errorf("a note for another job must not label this one; stderr=%q", v.stderr)
	}
	assertVerdict(t, "go-queue-gate", runGate(t, gateEnv("success", note)), true, "a green job with a stale note")
}

func TestTimeoutNoteIsWiredFromTheNamingStepThroughTheJobOutputToTheGate(t *testing.T) {
	doc := loadWf(t, rootOf(t), "go.yml")
	agg := pinJob(doc, aggregateJobName)
	naming, n := stepNamed(agg, namingStepName)
	if n != 1 {
		t.Fatalf("want one naming step, got %d", n)
	}
	if asString(naming["id"]) != timeoutNamingStepID {
		t.Errorf("the naming step id is %q, want %s", asString(naming["id"]), timeoutNamingStepID)
	}
	if got, want := asString(asMap(agg["outputs"])[timeoutOutputName]), "${{ steps."+timeoutNamingStepID+".outputs."+timeoutOutputName+" }}"; got != want {
		t.Errorf("go-storage-integration output %s is %q, want %q", timeoutOutputName, got, want)
	}
	shardCap := fmt.Sprint(pinJob(doc, shardJobName)["timeout-minutes"])
	if got := fmt.Sprint(asMap(naming["env"])["SHARD_TIMEOUT_MIN"]); got != shardCap {
		t.Errorf("SHARD_TIMEOUT_MIN is %q, want the shard timeout-minutes %q", got, shardCap)
	}
	gate := pinJob(doc, "go-queue-gate")
	var env yamlMap
	for _, s := range stepsOf(gate) {
		if asString(s["name"]) == "Aggregate the heavy Go results" {
			env = asMap(s["env"])
		}
	}
	if got, want := asString(env[gateNoteEnvPrefix+"1"]), "go-storage-integration|${{ needs.go-storage-integration.outputs."+timeoutOutputName+" }}"; got != want {
		t.Errorf("go-queue-gate %s1 is %q, want %q", gateNoteEnvPrefix, got, want)
	}
}

func TestShardTimeoutIsTwiceTheMeasuredWorstWall(t *testing.T) {
	root := rootOf(t)
	doc := loadWf(t, root, "go.yml")
	job := pinJob(doc, shardJobName)
	want := 2 * measuredShardWallSecs / 60
	if got := fmt.Sprint(job["timeout-minutes"]); got != fmt.Sprint(want) {
		t.Errorf("%s timeout-minutes is %s, want %d (2 x %d s, floored to whole minutes)", shardJobName, got, want, measuredShardWallSecs)
	}
	if got := fmt.Sprint(asMap(job["env"])["CHECK_GO_JOB_CAP_MIN"]); got != fmt.Sprint(want) {
		t.Errorf("%s CHECK_GO_JOB_CAP_MIN is %s, want %d", shardJobName, got, want)
	}
	script := readFile(t, filepath.Join(root, "ci", "check_go.sh"))
	body := regexp.MustCompile(`(?s)\nrun_integration_go_test\(\) \{.*?\n\}\n`).FindString(script)
	if !strings.Contains(body, `tmo="$(job_go_test_timeout 32)"`) {
		t.Errorf("run_integration_go_test must take its go test limit from job_go_test_timeout 32 (cap %d - 3 min)", want)
	}
}

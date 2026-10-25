package workflows_test

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The integration shard matrix stops on its first red shard, in the merge queue only. These tests pin three things:
// fail-fast is on for merge_group alone; a cancelled, failed or skipped shard can never make the go-storage-integration
// aggregate or the required go-queue-gate green; and the step that names the red shard can never change a verdict.

const (
	failFastGoYml     = ".github/workflows/go.yml"
	shardJobName      = "go-storage-integration-shard"
	aggregateJobName  = "go-storage-integration"
	aggregateStepName = "Require the plan and every shard to have passed"
	namingStepName    = "Name the red shard"
	wantFailFast      = "${{ github.event_name == 'merge_group' }}"
)

var failFastExpr = regexp.MustCompile(`^\$\{\{\s*github\.event_name\s*==\s*'([a-z_]+)'\s*\}\}$`)

// failFastFor evaluates the shard matrix's fail-fast value for one event, for the one expression shape allowed.
func failFastFor(doc yamlMap, event string) (bool, string) {
	raw := asMap(pinJob(doc, shardJobName)["strategy"])["fail-fast"]
	switch v := raw.(type) {
	case bool:
		return v, ""
	case string:
		m := failFastExpr.FindStringSubmatch(v)
		if m == nil {
			return false, fmt.Sprintf("fail-fast %q is not a single event comparison", v)
		}
		return m[1] == event, ""
	}
	return true, "fail-fast is absent (GitHub default is true)"
}

func failFastDefects(doc yamlMap) []string {
	var out []string
	if got := asMap(pinJob(doc, shardJobName)["strategy"])["fail-fast"]; got != wantFailFast {
		out = append(out, fmt.Sprintf("%s strategy.fail-fast is %v, want exactly %s", shardJobName, got, wantFailFast))
	}
	for _, ev := range []string{"pull_request", "push", "schedule", "workflow_dispatch", "release"} {
		if on, why := failFastFor(doc, ev); on || why != "" {
			out = append(out, fmt.Sprintf("fail-fast must stay false on %s (%s)", ev, why))
		}
	}
	if on, why := failFastFor(doc, "merge_group"); !on || why != "" {
		out = append(out, "fail-fast must be true on merge_group "+why)
	}
	for _, j := range []string{"go-container-smoke", "go-container-security", "go-container-reproducibility"} {
		if got := asMap(pinJob(doc, j)["strategy"])["fail-fast"]; got != nil && got != false {
			out = append(out, fmt.Sprintf("%s fail-fast is %v: the container matrices are out of scope and stay false", j, got))
		}
	}
	return out
}

func TestIntegrationShardFailFastIsMergeGroupOnly(t *testing.T) {
	root := rootOf(t)
	if d := failFastDefects(loadWf(t, root, "go.yml")); len(d) > 0 {
		t.Fatalf("%s", strings.Join(d, "\n"))
	}
	for _, m := range []mutation{
		{failFastGoYml, "      fail-fast: ${{ github.event_name == 'merge_group' }}\n      matrix: ${{ fromJSON(needs.go-storage-integration-plan.outputs.matrix) }}", "      fail-fast: false\n      matrix: ${{ fromJSON(needs.go-storage-integration-plan.outputs.matrix) }}"},
		{failFastGoYml, "      fail-fast: ${{ github.event_name == 'merge_group' }}\n      matrix: ${{ fromJSON(needs.go-storage-integration-plan.outputs.matrix) }}", "      fail-fast: true\n      matrix: ${{ fromJSON(needs.go-storage-integration-plan.outputs.matrix) }}"},
		{failFastGoYml, "      fail-fast: ${{ github.event_name == 'merge_group' }}\n      matrix: ${{ fromJSON(needs.go-storage-integration-plan.outputs.matrix) }}", "      matrix: ${{ fromJSON(needs.go-storage-integration-plan.outputs.matrix) }}"},
		{failFastGoYml, "github.event_name == 'merge_group' }}\n      matrix: ${{ fromJSON(needs.go-storage-integration-plan.outputs.matrix) }}", "github.event_name == 'push' }}\n      matrix: ${{ fromJSON(needs.go-storage-integration-plan.outputs.matrix) }}"},
		{failFastGoYml, "github.event_name == 'merge_group' }}\n      matrix: ${{ fromJSON(needs.go-storage-integration-plan.outputs.matrix) }}", "github.event_name == 'pull_request' }}\n      matrix: ${{ fromJSON(needs.go-storage-integration-plan.outputs.matrix) }}"},
		{failFastGoYml, "github.event_name == 'merge_group' }}\n      matrix: ${{ fromJSON(needs.go-storage-integration-plan.outputs.matrix) }}", "github.event_name != 'merge_group' }}\n      matrix: ${{ fromJSON(needs.go-storage-integration-plan.outputs.matrix) }}"},
		{failFastGoYml, "    name: go-container-smoke (${{ matrix.platform }})\n    runs-on: ubuntu-latest\n    timeout-minutes: 45\n    strategy:\n      fail-fast: false", "    name: go-container-smoke (${{ matrix.platform }})\n    runs-on: ubuntu-latest\n    timeout-minutes: 45\n    strategy:\n      fail-fast: true"},
	} {
		tmp := plantTree(t, root, []string{failFastGoYml}, m)
		if d := failFastDefects(loadWf(t, tmp, "go.yml")); len(d) == 0 {
			t.Errorf("planted fail-fast defect was not caught: %q -> %q", m.old, m.new)
		}
	}
}

// matrixResult is what GitHub reports as needs.<matrix job>.result for a set of leg results: any failed leg makes the
// job `failure`; with none failed, any cancelled leg makes it `cancelled`; a leg that never ran is `skipped`.
func matrixResult(legs ...string) string {
	rank := map[string]int{"success": 0, "skipped": 1, "cancelled": 2, "failure": 3}
	best, out := -1, "success"
	for _, l := range legs {
		if rank[l] > best {
			best, out = rank[l], l
		}
	}
	return out
}

func stepRun(t *testing.T, doc yamlMap, job, step string) string {
	t.Helper()
	s, n := stepNamed(pinJob(doc, job), step)
	if n != 1 {
		t.Fatalf("job %s has %d steps named %q, want 1", job, n, step)
	}
	return asString(s["run"])
}

func bashRun(t *testing.T, script string, env []string, args ...string) (int, string, string) {
	t.Helper()
	cmd := exec.Command("bash", append([]string{"-c", script, "bash"}, args...)...)
	cmd.Env = append([]string{"PATH=" + withShims("/usr/bin:/bin:/usr/local/bin")}, env...)
	var o, e bytes.Buffer
	cmd.Stdout, cmd.Stderr = &o, &e
	code := 0
	if err := cmd.Run(); err != nil {
		ee, ok := err.(*exec.ExitError)
		if !ok {
			t.Fatalf("running script: %v", err)
		}
		code = ee.ExitCode()
	}
	return code, o.String(), e.String()
}

// aggregateExit runs the real go-storage-integration verdict step; 0 is green.
func aggregateExit(t *testing.T, script, plan, legs, shard string) int {
	code, _, _ := bashRun(t, script, []string{"PLAN_RESULT=" + plan, "PLAN_LEGS=" + legs, "SHARD_RESULT=" + shard})
	return code
}

type aggRow struct {
	id           string
	plan, legs   string
	shardLegs    []string
	wantGreen    bool
	wantMatrixIs string
}

var aggRows = []aggRow{
	{"all-success", "success", "3", []string{"success", "success", "success"}, true, "success"},
	{"one-failure-rest-cancelled", "success", "3", []string{"failure", "cancelled", "cancelled"}, false, "failure"},
	{"failure-then-skipped-and-cancelled", "success", "4", []string{"failure", "cancelled", "skipped", "skipped"}, false, "failure"},
	{"all-cancelled", "success", "3", []string{"cancelled", "cancelled", "cancelled"}, false, "cancelled"},
	{"success-and-cancelled", "success", "3", []string{"success", "cancelled", "success"}, false, "cancelled"},
	{"all-skipped-where-legs-required", "success", "3", []string{"skipped", "skipped", "skipped"}, false, "skipped"},
	{"plan-selected-zero-legs-skipped", "success", "0", []string{}, true, ""},
	{"plan-cancelled", "cancelled", "3", []string{"success", "success", "success"}, false, "success"},
	{"plan-failure", "failure", "0", []string{}, false, ""},
	{"plan-skipped", "skipped", "0", []string{}, false, ""},
}

func aggregateRowResult(r aggRow) string {
	if len(r.shardLegs) == 0 {
		return "skipped"
	}
	return matrixResult(r.shardLegs...)
}

func aggregateDefects(t *testing.T, doc yamlMap) []string {
	t.Helper()
	script := stepRun(t, doc, aggregateJobName, aggregateStepName)
	var out []string
	for _, r := range aggRows {
		shard := aggregateRowResult(r)
		if r.wantMatrixIs != "" && shard != r.wantMatrixIs {
			t.Fatalf("test model: %s matrix result %s, want %s", r.id, shard, r.wantMatrixIs)
		}
		green := aggregateExit(t, script, r.plan, r.legs, shard) == 0
		if green != r.wantGreen {
			out = append(out, fmt.Sprintf("%s: plan=%s legs=%s shard=%s -> green=%v, want %v", r.id, r.plan, r.legs, shard, green, r.wantGreen))
		}
	}
	for _, shard := range []string{"cancelled", "failure", "skipped", ""} {
		if aggregateExit(t, script, "success", "5", shard) == 0 {
			out = append(out, fmt.Sprintf("shard=%q with 5 legs planned was green", shard))
		}
	}
	return out
}

func TestIntegrationAggregateNeverGreenOnCancelledFailedOrSkippedShards(t *testing.T) {
	root := rootOf(t)
	if d := aggregateDefects(t, loadWf(t, root, "go.yml")); len(d) > 0 {
		t.Fatalf("%s", strings.Join(d, "\n"))
	}
	for _, m := range []mutation{
		{failFastGoYml, `if [ "${SHARD_RESULT}" != "success" ]; then`, `if [ "${SHARD_RESULT}" = "failure" ]; then`},
		{failFastGoYml, `if [ "${SHARD_RESULT}" != "success" ]; then`, `if [ "${SHARD_RESULT}" = "skipped" ]; then`},
		{failFastGoYml, `if [ "${PLAN_LEGS}" = "0" ] && [ "${SHARD_RESULT}" = "skipped" ]; then`, `if [ "${SHARD_RESULT}" = "skipped" ]; then`},
		{failFastGoYml, `if [ "${PLAN_RESULT}" != "success" ]; then`, `if [ "${PLAN_RESULT}" = "failure" ]; then`},
	} {
		tmp := plantTree(t, root, []string{failFastGoYml}, m)
		if d := aggregateDefects(t, loadWf(t, tmp, "go.yml")); len(d) == 0 {
			t.Errorf("planted aggregate defect was not caught: %q -> %q", m.old, m.new)
		}
	}
}

func runQueueGate(t *testing.T, script, event, integration string) int {
	t.Helper()
	env := []string{
		"GATE_NAME=go-queue-gate", "EVENT_NAME=" + event, "GATE_HAS_SELECTOR=true",
		"CHANGES_RESULT=success", "CHANGES_CODE=true", "HEAVY_ON_PR=false",
		"GATED_JOB_1=go-storage-integration|unconditional|" + integration,
		"GATED_JOB_2=go-container-smoke|queue-heavy|success",
		"GATED_JOB_3=go-container-security|queue-heavy|success",
		"GATED_JOB_4=go-container-reproducibility|queue-heavy|success",
		"GATED_JOB_5=go-auth-compose-proof|queue-heavy|success",
		"GATED_JOB_6=go-auth-web-proxy-proof|queue-heavy|success",
		"GATED_JOB_7=graphql-wire-parity|queue-heavy|success",
	}
	code, _, _ := bashRun(t, readScript(t, script), env)
	return code
}

func readScript(t *testing.T, path string) string {
	t.Helper()
	return readFile(t, path)
}

// queueGateDefects drives the whole chain: shard leg results -> go-storage-integration verdict step -> its job result
// -> go-queue-gate (the real ci/aggregate_gate_results.sh, with go-queue-gate's real wiring for the first row).
func queueGateDefects(t *testing.T, aggScript, gateScript string) []string {
	t.Helper()
	var out []string
	for _, r := range aggRows {
		shard := aggregateRowResult(r)
		integration := "failure"
		if aggregateExit(t, aggScript, r.plan, r.legs, shard) == 0 {
			integration = "success"
		}
		for _, ev := range []string{"merge_group", "pull_request"} {
			green := runQueueGate(t, gateScript, ev, integration) == 0
			if green != r.wantGreen {
				out = append(out, fmt.Sprintf("%s on %s: go-queue-gate green=%v, want %v", r.id, ev, green, r.wantGreen))
			}
		}
	}
	for _, res := range []string{"cancelled", "failure", "skipped", ""} {
		for _, ev := range []string{"merge_group", "pull_request"} {
			if runQueueGate(t, gateScript, ev, res) == 0 {
				out = append(out, fmt.Sprintf("go-queue-gate was green on %s with go-storage-integration=%q", ev, res))
			}
		}
	}
	if runQueueGate(t, gateScript, "merge_group", "success") != 0 {
		out = append(out, "go-queue-gate was red with every job success")
	}
	return out
}

func TestQueueGateNeverGreenWhenAShardWasCancelledFailedOrSkipped(t *testing.T) {
	root := rootOf(t)
	doc := loadWf(t, root, "go.yml")
	agg := stepRun(t, doc, aggregateJobName, aggregateStepName)
	gate := filepath.Join(root, "ci", "aggregate_gate_results.sh")
	if d := queueGateDefects(t, agg, gate); len(d) > 0 {
		t.Fatalf("%s", strings.Join(d, "\n"))
	}
	// Negative control: a gate script that accepts `cancelled` must be caught.
	gateFile := "ci/aggregate_gate_results.sh"
	tmp := plantTree(t, root, []string{gateFile}, mutation{gateFile,
		"    *)\n      add_failure \"${job_name} reported '${result:-<empty>}'\"\n      ;;",
		"    cancelled) ;;\n    *)\n      add_failure \"${job_name} reported '${result:-<empty>}'\"\n      ;;"})
	if d := queueGateDefects(t, agg, filepath.Join(tmp, gateFile)); len(d) == 0 {
		t.Error("a gate script that accepts a cancelled result was not caught")
	}
	// Negative control: an aggregate step that passes cancelled must be caught through the gate too.
	tree := plantTree(t, root, []string{failFastGoYml}, mutation{failFastGoYml,
		`if [ "${SHARD_RESULT}" != "success" ]; then`, `if [ "${SHARD_RESULT}" = "failure" ]; then`})
	badAgg := stepRun(t, loadWf(t, tree, "go.yml"), aggregateJobName, aggregateStepName)
	if d := queueGateDefects(t, badAgg, gate); len(d) == 0 {
		t.Error("an aggregate step that passes a cancelled matrix was not caught through go-queue-gate")
	}
}

// The naming step is diagnostics only: it runs after the verdict step, only on failure, and is continue-on-error, so
// its own result cannot turn a red job green or a green job red.
func namingStepDefects(doc yamlMap) []string {
	var out []string
	job := pinJob(doc, aggregateJobName)
	steps := stepsOf(job)
	verdictAt, namingAt := -1, -1
	for i, s := range steps {
		switch asString(s["name"]) {
		case aggregateStepName:
			verdictAt = i
		case namingStepName:
			namingAt = i
		}
	}
	if verdictAt < 0 || namingAt < 0 {
		return []string{fmt.Sprintf("steps %q and %q must both exist", aggregateStepName, namingStepName)}
	}
	if namingAt < verdictAt {
		out = append(out, "the naming step must come after the verdict step")
	}
	v, n := steps[verdictAt], steps[namingAt]
	if _, has := v["continue-on-error"]; has {
		out = append(out, "the verdict step must not be continue-on-error")
	}
	if asString(v["if"]) != "" {
		out = append(out, "the verdict step must carry no if")
	}
	if n["continue-on-error"] != true {
		out = append(out, "the naming step must be continue-on-error: true")
	}
	if asString(n["if"]) != "failure()" {
		out = append(out, fmt.Sprintf("the naming step if is %q, want failure()", asString(n["if"])))
	}
	if len(steps) != 2 {
		out = append(out, fmt.Sprintf("go-storage-integration has %d steps, want the verdict step and the naming step", len(steps)))
	}
	if job["if"] != "always()" {
		out = append(out, "go-storage-integration must keep `if: always()`")
	}
	if job["name"] != "go-storage-integration" {
		out = append(out, "the required check name go-storage-integration must not change")
	}
	if got := asMap(job["permissions"]); got["actions"] != "read" || got["contents"] != "read" || len(got) != 2 {
		out = append(out, fmt.Sprintf("job permissions %v, want contents: read and actions: read only", got))
	}
	return out
}

const ghStubOK = `#!/usr/bin/env bash
case "$*" in
  *"/logs"*) printf '2026-10-25T10:00:00Z === RUN TestRole\n2026-10-25T10:00:01Z --- FAIL: TestRole (0.1s)\nFAIL\tdev/pkg\t0.2s\n'; exit 0 ;;
  *"/jobs?"*) printf 'failure\t777\tgo-storage-integration-shard-packages-7\thttps://example.test/job/777\ncancelled\t778\tgo-storage-integration-shard-packages-8\thttps://example.test/job/778\ncancelled\t779\tgo-storage-integration-shard-packages-9\thttps://example.test/job/779\n'; exit 0 ;;
esac
exit 1
`

const ghStubBroken = "#!/usr/bin/env bash\necho 'gh: boom' >&2\nexit 4\n"

func runNaming(t *testing.T, script, stub string) (int, string, string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "gh"), []byte(stub), 0o755); err != nil {
		t.Fatal(err)
	}
	summary := filepath.Join(dir, "summary.md")
	env := []string{"GH_TOKEN=x", "REPO=o/r", "RUN_ID=1", "GITHUB_STEP_SUMMARY=" + summary, "PATH=" + dir + ":/usr/bin:/bin"}
	cmd := exec.Command("bash", "-c", script)
	cmd.Env = env
	var o, e bytes.Buffer
	cmd.Stdout, cmd.Stderr = &o, &e
	code := 0
	if err := cmd.Run(); err != nil {
		ee, ok := err.(*exec.ExitError)
		if !ok {
			t.Fatal(err)
		}
		code = ee.ExitCode()
	}
	b, _ := os.ReadFile(summary)
	return code, o.String() + string(b), e.String()
}

func TestRedShardNamingIsBestEffortAndNeverGatesTheVerdict(t *testing.T) {
	root := rootOf(t)
	doc := loadWf(t, root, "go.yml")
	if d := namingStepDefects(doc); len(d) > 0 {
		t.Fatalf("%s", strings.Join(d, "\n"))
	}
	naming := stepRun(t, doc, aggregateJobName, namingStepName)

	code, out, _ := runNaming(t, naming, ghStubOK)
	for _, want := range []string{"go-storage-integration-shard-packages-7", "https://example.test/job/777", "TestRole", "2 cancelled"} {
		if code != 0 || !strings.Contains(out, want) {
			t.Errorf("naming output must contain %q; code=%d out=%q", want, code, out)
		}
	}
	// The naming step fails to fetch anything: it still exits 0 and the verdict step still decides alone.
	if code, _, _ := runNaming(t, naming, ghStubBroken); code != 0 {
		t.Errorf("a failing gh must not fail the naming step; code=%d", code)
	}
	verdict := stepRun(t, doc, aggregateJobName, aggregateStepName)
	if aggregateExit(t, verdict, "success", "3", "failure") == 0 || aggregateExit(t, verdict, "success", "3", "success") != 0 {
		t.Error("the verdict step must decide alone: failure red, success green")
	}

	for _, m := range []mutation{
		{failFastGoYml, "        continue-on-error: true\n        env:\n          GH_TOKEN", "        env:\n          GH_TOKEN"},
		{failFastGoYml, "      - name: " + namingStepName + "\n        if: failure()", "      - name: " + namingStepName + "\n        if: always()"},
		{failFastGoYml, "      actions: read\n    runs-on: ubuntu-latest\n    timeout-minutes: 5\n    env:\n      PLAN_RESULT", "      actions: write\n    runs-on: ubuntu-latest\n    timeout-minutes: 5\n    env:\n      PLAN_RESULT"},
	} {
		tmp := plantTree(t, root, []string{failFastGoYml}, m)
		if d := namingStepDefects(loadWf(t, tmp, "go.yml")); len(d) == 0 {
			t.Errorf("planted naming-step defect was not caught: %q -> %q", m.old, m.new)
		}
	}
}

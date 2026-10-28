package workflows_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

const (
	qualityRepo      = "full-chaos/dev-health-ops"
	qualitySha       = "0123456789abcdef0123456789abcdef01234567"
	qualityLegStep   = "Run Go quality leg"
	qualityLegPrefix = "go-quality-leg ("
)

var qualityLegNames = []string{
	"static", "test-1-of-4", "test-2-of-4", "test-3-of-4", "test-4-of-4",
	"race-1-of-5", "race-2-of-5", "race-3-of-5", "race-4-of-5", "race-5-of-5",
}

type qualityLeg struct{ name, job, step string }

func qualityLegs(mutate func(i int, l *qualityLeg)) []qualityLeg {
	var legs []qualityLeg
	for i, name := range qualityLegNames {
		l := qualityLeg{name: name, job: "success", step: "success"}
		if mutate != nil {
			mutate(i, &l)
		}
		legs = append(legs, l)
	}
	return legs
}

func qualityJobsJSON(legs []qualityLeg) string {
	entries := []string{
		`{"name":"go-quality-plan","conclusion":"success","steps":[]}`,
		`{"name":"go-quality","conclusion":"success","steps":[]}`,
	}
	for _, l := range legs {
		entries = append(entries, fmt.Sprintf(
			`{"name":"%s%s)","conclusion":"%s","steps":[{"name":"Set up job","conclusion":"success"},{"name":"%s","conclusion":"%s"}]}`,
			qualityLegPrefix, l.name, l.job, qualityLegStep, l.step))
	}
	return `{"jobs":[` + strings.Join(entries, ",") + `]}`
}

func qualityRunsJSON(sha, event, repo, conclusion string) string {
	return fmt.Sprintf(`{"workflow_runs":[{"id":21,"head_sha":"%s","conclusion":"%s","event":"%s","head_repository":{"full_name":"%s"}}]}`,
		sha, conclusion, event, repo)
}

func runQualityHelper(t *testing.T, sha string, runs string, jobs string, failRuns bool, idFile string) (bool, string) {
	t.Helper()
	if _, err := exec.LookPath("jq"); err != nil {
		t.Fatalf("jq is required by this test and is not on PATH: %v", err)
	}
	script := filepath.Join(rootOf(t), "ci", "queue_tested_sha.sh")
	dir := t.TempDir()
	write := func(name, content string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	write("gh", queueGhStub)
	write("runs.json", runs)
	if jobs != "" {
		write("jobs-21.json", jobs)
	}
	if failRuns {
		write("fail-runs", "")
	}
	command := exec.Command("bash", script, qualityRepo, sha, "go-quality.yml", qualityLegPrefix, qualityLegStep, "10")
	command.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "STUB_DIR="+dir)
	if idFile != "" {
		command.Env = append(command.Env, "QUEUE_TESTED_RUN_ID_FILE="+idFile)
	}
	err := command.Run()
	calls, _ := os.ReadFile(filepath.Join(dir, "calls"))
	return err == nil, string(calls)
}

// The go-quality form of ci/queue_tested_sha.sh answers only for a green merge_group run of this repository, for exactly
// this sha, in which all ten legs ran their gate in the latest attempt. Every other answer keeps the legs running.
func TestQueueTestedShaForGoQualityAnswersOnlyForAFullyRanQueueRun(t *testing.T) {
	good := qualityRunsJSON(qualitySha, "merge_group", qualityRepo, "success")
	allRan := qualityJobsJSON(qualityLegs(nil))
	cases := []struct {
		name     string
		sha      string
		runs     string
		jobs     string
		failRuns bool
		want     bool
	}{
		{"all ten legs ran green", qualitySha, good, allRan, false, true},
		{"a test leg was skipped by its plan", qualitySha, good,
			qualityJobsJSON(qualityLegs(func(i int, l *qualityLeg) {
				if i == 2 {
					l.step = "skipped"
				}
			})), false, false},
		{"a race leg was skipped by its plan", qualitySha, good,
			qualityJobsJSON(qualityLegs(func(i int, l *qualityLeg) {
				if i == 6 {
					l.step = "skipped"
				}
			})), false, false},
		{"the static leg step did not run", qualitySha, good,
			qualityJobsJSON(qualityLegs(func(i int, l *qualityLeg) {
				if i == 0 {
					l.step = "skipped"
				}
			})), false, false},
		{"a leg step failed", qualitySha, good,
			qualityJobsJSON(qualityLegs(func(i int, l *qualityLeg) {
				if i == 5 {
					l.step = "failure"
				}
			})), false, false},
		{"a leg job failed", qualitySha, good,
			qualityJobsJSON(qualityLegs(func(i int, l *qualityLeg) {
				if i == 5 {
					l.job = "failure"
				}
			})), false, false},
		{"a leg job was cancelled", qualitySha, good,
			qualityJobsJSON(qualityLegs(func(i int, l *qualityLeg) {
				if i == 1 {
					l.job = "cancelled"
				}
			})), false, false},
		{"only nine legs", qualitySha, good, qualityJobsJSON(qualityLegs(nil)[1:]), false, false},
		{"no leg job at all", qualitySha, good, qualityJobsJSON(nil), false, false},
		{"the run is from a fork repository", qualitySha, qualityRunsJSON(qualitySha, "merge_group", "someone/dev-health-ops", "success"), allRan, false, false},
		{"the run is not a merge_group run", qualitySha, qualityRunsJSON(qualitySha, "push", qualityRepo, "success"), allRan, false, false},
		{"the run is for another sha", qualitySha, qualityRunsJSON("fedcba9876543210fedcba9876543210fedcba98", "merge_group", qualityRepo, "success"), allRan, false, false},
		{"the latest attempt of the run failed (an older attempt was green)", qualitySha, qualityRunsJSON(qualitySha, "merge_group", qualityRepo, "failure"), allRan, false, false},
		{"the run is not finished", qualitySha, qualityRunsJSON(qualitySha, "merge_group", qualityRepo, "null"), allRan, false, false},
		{"the latest attempt has a failed leg while an older attempt had none", qualitySha, good,
			qualityJobsJSON(qualityLegs(func(i int, l *qualityLeg) {
				if i == 7 {
					l.job, l.step = "failure", "failure"
				}
			})), false, false},
		{"the runs call fails", qualitySha, "", "", true, false},
		{"the jobs call fails", qualitySha, good, "", false, false},
		{"a short sha", qualitySha[:12], good, allRan, false, false},
		{"an empty runs answer", qualitySha, `{"workflow_runs":[]}`, "", false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			idFile := filepath.Join(t.TempDir(), "run-id")
			got, calls := runQualityHelper(t, tc.sha, tc.runs, tc.jobs, tc.failRuns, idFile)
			if got != tc.want {
				t.Fatalf("exit ok = %v, want %v", got, tc.want)
			}
			recorded, err := os.ReadFile(idFile)
			if tc.want {
				if err != nil || strings.TrimSpace(string(recorded)) != "21" {
					t.Errorf("run id file = %q (%v), want 21", recorded, err)
				}
				for _, want := range []string{"workflows/go-quality.yml/runs", "event=merge_group", "head_sha=" + tc.sha, "status=completed", "actions/runs/21/jobs", "filter=latest"} {
					if !strings.Contains(calls, want) {
						t.Errorf("gh was called with %q, which lacks %q", calls, want)
					}
				}
			} else if err == nil {
				t.Errorf("run id file written on a refusal: %q", recorded)
			}
		})
	}
}

// A planted failing leg first, then more output than a pipe holds: the go-quality form keeps the SIGPIPE-safe shape.
func TestQueueTestedShaForGoQualityStaysRedOnAFailedLegInALongAnswer(t *testing.T) {
	legs := qualityLegs(func(i int, l *qualityLeg) {
		if i == 0 {
			l.step = "failure"
		}
	})
	for i := 0; i < 100000; i++ {
		legs = append(legs, qualityLeg{name: fmt.Sprintf("extra-%d", i), job: "success", step: "success"})
	}
	jobs := qualityJobsJSON(legs)
	runs := qualityRunsJSON(qualitySha, "merge_group", qualityRepo, "success")
	for attempt := 1; attempt <= 5; attempt++ {
		if ok, _ := runQualityHelper(t, qualitySha, runs, jobs, false, ""); ok {
			t.Fatalf("attempt %d: exit 0 although the first leg failed", attempt)
		}
	}
}

func qualityPlanStep(t *testing.T) yamlMap {
	t.Helper()
	job := pinJob(loadWf(t, rootOf(t), "go-quality.yml"), "go-quality-plan")
	for _, s := range stepsOf(job) {
		if asString(s["id"]) == "queue" {
			return s
		}
	}
	t.Fatal("go-quality-plan has no step with id queue")
	return nil
}

func TestGoQualityQueueLookupIsPushOnlyReadOnlyAndFailsOpen(t *testing.T) {
	root := rootOf(t)
	doc := loadWf(t, root, "go-quality.yml")
	step := qualityPlanStep(t)
	condition := asString(step["if"])
	for _, want := range []string{"github.event_name == 'push'", "github.ref == 'refs/heads/main'", "vars.MERGE_QUEUE == 'enabled'"} {
		if !strings.Contains(condition, want) {
			t.Errorf("queue step if %q lacks %q", condition, want)
		}
	}
	if b, _ := step["continue-on-error"].(bool); !b {
		t.Error("the queue lookup step must be continue-on-error: a lookup failure must run every leg, never fail the plan job")
	}
	run := asString(step["run"])
	if !strings.Contains(run, `bash ci/queue_tested_sha.sh "${GITHUB_REPOSITORY}" "${GITHUB_SHA}" go-quality.yml 'go-quality-leg (' 'Run Go quality leg' 8`) {
		t.Error("the lookup must call the helper for the pushed sha with the go-quality leg arguments")
	}
	if !strings.Contains(run, "QUEUE_TESTED_RUN_ID_FILE=") || !strings.Contains(run, `*[!0-9]*) run_id="" ;;`) {
		t.Error("the run id must come from the helper file and be digits only, else empty")
	}
	if got := strings.Count(run, "|| true"); got != 1 {
		t.Errorf("the lookup step has %d `|| true`, want 1", got)
	}
	env, _ := step["env"].(yamlMap)
	if asString(env["GH_TOKEN"]) != "${{ github.token }}" {
		t.Error("the lookup needs the workflow token")
	}
	for _, name := range []string{"QUEUE-TESTED SHA: legs skipped"} {
		if !strings.Contains(run, name) {
			t.Errorf("the skip notice %q is missing", name)
		}
	}
	if !strings.Contains(run, "run ${run_id}") {
		t.Error("the skip notice must name the queue run id")
	}
	plan := pinJob(doc, "go-quality-plan")
	outputs := asMap(plan["outputs"])
	if asString(outputs["queue_run"]) != "${{ steps.queue.outputs.queue_run }}" {
		t.Errorf("plan output queue_run = %q", asString(outputs["queue_run"]))
	}
}

func TestGoQualityPermissionsWidenOnlyForTheLookup(t *testing.T) {
	root := rootOf(t)
	doc := loadWf(t, root, "go-quality.yml")
	top := asMap(doc["permissions"])
	if len(top) != 2 || asString(top["contents"]) != "read" || asString(top["packages"]) != "read" {
		t.Errorf("workflow permissions = %v, want contents: read and packages: read only", top)
	}
	plan := asMap(pinJob(doc, "go-quality-plan")["permissions"])
	if len(plan) != 3 || asString(plan["contents"]) != "read" || asString(plan["packages"]) != "read" || asString(plan["actions"]) != "read" {
		t.Errorf("go-quality-plan permissions = %v, want contents, packages and actions, all read", plan)
	}
	for _, name := range []string{"go-quality-leg", "go-quality"} {
		if _, has := pinJob(doc, name)["permissions"]; has {
			t.Errorf("%s must not carry its own permissions block", name)
		}
	}
	raw, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "go-quality.yml"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "pull_request_target") {
		t.Error("go-quality.yml must not use pull_request_target")
	}
	on := asMap(doc["on"])
	if _, has := on["merge_group"]; !has {
		t.Error("go-quality.yml must still run on merge_group")
	}
}

func qualityRelevanceStep(t *testing.T) yamlMap {
	t.Helper()
	leg := pinJob(loadWf(t, rootOf(t), "go-quality.yml"), "go-quality-leg")
	step, count := stepNamed(leg, "Decide whether this leg runs, and on which packages")
	if count != 1 {
		t.Fatalf("%d relevance steps", count)
	}
	return step
}

func TestGoQualityLegSkipIsPushOnlyNeverStaticAndKeepsReceipts(t *testing.T) {
	root := rootOf(t)
	doc := loadWf(t, root, "go-quality.yml")
	step := qualityRelevanceStep(t)
	run := asString(step["run"])
	staticAt := strings.Index(run, `if [ "${LEG}" = "static" ]; then`)
	skipAt := strings.Index(run, `if [ "${GITHUB_EVENT_NAME}" = push ] && [ -n "${QUEUE_RUN}" ]; then`)
	caseAt := strings.Index(run, `case "${PLAN_VERDICT}" in`)
	if staticAt < 0 || skipAt < 0 || caseAt < 0 || !(staticAt < skipAt && skipAt < caseAt) {
		t.Fatalf("the queue skip must sit after the static exit and before the plan verdict: static=%d skip=%d case=%d", staticAt, skipAt, caseAt)
	}
	staticBlock := run[staticAt:skipAt]
	if !strings.Contains(staticBlock, "relevant=true") || !strings.Contains(staticBlock, "exit 0") {
		t.Error("the static leg must exit relevant=true before the queue rule can see it")
	}
	skipBlock := run[skipAt:caseAt]
	for _, want := range []string{"skipped: queue-tested sha", "${QUEUE_RUN}", "relevant=false", "warm=true"} {
		if !strings.Contains(skipBlock, want) {
			t.Errorf("queue skip block lacks %q", want)
		}
	}
	env, _ := step["env"].(yamlMap)
	if asString(env["QUEUE_RUN"]) != "${{ needs.go-quality-plan.outputs.queue_run }}" {
		t.Errorf("QUEUE_RUN = %q", asString(env["QUEUE_RUN"]))
	}

	// The skip is keyed on the plan output, which only a push can set, so a pull request or queue entry runs its legs.
	plan := qualityPlanStep(t)
	if !strings.Contains(asString(plan["if"]), "github.event_name == 'push'") {
		t.Error("only a push may look up a queue run")
	}

	// Required context names are the leg job name and the fan-in name, unchanged.
	leg := pinJob(doc, "go-quality-leg")
	if asString(leg["name"]) != "go-quality-leg (${{ matrix.name }})" {
		t.Errorf("leg job name = %q", asString(leg["name"]))
	}
	if asString(pinJob(doc, "go-quality")["name"]) != "go-quality" {
		t.Error("the fan-in must keep the name go-quality")
	}
	if _, has := leg["if"]; has {
		t.Error("a job-level if on the legs would report them skipped")
	}
	var names []string
	// CHAOS-11418: the full leg list (the one a push and a queue entry run) lives in ci/go_quality_legs.json.
	for _, e := range goQualityLegList(t, repoRoot(t)) {
		names = append(names, asString(e["name"]))
	}
	if strings.Join(names, ",") != strings.Join(qualityLegNames, ",") {
		t.Errorf("matrix legs %v differ from the pinned list %v", names, qualityLegNames)
	}
	if len(names) != 10 {
		t.Errorf("the helper is called with min-jobs 10, the matrix has %d legs", len(names))
	}
}

func TestGoQualityWarmPathBuildsOnlyOnAKeyMissAndSavesOnlyAfterABuild(t *testing.T) {
	leg := pinJob(loadWf(t, rootOf(t), "go-quality.yml"), "go-quality-leg")
	steps := map[string]yamlMap{}
	for _, s := range stepsOf(leg) {
		steps[asString(s["name"])] = s
	}
	warm := steps["Compile this leg's packages into the cache on a queue-skipped push (key miss only)"]
	if warm == nil {
		t.Fatal("the warm compile step is missing")
	}
	cond := asString(warm["if"])
	for _, want := range []string{"steps.relevance.outputs.warm == 'true'", "runner.environment == 'github-hosted'", "steps.go-cache.outputs.cache-hit != 'true'"} {
		if !strings.Contains(cond, want) {
			t.Errorf("warm step if %q lacks %q", cond, want)
		}
	}
	if !strings.Contains(asString(warm["run"]), "bash ci/check_go.sh ci-leg-warm ${{ matrix.args }}") {
		t.Errorf("warm step run = %q", asString(warm["run"]))
	}
	var save yamlMap
	for name, s := range steps {
		if strings.HasPrefix(name, "Save this leg's Go build and module cache") {
			save = s
		}
	}
	if save == nil {
		t.Fatal("the save step is missing")
	}
	saveIf := asString(save["if"])
	for _, want := range []string{"github.event_name == 'push'", "github.ref == 'refs/heads/main'", "steps.go-cache.outputs.cache-hit != 'true'", "steps.warm-build.outcome == 'success'"} {
		if !strings.Contains(saveIf, want) {
			t.Errorf("save step if %q lacks %q", saveIf, want)
		}
	}
	for name, s := range steps {
		if strings.Contains(asString(s["run"]), "ci-leg-warm") && name != "Compile this leg's packages into the cache on a queue-skipped push (key miss only)" {
			t.Errorf("step %q also runs the warm verb", name)
		}
	}
	for _, name := range []string{"Run Go quality leg"} {
		cond := asString(steps[name]["if"])
		if strings.Contains(cond, "warm") || cond != "steps.relevance.outputs.relevant == 'true'" {
			t.Errorf("step %q if %q: the real leg runs on relevant=true only", name, cond)
		}
	}
}

func TestGoQualityFanInReportsWhichLegsRan(t *testing.T) {
	fan := pinJob(loadWf(t, rootOf(t), "go-quality.yml"), "go-quality")
	step, count := stepNamed(fan, "Report which legs ran and which the queue rule skipped")
	if count != 1 {
		t.Fatalf("%d report steps", count)
	}
	run := asString(step["run"])
	for _, want := range []string{"static ran", "skipped: queue-tested sha, merge_group run ${QUEUE_RUN}", "every leg ran", "GITHUB_STEP_SUMMARY"} {
		if !strings.Contains(run, want) {
			t.Errorf("report step lacks %q", want)
		}
	}
	for _, name := range qualityLegNames[1:] {
		if !regexp.MustCompile(`\b` + regexp.QuoteMeta(name) + `\b`).MatchString(run) {
			t.Errorf("report step does not name leg %s", name)
		}
	}
	if !strings.Contains(asString(step["if"]), "github.event_name == 'push'") {
		t.Error("the report is a push report")
	}
}

func runQualityHelperMulti(t *testing.T, runs string, jobsByID map[string]string, extra ...string) (bool, string) {
	t.Helper()
	dir := t.TempDir()
	write := func(name, content string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	write("gh", queueGhStub)
	write("runs.json", runs)
	for id, jobs := range jobsByID {
		write("jobs-"+id+".json", jobs)
	}
	idFile := filepath.Join(dir, "run-id")
	args := append([]string{filepath.Join(rootOf(t), "ci", "queue_tested_sha.sh"), qualityRepo, qualitySha, "go-quality.yml", qualityLegPrefix, qualityLegStep, "10"}, extra...)
	command := exec.Command("bash", args...)
	command.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "STUB_DIR="+dir, "QUEUE_TESTED_RUN_ID_FILE="+idFile)
	err := command.Run()
	if _, statErr := os.Stat(idFile); err != nil && statErr == nil {
		t.Errorf("run id file written on a refusal")
	}
	return err == nil, idFile
}

// A failed jobs lookup ends the helper with exit 1, even when another run for the same sha is green.
func TestQueueTestedShaForGoQualityFailsClosedOnAJobsLookupError(t *testing.T) {
	green := qualityJobsJSON(qualityLegs(nil))
	twoRuns := `{"workflow_runs":[` +
		fmt.Sprintf(`{"id":21,"head_sha":"%[1]s","conclusion":"success","event":"merge_group","head_repository":{"full_name":"%[2]s"}},`, qualitySha, qualityRepo) +
		fmt.Sprintf(`{"id":22,"head_sha":"%[1]s","conclusion":"success","event":"merge_group","head_repository":{"full_name":"%[2]s"}}`, qualitySha, qualityRepo) +
		`]}`
	cases := []struct {
		name string
		jobs map[string]string
		want bool
	}{
		{"the first run's jobs call fails, the second is green", map[string]string{"22": green}, false},
		{"the first run has no leg jobs at all (not an error), the second is green", map[string]string{"21": qualityJobsJSON(nil), "22": green}, true},
		{"the first run has a failed leg, the second is green", map[string]string{"21": qualityJobsJSON(qualityLegs(func(i int, l *qualityLeg) {
			if i == 3 {
				l.step = "failure"
			}
		})), "22": green}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, _ := runQualityHelperMulti(t, twoRuns, tc.jobs)
			if got != tc.want {
				t.Fatalf("exit ok = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestQueueTestedShaRefusesExtraArguments(t *testing.T) {
	runs := qualityRunsJSON(qualitySha, "merge_group", qualityRepo, "success")
	green := map[string]string{"21": qualityJobsJSON(qualityLegs(nil))}
	if got, _ := runQualityHelperMulti(t, runs, green); !got {
		t.Fatal("the documented six arguments must pass")
	}
	if got, _ := runQualityHelperMulti(t, runs, green, "unexpected-extra-argument"); got {
		t.Fatal("a seventh argument must be refused")
	}
}

func TestQueueTestedShaRefusesTooFewArguments(t *testing.T) {
	script := filepath.Join(rootOf(t), "ci", "queue_tested_sha.sh")
	for _, args := range [][]string{{}, {qualityRepo}} {
		if err := exec.Command("bash", append([]string{script}, args...)...).Run(); err == nil {
			t.Errorf("%d argument(s) must be refused", len(args))
		}
	}
}

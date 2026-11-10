package workflows_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// ci/ready_reuse.sh names the legs a ready_for_review run may skip because they already passed on the same head sha.
// Each scenario serves a fixture world through a fake gh and checks the passed list; each plant removes one trust rule
// and must turn its scenario red.

const (
	rdyScript = "ci/ready_reuse.sh"
	rdyRepo   = "o/r"
	rdySelf   = 191
	rdyWf     = 7
	rdyPR     = 6048
)

var (
	rdyHead  = strings.Repeat("a", 40)
	rdyMerge = strings.Repeat("b", 40)
	rdyOther = strings.Repeat("c", 40)
)

type rdyJob struct {
	ID         int64  `json:"id"`
	Name       string `json:"name"`
	Status     string `json:"status"`
	Conclusion string `json:"conclusion"`
	RunAttempt int    `json:"run_attempt"`
	log        string
}

type rdyRun struct {
	ID         int64  `json:"id"`
	RunAttempt int    `json:"run_attempt"`
	Status     string `json:"status"`
	Event      string `json:"event"`
	HeadSHA    string `json:"head_sha"`
	// jobs per attempt number; the latest attempt is RunAttempt.
	jobs map[int][]rdyJob
}

type rdyWorld struct {
	polls     [][]rdyRun // the run list per poll; the last one repeats
	analyses  []map[string]any
	action    string
	units     string
	waitCap   string
	poll      string
	noRunsAPI bool
}

func rdyOK(id int64, name string) rdyJob {
	return rdyJob{ID: id, Name: name, Status: "completed", Conclusion: "success", RunAttempt: 1}
}

func rdyJobWith(id int64, name, conclusion string, attempt int) rdyJob {
	return rdyJob{ID: id, Name: name, Status: "completed", Conclusion: conclusion, RunAttempt: attempt}
}

func rdyDoneRun(id int64, jobs ...rdyJob) rdyRun {
	return rdyRun{ID: id, RunAttempt: 1, Status: "completed", Event: "pull_request", HeadSHA: rdyHead, jobs: map[int][]rdyJob{1: jobs}}
}

const rdyFakeGh = `#!/usr/bin/env bash
path=""
for a in "$@"; do
  case "$a" in api | --paginate | --allow-escape-sequences) ;; *) path="$a" ;; esac
done
printf '%s\n' "$path" >> "$FAKE_GH_DIR/calls.log"
key="$(printf '%s' "$path" | sed 's/[^A-Za-z0-9._-]/_/g')"
n="$(cat "$FAKE_GH_DIR/$key.count" 2> /dev/null || echo 0)"
n=$((n + 1))
echo "$n" > "$FAKE_GH_DIR/$key.count"
if [ -f "$FAKE_GH_DIR/$key.$n" ]; then cat "$FAKE_GH_DIR/$key.$n"; exit 0; fi
if [ -f "$FAKE_GH_DIR/$key" ]; then cat "$FAKE_GH_DIR/$key"; exit 0; fi
echo "fake gh: no fixture for $path" >&2
exit 1
`

var rdyKeyRe = regexp.MustCompile(`[^A-Za-z0-9._-]`)

func rdyWrite(dir, path, body string) {
	if err := os.WriteFile(filepath.Join(dir, rdyKeyRe.ReplaceAllString(path, "_")), []byte(body), 0o644); err != nil {
		panic(err)
	}
}

func rdyJSON(v any) string {
	raw, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(raw)
}

type rdyResult struct {
	code   int
	stdout string
	passed string
	reused string
	calls  string
}

// rdyRunScript serves the world and runs the script. Paths match what the script asks for, so a changed URL is a red test.
func rdyRunScript(script, work string, w rdyWorld) rdyResult {
	bin := filepath.Join(work, "bin")
	fix := filepath.Join(work, "fix")
	for _, d := range []string{bin, fix} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			panic(err)
		}
	}
	if err := os.WriteFile(filepath.Join(bin, "gh"), []byte(rdyFakeGh), 0o755); err != nil {
		panic(err)
	}
	rdyWrite(fix, fmt.Sprintf("repos/%s/actions/runs/%d", rdyRepo, rdySelf), rdyJSON(map[string]any{"id": rdySelf, "workflow_id": rdyWf}))
	list := fmt.Sprintf("repos/%s/actions/workflows/%d/runs?head_sha=%s&event=pull_request&per_page=100", rdyRepo, rdyWf, rdyHead)
	if !w.noRunsAPI {
		for i, runs := range w.polls {
			body := rdyJSON(map[string]any{"total_count": len(runs), "workflow_runs": runs})
			rdyWrite(fix, fmt.Sprintf("%s.%d", rdyKeyRe.ReplaceAllString(list, "_"), i+1), body)
			if i == len(w.polls)-1 {
				rdyWrite(fix, list, body)
			}
			for _, r := range runs {
				for a, jobs := range r.jobs {
					rdyWrite(fix, fmt.Sprintf("repos/%s/actions/runs/%d/attempts/%d/jobs?per_page=100", rdyRepo, r.ID, a),
						rdyJSON(map[string]any{"total_count": len(jobs), "jobs": jobs}))
					for _, j := range jobs {
						rdyWrite(fix, fmt.Sprintf("repos/%s/actions/jobs/%d/logs", rdyRepo, j.ID), j.log)
					}
				}
			}
		}
	}
	rdyWrite(fix, fmt.Sprintf("repos/%s/code-scanning/analyses?ref=refs/pull/%d/merge&per_page=100", rdyRepo, rdyPR), rdyJSON(w.analyses))
	action := w.action
	if action == "" {
		action = "ready_for_review"
	}
	waitCap, poll := w.waitCap, w.poll
	if waitCap == "" {
		waitCap = "30"
	}
	if poll == "" {
		poll = "0"
	}
	out := filepath.Join(work, "github_output")
	env := []string{
		"PATH=" + bin + ":/usr/bin:/bin:/usr/local/bin",
		"HOME=" + work,
		"FAKE_GH_DIR=" + fix,
		"GITHUB_OUTPUT=" + out,
		"EVENT_NAME=pull_request",
		"EVENT_ACTION=" + action,
		"REPO=" + rdyRepo,
		"RUN_ID=" + fmt.Sprint(rdySelf),
		"HEAD_SHA=" + rdyHead,
		"MERGE_SHA=" + rdyMerge,
		"PR_NUMBER=" + fmt.Sprint(rdyPR),
		"READY_REUSE_UNITS=" + w.units,
		"READY_REUSE_WAIT_S=" + waitCap,
		"READY_REUSE_POLL_S=" + poll,
	}
	res := cmdSpec{dir: work, env: env}.run("bash", script)
	r := rdyResult{code: res.code, stdout: res.stdout + res.stderr}
	if raw, err := os.ReadFile(out); err == nil {
		for _, line := range strings.Split(string(raw), "\n") {
			if v, found := strings.CutPrefix(line, "passed="); found {
				r.passed = v
			}
			if v, found := strings.CutPrefix(line, "reused="); found {
				r.reused = v
			}
		}
	}
	if raw, err := os.ReadFile(filepath.Join(fix, "calls.log")); err == nil {
		r.calls = string(raw)
	}
	return r
}

func rdyExpect(script, work string, w rdyWorld, wantPassed string, wantOut ...string) error {
	r := rdyRunScript(script, work, w)
	if r.code != 0 {
		return fmt.Errorf("exit %d, want 0:\n%s", r.code, r.stdout)
	}
	if r.passed != wantPassed {
		return fmt.Errorf("passed=%s, want %s:\n%s", r.passed, wantPassed, r.stdout)
	}
	for _, s := range wantOut {
		if neg, found := strings.CutPrefix(s, "!"); found {
			if strings.Contains(r.stdout, neg) {
				return fmt.Errorf("output has %q:\n%s", neg, r.stdout)
			}
		} else if !strings.Contains(r.stdout, s) {
			return fmt.Errorf("output lacks %q:\n%s", s, r.stdout)
		}
	}
	return nil
}

const (
	rdyIntegLine  = "2026-11-10T15:20:01.1234567Z all deterministic Go integration shards passed\n"
	rdyIntegZero  = "2026-11-10T15:20:01.1234567Z plan selected 0 integration shards; none ran (reason: planner)\n"
	rdyIntegEcho  = "2026-11-10T15:20:00.0000000Z \x1b[36;1m          echo \"all deterministic Go integration shards passed\"\x1b[0m\n"
	rdyReusedLine = "2026-11-10T15:20:01.0000000Z REUSED go-storage-integration from run 90 attempt 1 job 900\n"
	rdyUnitA      = "a exact 1 - leg-a"
	rdyUnitInteg  = "go-storage-integration-shard exact 1 integ go-storage-integration"
	rdyUnitVenue  = "venue-oracles-changed prefix 4 - venue-oracles (changed packages, shard "
	rdyUnitCodeQL = "analyze prefix 1 codeql-merge Analyze ("
	rdyUnitTwin   = "repro twin 2 - go-container-reproducibility"
)

func rdyIntegJob(id int64, log string) rdyJob {
	j := rdyOK(id, "go-storage-integration")
	j.log = log
	return j
}

func rdyVenueJobs(base int64, conclusion string) []rdyJob {
	var out []rdyJob
	for i := 1; i <= 4; i++ {
		out = append(out, rdyJobWith(base+int64(i), fmt.Sprintf("venue-oracles (changed packages, shard %d/4)", i), conclusion, 1))
	}
	return out
}

func readyReuseScenarios() []scenario {
	s := func(name string, w rdyWorld, want string, out ...string) scenario {
		return scenario{name: name, run: func(script, work string) error { return rdyExpect(script, work, w, want, out...) }}
	}
	olderAttempt := rdyRun{ID: 100, RunAttempt: 2, Status: "completed", Event: "pull_request", HeadSHA: rdyHead, jobs: map[int][]rdyJob{
		1: {rdyOK(11, "leg-a")},
		2: {rdyOK(11, "leg-a")}, // a stale listing: the job belongs to attempt 1, not the latest attempt 2
	}}
	pushRun := rdyDoneRun(100, rdyOK(11, "leg-a"))
	pushRun.Event = "push"
	otherSha := rdyDoneRun(100, rdyOK(11, "leg-a"))
	otherSha.HeadSHA = rdyOther
	inflight := rdyDoneRun(100, rdyOK(11, "leg-a"))
	inflight.Status = "in_progress"
	// #6048: a synchronize run with a draft payload at the same sha; its venue legs never started (one skipped row
	// with the unexpanded matrix name), and its ready-reuse job ended at once.
	draftPayload := rdyDoneRun(123, rdyJobWith(1230, "ready-reuse", "success", 1), rdyJobWith(1231, "venue-oracles (changed packages, shard ${{ matrix.shard }}/4)", "skipped", 1))
	draftPayloadInflight := draftPayload
	draftPayloadInflight.Status = "in_progress"
	newerWaiter := rdyRun{ID: 200, RunAttempt: 1, Status: "in_progress", Event: "pull_request", HeadSHA: rdyHead, jobs: map[int][]rdyJob{
		1: {{ID: 2000, Name: "ready-reuse", Status: "in_progress", RunAttempt: 1}},
	}}
	stuck := rdyDoneRun(100, rdyOK(11, "leg-a"))
	stuck.Status = "in_progress"
	codeqlRun := rdyDoneRun(100, rdyOK(31, "Analyze (go)"), rdyOK(32, "Analyze (actions)"))
	analysis := []map[string]any{{"tool": map[string]any{"name": "CodeQL"}, "commit_sha": rdyMerge}}
	return []scenario{
		s("success job reused", rdyWorld{units: rdyUnitA, polls: [][]rdyRun{{rdyDoneRun(100, rdyOK(11, "leg-a"))}}}, `["a"]`, "REUSED a from run 100 attempt 1 job 11"),
		s("skipped job in a successful run not reused", rdyWorld{units: rdyUnitA, polls: [][]rdyRun{{rdyDoneRun(100, rdyJobWith(11, "leg-a", "skipped", 1))}}}, `[]`, "RUN a:"),
		s("failed job not reused", rdyWorld{units: rdyUnitA, polls: [][]rdyRun{{rdyDoneRun(100, rdyJobWith(11, "leg-a", "failure", 1))}}}, `[]`),
		s("other sha not reused", rdyWorld{units: rdyUnitA, polls: [][]rdyRun{{otherSha}}}, `[]`),
		s("push event not reused", rdyWorld{units: rdyUnitA, polls: [][]rdyRun{{pushRun}}}, `[]`),
		s("older attempt not reused", rdyWorld{units: rdyUnitA, polls: [][]rdyRun{{olderAttempt}}}, `[]`),
		s("label-gated leg not started not reused", rdyWorld{units: rdyUnitVenue, polls: [][]rdyRun{{rdyDoneRun(100, rdyJobWith(51, "venue-oracles (changed packages, shard ${{ matrix.shard }}/4)", "skipped", 1))}}}, `[]`),
		s("matrix with all legs passed reused", rdyWorld{units: rdyUnitVenue, polls: [][]rdyRun{{rdyDoneRun(100, rdyVenueJobs(50, "success")...)}}}, `["venue-oracles-changed"]`, "job 51,52,53,54"),
		s("matrix below its leg count not reused", rdyWorld{units: rdyUnitVenue, polls: [][]rdyRun{{rdyDoneRun(100, rdyVenueJobs(50, "success")[:3]...)}}}, `[]`),
		s("integ evidence line reused", rdyWorld{units: rdyUnitInteg, polls: [][]rdyRun{{rdyDoneRun(100, rdyIntegJob(61, rdyIntegEcho+rdyIntegLine))}}}, `["go-storage-integration-shard"]`, "REUSED go-storage-integration-shard from run 100 attempt 1 job 61"),
		s("integ zero-shard planner line reused", rdyWorld{units: rdyUnitInteg, polls: [][]rdyRun{{rdyDoneRun(100, rdyIntegJob(61, rdyIntegZero))}}}, `["go-storage-integration-shard"]`),
		s("integ missing evidence line not reused", rdyWorld{units: rdyUnitInteg, polls: [][]rdyRun{{rdyDoneRun(100, rdyIntegJob(61, rdyIntegEcho))}}}, `[]`),
		s("integ REUSED-sourced job not reused", rdyWorld{units: rdyUnitInteg, polls: [][]rdyRun{{rdyDoneRun(100, rdyIntegJob(61, rdyReusedLine+rdyIntegLine))}}}, `[]`),
		s("in-flight run waited then reused", rdyWorld{units: rdyUnitA, polls: [][]rdyRun{{inflight}, {rdyDoneRun(100, rdyOK(11, "leg-a"))}}}, `["a"]`, "waiting for runs 100", "REUSED a from run 100"),
		s("6048 draft-payload run gives no reuse", rdyWorld{units: rdyUnitVenue, polls: [][]rdyRun{{draftPayload}}}, `[]`, "RUN venue-oracles-changed:"),
		s("6048 draft-payload run in flight is waited for", rdyWorld{units: rdyUnitVenue, polls: [][]rdyRun{{draftPayloadInflight}, {draftPayload}}}, `[]`, "waiting for runs 123"),
		s("newer waiting ready run not waited for", rdyWorld{units: rdyUnitA, waitCap: "3", poll: "1", polls: [][]rdyRun{{newerWaiter, rdyDoneRun(100, rdyOK(11, "leg-a"))}}}, `["a"]`, "!waiting for runs", "!READY REUSE WAIT CAP"),
		s("wait cap reached reuses nothing in flight", rdyWorld{units: rdyUnitA, waitCap: "0", polls: [][]rdyRun{{stuck}}}, `[]`, "READY REUSE WAIT CAP"),
		s("codeql with an analysis for the merge sha reused", rdyWorld{units: rdyUnitCodeQL, analyses: analysis, polls: [][]rdyRun{{codeqlRun}}}, `["analyze"]`),
		s("codeql without an analysis for the merge sha not reused", rdyWorld{units: rdyUnitCodeQL, polls: [][]rdyRun{{codeqlRun}}}, `[]`),
		s("twin with one leg run reused", rdyWorld{units: rdyUnitTwin, polls: [][]rdyRun{{rdyDoneRun(100, rdyOK(71, "go-container-reproducibility"), rdyJobWith(72, "go-container-reproducibility", "skipped", 1))}}}, `["repro"]`, "job 71"),
		s("twin with both legs skipped not reused", rdyWorld{units: rdyUnitTwin, polls: [][]rdyRun{{rdyDoneRun(100, rdyJobWith(71, "go-container-reproducibility", "skipped", 1), rdyJobWith(72, "go-container-reproducibility", "skipped", 1))}}}, `[]`),
		s("twin with a failed leg not reused", rdyWorld{units: rdyUnitTwin, polls: [][]rdyRun{{rdyDoneRun(100, rdyOK(71, "go-container-reproducibility"), rdyJobWith(72, "go-container-reproducibility", "failure", 1))}}}, `[]`),
		s("unit lines AND together", rdyWorld{units: rdyUnitA + "\na exact 1 - leg-b", polls: [][]rdyRun{{rdyDoneRun(100, rdyOK(11, "leg-a"), rdyJobWith(12, "leg-b", "skipped", 1))}}}, `[]`),
		s("newest passing run is the source", rdyWorld{units: rdyUnitA, polls: [][]rdyRun{{rdyDoneRun(100, rdyJobWith(11, "leg-a", "failure", 1)), rdyDoneRun(90, rdyOK(9, "leg-a"))}}}, `["a"]`, "from run 90"),
		s("no other run reuses nothing", rdyWorld{units: rdyUnitA, polls: [][]rdyRun{{}}}, `[]`, "READY REUSE NONE"),
		s("runs API error reuses nothing", rdyWorld{units: rdyUnitA, noRunsAPI: true}, `[]`, "READY REUSE NONE"),
		{name: "not a ready event reuses nothing and calls no API", run: func(script, work string) error {
			r := rdyRunScript(script, work, rdyWorld{units: rdyUnitA, action: "synchronize", polls: [][]rdyRun{{rdyDoneRun(100, rdyOK(11, "leg-a"))}}})
			if r.code != 0 || r.passed != "[]" || r.calls != "" {
				return fmt.Errorf("exit %d passed=%s calls=%q, want 0 [] and no API call:\n%s", r.code, r.passed, r.calls, r.stdout)
			}
			return nil
		}},
		{name: "malformed unit line is a wiring error", run: func(script, work string) error {
			r := rdyRunScript(script, work, rdyWorld{units: "a sometimes 1 - leg-a", polls: [][]rdyRun{{}}})
			if r.code != 2 || !strings.Contains(r.stdout, "READY REUSE INPUT ERROR") {
				return fmt.Errorf("exit %d, want 2 with READY REUSE INPUT ERROR:\n%s", r.code, r.stdout)
			}
			return nil
		}},
	}
}

func TestReadyReuseJudge(t *testing.T) {
	assertAllPass(t, scriptPath(t, rdyScript), readyReuseScenarios())
}

func TestReadyReuseJudgePlantedDefects(t *testing.T) {
	assertPlantsCaught(t, scriptPath(t, rdyScript), readyReuseScenarios(), []plant{
		p("drop the head sha check", `select(.head_sha == $sha and .event == "pull_request" and .id != $self)`, `select(.event == "pull_request" and .id != $self)`, "other sha not reused", "passed="),
		p("drop the event check", `select(.head_sha == $sha and .event == "pull_request" and .id != $self)`, `select(.head_sha == $sha and .id != $self)`, "push event not reused", "passed="),
		p("drop the attempt check", `select(.run_attempt == $a)`, `select(true)`, "older attempt not reused", "passed="),
		p("read the first attempt", `"\(.id) \(.run_attempt) \(.status)"`, `"\(.id) 1 \(.status)"`, "older attempt not reused", "passed="),
		p("count a skipped job as passed", `elif $ok == ($m | length) then $m`, `elif $ok >= 0 then $m`, "skipped job in a successful run not reused", "passed="),
		p("drop the leg count", `if ($m | length) < $min then empty`, `if false then empty`, "matrix below its leg count not reused", "passed="),
		p("drop the evidence line", `integ) integ_ok "${ids}" || return 1 ;;`, `integ) ;;`, "integ missing evidence line not reused", "passed="),
		p("allow a reuse of a reuse", `grep -qE "${reused_re}" <<< "${log}" && return 1`, `:`, "integ REUSED-sourced job not reused", "passed="),
		p("drop the codeql merge check", `codeql-merge) codeql_merge_ok || return 1 ;;`, `codeql-merge) ;;`, "codeql without an analysis for the merge sha not reused", "passed="),
		p("do not wait for runs in flight", `[ -z "${pending}" ] && break`, `break`, "in-flight run waited then reused", "passed="),
		p("wait for a newer waiting ready run", `if [ "${waiter:-0}" != 0 ]; then`, `if false; then`, "newer waiting ready run not waited for", "output has"),
		p("twin accepts a failed leg", `if $ok >= 1 and ($m | all(.conclusion == "success" or .conclusion == "skipped"))`, `if $ok >= 1`, "twin with a failed leg not reused", "passed="),
	})
}

// A gate counts a skipped job as passed only when ready-reuse named that job's unit, on a pull_request run, in the
// "unit=run:attempt:job" form; it then prints the REUSED line. Any other skip keeps its policy verdict.
func TestAggregateGateCountsOnlyANamedReuse(t *testing.T) {
	base := map[string]string{
		"GATE_NAME": "go-queue-gate", "EVENT_NAME": "pull_request", "CHANGES_RESULT": "success", "CHANGES_CODE": "true",
		"HEAVY_ON_PR": "true", "GATED_JOB_1": "go-container-smoke|queue-heavy|skipped", "GATE_REUSE_1": "go-container-smoke|go-container",
	}
	with := func(kv ...string) map[string]string {
		env := map[string]string{}
		for k, v := range base {
			env[k] = v
		}
		for i := 0; i+1 < len(kv); i += 2 {
			if kv[i+1] == "" {
				delete(env, kv[i])
			} else {
				env[kv[i]] = kv[i+1]
			}
		}
		return env
	}
	v := runGate(t, with("READY_REUSED", "graphql-wire-parity=7:1:70 go-container=42:2:420,421"))
	assertVerdict(t, "go-queue-gate", v, true, "a skip whose unit ready-reuse named")
	if !strings.Contains(v.stdout, "REUSED go-container-smoke from run 42 attempt 2 job 420,421") {
		t.Fatalf("the reused skip must print its source run: %q", v.stdout)
	}
	for _, c := range []struct {
		name string
		env  map[string]string
	}{
		{"no ready-reuse output", with()},
		{"another unit named", with("READY_REUSED", "graphql-wire-parity=7:1:70")},
		{"a merge_group run", with("READY_REUSED", "go-container=42:2:420", "EVENT_NAME", "merge_group")},
		{"a malformed source", with("READY_REUSED", "go-container=42:two:420")},
		{"no reuse row for the job", with("READY_REUSED", "go-container=42:2:420", "GATE_REUSE_1", "")},
	} {
		assertVerdict(t, "go-queue-gate", runGate(t, c.env), false, c.name)
	}
	failed := runGate(t, with("READY_REUSED", "go-container=42:2:420", "GATED_JOB_1", "go-container-smoke|queue-heavy|failure"))
	assertVerdict(t, "go-queue-gate", failed, false, "a failed job whose unit ready-reuse named")
}

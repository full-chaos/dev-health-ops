package scripts_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// `ci/venue-main-alert.sh`: a red main-push `Venue oracles` run opens or updates ONE GitHub issue; a green executed run
// closes it; a skipped or stale run changes nothing. The script reads the GitHub API through `gh`; these tests feed it a
// fake `gh` (VENUE_ALERT_GH and LAST_VENUE_RUN_GH), so no network and no token are needed. The fake records every call.

const alertStep = "Run this shard's venue differential oracles against the live Python api"
const alertLabel = "venue-oracles-main-red"
const venueTitle = "main-red: Venue oracles run is red on main"
const gqTitle = "main-red: Go quality run is red on main"

const fakeGh = `#!/usr/bin/env bash
set -eu
fx="$FAKE_FIXTURE"
printf '%s\n' "$*" >> "$FAKE_CALLS"
if [ "$1" = "api" ]; then
  endpoint="$2"
  case "${endpoint}" in
    */workflows/*/runs*) n="${endpoint##*per_page=}"; jq ".runs | .workflow_runs |= .[:${n}]" "${fx}" ;;
    */actions/runs/*/jobs*) id="${endpoint#*/actions/runs/}"; id="${id%%/*}"
      expr=""; for a in "$@"; do if [ "${prev:-}" = "--jq" ]; then expr="$a"; fi; prev="$a"; done
      if [ -n "${expr}" ]; then jq ".jobs[\"${id}\"]" "${fx}" | jq -r "${expr}"; else jq ".jobs[\"${id}\"]" "${fx}"; fi ;;
    *) exit 9 ;;
  esac
  exit 0
fi
if [ "${FAKE_GH_FAIL_ISSUE_LIST:-0}" = "1" ] && [ "$1 $2" = "issue list" ]; then exit 1; fi
case "$1 $2" in
  "issue list")
    case "$*" in
      *number,title*) jq -c '[.issues[] | select(.legacy | not) | {number, title}]' "${fx}" ;;
      *) jq -r '[.issues[] | select(.legacy) | .number] | sort | .[0] // empty' "${fx}" ;;
    esac ;;
  "issue view") jq -r --argjson n "$3" '.issues[] | select(.number == $n) | ([.body] + .comments) | join("\n")' "${fx}" ;;
  "issue create") echo "https://github.com/o/r/issues/99" ;;
  "issue comment"|"issue close"|"label create") ;;
  *) exit 9 ;;
esac
`

type alertRun struct {
	id         int
	conclusion string
	jobs       []map[string]any
}

type alertIssue struct {
	number   int
	body     string
	comments []string
	title    string
	legacy   bool
}

func alertJob(shard int, oracle string) map[string]any {
	steps := []map[string]any{{"name": "Set up job", "conclusion": "success"}}
	if oracle != "absent" {
		steps = append(steps, map[string]any{"name": alertStep, "conclusion": oracle})
	}
	return map[string]any{"name": fmt.Sprintf("venue-oracles (shard %d/3)", shard), "steps": steps}
}

func alertLegs(conclusions ...string) []map[string]any {
	var out []map[string]any
	for i, c := range conclusions {
		out = append(out, alertJob(i+1, c))
	}
	return out
}

var (
	alertGreen   = alertLegs("success", "success", "success")
	alertRed     = alertLegs("success", "failure", "success")
	alertSkipped = alertLegs("absent", "absent", "absent")
	alertPartial = alertLegs("success", "failure", "absent")
)

type alertResult struct {
	calls  string
	res    cmdResult
	writes []string
	api    []string
}

func runAlert(t *testing.T, runs []alertRun, runID int, issues []alertIssue, extra ...string) alertResult {
	t.Helper()
	return runAlertAt(t, rootOf(t), runs, runID, issues, extra...)
}

func runAlertAt(t *testing.T, root string, runs []alertRun, runID int, issues []alertIssue, extra ...string) alertResult {
	t.Helper()
	dir := t.TempDir()
	var wr []map[string]any
	jobs := map[string]any{}
	for _, r := range runs {
		wr = append(wr, map[string]any{"id": r.id, "head_sha": fmt.Sprintf("%040x", r.id), "conclusion": r.conclusion})
		jobs[fmt.Sprint(r.id)] = map[string]any{"jobs": r.jobs}
	}
	var iss []map[string]any
	for _, i := range issues {
		c := i.comments
		if c == nil {
			c = []string{}
		}
		title := i.title
		if title == "" {
			title = venueTitle
		}
		iss = append(iss, map[string]any{"number": i.number, "body": i.body, "comments": c, "title": title, "legacy": i.legacy})
	}
	if iss == nil {
		iss = []map[string]any{}
	}
	fx, err := json.Marshal(map[string]any{"runs": map[string]any{"workflow_runs": wr}, "jobs": jobs, "issues": iss})
	if err != nil {
		t.Fatal(err)
	}
	fixture, calls, gh := filepath.Join(dir, "fixture.json"), filepath.Join(dir, "calls.log"), filepath.Join(dir, "fake-gh")
	for path, body := range map[string][]byte{fixture: fx, calls: nil, gh: []byte(fakeGh)} {
		mode := os.FileMode(0o644)
		if path == gh {
			mode = 0o755
		}
		if err := os.WriteFile(path, body, mode); err != nil {
			t.Fatal(err)
		}
	}
	conclusion := "unknown"
	for _, r := range runs {
		if r.id == runID {
			conclusion = r.conclusion
			break
		}
	}
	env := inheritedEnv(nil, "VENUE_ALERT_GH="+gh, "LAST_VENUE_RUN_GH="+gh, "FAKE_FIXTURE="+fixture, "FAKE_CALLS="+calls,
		"GITHUB_REPOSITORY=o/r", fmt.Sprintf("RUN_ID=%d", runID), "RUN_CONCLUSION="+conclusion, fmt.Sprintf("RUN_SHA=%040x", runID))
	env = append(env, extra...)
	res := freeSpec{dir: root, env: env}.run("bash", "ci/venue-main-alert.sh")
	var out alertResult
	out.res = res
	out.calls = readText(t, calls)
	for _, l := range strings.Split(readText(t, calls), "\n") {
		for _, p := range []string{"issue create", "issue comment", "issue close", "label create"} {
			if strings.HasPrefix(l, p) {
				out.writes = append(out.writes, l)
			}
		}
		if strings.HasPrefix(l, "api ") {
			out.api = append(out.api, l)
		}
	}
	return out
}

func TestVenueMainAlertScript(t *testing.T) {
	t.Parallel()
	ok := func(t *testing.T, r alertResult) {
		t.Helper()
		if r.res.code != 0 {
			t.Fatalf("exit %d: %s", r.res.code, r.res.stderr)
		}
	}
	prefix := func(w string) string { return strings.SplitN(w, " -R", 2)[0] }
	onWrite := func(t *testing.T, r alertResult, n int, want string) {
		t.Helper()
		if len(r.writes) != n || (n > 0 && !strings.HasPrefix(r.writes[0], want)) {
			t.Fatalf("writes %v, want %d starting %q", r.writes, n, want)
		}
	}

	t.Run("a failed run opens one labelled issue", func(t *testing.T) {
		t.Parallel()
		r := runAlert(t, []alertRun{{300, "failure", alertRed}, {200, "success", alertGreen}}, 300, nil)
		ok(t, r)
		if len(r.writes) != 3 || prefix(r.writes[0]) != "label create main-red" || prefix(r.writes[1]) != "label create "+alertLabel || prefix(r.writes[2]) != "issue create" {
			t.Fatalf("writes %v", r.writes)
		}
		for _, w := range []string{"--label main-red", "--label " + alertLabel, "actions/runs/300", "concluded `failure`", "--title " + venueTitle} {
			if !strings.Contains(r.writes[2], w) {
				t.Errorf("create call lacks %q: %s", w, r.writes[2])
			}
		}
	})
	t.Run("the conclusion alone decides red", func(t *testing.T) {
		t.Parallel()
		for _, c := range []struct {
			conclusion string
			jobs       []map[string]any
		}{{"failure", alertGreen}, {"failure", alertSkipped}, {"failure", alertPartial}, {"timed_out", alertSkipped}, {"startup_failure", alertSkipped}, {"cancelled", alertSkipped}} {
			r := runAlert(t, []alertRun{{300, c.conclusion, c.jobs}}, 300, nil)
			ok(t, r)
			if len(r.writes) == 0 || !strings.HasPrefix(r.writes[len(r.writes)-1], "issue create") || !strings.Contains(r.writes[len(r.writes)-1], "concluded `"+c.conclusion+"`") {
				t.Errorf("%s: writes %v api %v", c.conclusion, r.writes, r.api)
			}
		}
	})
	t.Run("a red run behind many skipped runs still opens the issue", func(t *testing.T) {
		t.Parallel()
		runs := []alertRun{{1100, "failure", alertRed}}
		for i := 0; i < 40; i++ {
			runs = append(runs, alertRun{1000 + i, "success", alertSkipped})
		}
		r := runAlert(t, runs, 1100, nil)
		ok(t, r)
		if len(r.writes) == 0 || !strings.HasPrefix(r.writes[len(r.writes)-1], "issue create") {
			t.Fatalf("writes %v", r.writes)
		}
	})
	t.Run("a failed run with an open issue comments instead of a second issue", func(t *testing.T) {
		t.Parallel()
		r := runAlert(t, []alertRun{{300, "failure", alertRed}}, 300, []alertIssue{{41, "Run https://github.com/o/r/actions/runs/200 at x", nil, "", false}})
		ok(t, r)
		onWrite(t, r, 1, "issue comment 41 ")
		if !strings.Contains(r.writes[0], "actions/runs/300") {
			t.Errorf("comment lacks the run: %s", r.writes[0])
		}
	})
	t.Run("a run already on the issue is not commented twice", func(t *testing.T) {
		t.Parallel()
		r := runAlert(t, []alertRun{{300, "failure", alertRed}}, 300, []alertIssue{{41, "", []string{"Still red. Run https://github.com/o/r/actions/runs/300 at x"}, "", false}})
		ok(t, r)
		onWrite(t, r, 0, "")
	})
	t.Run("a run id that is a prefix of a reported id is still reported", func(t *testing.T) {
		t.Parallel()
		r := runAlert(t, []alertRun{{30, "failure", alertRed}}, 30, []alertIssue{{41, "Run https://github.com/o/r/actions/runs/300 at x", nil, "", false}})
		ok(t, r)
		onWrite(t, r, 1, "issue comment 41 ")
	})
	t.Run("the lowest numbered open issue is the one", func(t *testing.T) {
		t.Parallel()
		r := runAlert(t, []alertRun{{300, "failure", alertRed}}, 300, []alertIssue{{50, "", nil, "", false}, {41, "", nil, "", false}})
		ok(t, r)
		onWrite(t, r, 1, "issue comment 41 ")
	})
	t.Run("a proven executed green run closes the open issue", func(t *testing.T) {
		t.Parallel()
		r := runAlert(t, []alertRun{{300, "success", alertGreen}, {200, "failure", alertRed}}, 300, []alertIssue{{41, "", nil, "", false}})
		ok(t, r)
		onWrite(t, r, 1, "issue close 41 ")
		if !strings.Contains(r.writes[0], "actions/runs/300") {
			t.Errorf("close lacks the run: %s", r.writes[0])
		}
	})
	t.Run("a skipped success never closes the issue", func(t *testing.T) {
		t.Parallel()
		r := runAlert(t, []alertRun{{300, "success", alertSkipped}, {200, "failure", alertRed}}, 300, []alertIssue{{41, "", nil, "", false}})
		ok(t, r)
		onWrite(t, r, 0, "")
		if !strings.Contains(r.res.stdout, "stays open") {
			t.Errorf("stdout lacks 'stays open': %s", r.res.stdout)
		}
	})
	t.Run("a success that is not the newest executed run closes nothing", func(t *testing.T) {
		t.Parallel()
		r := runAlert(t, []alertRun{{400, "success", alertGreen}, {300, "success", alertGreen}}, 300, []alertIssue{{41, "", nil, "", false}})
		ok(t, r)
		onWrite(t, r, 0, "")
	})
	t.Run("a success with no open issue reads nothing", func(t *testing.T) {
		t.Parallel()
		r := runAlert(t, []alertRun{{300, "success", alertGreen}}, 300, nil)
		ok(t, r)
		if len(r.writes) != 0 || len(r.api) != 0 {
			t.Fatalf("writes %v api %v", r.writes, r.api)
		}
	})
	t.Run("other conclusions do nothing", func(t *testing.T) {
		t.Parallel()
		for _, c := range []string{"skipped", "neutral", "action_required", "stale"} {
			r := runAlert(t, []alertRun{{300, c, alertGreen}}, 300, []alertIssue{{41, "", nil, "", false}})
			ok(t, r)
			onWrite(t, r, 0, "")
		}
	})
	t.Run("a failed issue read fails the job", func(t *testing.T) {
		t.Parallel()
		r := runAlert(t, []alertRun{{300, "failure", alertRed}}, 300, nil, "FAKE_GH_FAIL_ISSUE_LIST=1")
		if r.res.code == 0 || len(r.writes) != 0 {
			t.Fatalf("exit %d writes %v", r.res.code, r.writes)
		}
	})
	t.Run("a failed state read fails the job and closes nothing", func(t *testing.T) {
		t.Parallel()
		r := runAlert(t, []alertRun{{300, "success", alertGreen}}, 300, []alertIssue{{41, "", nil, "", false}}, "VENUE_ALERT_LAST_RUN=/nonexistent/last-run.sh")
		if r.res.code == 0 || len(r.writes) != 0 {
			t.Fatalf("exit %d writes %v", r.res.code, r.writes)
		}
	})
	t.Run("a dry run prints the write and does not make it", func(t *testing.T) {
		t.Parallel()
		r := runAlert(t, []alertRun{{300, "failure", alertRed}}, 300, nil, "DRY_RUN=1")
		ok(t, r)
		if len(r.writes) != 0 || !strings.Contains(r.res.stdout, "DRY-RUN: gh issue create") {
			t.Fatalf("writes %v stdout %s", r.writes, r.res.stdout)
		}
	})
	t.Run("inputs are validated", func(t *testing.T) {
		t.Parallel()
		for _, name := range []string{"RUN_ID", "RUN_CONCLUSION", "RUN_SHA"} {
			r := runAlert(t, []alertRun{{300, "failure", alertRed}}, 300, nil, name+"=x;rm")
			if r.res.code != 2 || len(r.writes) != 0 {
				t.Errorf("%s: exit %d writes %v", name, r.res.code, r.writes)
			}
		}
	})
}

func gqJob(name, conclusion string) map[string]any {
	return map[string]any{"name": name, "conclusion": conclusion}
}

func gqJobs(test, race string) []map[string]any {
	return []map[string]any{gqJob("go-quality-plan", "success"), gqJob("go-quality-leg (test-1-of-4)", test), gqJob("go-quality-leg (test-2-of-4)", test),
		gqJob("go-quality-leg (race-1-of-6)", race), gqJob("go-quality-leg (static)", "success"), gqJob("go-quality", "success")}
}

// The generic workflows (CHAOS-13243): one main-red issue per workflow, closed only by the newest EXECUTED green run.
func TestMainRedAlertScriptGenericWorkflow(t *testing.T) {
	t.Parallel()
	gq := "RUN_WORKFLOW=Go quality"
	green, red, skipped := gqJobs("success", "success"), gqJobs("failure", "success"), gqJobs("skipped", "skipped")
	partial := gqJobs("success", "skipped")
	openIssue := alertIssue{41, "Run https://github.com/o/r/actions/runs/200 at x", nil, gqTitle, false}
	ok := func(t *testing.T, r alertResult) {
		t.Helper()
		if r.res.code != 0 {
			t.Fatalf("exit %d: %s", r.res.code, r.res.stderr)
		}
	}
	t.Run("a red run opens one main-red issue naming the failed jobs and the run", func(t *testing.T) {
		t.Parallel()
		r := runAlert(t, []alertRun{{300, "failure", red}}, 300, nil, gq)
		ok(t, r)
		if len(r.writes) != 2 || !strings.HasPrefix(r.writes[0], "label create main-red") || !strings.HasPrefix(r.writes[1], "issue create") {
			t.Fatalf("writes %v", r.writes)
		}
		for _, w := range []string{"--title " + gqTitle, "--label main-red", "actions/runs/300", "Failed jobs: go-quality-leg (test-1-of-4), go-quality-leg (test-2-of-4)"} {
			if !strings.Contains(r.calls, w) {
				t.Errorf("calls lack %q:\n%s", w, r.calls)
			}
		}
		if strings.Contains(r.calls, alertLabel) {
			t.Errorf("a generic issue carries the venue label:\n%s", r.calls)
		}
	})
	t.Run("a red run again comments once", func(t *testing.T) {
		t.Parallel()
		r := runAlert(t, []alertRun{{300, "failure", red}}, 300, []alertIssue{openIssue}, gq)
		ok(t, r)
		if len(r.writes) != 1 || !strings.HasPrefix(r.writes[0], "issue comment 41 ") {
			t.Fatalf("writes %v", r.writes)
		}
		again := openIssue
		again.comments = []string{"Still red. Run https://github.com/o/r/actions/runs/300 at x"}
		r = runAlert(t, []alertRun{{300, "failure", red}}, 300, []alertIssue{again}, gq)
		ok(t, r)
		if len(r.writes) != 0 {
			t.Fatalf("second alert for one run: %v", r.writes)
		}
	})
	t.Run("one issue per workflow: another workflow's issue is not reused", func(t *testing.T) {
		t.Parallel()
		other := alertIssue{41, "", nil, "main-red: Tests run is red on main", false}
		r := runAlert(t, []alertRun{{300, "failure", red}}, 300, []alertIssue{other}, gq)
		ok(t, r)
		if len(r.writes) != 2 || !strings.HasPrefix(r.writes[1], "issue create") {
			t.Fatalf("writes %v", r.writes)
		}
	})
	t.Run("a cancelled run opens nothing and closes nothing", func(t *testing.T) {
		t.Parallel()
		r := runAlert(t, []alertRun{{300, "cancelled", skipped}}, 300, nil, gq)
		ok(t, r)
		if len(r.writes) != 0 {
			t.Fatalf("cancelled run wrote %v", r.writes)
		}
		r = runAlert(t, []alertRun{{300, "cancelled", skipped}, {200, "failure", red}}, 300, []alertIssue{openIssue}, gq)
		ok(t, r)
		if len(r.writes) != 0 {
			t.Fatalf("cancelled run wrote %v", r.writes)
		}
	})
	t.Run("a success with the test and race legs skipped does not close", func(t *testing.T) {
		t.Parallel()
		r := runAlert(t, []alertRun{{300, "success", skipped}, {200, "failure", red}}, 300, []alertIssue{openIssue}, gq)
		ok(t, r)
		if len(r.writes) != 0 || !strings.Contains(r.res.stdout, "stays open") {
			t.Fatalf("writes %v stdout %s", r.writes, r.res.stdout)
		}
	})
	t.Run("a success with only some legs executed does not close", func(t *testing.T) {
		t.Parallel()
		r := runAlert(t, []alertRun{{300, "success", partial}, {200, "failure", red}}, 300, []alertIssue{openIssue}, gq)
		ok(t, r)
		if len(r.writes) != 0 {
			t.Fatalf("writes %v", r.writes)
		}
	})
	t.Run("a skipped success after a cancelled run still does not close", func(t *testing.T) {
		t.Parallel()
		r := runAlert(t, []alertRun{{400, "success", skipped}, {300, "cancelled", skipped}, {200, "failure", red}}, 400, []alertIssue{openIssue}, gq)
		ok(t, r)
		if len(r.writes) != 0 {
			t.Fatalf("writes %v", r.writes)
		}
	})
	t.Run("a green executed run closes the issue", func(t *testing.T) {
		t.Parallel()
		r := runAlert(t, []alertRun{{300, "success", green}, {250, "cancelled", skipped}, {200, "failure", red}}, 300, []alertIssue{openIssue}, gq)
		ok(t, r)
		if len(r.writes) != 1 || !strings.HasPrefix(r.writes[0], "issue close 41 ") || !strings.Contains(r.writes[0], "actions/runs/300") {
			t.Fatalf("writes %v", r.writes)
		}
	})
	t.Run("a green run behind a newer executed red run closes nothing", func(t *testing.T) {
		t.Parallel()
		r := runAlert(t, []alertRun{{400, "failure", red}, {300, "success", green}}, 300, []alertIssue{openIssue}, gq)
		ok(t, r)
		if len(r.writes) != 0 {
			t.Fatalf("writes %v", r.writes)
		}
	})
	t.Run("a workflow outside the table does nothing", func(t *testing.T) {
		t.Parallel()
		r := runAlert(t, []alertRun{{300, "failure", red}}, 300, nil, "RUN_WORKFLOW=Docs Drift")
		ok(t, r)
		if len(r.writes) != 0 {
			t.Fatalf("writes %v", r.writes)
		}
	})
	t.Run("the legacy venue issue is still found", func(t *testing.T) {
		t.Parallel()
		r := runAlert(t, []alertRun{{300, "failure", alertRed}}, 300, []alertIssue{{41, "", nil, "Main venue-oracles run is red", true}})
		ok(t, r)
		if len(r.writes) != 1 || !strings.HasPrefix(r.writes[0], "issue comment 41 ") {
			t.Fatalf("writes %v", r.writes)
		}
	})
	t.Run("a workflow name with shell characters is refused", func(t *testing.T) {
		t.Parallel()
		r := runAlert(t, []alertRun{{300, "failure", red}}, 300, nil, "RUN_WORKFLOW=Go;rm")
		if r.res.code != 2 || len(r.writes) != 0 {
			t.Fatalf("exit %d writes %v", r.res.code, r.writes)
		}
	})
}

// mainRedBattery runs the decisive cells against the scripts under root and returns the names of the cells that failed.
func mainRedBattery(t *testing.T, root string) []string {
	t.Helper()
	gq := "RUN_WORKFLOW=Go quality"
	green, red, skipped := gqJobs("success", "success"), gqJobs("failure", "success"), gqJobs("skipped", "skipped")
	noRace := []map[string]any{gqJob("go-quality-plan", "success"), gqJob("go-quality-leg (test-1-of-4)", "success"), gqJob("go-quality-leg (static)", "success")}
	issue := alertIssue{41, "Run https://github.com/o/r/actions/runs/200 at x", nil, gqTitle, false}
	var failed []string
	check := func(name string, r alertResult, writes int, prefix, stdout string) {
		bad := r.res.code != 0 || len(r.writes) != writes
		if !bad && writes > 0 && !strings.HasPrefix(r.writes[len(r.writes)-1], prefix) {
			bad = true
		}
		if !bad && stdout != "" && !strings.Contains(r.res.stdout, stdout) {
			bad = true
		}
		if bad {
			failed = append(failed, name)
		}
	}
	r := runAlertAt(t, root, []alertRun{{300, "failure", red}}, 300, nil, gq)
	check("red run opens", r, 2, "issue create", "")
	if !strings.Contains(r.calls, "--title "+gqTitle) {
		failed = append(failed, "title carries main-red")
	}
	if !strings.Contains(r.calls, "Failed jobs: go-quality-leg (test-1-of-4)") {
		failed = append(failed, "issue names the failed jobs")
	}
	check("red run again comments", runAlertAt(t, root, []alertRun{{300, "failure", red}}, 300, []alertIssue{issue}, gq), 1, "issue comment 41 ", "")
	issue2 := issue
	issue2.comments = []string{"Still red. Run https://github.com/o/r/actions/runs/300 at x"}
	check("red run comments once", runAlertAt(t, root, []alertRun{{300, "failure", red}}, 300, []alertIssue{issue2}, gq), 0, "", "")
	check("cancelled by a newer push does not open", runAlertAt(t, root, []alertRun{{300, "cancelled", skipped}}, 300, nil, gq), 0, "", "")
	check("skipped legs do not close", runAlertAt(t, root, []alertRun{{300, "success", skipped}, {200, "failure", red}}, 300, []alertIssue{issue}, gq), 0, "", "stays open")
	check("a missing leg does not close", runAlertAt(t, root, []alertRun{{300, "success", noRace}, {200, "failure", red}}, 300, []alertIssue{issue}, gq), 0, "", "stays open")
	check("green executed run closes", runAlertAt(t, root, []alertRun{{300, "success", green}, {200, "failure", red}}, 300, []alertIssue{issue}, gq), 1, "issue close 41 ", "")
	check("a newer cancelled run is ignored by the close proof", runAlertAt(t, root, []alertRun{{400, "cancelled", skipped}, {300, "success", green}, {200, "failure", red}}, 300, []alertIssue{issue}, gq), 1, "issue close 41 ", "")
	check("a green run behind a newer red closes nothing", runAlertAt(t, root, []alertRun{{400, "failure", red}, {300, "success", green}}, 300, []alertIssue{issue}, gq), 0, "", "")
	check("another workflow's issue is not reused", runAlertAt(t, root, []alertRun{{300, "failure", red}}, 300, []alertIssue{{41, "", nil, "main-red: Tests run is red on main", false}}, gq), 2, "issue create", "")
	check("venue cancelled still opens", runAlertAt(t, root, []alertRun{{300, "cancelled", alertSkipped}}, 300, nil), 3, "issue create", "")
	return failed
}

func TestMainRedAlertMutants(t *testing.T) {
	t.Parallel()
	root := rootOf(t)
	if got := mainRedBattery(t, root); len(got) != 0 {
		t.Fatalf("the battery fails on the unmutated scripts: %v", got)
	}
	const alert, last = "ci/venue-main-alert.sh", "ci/last-venue-run.sh"
	cases := []struct {
		name, file, old, new, caught string
		all                          bool
	}{
		{"cancel mode ignored", alert, `if [ "${cancel_mode}" != "red" ]; then`, `if false; then`, "cancelled by a newer push does not open", false},
		{"cancelled always ignored", alert, `if [ "${cancel_mode}" != "red" ]; then`, `if true; then`, "venue cancelled still opens", false},
		{"close without the state proof", alert, `"venue-main: GREEN last-executed=${run_id} "*)`, `*)`, "skipped legs do not close", false},
		{"title loses main-red", alert, `title="main-red: ${workflow}`, `title="${workflow}`, "title carries main-red", false},
		{"dedupe off", alert, `grep -Eq "actions/runs/${run_id}`, `grep -Eq "actions/runs/NEVER${run_id}`, "red run comments once", false},
		{"witnesses dropped", alert, `[ "${witnesses}" = "-" ] ||`, `true ||`, "green executed run closes", false},
		{"issue lookup ignores the title", alert, `select(.title == $t)`, `select(.title != "")`, "another workflow's issue is not reused", false},
		{"failed jobs not named", alert, `select(.conclusion == "failure" or`, `select(.conclusion == "none" or`, "issue names the failed jobs", false},
		{"cancelled run counted by the close proof", last, `if [ "${conclusion}" = "cancelled" ]; then continue; fi`, `:`, "a newer cancelled run is ignored by the close proof", false},
		{"a witness with no jobs counts as executed", last, `length > 0 and `, ``, "a missing leg does not close", true},
		{"skipped jobs count as success", last, `. == "success"`, `. != "failure"`, "skipped legs do not close", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			tree := t.TempDir()
			if err := os.MkdirAll(filepath.Join(tree, "ci"), 0o755); err != nil {
				t.Fatal(err)
			}
			for _, f := range []string{alert, last, "ci/main-red-workflows.tsv"} {
				body := readText(t, filepath.Join(root, f))
				if f == c.file {
					if !strings.Contains(body, c.old) {
						t.Fatalf("mutation target %q is not in %s", c.old, f)
					}
					n := 1
					if c.all {
						n = -1
					}
					body = strings.Replace(body, c.old, c.new, n)
				}
				if err := os.WriteFile(filepath.Join(tree, f), []byte(body), 0o755); err != nil {
					t.Fatal(err)
				}
			}
			got := mainRedBattery(t, tree)
			if len(got) == 0 {
				t.Fatalf("mutant survived: %s", c.name)
			}
			if c.caught != "" && !containsString(got, c.caught) {
				t.Fatalf("mutant caught by %v, want %q among them", got, c.caught)
			}
		})
	}
}

func containsString(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}

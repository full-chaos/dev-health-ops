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

const fakeGh = `#!/usr/bin/env bash
set -eu
fx="$FAKE_FIXTURE"
printf '%s\n' "$*" >> "$FAKE_CALLS"
if [ "$1" = "api" ]; then
  endpoint="$2"
  case "${endpoint}" in
    */workflows/venue-oracles.yml/runs*) n="${endpoint##*per_page=}"; jq ".runs | .workflow_runs |= .[:${n}]" "${fx}" ;;
    */actions/runs/*/jobs*) id="${endpoint#*/actions/runs/}"; id="${id%%/*}"; jq ".jobs[\"${id}\"]" "${fx}" ;;
    *) exit 9 ;;
  esac
  exit 0
fi
if [ "${FAKE_GH_FAIL_ISSUE_LIST:-0}" = "1" ] && [ "$1 $2" = "issue list" ]; then exit 1; fi
case "$1 $2" in
  "issue list") jq -r '[.issues[].number] | sort | .[0] // empty' "${fx}" ;;
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
	res    cmdResult
	writes []string
	api    []string
}

func runAlert(t *testing.T, runs []alertRun, runID int, issues []alertIssue, extra ...string) alertResult {
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
		iss = append(iss, map[string]any{"number": i.number, "body": i.body, "comments": c})
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
	res := freeSpec{dir: rootOf(t), env: env}.run("bash", "ci/venue-main-alert.sh")
	var out alertResult
	out.res = res
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
		if len(r.writes) != 2 || prefix(r.writes[0]) != "label create "+alertLabel || prefix(r.writes[1]) != "issue create" {
			t.Fatalf("writes %v", r.writes)
		}
		for _, w := range []string{"--label " + alertLabel, "actions/runs/300", "concluded `failure`", "--title Main venue-oracles run is red"} {
			if !strings.Contains(r.writes[1], w) {
				t.Errorf("create call lacks %q: %s", w, r.writes[1])
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
			if len(r.writes) == 0 || !strings.HasPrefix(r.writes[len(r.writes)-1], "issue create") || !strings.Contains(r.writes[len(r.writes)-1], "concluded `"+c.conclusion+"`") || len(r.api) != 0 {
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
		r := runAlert(t, []alertRun{{300, "failure", alertRed}}, 300, []alertIssue{{41, "Run https://github.com/o/r/actions/runs/200 at x", nil}})
		ok(t, r)
		onWrite(t, r, 1, "issue comment 41 ")
		if !strings.Contains(r.writes[0], "actions/runs/300") {
			t.Errorf("comment lacks the run: %s", r.writes[0])
		}
	})
	t.Run("a run already on the issue is not commented twice", func(t *testing.T) {
		t.Parallel()
		r := runAlert(t, []alertRun{{300, "failure", alertRed}}, 300, []alertIssue{{41, "", []string{"Still red. Run https://github.com/o/r/actions/runs/300 at x"}}})
		ok(t, r)
		onWrite(t, r, 0, "")
	})
	t.Run("a run id that is a prefix of a reported id is still reported", func(t *testing.T) {
		t.Parallel()
		r := runAlert(t, []alertRun{{30, "failure", alertRed}}, 30, []alertIssue{{41, "Run https://github.com/o/r/actions/runs/300 at x", nil}})
		ok(t, r)
		onWrite(t, r, 1, "issue comment 41 ")
	})
	t.Run("the lowest numbered open issue is the one", func(t *testing.T) {
		t.Parallel()
		r := runAlert(t, []alertRun{{300, "failure", alertRed}}, 300, []alertIssue{{50, "", nil}, {41, "", nil}})
		ok(t, r)
		onWrite(t, r, 1, "issue comment 41 ")
	})
	t.Run("a proven executed green run closes the open issue", func(t *testing.T) {
		t.Parallel()
		r := runAlert(t, []alertRun{{300, "success", alertGreen}, {200, "failure", alertRed}}, 300, []alertIssue{{41, "", nil}})
		ok(t, r)
		onWrite(t, r, 1, "issue close 41 ")
		if !strings.Contains(r.writes[0], "actions/runs/300") {
			t.Errorf("close lacks the run: %s", r.writes[0])
		}
	})
	t.Run("a skipped success never closes the issue", func(t *testing.T) {
		t.Parallel()
		r := runAlert(t, []alertRun{{300, "success", alertSkipped}, {200, "failure", alertRed}}, 300, []alertIssue{{41, "", nil}})
		ok(t, r)
		onWrite(t, r, 0, "")
		if !strings.Contains(r.res.stdout, "stays open") {
			t.Errorf("stdout lacks 'stays open': %s", r.res.stdout)
		}
	})
	t.Run("a success that is not the newest executed run closes nothing", func(t *testing.T) {
		t.Parallel()
		r := runAlert(t, []alertRun{{400, "success", alertGreen}, {300, "success", alertGreen}}, 300, []alertIssue{{41, "", nil}})
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
			r := runAlert(t, []alertRun{{300, c, alertGreen}}, 300, []alertIssue{{41, "", nil}})
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
		r := runAlert(t, []alertRun{{300, "success", alertGreen}}, 300, []alertIssue{{41, "", nil}}, "VENUE_ALERT_LAST_RUN=/nonexistent/last-run.sh")
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

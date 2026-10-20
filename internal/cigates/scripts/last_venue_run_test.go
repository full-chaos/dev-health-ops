package scripts_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// CHAOS-10174 PR C: Go port of tests/tooling/test_last_venue_run.py. ci/last-venue-run.sh reads the GitHub API through
// `gh`; the tests feed it fixtures through LAST_VENUE_RUN_GH, so no network and no token are needed. Definition of
// record (Trap #408): the green main venue is the newest run whose oracle step EXECUTED and is green, with no later
// EXECUTED run failed. A skipped run is neither green nor red.
//
// DELETED (not ported): test_definition_of_record_is_in_the_workflow_and_script_headers. It asserted the text
// "Trap #408" and "EXECUTED" in two file headers; no script, workflow or Go test reads either header, so a deleted
// phrase breaks nothing.

const venueOracleStep = "Run this shard's venue differential oracles against the live Python api"

type venueJob struct {
	Name  string      `json:"name"`
	Steps []venueStep `json:"steps"`
}

type venueStep struct {
	Name       string `json:"name"`
	Conclusion string `json:"conclusion"`
}

type venueRun struct {
	id     int
	concl  string
	legs   []string
	noJobs bool
}

func venueLegs(conclusions ...string) []string { return conclusions }

func repeatLegs(c string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = c
	}
	return out
}

func (r venueRun) jobs() []venueJob {
	var jobs []venueJob
	for i, c := range r.legs {
		steps := []venueStep{{"Set up job", "success"}}
		if c != "absent" {
			steps = append(steps, venueStep{venueOracleStep, c})
		}
		jobs = append(jobs, venueJob{Name: fmt.Sprintf("venue-oracles (shard %d/5)", i+1), Steps: steps})
	}
	return jobs
}

func runVenueScript(script, work string, runs []venueRun, args ...string) (cmdResult, error) {
	type apiRun struct {
		ID         int    `json:"id"`
		HeadSHA    string `json:"head_sha"`
		Conclusion string `json:"conclusion"`
	}
	var workflowRuns []apiRun
	jobs := map[string]any{}
	for _, r := range runs {
		workflowRuns = append(workflowRuns, apiRun{r.id, fmt.Sprintf("%040x", r.id), r.concl})
		jobs[fmt.Sprint(r.id)] = map[string]any{"jobs": r.jobs()}
	}
	fixture, err := json.Marshal(map[string]any{"runs": map[string]any{"workflow_runs": workflowRuns}, "jobs": jobs})
	if err != nil {
		return cmdResult{}, err
	}
	fx := filepath.Join(work, "fixture.json")
	if err := os.WriteFile(fx, fixture, 0o644); err != nil {
		return cmdResult{}, err
	}
	gh := filepath.Join(work, "fake-gh")
	body := "#!/usr/bin/env bash\nset -eu\nendpoint=\"$2\"\nfx=\"" + fx + "\"\n" +
		"case \"${endpoint}\" in\n" +
		"  */workflows/venue-oracles.yml/runs*) jq .runs \"${fx}\" ;;\n" +
		"  */actions/runs/*/jobs*) id=\"${endpoint#*/actions/runs/}\"; id=\"${id%%/*}\"; jq \".jobs[\\\"${id}\\\"]\" \"${fx}\" ;;\n" +
		"  *) exit 9 ;;\nesac\n"
	if err := os.WriteFile(gh, []byte(body), 0o755); err != nil {
		return cmdResult{}, err
	}
	env := inheritedEnv([]string{"LAST_VENUE_RUN_GH"}, "LAST_VENUE_RUN_GH="+gh)
	return cmdSpec{env: env}.run("bash", append([]string{script}, args...)...), nil
}

func lastLine(res cmdResult) string {
	lines := strings.Split(strings.TrimSpace(res.stdout), "\n")
	return lines[len(lines)-1]
}

func venueVerdict(res cmdResult, err error, code int, prefix string, contains ...string) error {
	if err != nil {
		return err
	}
	line := lastLine(res)
	if res.code != code || !strings.HasPrefix(line, prefix) {
		return fmt.Errorf("exit %d verdict %q, want exit %d and prefix %q (stderr %q)", res.code, line, code, prefix, res.stderr)
	}
	for _, c := range contains {
		if !strings.Contains(res.stdout, c) {
			return fmt.Errorf("output lacks %q, want it (got %q)", c, res.stdout)
		}
	}
	return nil
}

func lastVenueRunScenarios() []scenario {
	allOK := venueLegs(repeatLegs("success", 5)...)
	skipped := venueLegs(repeatLegs("absent", 5)...)
	return []scenario{
		{"executed_green", func(s, w string) error {
			res, err := runVenueScript(s, w, []venueRun{{id: 3, concl: "success", legs: allOK}})
			return venueVerdict(res, err, 0, "venue-main: GREEN last-executed=3 ", "skipped-since=0")
		}},
		{"skipped_run_after_green_names_the_executed_run", func(s, w string) error {
			res, err := runVenueScript(s, w, []venueRun{{id: 9, concl: "success", legs: skipped}, {id: 5, concl: "success", legs: allOK}})
			return venueVerdict(res, err, 0, "venue-main: GREEN last-executed=5 ", "skipped-since=1", "run=9 ", "class=skipped")
		}},
		{"skipped_only_is_none_never_green", func(s, w string) error {
			res, err := runVenueScript(s, w, []venueRun{{id: 9, concl: "success", legs: skipped}, {id: 8, concl: "success", legs: skipped}})
			return venueVerdict(res, err, 3, "venue-main: NONE ", "skipped-since=2")
		}},
		{"executed_red_then_skipped_stays_red", func(s, w string) error {
			red := venueLegs("success", "failure", "success", "success", "success")
			res, err := runVenueScript(s, w, []venueRun{{id: 9, concl: "success", legs: skipped}, {id: 5, concl: "failure", legs: red}})
			return venueVerdict(res, err, 1, "venue-main: RED last-executed=5 ", "conclusion=failure", "skipped-since=1")
		}},
		{"newer_executed_green_after_red_is_green", func(s, w string) error {
			red := venueLegs("failure", "success", "success", "success", "success")
			res, err := runVenueScript(s, w, []venueRun{{id: 9, concl: "success", legs: allOK}, {id: 5, concl: "failure", legs: red}})
			return venueVerdict(res, err, 0, "venue-main: GREEN last-executed=9 ")
		}},
		{"cancelled_leg_is_incomplete_not_green", func(s, w string) error {
			cancelled := venueLegs("success", "success", "success", "success", "absent")
			res, err := runVenueScript(s, w, []venueRun{{id: 7, concl: "cancelled", legs: cancelled}})
			return venueVerdict(res, err, 1, "venue-main: INCOMPLETE last-executed=7 ")
		}},
		{"no_runs_is_none", func(s, w string) error {
			res, err := runVenueScript(s, w, nil)
			return venueVerdict(res, err, 3, "venue-main: NONE ")
		}},
		{"api_failure_is_a_usage_error_not_a_verdict", func(s, w string) error {
			res := cmdSpec{env: inheritedEnv([]string{"LAST_VENUE_RUN_GH"}, "LAST_VENUE_RUN_GH=false")}.run("bash", s)
			if res.code != 2 || strings.Contains(res.stdout, "venue-main:") {
				return fmt.Errorf("exit %d stdout %q, want exit 2 and no verdict line", res.code, res.stdout)
			}
			return nil
		}},
		{"summary_mode_is_markdown_with_the_verdict", func(s, w string) error {
			res, err := runVenueScript(s, w, []venueRun{{id: 5, concl: "success", legs: allOK}}, "--summary")
			if err != nil {
				return err
			}
			if res.code != 0 || !strings.HasPrefix(res.stdout, "### Main venue-oracles state") || !strings.Contains(res.stdout, "venue-main: GREEN") {
				return fmt.Errorf("summary output %q exit %d, want markdown heading and a GREEN verdict", res.stdout, res.code)
			}
			return nil
		}},
		{"bad_limit_is_rejected", func(s, w string) error {
			res, err := runVenueScript(s, w, nil, "--limit", "x")
			if err != nil {
				return err
			}
			if res.code != 2 {
				return fmt.Errorf("exit %d, want exit 2 for a non-numeric --limit", res.code)
			}
			return nil
		}},
	}
}

func TestLastVenueRunScript(t *testing.T) {
	if _, err := os.Stat("/usr/bin/jq"); err != nil {
		t.Skip("ci/last-venue-run.sh needs jq, and the fixtures gh uses it too")
	}
	script := scriptPath(t, "ci/last-venue-run.sh")
	scenarios := lastVenueRunScenarios()
	assertAllPass(t, script, scenarios)
	skippedCase := "skipped|none) skipped_since=$((skipped_since + 1)) ;;"
	assertPlantsCaught(t, script, scenarios, []plant{
		p("skipped run read as the last executed green", skippedCase,
			`skipped|none) found=1; verdict="GREEN"; last_id="${id}"; last_sha="${sha}"; last_conclusion="${conclusion}" ;;`,
			"executed_red_then_skipped_stays_red", "want"),
		p("a partial run read as green", `if [ "${class}" = "partial" ]; then verdict="INCOMPLETE"`, `if [ "${class}" = "partial" ]; then verdict="GREEN"`,
			"cancelled_leg_is_incomplete_not_green", "want"),
		p("a red leg read as green", `elif [ "${ok}" -eq "${total}" ]; then verdict="GREEN"`, `elif true; then verdict="GREEN"`,
			"executed_red_then_skipped_stays_red", "want"),
		p("an older run overrides the newest executed one", `if [ "${found}" -eq 0 ]; then`, `if true; then`,
			"newer_executed_green_after_red_is_green", "want"),
		p("no run read as success", `*) exit 3 ;;`, `*) exit 0 ;;`,
			"no_runs_is_none", "want"),
		p("api failure swallowed", `|| { printf 'last-venue-run: could not list the venue-oracles runs\n' >&2; exit 2; }`, `|| true`,
			"api_failure_is_a_usage_error_not_a_verdict", "want"),
		p("summary mode dropped", `if [ "${summary}" -eq 1 ]; then`, `if false; then`,
			"summary_mode_is_markdown_with_the_verdict", "want"),
		p("limit not validated", `""|*[!0-9]*)`, `""|NEVER)`,
			"bad_limit_is_rejected", "want"),
		p("oracle step filter drifted", `startswith("Run this shard")`, `startswith("Run thix shard")`,
			"executed_green", "want"),
	})
}

// venueWorkflowDefects returns every way venue-oracles.yml disagrees with ci/last-venue-run.sh: the script keys on the
// oracle step name prefix and on the job name prefix at run time, and the skip report step must be loud, best-effort
// and unable to fail the run.
func venueWorkflowDefects(workflow, script string) []string {
	var doc struct {
		Permissions map[string]string `yaml:"permissions"`
		Jobs        map[string]struct {
			Steps []struct {
				Name string            `yaml:"name"`
				If   string            `yaml:"if"`
				Run  string            `yaml:"run"`
				Env  map[string]string `yaml:"env"`
			} `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal([]byte(workflow), &doc); err != nil {
		return []string{"workflow is not valid YAML: " + err.Error()}
	}
	var defects []string
	steps := doc.Jobs["venue-oracles"].Steps
	foundOracle := false
	for _, s := range steps {
		foundOracle = foundOracle || s.Name == venueOracleStep
	}
	if !foundOracle {
		defects = append(defects, fmt.Sprintf("job venue-oracles has no step named %q", venueOracleStep))
	}
	for _, filter := range []string{`startswith("Run this shard")`, `startswith("venue-oracles")`} {
		if !strings.Contains(script, filter) {
			defects = append(defects, "ci/last-venue-run.sh no longer filters with "+filter)
		}
	}
	skipFound := false
	for _, s := range steps {
		if !strings.HasPrefix(s.Name, "Report that the gate was skipped") {
			continue
		}
		skipFound = true
		if s.If != "steps.relevance.outputs.relevant != 'true'" {
			defects = append(defects, fmt.Sprintf("skip report step runs on %q", s.If))
		}
		for _, need := range []string{"::notice title=VENUE ORACLES NOT RUN::", "VENUE ORACLES NOT RUN:", "GITHUB_STEP_SUMMARY", "ci/last-venue-run.sh", "|| true"} {
			if !strings.Contains(s.Run, need) {
				defects = append(defects, fmt.Sprintf("skip report step lacks %q", need))
			}
		}
		if _, ok := s.Env["GH_TOKEN"]; !ok {
			defects = append(defects, "skip report step has no GH_TOKEN")
		}
		if strings.Contains(s.Run, "exit 1") {
			defects = append(defects, "skip report step can fail the run (exit 1)")
		}
	}
	if !skipFound {
		defects = append(defects, "no step named 'Report that the gate was skipped...'")
	}
	if doc.Permissions["actions"] != "read" {
		defects = append(defects, "workflow permissions lack actions: read")
	}
	return defects
}

func TestVenueWorkflowWiresTheLastVenueRunScript(t *testing.T) {
	workflow := readText(t, scriptPath(t, ".github/workflows/venue-oracles.yml"))
	script := readText(t, scriptPath(t, "ci/last-venue-run.sh"))
	if defects := venueWorkflowDefects(workflow, script); len(defects) > 0 {
		t.Fatalf("venue-oracles.yml and ci/last-venue-run.sh disagree:\n  %s", strings.Join(defects, "\n  "))
	}
	plants := []struct {
		name, text, old, repl string
		isScript              bool
		want                  string
	}{
		{"oracle step renamed", "wf", "- name: " + venueOracleStep, "- name: Run the shard's venue oracles", false, "has no step named"},
		{"script filter drifted", "script", `startswith("Run this shard")`, `startswith("Run the shard")`, true, "no longer filters"},
		{"skip step not gated on relevance", "wf", "if: steps.relevance.outputs.relevant != 'true'\n        env:\n          GH_TOKEN", "if: always()\n        env:\n          GH_TOKEN", false, "skip report step runs on"},
		{"skip step API read can fail the run", "wf", `2>/dev/null | tail -n 1 || true)`, `2>/dev/null | tail -n 1)`, false, "|| true"},
		{"skip step loses its notice", "wf", `echo "::notice title=VENUE ORACLES NOT RUN::`, `echo "notice: `, false, "::notice title=VENUE ORACLES NOT RUN::"},
		{"actions: read permission dropped", "wf", "  actions: read\n", "", false, "actions: read"},
	}
	for _, pl := range plants {
		pl := pl
		t.Run("plant/"+pl.name, func(t *testing.T) {
			wf, sc := workflow, script
			target := &wf
			if pl.isScript {
				target = &sc
			}
			if n := strings.Count(*target, pl.old); n != 1 {
				t.Fatalf("anchor %q occurs %d times, want exactly 1", pl.old, n)
			}
			*target = strings.Replace(*target, pl.old, pl.repl, 1)
			defects := strings.Join(venueWorkflowDefects(wf, sc), "\n")
			if !strings.Contains(defects, pl.want) {
				t.Fatalf("planted defect %q not caught: defects were %q, want one mentioning %q", pl.name, defects, pl.want)
			}
		})
	}
}

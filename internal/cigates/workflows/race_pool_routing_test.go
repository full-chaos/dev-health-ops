package workflows_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// The race legs of go-quality-leg may run on the ARC pool named by vars.CI_RACE_RUNNER. ops is a
// public repo: the pool term must carry the push-to-main and same-repository guards, the variable
// unset must mean hosted, and a pool job must run with Ryuk off and prove at start that its docker daemon holds no testcontainers.

const raceHostedFallback = "'ubuntu-latest' }}"

func goQualityLeg(t *testing.T) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(authProofRoot(t), ".github", "workflows", "go-quality.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Jobs map[string]map[string]any `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	leg := doc.Jobs["go-quality-leg"]
	if leg == nil {
		t.Fatal("go-quality-leg moved or was renamed")
	}
	return leg
}

func TestRaceLegsRouteToThePoolOnlyOnPushToMainOfThisRepo(t *testing.T) {
	leg := goQualityLeg(t)
	runsOn, _ := leg["runs-on"].(string)
	want := "${{ (" + racePoolTerm + ") && vars.CI_RACE_RUNNER || "
	if !strings.HasPrefix(runsOn, want) {
		t.Fatalf("runs-on must start with the guarded CI_RACE_RUNNER term\n got: %s\nwant prefix: %s", runsOn, want)
	}
	if !strings.HasSuffix(runsOn, raceHostedFallback) {
		t.Fatalf("runs-on must end in the hosted fallback, got: %s", runsOn)
	}
	if strings.Count(runsOn, "vars.CI_RACE_RUNNER") != 2 {
		t.Fatalf("CI_RACE_RUNNER must appear only in the guard and the value, got: %s", runsOn)
	}
	timeout, _ := leg["timeout-minutes"].(string)
	if !strings.HasPrefix(timeout, "${{ ("+racePoolTerm+") && ") || !strings.HasSuffix(timeout, " && 45 || 30 }}") {
		t.Fatalf("timeout-minutes must be 45 on the pool path and 30 otherwise, got: %s", timeout)
	}
}

func TestRaceLegsOnThePoolRunWithoutRyukAndAssertAnEmptyDaemonAtStart(t *testing.T) {
	leg := goQualityLeg(t)
	env, _ := leg["env"].(map[string]any)
	wantRyuk := "${{ (" + racePoolTerm + ") && 'true' || 'false' }}"
	if env["TESTCONTAINERS_RYUK_DISABLED"] != wantRyuk {
		t.Fatalf("TESTCONTAINERS_RYUK_DISABLED must be on only for the pool legs\n got: %v\nwant: %s", env["TESTCONTAINERS_RYUK_DISABLED"], wantRyuk)
	}
	ryukArm := strings.TrimSuffix(strings.TrimPrefix(wantRyuk, "${{ ("), ") && 'true' || 'false' }}")
	for name, over := range map[string]map[string]string{
		"static":   {"matrix.pool": ""},
		"test":     {"matrix.pool": ""},
		"pull req": {"github.event_name": "pull_request", "github.ref": "refs/pull/1/merge"},
	} {
		if evalRacePoolTerm(t, ryukArm, poolContext(over)) {
			t.Fatalf("%s must not inherit TESTCONTAINERS_RYUK_DISABLED=true", name)
		}
	}
	if !evalRacePoolTerm(t, ryukArm, poolContext(nil)) {
		t.Fatal("the pool legs must run with Ryuk disabled")
	}
	if got, _ := env["DEV_HEALTH_GO_RACE_TEST_TIMEOUT"].(string); !strings.Contains(got, "'25m'") {
		t.Fatalf("the pool race path needs a per-package go test timeout above the 600 s default, got %q", got)
	}
	steps, _ := leg["steps"].([]any)
	found := false
	for _, s := range steps {
		step, _ := s.(map[string]any)
		if run, _ := step["run"].(string); strings.Contains(run, "ci/assert_no_testcontainers.sh") {
			found = step["if"] == "matrix.pool && vars.CI_RACE_RUNNER != '' && github.event_name == 'push' && github.ref == 'refs/heads/main' && github.repository == 'full-chaos/dev-health-ops'"
		}
	}
	if !found {
		t.Fatal("go-quality-leg needs a pool-only step that runs ci/assert_no_testcontainers.sh")
	}
	for _, s := range steps {
		step, _ := s.(map[string]any)
		if run, _ := step["run"].(string); strings.Contains(run, "reap_testcontainers") {
			t.Fatal("the session reap step is gone: each pool pod has its own docker daemon")
		}
	}
}

func runAssertWithFakeDocker(t *testing.T, fakeDocker string) (string, error) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "docker"), []byte("#!/usr/bin/env bash\n"+fakeDocker+"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("bash", filepath.Join(authProofRoot(t), "ci", "assert_no_testcontainers.sh"))
	cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"))
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func TestAssertNoTestcontainersAtJobStart(t *testing.T) {
	if out, err := runAssertWithFakeDocker(t, "exit 0"); err != nil {
		t.Fatalf("an empty daemon must pass, got %v: %s", err, out)
	}
	out, err := runAssertWithFakeDocker(t, `[ "$1 $2 $3" = "ps -aq --filter" ] && [ "$4" = "label=org.testcontainers=true" ] && echo abc123def456`)
	if err == nil || !strings.Contains(out, "abc123def456") {
		t.Fatalf("a labelled container must fail the step and name its id, got %v: %s", err, out)
	}
	stopped := `case " $* " in *" -a "*|*" -aq "*) echo dead0beef001;; esac`
	if out, err := runAssertWithFakeDocker(t, stopped); err == nil || !strings.Contains(out, "dead0beef001") {
		t.Fatalf("a stopped labelled container must fail the step too (docker ps -a), got %v: %s", err, out)
	}
	if out, err := runAssertWithFakeDocker(t, "echo daemon down >&2; exit 1"); err == nil {
		t.Fatalf("a docker failure must exit nonzero, got: %s", out)
	}
}

// evalRacePoolTerm evaluates a term made only of `a && b` clauses: `matrix.pool`, `x == 'lit'` and `x != 'lit'`.
func evalRacePoolTerm(t *testing.T, term string, ctx map[string]string) bool {
	t.Helper()
	for _, clause := range strings.Split(term, " && ") {
		clause = strings.TrimSpace(clause)
		switch {
		case clause == "matrix.pool":
			if ctx[clause] != "true" {
				return false
			}
		case strings.Contains(clause, " == '"), strings.Contains(clause, " != '"):
			op := " == "
			if strings.Contains(clause, " != ") {
				op = " != "
			}
			lhs, rhs, _ := strings.Cut(clause, op)
			lit := strings.Trim(rhs, "'")
			if (ctx[lhs] == lit) != (op == " == ") {
				return false
			}
		default:
			t.Fatalf("unsupported clause %q in the pool term", clause)
		}
	}
	return true
}

func poolContext(over map[string]string) map[string]string {
	ctx := map[string]string{
		"matrix.pool":         "true",
		"vars.CI_RACE_RUNNER": "oci-arc-runners",
		"github.event_name":   "push",
		"github.ref":          "refs/heads/main",
		"github.repository":   "full-chaos/dev-health-ops",
	}
	for k, v := range over {
		ctx[k] = v
	}
	return ctx
}

func TestRacePoolTermRoutesByEvaluation(t *testing.T) {
	leg := goQualityLeg(t)
	runsOn, _ := leg["runs-on"].(string)
	term := strings.TrimPrefix(runsOn, "${{ (")
	term, _, _ = strings.Cut(term, ") && vars.CI_RACE_RUNNER || ")
	cases := []struct {
		name string
		over map[string]string
		want bool
	}{
		{"eligible push to main", nil, true},
		{"variable unset", map[string]string{"vars.CI_RACE_RUNNER": ""}, false},
		{"pull request", map[string]string{"github.event_name": "pull_request", "github.ref": "refs/pull/1/merge"}, false},
		{"push to another branch", map[string]string{"github.ref": "refs/heads/feature"}, false},
		{"push to main of another repository", map[string]string{"github.repository": "someone/dev-health-ops"}, false},
		{"leg without the pool flag", map[string]string{"matrix.pool": ""}, false},
		{"workflow dispatch", map[string]string{"github.event_name": "workflow_dispatch"}, false},
	}
	for _, c := range cases {
		if got := evalRacePoolTerm(t, term, poolContext(c.over)); got != c.want {
			t.Errorf("%s: pool = %v, want %v (term %q)", c.name, got, c.want, term)
		}
	}
}

func TestRacePoolTermPlantedDefectsRoute(t *testing.T) {
	planted := map[string]string{
		"no repository clause": strings.Replace(racePoolTerm, " && github.repository == 'full-chaos/dev-health-ops'", "", 1),
		"no push clause":       strings.Replace(racePoolTerm, "github.event_name == 'push' && ", "", 1),
		"no main clause":       strings.Replace(racePoolTerm, " && github.ref == 'refs/heads/main'", "", 1),
		"no variable clause":   strings.Replace(racePoolTerm, "vars.CI_RACE_RUNNER != '' && ", "", 1),
	}
	bad := map[string]map[string]string{
		"no repository clause": {"github.repository": "someone/dev-health-ops"},
		"no push clause":       {"github.event_name": "pull_request"},
		"no main clause":       {"github.ref": "refs/heads/feature"},
		"no variable clause":   {"vars.CI_RACE_RUNNER": ""},
	}
	for name, term := range planted {
		if term == racePoolTerm {
			t.Fatalf("%s: the mutation changed nothing", name)
		}
		if !evalRacePoolTerm(t, term, poolContext(bad[name])) {
			t.Errorf("%s: the planted term did not route a case it must not route, so the evaluation cannot catch it", name)
		}
	}
}

func TestRaceLegsTurnSetupGoCacheOffOnThePool(t *testing.T) {
	leg := goQualityLeg(t)
	steps, _ := leg["steps"].([]any)
	for _, s := range steps {
		step, _ := s.(map[string]any)
		if uses, _ := step["uses"].(string); strings.HasPrefix(uses, "actions/setup-go@") {
			with, _ := step["with"].(map[string]any)
			// off everywhere (CHAOS-10486: the hosted legs restore their own cache) is off on the pool path too
			if off, ok := with["cache"].(bool); ok && !off {
				return
			}
			cache, _ := with["cache"].(string)
			if !strings.Contains(cache, racePoolTerm) {
				t.Fatalf("setup-go cache must be off on the CI_RACE_RUNNER path, got %q", cache)
			}
			return
		}
	}
	t.Fatal("no setup-go step in go-quality-leg")
}

// Only the three race legs are budgeted for the pool (queue measured 7-10 min hosted vs under 1 min pool): static and test stay hosted.
func TestOnlyTheThreeRaceLegsCarryThePoolFlag(t *testing.T) {
	leg := goQualityLeg(t)
	strategy, _ := leg["strategy"].(map[string]any)
	matrix, _ := strategy["matrix"].(map[string]any)
	include, _ := matrix["include"].([]any)
	got := map[string]bool{}
	for _, e := range include {
		entry, _ := e.(map[string]any)
		name, _ := entry["name"].(string)
		if pool, _ := entry["pool"].(bool); pool {
			got[name] = true
		}
	}
	want := map[string]bool{"race-1-of-3": true, "race-2-of-3": true, "race-3-of-3": true}
	if len(got) != len(want) || !got["race-1-of-3"] || !got["race-2-of-3"] || !got["race-3-of-3"] {
		t.Fatalf("pool-flagged legs = %v, want exactly race-1-of-3, race-2-of-3 and race-3-of-3", got)
	}
}

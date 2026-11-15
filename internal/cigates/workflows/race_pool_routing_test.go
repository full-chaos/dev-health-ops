package workflows_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// The race legs of go-quality-leg ran on the ARC pool named by vars.CI_RACE_RUNNER until the pool was retired (D6994).
// They are hosted now: the hosted label, the hosted budgets, Ryuk on, no pool-only step and no pool flag on any leg.

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

func TestRaceLegsRunHosted(t *testing.T) {
	leg := goQualityLeg(t)
	if got := leg["runs-on"]; got != "ubuntu-latest" {
		t.Fatalf("go-quality-leg runs-on = %v, want ubuntu-latest", got)
	}
	if got := leg["timeout-minutes"]; got != 45 {
		t.Fatalf("timeout-minutes = %v, want 45 (CHAOS-13036: 30m per package, 45 job minutes)", got)
	}
	env, _ := leg["env"].(map[string]any)
	if got := env["CHECK_GO_JOB_CAP_MIN"]; got != 45 {
		t.Fatalf("CHECK_GO_JOB_CAP_MIN = %v, want 45", got)
	}
	if got := env["TESTCONTAINERS_RYUK_DISABLED"]; got != "false" {
		t.Fatalf("TESTCONTAINERS_RYUK_DISABLED = %v, want 'false': Ryuk reaps the hosted runner's containers", got)
	}
	steps, _ := leg["steps"].([]any)
	for _, s := range steps {
		step, _ := s.(map[string]any)
		run, _ := step["run"].(string)
		if strings.Contains(run, "ci/assert_no_testcontainers.sh") || strings.Contains(run, "reap_testcontainers") {
			t.Fatalf("a pool-only step is back in go-quality-leg: %v", step["name"])
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

// CHAOS-10486: setup-go's own cache stays off on every leg; the hosted legs restore their own per-leg cache.
func TestRaceLegsKeepSetupGoCacheOff(t *testing.T) {
	leg := goQualityLeg(t)
	steps, _ := leg["steps"].([]any)
	for _, s := range steps {
		step, _ := s.(map[string]any)
		if uses, _ := step["uses"].(string); strings.HasPrefix(uses, "actions/setup-go@") {
			with, _ := step["with"].(map[string]any)
			if off, ok := with["cache"].(bool); !ok || off {
				t.Fatalf("setup-go cache must be false in go-quality-leg, got %v", with["cache"])
			}
			return
		}
	}
	t.Fatal("no setup-go step in go-quality-leg")
}

// No leg carries the retired pool flag.
func TestNoLegCarriesThePoolFlag(t *testing.T) {
	for _, entry := range goQualityLegList(t, authProofRoot(t)) {
		if _, ok := entry["pool"]; ok {
			t.Fatalf("leg %v carries the retired pool flag", entry["name"])
		}
	}
}

const raceTimeoutExpr = "${{ github.event_name == 'merge_group' && '25m' || '30m' }}"

// raceTimeoutProblems pins the exact expression and evaluates it per event: 25m in the merge queue (CHAOS-12856), 30m
// everywhere else.
func raceTimeoutProblems(expr string) []string {
	var bad []string
	if expr != raceTimeoutExpr {
		bad = append(bad, "not the pinned expression: "+expr)
	}
	cond, rest, ok := strings.Cut(strings.TrimSuffix(strings.TrimPrefix(expr, "${{ "), " }}"), " && ")
	mq, other, ok2 := strings.Cut(rest, " || ")
	if !ok || !ok2 || cond != "github.event_name == 'merge_group'" {
		return append(bad, "unparsed: "+expr)
	}
	for event, want := range map[string]string{"merge_group": "'25m'", "push": "'30m'", "pull_request": "'30m'", "schedule": "'30m'", "workflow_dispatch": "'30m'"} {
		got := other
		if event == "merge_group" {
			got = mq
		}
		if got != want {
			bad = append(bad, event+": timeout "+got+", want "+want)
		}
	}
	return bad
}

func TestRaceTestTimeoutIsPinnedPerLeg(t *testing.T) {
	env, _ := goQualityLeg(t)["env"].(map[string]any)
	got, _ := env["DEV_HEALTH_GO_RACE_TEST_TIMEOUT"].(string)
	for _, p := range raceTimeoutProblems(got) {
		t.Error(p)
	}
}

func TestRaceTestTimeoutPlantedMutantsAreRed(t *testing.T) {
	for name, mutant := range map[string]string{
		"unconditional 25m": "${{ '25m' }}",
		"25m on both arms":  "${{ github.event_name == 'merge_group' && '25m' || '25m' }}",
		"inverted arms":     "${{ github.event_name == 'merge_group' && '30m' || '25m' }}",
		"wrong event":       "${{ github.event_name == 'push' && '25m' || '30m' }}",
	} {
		if len(raceTimeoutProblems(mutant)) == 0 {
			t.Errorf("%s: the planted mutant passes the timeout pin", name)
		}
	}
}

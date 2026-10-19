package cigates_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// The race legs of go-quality-leg may run on the ARC pool named by vars.CI_RACE_RUNNER. ops is a
// public repo: the pool term must carry the push-to-main and same-repository guards, the variable
// unset must mean hosted, and a pool job must run with Ryuk off, a per-job session id and a reap.

const (
	racePoolTerm = "matrix.heavy && vars.CI_RACE_RUNNER != '' && github.event_name == 'push' && " +
		"github.ref == 'refs/heads/main' && github.repository == 'full-chaos/dev-health-ops'"
	raceHostedFallback = "'ubuntu-latest' }}"
)

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

func TestRaceLegsOnThePoolRunWithoutRyukAndReapTheirContainers(t *testing.T) {
	leg := goQualityLeg(t)
	env, _ := leg["env"].(map[string]any)
	if env["TESTCONTAINERS_RYUK_DISABLED"] != "true" {
		t.Fatalf("TESTCONTAINERS_RYUK_DISABLED must be 'true', got %v", env["TESTCONTAINERS_RYUK_DISABLED"])
	}
	session, _ := env["TESTCONTAINERS_SESSION_ID"].(string)
	for _, part := range []string{"github.run_id", "github.run_attempt", "github.job", "strategy.job-index"} {
		if !strings.Contains(session, part) {
			t.Fatalf("TESTCONTAINERS_SESSION_ID must be per job and carry %s, got %q", part, session)
		}
	}
	if got, _ := env["DEV_HEALTH_GO_RACE_TEST_TIMEOUT"].(string); !strings.Contains(got, "'25m'") {
		t.Fatalf("the pool race path needs a per-package go test timeout above the 600 s default, got %q", got)
	}
	steps, _ := leg["steps"].([]any)
	found := false
	for _, s := range steps {
		step, _ := s.(map[string]any)
		if run, _ := step["run"].(string); strings.Contains(run, "ci/reap_testcontainers_session.sh") {
			found = step["if"] == "always()"
		}
	}
	if !found {
		t.Fatal("go-quality-leg needs an always() step that runs ci/reap_testcontainers_session.sh")
	}
}

func TestRaceLegsTurnSetupGoCacheOffOnThePool(t *testing.T) {
	leg := goQualityLeg(t)
	steps, _ := leg["steps"].([]any)
	for _, s := range steps {
		step, _ := s.(map[string]any)
		if uses, _ := step["uses"].(string); strings.HasPrefix(uses, "actions/setup-go@") {
			with, _ := step["with"].(map[string]any)
			cache, _ := with["cache"].(string)
			if !strings.Contains(cache, racePoolTerm) {
				t.Fatalf("setup-go cache must be off on the CI_RACE_RUNNER path, got %q", cache)
			}
			return
		}
	}
	t.Fatal("no setup-go step in go-quality-leg")
}

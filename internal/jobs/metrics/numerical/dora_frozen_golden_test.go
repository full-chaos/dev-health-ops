package numerical

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestComputeDORAMatchesFrozenPythonGolden is the permanent regression guard
// for ComputeDORA, split out of the (retired) shared
// remaining_metrics_python_golden.json / TestRemainingMetricsGoldenMatchesLivePython
// pair when job_dora.py/compute_dora.py were deleted outright (CHAOS-5336).
//
// Same shape as the earlier hotspot_score retirement (CHAOS-5234/CHAOS-3092:
// compute_file_hotspots deleted, its frozen cases split VERBATIM into their
// own file with no generator, filehotspots/fma_golden_test.go now reads it
// directly): this file's one case is the EXACT "dora" entry that used to
// live under remaining_metrics_python_golden.json's "dora" key, generated
// from real production Python on 2026-07-23 and never regenerated since.
// There is no live-Python comparison here and none is possible any more --
// DORAExecutor (native Go) is the sole producer now, confirmed by
// TestDORAExecutorDerivesRestoreTimeFromAMappedPagerDutyIncident
// (dora_pagerduty_incident_restore_time_integration_test.go) proving the
// native executor's canonical-incidents path end-to-end against a real
// database. This test proves the pure arithmetic (ComputeDORA) never
// regresses from the frozen expectation; it is not, and was never meant to
// be, a substitute for that end-to-end proof.
func TestComputeDORAMatchesFrozenPythonGolden(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(
		numericalRepoRoot(t), "tests", "fixtures", "dora_metrics_python_golden.json"))
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var golden struct {
		Cases []struct {
			Day         string `json:"day"`
			Deployments []struct {
				RepoID     string `json:"repo_id"`
				Status     string `json:"status"`
				DeployedAt string `json:"deployed_at"`
				StartedAt  string `json:"started_at"`
				MergedAt   string `json:"merged_at"`
			} `json:"deployments"`
			Incidents []struct {
				RepoID     string `json:"repo_id"`
				StartedAt  string `json:"started_at"`
				ResolvedAt string `json:"resolved_at"`
			} `json:"incidents"`
			Expected []DORAMetric `json:"expected"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatalf("parse golden: %v", err)
	}
	if len(golden.Cases) == 0 {
		t.Fatal("golden is empty -- an empty fixture agrees with every implementation")
	}

	parseGoldenTime := func(value string) time.Time {
		if value == "" {
			return time.Time{}
		}
		result, err := time.Parse(time.RFC3339, value)
		if err != nil {
			t.Fatalf("parse time %q: %v", value, err)
		}
		return result
	}

	for index, testCase := range golden.Cases {
		day, err := time.Parse("2006-01-02", testCase.Day)
		if err != nil {
			t.Fatalf("case %d: parse day: %v", index, err)
		}
		deployments := make([]Deployment, 0, len(testCase.Deployments))
		for _, value := range testCase.Deployments {
			deployments = append(deployments, Deployment{
				RepoID:     value.RepoID,
				Status:     value.Status,
				DeployedAt: parseGoldenTime(value.DeployedAt),
				StartedAt:  parseGoldenTime(value.StartedAt),
				MergedAt:   parseGoldenTime(value.MergedAt),
			})
		}
		incidents := make([]Incident, 0, len(testCase.Incidents))
		for _, value := range testCase.Incidents {
			incidents = append(incidents, Incident{
				RepoID:     value.RepoID,
				StartedAt:  parseGoldenTime(value.StartedAt),
				ResolvedAt: parseGoldenTime(value.ResolvedAt),
			})
		}
		got := ComputeDORA(day, deployments, incidents)
		gotJSON, err := json.Marshal(got)
		if err != nil {
			t.Fatalf("case %d: marshal got: %v", index, err)
		}
		wantJSON, err := json.Marshal(testCase.Expected)
		if err != nil {
			t.Fatalf("case %d: marshal want: %v", index, err)
		}
		if string(gotJSON) != string(wantJSON) {
			t.Fatalf("case %d: ComputeDORA() = %s, want %s", index, gotJSON, wantJSON)
		}
	}
}

package numerical

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// deployGoldenPath is the checked-in fixture, originally generated from REAL
// production Python (compute_deploy_metrics_daily) before CHAOS-5234/
// CHAOS-3092 deleted it (deploy is NATIVE, DeployExecutor, CHAOS-4293) -- see
// this repo's own history for the generator that produced it, deleted in
// the same PR alongside its live-Python rot guard
// (deploy_golden_rot_guard_test.go, also deleted).
func deployGoldenPath() string {
	return filepath.Join("..", "..", "..", "..", "tests", "fixtures", "daily_deploy_python_golden.json")
}

type deployGoldenExpectedRow struct {
	RepoID                 string   `json:"repo_id"`
	DeploymentsCount       int      `json:"deployments_count"`
	FailedDeploymentsCount int      `json:"failed_deployments_count"`
	DeployTimeP50Hours     *float64 `json:"deploy_time_p50_hours"`
	LeadTimeP50Hours       *float64 `json:"lead_time_p50_hours"`
}

type deployGoldenFixture struct {
	Deploy []struct {
		Label       string `json:"label"`
		Day         string `json:"day"`
		Deployments []struct {
			RepoID       string  `json:"repo_id"`
			Status       *string `json:"status"`
			StartedAt    *string `json:"started_at"`
			FinishedAt   *string `json:"finished_at"`
			DeployedAt   *string `json:"deployed_at"`
			MergedAt     *string `json:"merged_at"`
			DeploymentID string  `json:"deployment_id"`
		} `json:"deployments"`
		Expected []deployGoldenExpectedRow `json:"expected"`
	} `json:"deploy"`
}

func parseDeployTime(t *testing.T, raw *string) time.Time {
	t.Helper()
	if raw == nil {
		return time.Time{}
	}
	value, err := time.Parse(time.RFC3339, *raw)
	if err != nil {
		t.Fatalf("parse %q: %v", *raw, err)
	}
	return value
}

// TestComputeDeployMetricsGoldenParity proves Go's ComputeDeployMetrics
// reproduces the frozen Python golden (CHAOS-4293). The live-Python rot
// guard this file used to have a counterpart for
// (deploy_golden_rot_guard_test.go) was retired in the same PR that deleted
// the Python compute it compared against (CHAOS-5234/CHAOS-3092) -- this
// frozen-bits test is now the only regression guard, per chris's ruling
// that the frozen golden + this test are the ongoing contract once Python
// is out of the loop.
func TestComputeDeployMetricsGoldenParity(t *testing.T) {
	data, err := os.ReadFile(deployGoldenPath())
	if err != nil {
		t.Fatal(err)
	}
	var fixture deployGoldenFixture
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if len(fixture.Deploy) == 0 {
		t.Fatal("golden fixture has no deploy cases")
	}

	// computed_at is frozen in the generator (2026-08-25T00:00:00Z) -- not
	// asserted per-row here since Go's ComputeDeployMetrics just stamps it
	// through; the golden's own `expected` rows never encode computed_at.
	computedAt := time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC)

	for _, testCase := range fixture.Deploy {
		t.Run(testCase.Label, func(t *testing.T) {
			day, err := time.Parse("2006-01-02", testCase.Day)
			if err != nil {
				t.Fatal(err)
			}
			rows := make([]DeployRow, len(testCase.Deployments))
			for index, row := range testCase.Deployments {
				status := ""
				if row.Status != nil {
					status = *row.Status
				}
				rows[index] = DeployRow{
					RepoID:     row.RepoID,
					Status:     status,
					StartedAt:  parseDeployTime(t, row.StartedAt),
					FinishedAt: parseDeployTime(t, row.FinishedAt),
					DeployedAt: parseDeployTime(t, row.DeployedAt),
					MergedAt:   parseDeployTime(t, row.MergedAt),
				}
			}

			got := ComputeDeployMetrics(day, rows, computedAt)
			if len(got) != len(testCase.Expected) {
				t.Fatalf("case %s: got %d rows, want %d (got=%#v)", testCase.Label, len(got), len(testCase.Expected), got)
			}
			for index, want := range testCase.Expected {
				row := got[index]
				if row.RepoID != want.RepoID ||
					row.DeploymentsCount != want.DeploymentsCount ||
					row.FailedDeploymentsCount != want.FailedDeploymentsCount ||
					!floatPtrEqual(row.DeployTimeP50Hours, want.DeployTimeP50Hours) ||
					!floatPtrEqual(row.LeadTimeP50Hours, want.LeadTimeP50Hours) {
					t.Fatalf("case %s row %d:\n got=%#v (deploy=%v, lead=%v)\nwant=%#v",
						testCase.Label, index, row, floatPtrString(row.DeployTimeP50Hours), floatPtrString(row.LeadTimeP50Hours), want)
				}
			}
		})
	}
}

func floatPtrEqual(a, b *float64) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func floatPtrString(v *float64) string {
	if v == nil {
		return "nil"
	}
	return fmt.Sprintf("%v", *v)
}

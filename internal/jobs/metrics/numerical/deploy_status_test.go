package numerical

import (
	"testing"
	"time"
)

// CHAOS-9621: one status meaning for deployment failure rate (DORA) and the
// deploy metrics of a day, by value. A canceled deployment never reached its
// environment: it is not a deployment and not a failure. Every other status
// that is not a failure stays a deployment.
func TestDeploymentStatusMeaningInBothDeploymentMetrics(t *testing.T) {
	day := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	at := day.Add(10 * time.Hour)
	for _, tc := range []struct {
		status          string
		deployments     int
		failed          int
		failureRateRows bool
	}{
		{"success", 1, 0, true},
		{"failure", 1, 1, true},
		{"failed", 1, 1, true},
		{"error", 1, 1, true},
		{" FAILURE ", 1, 1, true},
		{"canceled", 0, 0, false},
		{"cancelled", 0, 0, false},
		{"inactive", 1, 0, true},
		{"blocked", 1, 0, true},
		{"in_progress", 1, 0, true},
		{"", 1, 0, true},
		{"weird", 1, 0, true},
	} {
		metrics := ComputeDeployMetrics(day, []DeployRow{{RepoID: "r", Status: tc.status, DeployedAt: at}}, day)
		if tc.deployments == 0 {
			if len(metrics) != 0 {
				t.Errorf("status %q: deploy metrics %+v, want none", tc.status, metrics)
			}
		} else if len(metrics) != 1 || metrics[0].DeploymentsCount != tc.deployments || metrics[0].FailedDeploymentsCount != tc.failed {
			t.Errorf("status %q: deploy metrics %+v, want %d deployments, %d failed", tc.status, metrics, tc.deployments, tc.failed)
		}

		dora := ComputeDORA(day, []Deployment{{RepoID: "r", Status: tc.status, DeployedAt: at}}, nil)
		var frequency, rate *float64
		for index := range dora {
			switch dora[index].Name {
			case "deployment_frequency":
				frequency = &dora[index].Value
			case "deployment_failure_rate":
				rate = &dora[index].Value
			}
		}
		if !tc.failureRateRows {
			if frequency != nil || rate != nil {
				t.Errorf("status %q: DORA rows %+v, want none", tc.status, dora)
			}
			continue
		}
		if frequency == nil || *frequency != float64(tc.deployments) || rate == nil || *rate != float64(tc.failed) {
			t.Errorf("status %q: DORA rows %+v, want frequency %d and failure rate %d", tc.status, dora, tc.deployments, tc.failed)
		}
	}
}

// A canceled deployment beside real ones leaves the denominator alone.
func TestACanceledDeploymentLeavesTheDenominatorAlone(t *testing.T) {
	day := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	at := day.Add(10 * time.Hour)
	rows := []Deployment{
		{RepoID: "r", Status: "failed", DeployedAt: at},
		{RepoID: "r", Status: "success", DeployedAt: at},
		{RepoID: "r", Status: "canceled", DeployedAt: at},
		{RepoID: "r", Status: "canceled", DeployedAt: at},
	}
	for _, row := range ComputeDORA(day, rows, nil) {
		if row.Name == "deployment_failure_rate" && row.Value != 0.5 {
			t.Errorf("deployment failure rate %v, want 0.5 (1 failed of 2 deployments)", row.Value)
		}
		if row.Name == "deployment_frequency" && row.Value != 2 {
			t.Errorf("deployment frequency %v, want 2", row.Value)
		}
	}
	deploys := make([]DeployRow, len(rows))
	for index, row := range rows {
		deploys[index] = DeployRow{RepoID: row.RepoID, Status: row.Status, DeployedAt: row.DeployedAt}
	}
	metrics := ComputeDeployMetrics(day, deploys, day)
	if len(metrics) != 1 || metrics[0].DeploymentsCount != 2 || metrics[0].FailedDeploymentsCount != 1 {
		t.Errorf("deploy metrics %+v, want 2 deployments and 1 failed", metrics)
	}
}

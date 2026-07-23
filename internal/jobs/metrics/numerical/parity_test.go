package numerical

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type goldenFixture struct {
	DORA []struct {
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
	} `json:"dora"`
	Capacity []struct {
		History     []int     `json:"history"`
		Values      []int     `json:"values"`
		Percentiles []float64 `json:"percentiles"`
		Expected    []int     `json:"expected"`
		Mean        float64   `json:"mean"`
		Stddev      float64   `json:"stddev"`
	} `json:"capacity"`
	Complexity []struct {
		Files    []ComplexityFile  `json:"files"`
		Expected ComplexitySummary `json:"expected"`
	} `json:"complexity"`
	ReleaseConfidence []struct {
		Coverage          float64 `json:"coverage"`
		TotalSessions     int     `json:"total_sessions"`
		ConcurrentDeploys int     `json:"concurrent_deploys"`
		MinimumSessions   int     `json:"minimum_sessions"`
		Expected          float64 `json:"expected"`
	} `json:"release_confidence"`
}

func TestPythonNumericalGoldenParity(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "tests", "fixtures", "remaining_metrics_python_golden.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture goldenFixture
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	for index, testCase := range fixture.DORA {
		day, err := time.Parse("2006-01-02", testCase.Day)
		if err != nil {
			t.Fatal(err)
		}
		deployments := make([]Deployment, 0, len(testCase.Deployments))
		for _, value := range testCase.Deployments {
			deployments = append(deployments, Deployment{
				RepoID: value.RepoID, Status: value.Status,
				DeployedAt: parseTime(t, value.DeployedAt),
				StartedAt:  parseTime(t, value.StartedAt),
				MergedAt:   parseTime(t, value.MergedAt),
			})
		}
		incidents := make([]Incident, 0, len(testCase.Incidents))
		for _, value := range testCase.Incidents {
			incidents = append(incidents, Incident{
				RepoID:     value.RepoID,
				StartedAt:  parseTime(t, value.StartedAt),
				ResolvedAt: parseTime(t, value.ResolvedAt),
			})
		}
		if got := ComputeDORA(day, deployments, incidents); !equalJSON(got, testCase.Expected) {
			t.Fatalf("dora case %d = %#v, want %#v", index, got, testCase.Expected)
		}
	}
	for index, testCase := range fixture.Capacity {
		if got := IntegerPercentiles(testCase.Values, testCase.Percentiles); !equalJSON(got, testCase.Expected) {
			t.Fatalf("capacity percentiles case %d = %#v, want %#v", index, got, testCase.Expected)
		}
		got := ThroughputStatistics(testCase.History)
		if !close(got.Mean, testCase.Mean) || !close(got.Stddev, testCase.Stddev) {
			t.Fatalf("capacity stats case %d = %#v, want mean=%f stddev=%f", index, got, testCase.Mean, testCase.Stddev)
		}
	}
	for index, testCase := range fixture.Complexity {
		if got := AggregateComplexity(testCase.Files); !equalJSON(got, testCase.Expected) {
			t.Fatalf("complexity case %d = %#v, want %#v", index, got, testCase.Expected)
		}
	}
	for index, testCase := range fixture.ReleaseConfidence {
		got := ReleaseImpactConfidence(testCase.Coverage, testCase.TotalSessions, testCase.ConcurrentDeploys, testCase.MinimumSessions)
		if !close(got, testCase.Expected) {
			t.Fatalf("release confidence case %d = %f, want %f", index, got, testCase.Expected)
		}
	}
}

func parseTime(t *testing.T, value string) time.Time {
	t.Helper()
	if value == "" {
		return time.Time{}
	}
	result, err := time.Parse(time.RFC3339, value)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func close(left, right float64) bool {
	difference := left - right
	if difference < 0 {
		difference = -difference
	}
	return difference < 1e-12
}

func equalJSON(left, right any) bool {
	leftJSON, _ := json.Marshal(left)
	rightJSON, _ := json.Marshal(right)
	return string(leftJSON) == string(rightJSON)
}

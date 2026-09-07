package numerical

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// CHAOS-5336 dropped this golden's "dora" section (and ComputeDORA's
// matching case here) along with job_dora.py/compute_dora.py -- the native
// DORAExecutor has no Python fallback left to guard against drifting from.
// ComputeDORA's frozen-golden coverage moved to its own dedicated file with
// no generator: see dora_frozen_golden_test.go /
// tests/fixtures/dora_metrics_python_golden.json, same shape as the earlier
// hotspot_score split. capacity is untouched here -- compute_capacity.py
// stays live (the GraphQL capacity resolver calls it directly, a separate
// epic), so this shared golden's live-Python drift check still applies to it.
type goldenFixture struct {
	Capacity []struct {
		History     []int     `json:"history"`
		Values      []int     `json:"values"`
		Percentiles []float64 `json:"percentiles"`
		Expected    []int     `json:"expected"`
		Mean        float64   `json:"mean"`
		Stddev      float64   `json:"stddev"`
	} `json:"capacity"`
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
	for index, testCase := range fixture.Capacity {
		if got := IntegerPercentiles(testCase.Values, testCase.Percentiles); !equalJSON(got, testCase.Expected) {
			t.Fatalf("capacity percentiles case %d = %#v, want %#v", index, got, testCase.Expected)
		}
		got := ThroughputStatistics(testCase.History)
		if !close(got.Mean, testCase.Mean) || !close(got.Stddev, testCase.Stddev) {
			t.Fatalf("capacity stats case %d = %#v, want mean=%f stddev=%f", index, got, testCase.Mean, testCase.Stddev)
		}
	}
	for index, testCase := range fixture.ReleaseConfidence {
		got := ReleaseImpactConfidence(testCase.Coverage, testCase.TotalSessions, testCase.ConcurrentDeploys, testCase.MinimumSessions)
		if !close(got, testCase.Expected) {
			t.Fatalf("release confidence case %d = %f, want %f", index, got, testCase.Expected)
		}
	}
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

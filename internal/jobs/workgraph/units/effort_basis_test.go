package units

import "testing"

func TestComputeEffortBasis(t *testing.T) {
	churn := map[string]float64{"r@a": 10, "r@zero": 0}
	cases := []struct {
		name    string
		commits []string
		churn   map[string]float64
		effort  float64
		want    string
	}{
		{"no commits", nil, churn, 0, EffortBasisMeasured},
		{"all have stats", []string{"r@a", "r@zero"}, churn, 10, EffortBasisMeasured},
		{"stats row of zero is measured", []string{"r@zero"}, churn, 0, EffortBasisMeasured},
		{"duplicate id with stats", []string{"r@a", "r@a"}, churn, 20, EffortBasisMeasured},
		{"some lack stats", []string{"r@a", "r@missing"}, churn, 10, EffortBasisPartialCapped},
		{"all lack stats, zero effort", []string{"r@x", "r@y"}, churn, 0, EffortBasisUnmeasured},
		{"all lack stats, effort from PR churn", []string{"r@x"}, churn, 7, EffortBasisPartialCapped},
		{"nil churn map", []string{"r@x"}, nil, 0, EffortBasisUnmeasured},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ComputeEffortBasis(tc.commits, tc.churn, Effort{Metric: EffortMetricChurnLOC, Value: tc.effort})
			if got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

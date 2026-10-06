package home

import "testing"

func TestCoveragePercentDistinguishesAbsentDenominatorFromObservedZero(t *testing.T) {
	tests := []struct {
		name                   string
		numerator, denominator float64
		want                   *float64
	}{
		{name: "no records", numerator: 0, denominator: 0, want: nil},
		{name: "observed zero", numerator: 0, denominator: 3, want: floatPtr(0)},
		{name: "observed value", numerator: 1, denominator: 2, want: floatPtr(50)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := coveragePercent(tt.numerator, tt.denominator)
			if tt.want == nil {
				if got != nil {
					t.Fatalf("coveragePercent(%v, %v) = %v, want nil", tt.numerator, tt.denominator, *got)
				}
				return
			}
			if got == nil || *got != *tt.want {
				t.Fatalf("coveragePercent(%v, %v) = %v, want %v", tt.numerator, tt.denominator, got, *tt.want)
			}
		})
	}
}

func TestCoverageObservedValuesExcludesUnavailableMeasurements(t *testing.T) {
	coverage := Coverage{
		ReposCoveredPct:          nil,
		PRsLinkedToIssuesPct:     floatPtr(0),
		IssuesWithCycleStatesPct: floatPtr(50),
	}
	got := coverage.ObservedValues()
	if len(got) != 2 || got["prs_linked_to_issues_pct"] != 0 || got["issues_with_cycle_states_pct"] != 50 {
		t.Fatalf("ObservedValues() = %#v, want only the two observed measurements", got)
	}
	if _, exists := got["repos_covered_pct"]; exists {
		t.Fatalf("ObservedValues() = %#v, included unavailable repos coverage", got)
	}
}

func floatPtr(v float64) *float64 { return &v }

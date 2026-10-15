package units

import (
	"math"
	"testing"
)

// TestComputeIssueSizeAcrossProviders pins the one sizing rule for every provider: each provider writes its
// size to work_items.story_points (Jira story points, GitLab weight, Linear estimate, GitHub Projects number
// field), and a unit's size is the sum of the positive sizes of its issues; an unsized unit weighs 1.
func TestComputeIssueSizeAcrossProviders(t *testing.T) {
	cases := []struct {
		name        string
		issues      []string
		points      map[string]float64
		wantValue   float64
		wantSized   int
		wantIssues  int
		wantWeight  float64
		wantIsSized bool
	}{
		{name: "jira story points", issues: []string{"jira:PAY-1", "jira:PAY-2"}, points: map[string]float64{"jira:PAY-1": 3, "jira:PAY-2": 5}, wantValue: 8, wantSized: 2, wantIssues: 2, wantWeight: 8, wantIsSized: true},
		{name: "gitlab weight", issues: []string{"gitlab:group/billing#7"}, points: map[string]float64{"gitlab:group/billing#7": 2}, wantValue: 2, wantSized: 1, wantIssues: 1, wantWeight: 2, wantIsSized: true},
		{name: "linear estimate", issues: []string{"linear:ENG-4"}, points: map[string]float64{"linear:ENG-4": 0.5}, wantValue: 0.5, wantSized: 1, wantIssues: 1, wantWeight: 0.5, wantIsSized: true},
		{name: "github projects number field", issues: []string{"gh:acme/api#12"}, points: map[string]float64{"gh:acme/api#12": 13}, wantValue: 13, wantSized: 1, wantIssues: 1, wantWeight: 13, wantIsSized: true},
		{name: "jira unsized counts one", issues: []string{"jira:PAY-3"}, points: map[string]float64{}, wantIssues: 1, wantWeight: 1},
		{name: "gitlab unsized counts one", issues: []string{"gitlab:group/billing#8"}, points: nil, wantIssues: 1, wantWeight: 1},
		{name: "linear unsized counts one", issues: []string{"linear:ENG-5"}, points: map[string]float64{"linear:ENG-9": 3}, wantIssues: 1, wantWeight: 1},
		{name: "github unsized counts one", issues: []string{"gh:acme/api#13"}, points: map[string]float64{}, wantIssues: 1, wantWeight: 1},
		{name: "pull request only unit counts one", issues: nil, points: map[string]float64{"gh:acme/api#12": 13}, wantWeight: 1},
		{name: "zero points is no size", issues: []string{"jira:PAY-4"}, points: map[string]float64{"jira:PAY-4": 0}, wantIssues: 1, wantWeight: 1},
		{name: "negative points is no size", issues: []string{"jira:PAY-5"}, points: map[string]float64{"jira:PAY-5": -2}, wantIssues: 1, wantWeight: 1},
		{name: "NaN is no size", issues: []string{"linear:ENG-6"}, points: map[string]float64{"linear:ENG-6": math.NaN()}, wantIssues: 1, wantWeight: 1},
		{name: "infinity is no size", issues: []string{"linear:ENG-7"}, points: map[string]float64{"linear:ENG-7": math.Inf(1)}, wantIssues: 1, wantWeight: 1},
		{name: "partly sized unit is the sum of its sized issues", issues: []string{"jira:PAY-6", "jira:PAY-7", "linear:ENG-8"}, points: map[string]float64{"jira:PAY-6": 3, "linear:ENG-8": 2}, wantValue: 5, wantSized: 2, wantIssues: 3, wantWeight: 5, wantIsSized: true},
		{name: "a sum that overflows is no size", issues: []string{"jira:PAY-8", "jira:PAY-9"}, points: map[string]float64{"jira:PAY-8": math.MaxFloat64, "jira:PAY-9": math.MaxFloat64}, wantIssues: 2, wantWeight: 1},
		{name: "a repeated issue counts once", issues: []string{"jira:PAY-1", "jira:PAY-1"}, points: map[string]float64{"jira:PAY-1": 3}, wantValue: 3, wantSized: 1, wantIssues: 1, wantWeight: 3, wantIsSized: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ComputeIssueSize(tc.issues, tc.points)
			if got.Value != tc.wantValue || got.SizedIssues != tc.wantSized || got.Issues != tc.wantIssues {
				t.Fatalf("ComputeIssueSize = %+v, want value %v sized %d issues %d", got, tc.wantValue, tc.wantSized, tc.wantIssues)
			}
			if got.Weight() != tc.wantWeight {
				t.Fatalf("Weight() = %v, want %v", got.Weight(), tc.wantWeight)
			}
			if got.Sized() != tc.wantIsSized {
				t.Fatalf("Sized() = %v, want %v", got.Sized(), tc.wantIsSized)
			}
		})
	}
}

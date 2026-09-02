package units

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"
)

type effortCase struct {
	Label       string                     `json:"label"`
	IssueIDs    []string                   `json:"issue_ids"`
	PRIDs       []string                   `json:"pr_ids"`
	CommitIDs   []string                   `json:"commit_ids"`
	PRChurn     map[string]json.RawMessage `json:"pr_churn"`
	CommitChurn map[string]json.RawMessage `json:"commit_churn"`
	ActiveHours map[string]json.RawMessage `json:"active_hours"`
	Metric      string                     `json:"metric"`
	Value       json.RawMessage            `json:"value"`
}

func loadEffortCases(t *testing.T) []effortCase {
	t.Helper()
	path := filepath.Join(
		repositoryRootPath(t), "tests", "fixtures", "effort_python_golden.json",
	)
	raw, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		t.Fatalf("read effort golden: %v (regenerate with: uv run python "+
			"tests/fixtures/generate_effort_golden.py)", err)
	}
	var golden struct {
		Cases []effortCase `json:"cases"`
	}
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatalf("parse effort golden: %v", err)
	}
	if len(golden.Cases) == 0 {
		t.Fatal("golden contains no cases")
	}
	return golden.Cases
}

func decodeChurn(t *testing.T, raw map[string]json.RawMessage) map[string]float64 {
	t.Helper()
	out := make(map[string]float64, len(raw))
	for key, value := range raw {
		out[key] = decodeFloat(t, value)
	}
	return out
}

// TestComputeEffortMatchesLivePython drives the port against outputs captured
// from _effort_from_work_unit itself.
func TestComputeEffortMatchesLivePython(t *testing.T) {
	var sawNegativeFallThrough, sawNaNFallThrough, sawDuplicate bool

	for _, testCase := range loadEffortCases(t) {
		t.Run(testCase.Label, func(t *testing.T) {
			got := ComputeEffort(EffortInput{
				IssueIDs:    testCase.IssueIDs,
				PRIDs:       testCase.PRIDs,
				CommitIDs:   testCase.CommitIDs,
				PRChurn:     decodeChurn(t, testCase.PRChurn),
				CommitChurn: decodeChurn(t, testCase.CommitChurn),
				ActiveHours: decodeChurn(t, testCase.ActiveHours),
			})

			if got.Metric != testCase.Metric {
				t.Errorf("metric = %q, python = %q", got.Metric, testCase.Metric)
			}
			want := decodeFloat(t, testCase.Value)
			if !sameFloat(got.Value, want) {
				t.Errorf("value = %v, python = %v", got.Value, want)
			}
		})

		switch {
		case testCase.Label == "negative_commit_falls_to_pr":
			sawNegativeFallThrough = true
		case testCase.Label == "nan_commit_falls_through":
			sawNaNFallThrough = true
		case testCase.Label == "duplicate_ids_double_count":
			sawDuplicate = true
		}
	}

	// Guard the corpus. Each of these is a case where a plausible alternative
	// spelling of the gate passes everything else.
	if !sawNegativeFallThrough {
		t.Error("no negative-total case: `total > 0` versus `total != 0` would be " +
			"untested, and the two select different tiers")
	}
	if !sawNaNFallThrough {
		t.Error("no NaN case: the fall-through on an unordered comparison would be " +
			"untested")
	}
	if !sawDuplicate {
		t.Error("no duplicate-id case: summing the MAP instead of the ID LIST would " +
			"be untested, and it halves the effort of any unit listing an id twice")
	}
}

// TestEffortGateIsGreaterThanZeroNotNonZero pins the three spellings apart.
//
// `total > 0`, `total != 0` and "the tier has entries" all agree on ordinary
// positive data and disagree on everything interesting. The corpus covers this,
// but the distinction is stated here because it is a single character in the
// source and reads as arbitrary.
func TestEffortGateIsGreaterThanZeroNotNonZero(t *testing.T) {
	// Negative commit churn, positive PR churn. Python takes the PR tier.
	negative := ComputeEffort(EffortInput{
		CommitIDs: []string{"c"}, CommitChurn: map[string]float64{"c": -5},
		PRIDs: []string{"p"}, PRChurn: map[string]float64{"p": 20},
	})
	if negative.Value != 20 {
		t.Errorf("negative commit churn: value = %v, want 20 from the PR tier -- "+
			"a `!= 0` gate would have selected -5", negative.Value)
	}

	// NaN commit churn. `nan > 0` is false in both languages, so it falls
	// through; a gate written as `!math.IsNaN(t) && t != 0` would too, but one
	// written as `total != 0` would SELECT the NaN, since NaN != 0 is true.
	nan := ComputeEffort(EffortInput{
		CommitIDs: []string{"c"}, CommitChurn: map[string]float64{"c": math.NaN()},
		PRIDs: []string{"p"}, PRChurn: map[string]float64{"p": 7},
	})
	if nan.Value != 7 {
		t.Errorf("NaN commit churn: value = %v, want 7 from the PR tier -- a `!= 0` "+
			"gate would have selected NaN, since NaN != 0 is TRUE", nan.Value)
	}

	// Everything non-positive: churn_loc 0.0, NOT active_hours and NOT the sum.
	none := ComputeEffort(EffortInput{
		CommitIDs: []string{"c"}, CommitChurn: map[string]float64{"c": -5},
		IssueIDs: []string{"i"}, ActiveHours: map[string]float64{"i": -2},
	})
	if none.Metric != EffortMetricChurnLOC || none.Value != 0 {
		t.Errorf("all non-positive: got (%q, %v), want (churn_loc, 0) -- the final "+
			"fallback names churn_loc even with no commits or PRs",
			none.Metric, none.Value)
	}

	// The smallest denormal is still strictly positive and still wins.
	tiny := ComputeEffort(EffortInput{
		CommitIDs: []string{"c"}, CommitChurn: map[string]float64{"c": math.SmallestNonzeroFloat64},
		PRIDs: []string{"p"}, PRChurn: map[string]float64{"p": 100},
	})
	if tiny.Value != math.SmallestNonzeroFloat64 {
		t.Errorf("smallest denormal: value = %v, want it to win its tier", tiny.Value)
	}
}

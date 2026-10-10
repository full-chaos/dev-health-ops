package prrework

import (
	"math"
	"strconv"
	"testing"
)

func TestRateTellsUnknownNotApplicableAndAMeasuredZeroApart(t *testing.T) {
	for name, testCase := range map[string]struct {
		counts   Counts
		state    State
		value    *float64
		coverage *float64
	}{
		"no merged pull request":                 {Counts{}, StateNotApplicableNoMerged, nil, nil},
		"merged, none reviewed":                  {Counts{Merged: 4}, StateUnknown, nil, ptr(0)},
		"merged, a provider with no signal":      {Counts{Merged: 4, NoSignal: 4}, StateNotApplicableNoSignal, nil, ptr(0)},
		"two providers, none reviewed":           {Counts{Merged: 6, NoSignal: 2}, StateUnknown, nil, ptr(0)},
		"reviewed, no rework":                    {Counts{Merged: 4, Reviewed: 2}, StateMeasured, ptr(0), ptr(0.5)},
		"reviewed, rework":                       {Counts{Merged: 4, Reviewed: 2, Rework: 1}, StateMeasured, ptr(0.5), ptr(0.5)},
		"reviewed beside a provider with no one": {Counts{Merged: 10, Reviewed: 4, Rework: 1, NoSignal: 5}, StateMeasured, ptr(0.25), ptr(0.4)},
	} {
		got := Rate(testCase.counts)
		if got.State != testCase.state || !same(got.Value, testCase.value) || !same(got.Coverage, testCase.coverage) {
			t.Errorf("%s: Rate(%+v) = state %q value %s coverage %s, want %q %s %s", name, testCase.counts,
				got.State, show(got.Value), show(got.Coverage), testCase.state, show(testCase.value), show(testCase.coverage))
		}
	}
}

func TestEvaluateGivesNoStateToAViewWithNoStoredCounts(t *testing.T) {
	if got := Evaluate(View{Counts: Counts{Merged: 9}}); got.State != StateNoStoredCounts || got.Value != nil || got.StateOrNil() != nil {
		t.Errorf("a view with no stored counts = %+v, want no value and no state", got)
	}
	got := Evaluate(View{Counts: Counts{Merged: 2, Reviewed: 2}, StoredRows: 1})
	if got.State != StateMeasured || got.Value == nil || *got.Value != 0 || got.StateOrNil() == nil {
		t.Errorf("two reviewed pull requests and no rework = %+v, want a measured 0", got)
	}
}

func TestCountMergedKeepsUnreviewedPullRequestsOutOfTheRatio(t *testing.T) {
	pullRequests := []PullRequest{
		{},                // no review data
		{ReviewsCount: 2}, // reviewed, no rework
		{ReviewsCount: 1, ChangesRequestedCount: 1},
		{ChangesRequestedCount: 2}, // a changes-requested review is review evidence
	}
	if got, want := CountMerged(pullRequests, true), (Counts{Merged: 4, Reviewed: 3, Rework: 2}); got != want {
		t.Errorf("with the signal: %+v, want %+v", got, want)
	}
	if got, want := CountMerged(pullRequests, false), (Counts{Merged: 4, NoSignal: 4}); got != want {
		t.Errorf("with no signal: %+v, want %+v", got, want)
	}
	if got := CountMerged(nil, true); got != (Counts{}) {
		t.Errorf("no merged pull request: %+v, want zero counts", got)
	}
	sum := CountMerged(pullRequests, true).Add(CountMerged(pullRequests[:1], false))
	if want := (Counts{Merged: 5, Reviewed: 3, Rework: 2, NoSignal: 1}); sum != want {
		t.Errorf("sum of two repositories: %+v, want %+v", sum, want)
	}
}

func ptr(v float64) *float64 { return &v }

func same(a, b *float64) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func show(v *float64) string {
	if v == nil {
		return "nil"
	}
	return strconv.FormatFloat(*v, 'f', -1, 64)
}

// The coverage of a view is over EVERY merged pull request the view stores: a
// row with no counts (a day stored before the counts existed) is in the
// denominator only. The ratio and its state stay a statement about the rows
// that hold counts.
func TestEvaluateTakesTheCoverageOverEveryStoredRow(t *testing.T) {
	near := func(got *float64, want float64) bool { return got != nil && math.Abs(*got-want) < 1e-12 }
	counted := Counts{Merged: 12, Reviewed: 5, Rework: 1}
	for _, test := range []struct {
		name     string
		view     View
		state    State
		coverage float64 // -1: nil
	}{
		{"every row holds counts", View{Counts: counted, StoredRows: 1, MergedOfEveryRow: 12}, StateMeasured, 5.0 / 12},
		{"one counted row and rows with 88 more merged pull requests and no counts", View{Counts: counted, StoredRows: 1, MergedOfEveryRow: 100}, StateMeasured, 0.05},
		{"a view that gives no sum of every row keeps the coverage of its counts", View{Counts: counted, StoredRows: 1}, StateMeasured, 5.0 / 12},
		{"a sum of every row below the counts is not used", View{Counts: counted, StoredRows: 1, MergedOfEveryRow: 3}, StateMeasured, 5.0 / 12},
		{"no review data, and rows with no counts", View{Counts: Counts{Merged: 3}, StoredRows: 1, MergedOfEveryRow: 30}, StateUnknown, 0},
		{"no signal, and rows with no counts", View{Counts: Counts{Merged: 4, NoSignal: 4}, StoredRows: 1, MergedOfEveryRow: 30}, StateNotApplicableNoSignal, 0},
		{"a counted day with no merged pull request, and rows with no counts that hold some", View{StoredRows: 1, MergedOfEveryRow: 30}, StateNotApplicableNoMerged, 0},
		{"a counted day with no merged pull request and nothing else", View{StoredRows: 1}, StateNotApplicableNoMerged, -1},
		{"no row holds counts", View{MergedOfEveryRow: 30}, StateNoStoredCounts, -1},
	} {
		t.Run(test.name, func(t *testing.T) {
			outcome := Evaluate(test.view)
			if outcome.State != test.state {
				t.Errorf("state %q, want %q", outcome.State, test.state)
			}
			if test.coverage < 0 {
				if outcome.Coverage != nil {
					t.Errorf("coverage %v, want none", *outcome.Coverage)
				}
				return
			}
			if !near(outcome.Coverage, test.coverage) {
				t.Errorf("coverage %v, want %v", outcome.Coverage, test.coverage)
			}
			// The ratio is of the counted rows, whatever the other rows hold.
			if rate := Rate(test.view.Counts); (rate.Value == nil) != (outcome.Value == nil) || (rate.Value != nil && *rate.Value != *outcome.Value) {
				t.Errorf("the ratio changed with the rows that hold no counts")
			}
		})
	}
}

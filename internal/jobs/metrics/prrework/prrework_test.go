package prrework

import (
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

func TestOnlyAProviderWithTheEventHasTheReworkSignal(t *testing.T) {
	for provider, want := range map[string]bool{
		"github": true, " GitHub ": true, "gitlab": false, "local": false, "": false, "bitbucket": false, "custom:push": false,
	} {
		if got := ProviderHasReworkSignal(provider); got != want {
			t.Errorf("ProviderHasReworkSignal(%q) = %v, want %v", provider, got, want)
		}
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

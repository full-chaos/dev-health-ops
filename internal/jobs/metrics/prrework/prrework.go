// Package prrework holds the one definition of the pull request rework ratio:
// of the merged pull requests that HAVE review evidence, the share that got a
// changes-requested review, measured over a view's window and subject.
//
// The writer (the repo_user_commit daily family) counts each repository and
// day and stores the counts in repo_metrics_daily. Every reader sums those
// counts over the view (one repository, a team's owned repositories, or the
// organization) and applies Rate in Go or WindowRateSQL in ClickHouse. Both
// encode the same rule:
//
//   - no stored counts in the view                         -> no value, no state
//   - no merged pull request                               -> no value, not applicable
//   - no reviewed pull request, and every merged pull
//     request is of a provider with no changes-requested
//     event                                                -> no value, not applicable
//   - no reviewed pull request                             -> no value, unknown
//   - else rework / reviewed; 0 is a measured 0
//
// Missing is not healthy: a pull request with no review data says nothing
// about rework, so it is in neither the numerator nor the denominator. The
// ratio it replaces divided by ALL merged pull requests and stored 0 when
// nothing was reviewed, so a repository whose reviews were never read showed
// a measured 0%.
//
// A window is a ratio of sums, never a mean of daily ratios: a day with one
// reviewed pull request must not weigh as much as a day with fifty.
package prrework

// State says why a view has a ratio or not. Unknown, not applicable and a
// measured 0 are different answers, and a reader must be able to show which
// one it has.
type State string

const (
	// StateMeasured: one reviewed pull request or more. The value may be 0.
	StateMeasured State = "measured"
	// StateNotApplicableNoMerged: stored counts, and no merged pull request
	// in the view.
	StateNotApplicableNoMerged State = "not_applicable_no_merged_pull_requests"
	// StateNotApplicableNoSignal: every merged pull request of the view is of
	// a provider that has no changes-requested event, so the numerator can
	// never be other than 0.
	StateNotApplicableNoSignal State = "not_applicable_no_rework_signal"
	// StateUnknown: merged pull requests of a provider that has the event,
	// and none of them has review data.
	StateUnknown State = "unknown_no_review_evidence"
	// StateNoStoredCounts: the view holds no row with stored counts: the days
	// were never computed, or were computed before the counts existed. It is
	// the empty string: a reader serves it as a null state.
	StateNoStoredCounts State = ""
)

// Counts are the stored inputs of one repository and day, or their sum over a
// view.
type Counts struct {
	// Merged is every pull request merged on the day.
	Merged uint64
	// Reviewed is the merged pull requests that have review evidence and are
	// of a provider with a changes-requested event.
	Reviewed uint64
	// Rework is the reviewed pull requests with a changes-requested review.
	Rework uint64
	// NoSignal is the merged pull requests of a provider that has no
	// changes-requested event.
	NoSignal uint64
}

// Add returns the sum of two counts.
func (c Counts) Add(o Counts) Counts {
	return Counts{
		Merged: c.Merged + o.Merged, Reviewed: c.Reviewed + o.Reviewed,
		Rework: c.Rework + o.Rework, NoSignal: c.NoSignal + o.NoSignal,
	}
}

// Outcome is the answer for a view: the ratio when it is measured, the state
// that says why it is or is not, and the coverage of the measure.
type Outcome struct {
	Value *float64
	State State
	// Coverage is reviewed / merged: the share of the merged pull requests the
	// ratio speaks for. Nil when no pull request merged.
	Coverage *float64
}

// StateOrNil is the state as a reader serves it: nil for StateNoStoredCounts.
func (o Outcome) StateOrNil() *string {
	if o.State == StateNoStoredCounts {
		return nil
	}
	state := string(o.State)
	return &state
}

// View is the summed counts of a view (a window and a subject) and the number
// of stored rows that hold counts.
type View struct {
	Counts
	StoredRows uint64
}

// Evaluate is the one rule every reader applies to a view. A view with no
// stored counts has no state; any other view has the Rate of its counts.
func Evaluate(v View) Outcome {
	if v.StoredRows == 0 {
		return Outcome{State: StateNoStoredCounts}
	}
	return Rate(v.Counts)
}

// Rate applies the rule of this package to the counts of stored rows.
func Rate(c Counts) Outcome {
	if c.Merged == 0 {
		return Outcome{State: StateNotApplicableNoMerged}
	}
	coverage := float64(c.Reviewed) / float64(c.Merged)
	if c.Reviewed == 0 {
		if c.NoSignal >= c.Merged {
			return Outcome{State: StateNotApplicableNoSignal, Coverage: &coverage}
		}
		return Outcome{State: StateUnknown, Coverage: &coverage}
	}
	value := float64(c.Rework) / float64(c.Reviewed)
	return Outcome{Value: &value, State: StateMeasured, Coverage: &coverage}
}

// PullRequest is what the count needs from one merged pull request.
type PullRequest struct {
	// ReviewsCount is the number of reviews the sync stored on the pull
	// request row. A pull request has review evidence when it is above 0.
	ReviewsCount int
	// ChangesRequestedCount is the number of changes-requested reviews.
	ChangesRequestedCount int
}

// CountMerged counts the merged pull requests of one repository and day.
// hasSignal says that the repository's provider can store a changes-requested
// review. The caller takes that from the provider layer's declaration
// (providerfoundation.EmitsPullRequestReviewState); this package knows no
// provider. Without the signal no pull request counts as reviewed: its 0 is
// the provider's shape, not a measure.
//
// A pull request with a changes-requested review has review evidence by that
// fact, whatever its review count says.
func CountMerged(pullRequests []PullRequest, hasSignal bool) Counts {
	counts := Counts{Merged: uint64(len(pullRequests))}
	if !hasSignal {
		counts.NoSignal = counts.Merged
		return counts
	}
	for _, pullRequest := range pullRequests {
		rework := pullRequest.ChangesRequestedCount > 0
		if pullRequest.ReviewsCount > 0 || rework {
			counts.Reviewed++
			if rework {
				counts.Rework++
			}
		}
	}
	return counts
}

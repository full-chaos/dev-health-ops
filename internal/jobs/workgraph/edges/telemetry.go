package edges

import (
	"fmt"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

// observedOutcome maps each derivation Outcome to its metric label.
//
// This is written out rather than done by string conversion, even though the
// two vocabularies currently spell every value identically. A conversion would
// compile forever: a new Outcome added to the deriver would silently become a
// new label the collector refuses (or, worse, silently accepts), and the first
// evidence would be a missing series on a dashboard nobody is looking at.
// Written out, a new Outcome fails TestEveryOutcomeHasAMetricLabel instead.
var observedOutcome = map[Outcome]jobruntime.WorkGraphIssueEdgeOutcome{
	OutcomeEmitted:         jobruntime.WorkGraphIssueEdgeEmitted,
	OutcomeDeduped:         jobruntime.WorkGraphIssueEdgeDeduped,
	OutcomeSkippedEmptyID:  jobruntime.WorkGraphIssueEdgeSkippedEmptyID,
	OutcomeSkippedPRShaped: jobruntime.WorkGraphIssueEdgeSkippedPRShaped,
	OutcomeMalformedPRID:   jobruntime.WorkGraphIssueEdgeMalformedPRID,
}

// ObserveDerivation publishes one build's per-row tally.
//
// A nil observer is refused rather than tolerated. The alternative -- treating
// "no observer" as "do not measure" -- is how a cutover ships with its
// telemetry quietly unwired, which is the failure this whole counter exists to
// make impossible for the derivation itself.
func ObserveDerivation(
	observer jobruntime.WorkGraphIssueEdgesObserver, result DeriveResult, rowsRead int, duration time.Duration,
) error {
	if observer == nil {
		return fmt.Errorf("work graph issue edge observer is required")
	}
	tally := make(map[jobruntime.WorkGraphIssueEdgeOutcome]int, len(result.Counts))
	for outcome, count := range result.Counts {
		label, known := observedOutcome[outcome]
		if !known {
			return fmt.Errorf("derivation outcome %q has no metric label", outcome)
		}
		tally[label] = count
	}
	return observer.ObserveWorkGraphIssueEdges(tally, rowsRead, duration)
}

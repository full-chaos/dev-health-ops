package pgmigrate

import (
	"errors"
	"log/slog"
)

// Outcome labels of the final "migrate outcome" line. Each is a claim about the
// database, so each is decided from state read back after the run, never from the
// error alone.
const (
	OutcomeCommitted  = "committed"   // the database holds a state this run wrote
	OutcomeRolledBack = "rolled_back" // the run tried to write, failed, and the read-back shows what it held before
	OutcomeRefused    = "refused"     // decided before any write: nothing was attempted
	OutcomeNoop       = "noop"        // succeeded with nothing to do (already at the target)
	OutcomeUnknown    = "unknown"     // the read-back did not happen
	OutcomePartial    = "partial"     // upgrade only: a later chain revision failed after earlier ones committed (one transaction each)
)

// runFacts is everything the label depends on.
type runFacts struct {
	Refused    bool  // a refusal decided before any write
	Err        error // the run's error, after any verification
	ReadFailed bool  // the read-back failed
	Changed    bool  // the read-back differs from the state before the run
}

func outcomeOf(f runFacts) string {
	switch {
	case f.ReadFailed:
		return OutcomeUnknown
	case f.Refused:
		return OutcomeRefused
	case f.Err == nil && !f.Changed:
		return OutcomeNoop
	case f.Err == nil:
		return OutcomeCommitted
	case !f.Changed:
		return OutcomeRolledBack
	default:
		return OutcomePartial
	}
}

// isRefusal reports whether err is a refusal decided before any write.
func isRefusal(err error) bool {
	var below BelowHeadError
	var ahead AheadOfBuildError
	var foreign ForeignDatabaseError
	var mismatch SchemaMismatchError
	var down DowngradeRefusal
	return errors.As(err, &below) || errors.As(err, &ahead) || errors.As(err, &foreign) ||
		errors.As(err, &mismatch) || errors.As(err, &down)
}

func logRunOutcome(logger *slog.Logger, direction string, from []string, requested string, observed []string, outcome string) {
	logger.Info("migrate outcome", "direction", direction, "from", from, "requested", requested, "observed", observed, "outcome", outcome)
}

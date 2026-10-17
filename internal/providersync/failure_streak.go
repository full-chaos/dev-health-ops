package providersync

import "time"

// Back-off knobs for a source whose work-items unit keeps failing the same way
// (CHAOS-8802, decision D6060).
const (
	// FailureStreakThreshold is the number of equal failures in a row after
	// which a source is slowed down and shown as needing attention.
	FailureStreakThreshold = 3
	// FailureStreakRetryInterval is how often a slowed-down source is tried.
	FailureStreakRetryInterval = 24 * time.Hour
	// NeedsAttentionReasonPrefix starts the unavailable_reason stamped on the
	// dataset row; the cause class follows it.
	NeedsAttentionReasonPrefix = "needs_attention:"
	// WindowHalvingFloor is the smallest window a halving can reach.
	WindowHalvingFloor = 24 * time.Hour

	causeClassResultTooLarge = "result_too_large"
	causeClassTimeLimit      = "time_limit"
)

// UnitOutcome is one terminal unit of a source and dataset, newest first when
// given to FailureStreakOf.
type UnitOutcome struct {
	Failed     bool
	CauseClass string
	At         time.Time
}

// FailureStreak counts the newest units of one source and dataset that failed
// with the same cause class, with no success between them.
type FailureStreak struct {
	Count  int
	Class  string
	LastAt time.Time
}

// FailureStreakOf reads the streak off terminal units given newest first. A
// success, or a failure of another class, ends it.
func FailureStreakOf(outcomes []UnitOutcome) FailureStreak {
	var streak FailureStreak
	for _, outcome := range outcomes {
		if !outcome.Failed {
			break
		}
		if streak.Count == 0 {
			streak.Class, streak.LastAt = outcome.CauseClass, outcome.At
		} else if outcome.CauseClass != streak.Class {
			break
		}
		streak.Count++
	}
	return streak
}

// ShrinksWindow reports whether a failure of this class is cured by a smaller
// window: the unit result was above the write bound, or the job time limit
// ended the attempt.
func (streak FailureStreak) ShrinksWindow() bool {
	return streak.Count > 0 && (streak.Class == causeClassResultTooLarge || streak.Class == causeClassTimeLimit)
}

// SlowedDownAt reports whether the source must not be planned at now: the
// streak reached the threshold and its newest failure is younger than the
// retry interval.
func (streak FailureStreak) SlowedDownAt(now time.Time) bool {
	return streak.Count >= FailureStreakThreshold && now.Sub(streak.LastAt) < FailureStreakRetryInterval
}

// NeedsAttentionReason is the unavailable_reason stamped on the dataset rows.
func (streak FailureStreak) NeedsAttentionReason() string {
	return NeedsAttentionReasonPrefix + streak.Class
}

// RecentTerminalUnitsSQL lists the newest terminal units of one dataset per
// source of an integration, newest first, at most $4 per source. Arguments:
// org id, integration id, dataset key, per-source limit.
const RecentTerminalUnitsSQL = `
SELECT source_id::text, status = 'failed', COALESCE(result::jsonb ->> 'cause_class', ''), updated_at
FROM (
  SELECT source_id, status, result, updated_at,
         row_number() OVER (PARTITION BY source_id ORDER BY updated_at DESC, id DESC) AS position
  FROM public.sync_run_units
  WHERE org_id = $1 AND integration_id = $2::uuid AND dataset_key = $3
    AND status IN ('success', 'failed')
) AS recent
WHERE position <= $4
ORDER BY source_id, position`

// RecentTerminalUnitsLimit bounds how far back RecentTerminalUnitsSQL looks;
// it is above the threshold so a longer streak still reads as one.
const RecentTerminalUnitsLimit = 8

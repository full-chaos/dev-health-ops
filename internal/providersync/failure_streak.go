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
	// ShrinkCount counts the newest failures in a row whose class is cured by
	// a smaller window (result too large or time limit), in any mix: each of
	// them halves the next window.
	ShrinkCount int
}

// FailureStreakOf reads the streak off terminal units given newest first. A
// success, or a failure of another class, ends it.
func FailureStreakOf(outcomes []UnitOutcome) FailureStreak {
	var streak FailureStreak
	sameClass, shrinking := true, true
	for _, outcome := range outcomes {
		if !outcome.Failed {
			break
		}
		class := outcome.CauseClass
		if !ValidCauseClass(class) {
			class = CauseClassUnclassified
		}
		if streak.Count == 0 {
			streak.Class, streak.LastAt = class, outcome.At
		}
		if sameClass && class == streak.Class {
			streak.Count++
		} else {
			sameClass = false
		}
		if shrinking && shrinksWindow(class) {
			streak.ShrinkCount++
		} else {
			shrinking = false
		}
		if !sameClass && !shrinking {
			break
		}
	}
	return streak
}

func shrinksWindow(class string) bool {
	return class == causeClassResultTooLarge || class == causeClassTimeLimit
}

// ShrinksWindow reports whether the newest failures are cured by a smaller
// window: the unit result was above the write bound, or the job time limit
// ended the attempt.
func (streak FailureStreak) ShrinksWindow() bool { return streak.ShrinkCount > 0 }

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
// org id, integration id, dataset key, per-source limit. Each source reads
// only its own newest rows (a LATERAL top-N), so the cost no longer grows
// with the whole integration history, and the wide result column is read for
// the kept rows only.
const RecentTerminalUnitsSQL = `
SELECT s.id::text, recent.failed, recent.cause_class, recent.updated_at
FROM public.integration_sources AS s
CROSS JOIN LATERAL (
  SELECT u.id, u.status = 'failed' AS failed,
         COALESCE(u.result::jsonb ->> 'cause_class', '') AS cause_class, u.updated_at
  FROM public.sync_run_units AS u
  WHERE u.org_id = $1 AND u.integration_id = $2::uuid AND u.source_id = s.id
    AND u.dataset_key = $3 AND u.status IN ('success', 'failed')
  ORDER BY u.updated_at DESC, u.id DESC
  LIMIT $4
) AS recent
WHERE s.org_id = $1 AND s.integration_id = $2::uuid
ORDER BY s.id, recent.updated_at DESC, recent.id DESC`

// RecentTerminalUnitsLimit bounds how far back RecentTerminalUnitsSQL looks;
// it is above the threshold so a longer streak still reads as one.
const RecentTerminalUnitsLimit = 8

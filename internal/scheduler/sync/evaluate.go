package sync

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Evaluate mirrors the legacy Python scheduler's pure decision gates. It
// deliberately omits organization and feature checks because those require
// business services and are not part of this dormant read-only foundation.
// A true TimingEligible result therefore must never authorize dispatch.
func Evaluate(candidate Candidate, observedAt time.Time) Evaluation {
	result, _ := evaluateContext(context.Background(), candidate, observedAt)
	return result
}

func evaluateContext(ctx context.Context, candidate Candidate, observedAt time.Time) (Evaluation, error) {
	if ctx == nil {
		return Evaluation{}, fmt.Errorf("scheduler evaluation context is required")
	}
	if err := ctx.Err(); err != nil {
		return Evaluation{}, err
	}
	observedAt = observedAt.UTC()
	result := Evaluation{
		ConfigID:           candidate.ConfigID,
		ObservedAt:         observedAt,
		Decision:           DecisionNotDue,
		RunningMarker:      RunningNotSet,
		Timezone:           "UTC",
		EligibilityScope:   ScheduleMarkerEvaluationScope,
		CronGrammarVersion: CronGrammarVersion,
	}
	if !candidate.Active {
		result.Decision = DecisionInactive
		return result, nil
	}
	if candidate.ScheduleCron == "" {
		result.Decision = DecisionManual
		return result, nil
	}

	cronExpr := candidate.ScheduleCron
	timezoneName := candidate.ScheduleTZ
	if candidate.Job != nil {
		if candidate.Job.Status != activeJobStatus {
			result.Decision = DecisionInactiveJob
			return result, nil
		}
		cronExpr = candidate.Job.ScheduleCron
		timezoneName = candidate.Job.Timezone
		result.RunningMarker = runningMarkerState(candidate.Job, observedAt)
		if result.RunningMarker == RunningFresh {
			result.Decision = DecisionFreshRunning
			return result, nil
		}
		// Python's persisted next-run gate precedes cron parsing and due-ness.
		// This preserves that classification even for malformed cron text or
		// a cron occurrence that would independently be not due.
		if candidate.Job.NextRunAt != nil && candidate.Job.NextRunAt.UTC().After(observedAt) {
			result.Decision = DecisionNextRunGate
			return result, nil
		}
	}
	if timezoneName != "" {
		result.Timezone = timezoneName
	}

	// CHAOS-3936: last_sync_at advances only when a sync COMPLETES, so a run
	// that never completes freezes the base and every later tick recomputes the
	// same already-minted instant forever -- the schedule stops making forward
	// progress precisely when a failure means it must. The occurrence ledger
	// records what was already handed off regardless of that run's outcome, so
	// taking the later of the two keeps the base moving across a failed run
	// while leaving a config that has never minted an occurrence (the Python
	// dispatch path, or a brand new config) on the exact Python-parity base.
	base := candidate.CreatedAt
	if candidate.LastSyncAt != nil {
		base = *candidate.LastSyncAt
	}
	if candidate.LastOccurrenceAt != nil && candidate.LastOccurrenceAt.After(base) {
		base = *candidate.LastOccurrenceAt
	}
	result.Base = base.UTC()
	next, fallback, err := nextOccurrenceContext(ctx, cronExpr, result.Base, result.Timezone)
	result.TimezoneFallback = fallback
	if fallback {
		result.Timezone = "UTC"
	}
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return result, err
		}
		if errors.Is(err, ErrUnsupportedCron) {
			result.Decision = DecisionUnsupportedCron
			return result, nil
		}
		result.Decision = DecisionInvalidCron
		return result, nil
	}
	result.NextOccurrence = &next
	result.Due = !next.After(observedAt)
	if !result.Due {
		result.Decision = DecisionNotDue
		return result, nil
	}
	result.TimingEligible = true
	result.Decision = DecisionScheduleDue
	return result, nil
}

func runningMarkerState(job *Job, observedAt time.Time) RunningMarkerState {
	if job == nil || !job.IsRunning {
		return RunningNotSet
	}
	marker := job.LastRunAt
	if marker == nil {
		marker = job.UpdatedAt
	}
	if marker == nil || observedAt.Sub(marker.UTC()) > staleRunningTTL {
		return RunningStale
	}
	return RunningFresh
}

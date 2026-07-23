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

	base := candidate.CreatedAt
	if candidate.LastSyncAt != nil {
		base = *candidate.LastSyncAt
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

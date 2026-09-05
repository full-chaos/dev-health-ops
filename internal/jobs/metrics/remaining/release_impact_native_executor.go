package remaining

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

// Native release_impact executor (CHAOS-4296), replacing the Python bridge.
//
// SCOPE DERIVATION IS IDENTICAL TO PYTHON, deliberately. CHAOS-4258 observes
// that deriving scope from telemetry alone makes "no releases happened" and
// "telemetry ingestion is broken" indistinguishable -- both write zero rows.
// The fix in THIS PR is to make the second case OBSERVABLE without changing
// which rows exist: when a day yields no telemetry scope but deployments did
// occur, the executor reports a degraded signal. It does not invent rows.
//
// Emitting rows for deployment-derived releases was considered and deferred
// (team-lead ruling, PR1): `environment` is not derivable from `deployments`,
// so any such row needs a sentinel, and three LIVE readers would immediately
// start seeing it -- query-api Measures, ff_validation's join-integrity check,
// and the GraphQL flag metrics. That is a row-semantics change owed a design
// with those owners, not a side effect of a port.

var errReleaseImpactUnavailable = errors.New("release_impact native executor unavailable")

// ReleaseImpactObserver reports what the native executor actually did.
type ReleaseImpactObserver interface {
	// ObserveReleaseImpactPartition reports one completed partition: days
	// processed and rows written across the recomputation window.
	ObserveReleaseImpactPartition(days, rowsWritten int) error

	// ObserveReleaseImpactDegradedMissingTelemetry reports the CHAOS-4258
	// state: deployments EXIST for the day but telemetry produced no scope, so
	// the zero-row result is a data gap rather than a quiet day.
	//
	// This is the whole point of the counter. Before it, both cases reported
	// rows_written=0 and the existing zero-row counter could say a partition
	// was empty but never WHY. Measured on the local stack 2026-09-05: 29
	// deployments in the last 7 days against 0 telemetry rows -- every one of
	// those days would fire this and previously fired nothing.
	ObserveReleaseImpactDegradedMissingTelemetry(orgID string, day time.Time, deployments int) error
}

// ReleaseImpactExecutor computes the family natively.
type ReleaseImpactExecutor struct {
	reader   *ReleaseImpactReader
	observer ReleaseImpactObserver
	logger   *slog.Logger
	nowUTC   func() time.Time
}

// NewReleaseImpactExecutor fails closed on a nil connection, matching the
// sibling native executors: a database this code cannot read must refuse the
// kind once and loudly rather than claim partitions and fail each one.
// logger is optional; when set it carries the CHAOS-4258 degraded-signal log
// line (org_id/day/deployments), which is deliberately NOT a metric label --
// see releaseImpactDegradedMissingTelemetry's field comment in telemetry.go.
func NewReleaseImpactExecutor(
	conn driver.Conn, observer ReleaseImpactObserver, logger *slog.Logger,
) (*ReleaseImpactExecutor, error) {
	if conn == nil {
		return nil, errReleaseImpactUnavailable
	}
	return &ReleaseImpactExecutor{
		reader:   NewReleaseImpactReader(conn),
		observer: observer,
		logger:   logger,
		nowUTC:   func() time.Time { return time.Now().UTC() },
	}, nil
}

// DayScope is the per-day scope decision, separated from I/O so the CHAOS-4258
// branch is unit-testable without a database.
type DayScope struct {
	Pairs           []ReleaseEnvPair
	TotalReleases   int
	DegradedNoTelem bool
}

// ClassifyDayScope applies the CHAOS-4258 rule to one day's readings.
//
// The three cases are distinct and only two of them existed before:
//   - pairs present                 -> compute normally (Python's only path)
//   - no pairs, no deployments      -> a genuinely quiet day, zero rows, correct
//   - no pairs, deployments present -> DEGRADED: a real gap, previously
//     indistinguishable from the quiet day
func ClassifyDayScope(pairs []ReleaseEnvPair, totalReleases int) DayScope {
	return DayScope{
		Pairs:           pairs,
		TotalReleases:   totalReleases,
		DegradedNoTelem: len(pairs) == 0 && totalReleases > 0,
	}
}

// RecomputationWindow ports compute_release_impact_daily's day loop
// (release_impact.py:62-84): the last N days ENDING AT day, inclusive.
//
// Returned oldest-first, matching Python's `current = start_day` ascending
// walk, so any per-day side effect (the degraded counter included) fires in the
// same order Python's logging did.
func RecomputationWindow(day time.Time, windowDays int) []time.Time {
	if windowDays < 1 {
		windowDays = 1
	}
	out := make([]time.Time, 0, windowDays)
	start := day.AddDate(0, 0, -(windowDays - 1))
	for d := start; !d.After(day); d = d.AddDate(0, 0, 1) {
		out = append(out, d)
	}
	return out
}

// ScopeForDay reads both scope inputs for one day and classifies them.
func (e *ReleaseImpactExecutor) ScopeForDay(
	ctx context.Context, orgID string, day time.Time,
) (DayScope, error) {
	pairs, err := e.reader.FindReleaseEnvPairs(ctx, orgID, day)
	if err != nil {
		return DayScope{}, err
	}
	// The denominator read is needed for coverage_ratio anyway when pairs
	// exist, and is what makes the degraded case detectable when they do not --
	// so it runs unconditionally rather than only on the empty branch. Python
	// reads it only when pairs exist; issuing it always is the ONLY read-shape
	// difference from Python in this PR, and it cannot change any written
	// value because coverage_ratio uses the same number either way.
	total, err := e.reader.CountTotalReleases(ctx, orgID, day)
	if err != nil {
		return DayScope{}, err
	}
	return ClassifyDayScope(pairs, total), nil
}

// ReportDegraded emits the CHAOS-4258 signal for a day: the counter (via the
// observer) and a structured log carrying the org/day/deployments detail the
// counter deliberately does not (see releaseImpactDegradedMissingTelemetry's
// field comment in telemetry.go).
func (e *ReleaseImpactExecutor) ReportDegraded(orgID string, day time.Time, scope DayScope) error {
	if !scope.DegradedNoTelem {
		return nil
	}
	if e.logger != nil {
		e.logger.Warn(
			"release_impact: deployments exist but telemetry produced no scope (CHAOS-4258)",
			"org_id", orgID, "day", day.Format("2006-01-02"), "deployments", scope.TotalReleases,
		)
	}
	if e.observer == nil {
		return nil
	}
	return e.observer.ObserveReleaseImpactDegradedMissingTelemetry(orgID, day, scope.TotalReleases)
}

// nowOrRefuse stamps computed_at ONCE per partition, before the day loop --
// mirroring DORA's nowOrRefuse (dora_native.go / executor_clock.go): every row
// a backfill writes shares one timestamp, matching compute_release_impact_daily
// taking datetime.now(UTC) once at release_impact.py:61, before its day loop.
func (e *ReleaseImpactExecutor) nowOrRefuse() (time.Time, error) {
	if e.nowUTC == nil {
		return time.Time{}, errReleaseImpactUnavailable
	}
	return e.nowUTC(), nil
}

// ComputePartition runs release_impact for one partition, satisfying the same
// one-method CompatibilityExecutor contract DORAExecutor/CapacityExecutor do.
func (e *ReleaseImpactExecutor) ComputePartition(
	ctx context.Context, run Run, partition Partition,
) (CompatibilityOutcome, error) {
	if e == nil || e.reader == nil {
		return CompatibilityOutcome{}, errReleaseImpactUnavailable
	}
	if strings.TrimSpace(run.OrganizationID) == "" {
		return CompatibilityOutcome{}, jobruntime.WithSafeCause(fmt.Errorf(
			"%w: partition %s has no organization", ErrInvalidState, partition.ID))
	}
	var scope releaseImpactScope
	if err := json.Unmarshal(partition.Scope, &scope); err != nil {
		return CompatibilityOutcome{}, jobruntime.WithSafeCause(fmt.Errorf(
			"%w: partition %s scope: %v", ErrInvalidState, partition.ID, err))
	}
	triggerDay, err := time.Parse("2006-01-02", scope.Day)
	if err != nil {
		return CompatibilityOutcome{}, jobruntime.WithSafeCause(fmt.Errorf(
			"%w: partition %s day %q", ErrInvalidState, partition.ID, scope.Day))
	}
	if scope.BackfillDays < 1 || scope.RecomputationWindowDays < 1 {
		return CompatibilityOutcome{}, jobruntime.WithSafeCause(fmt.Errorf(
			"%w: partition %s backfill_days/recomputation_window_days", ErrInvalidState, partition.ID))
	}

	computedAt, err := e.nowOrRefuse()
	if err != nil {
		return CompatibilityOutcome{}, err
	}

	// One trigger day per scope.Day today (BackfillDays > 1 is a manual-backfill
	// affordance the producer does not use yet); dayRange keeps this correct if
	// it ever does.
	triggerDays := dayRange(triggerDay, scope.BackfillDays)

	var rowsWritten int
	for _, trigger := range triggerDays {
		// compute_release_impact_daily (release_impact.py:47-85): recomputes
		// the last recomputation_window_days days ENDING at trigger, oldest
		// first, matching Python's ascending current += timedelta(days=1) walk.
		for _, current := range RecomputationWindow(trigger, scope.RecomputationWindowDays) {
			written, err := e.computeOneDay(ctx, run.OrganizationID, current, computedAt)
			if err != nil {
				return CompatibilityOutcome{}, err
			}
			rowsWritten += written
		}
	}

	if e.observer != nil {
		// Telemetry failure never fails the partition: the work is already
		// durably written, and losing a counter must not cause a retry that
		// writes it a second time (same discipline DORAExecutor uses).
		_ = e.observer.ObserveReleaseImpactPartition(len(triggerDays), rowsWritten)
	}
	return CompatibilityOutcome{RowsWritten: &rowsWritten}, nil
}

// computeOneDay ports _compute_day (release_impact.py:88-122): the scope
// decision, the CHAOS-4258 degraded signal, and -- when pairs exist -- every
// per-pair record, written in one batch.
func (e *ReleaseImpactExecutor) computeOneDay(
	ctx context.Context, orgID string, day time.Time, computedAt time.Time,
) (int, error) {
	scope, err := e.ScopeForDay(ctx, orgID, day)
	if err != nil {
		return 0, err
	}
	// Telemetry failure never fails the partition -- the same discipline
	// DORAExecutor.ComputePartition uses for its observer call.
	_ = e.ReportDegraded(orgID, day, scope)
	if len(scope.Pairs) == 0 {
		// Both the quiet day and the degraded day return zero rows here,
		// matching _compute_day's `if not release_env_pairs: return []`
		// verbatim -- PR1 does not invent rows for the degraded case (see
		// design.md §3, option (i)).
		return 0, nil
	}

	// releasesWithTelemetry = len({r for r, _e in release_env_pairs}) --
	// release_impact.py:101 dedupes on release_ref ALONE, not the
	// (release_ref, environment) pair, so two environments of the same
	// release count once in the coverage_ratio numerator.
	distinctReleases := make(map[string]struct{}, len(scope.Pairs))
	for _, pair := range scope.Pairs {
		distinctReleases[pair.ReleaseRef] = struct{}{}
	}
	coverageRatio := CoverageRatio(len(distinctReleases), scope.TotalReleases)

	rows := make([]releaseImpactRow, 0, len(scope.Pairs))
	for _, pair := range scope.Pairs {
		row, err := e.computeReleaseEnv(ctx, orgID, day, pair, coverageRatio, computedAt)
		if err != nil {
			return 0, err
		}
		rows = append(rows, row)
	}
	return e.reader.writeReleaseImpactRows(ctx, rows)
}

// computeReleaseEnv ports _compute_release_env (release_impact.py:458-584) for
// one (release_ref, environment) pair.
func (e *ReleaseImpactExecutor) computeReleaseEnv(
	ctx context.Context, orgID string, day time.Time, pair ReleaseEnvPair,
	coverageRatio float64, computedAt time.Time,
) (releaseImpactRow, error) {
	deployTS, err := e.reader.DeployTimestamp(ctx, orgID, pair.ReleaseRef, pair.Environment)
	if err != nil {
		return releaseImpactRow{}, err
	}
	repoID, err := e.reader.RepoIDForRelease(ctx, orgID, pair.ReleaseRef, pair.Environment)
	if err != nil {
		return releaseImpactRow{}, err
	}

	var (
		frictionDelta, postFrictionRate *float64
		errorDelta, postErrorRate       *float64
		frictionSessions, errorSessions int
		timeToFirstIssue                *float64
		concurrent                      int
		firstSpike                      *time.Time
	)

	if deployTS != nil {
		frictionDelta, postFrictionRate, err = e.computeDeltaFor(
			ctx, orgID, pair, *deployTS, "friction.%", MinSessionsFriction)
		if err != nil {
			return releaseImpactRow{}, err
		}
		errorDelta, postErrorRate, err = e.computeDeltaFor(
			ctx, orgID, pair, *deployTS, "error.%", MinEventsError)
		if err != nil {
			return releaseImpactRow{}, err
		}

		postStart := *deployTS
		postEnd := deployTS.Add(PostWindowHours * time.Hour)
		_, frictionSessions, _, err = e.reader.SignalRateRaw(
			ctx, orgID, pair.ReleaseRef, pair.Environment, "friction.%", postStart, postEnd)
		if err != nil {
			return releaseImpactRow{}, err
		}
		_, errorSessions, _, err = e.reader.SignalRateRaw(
			ctx, orgID, pair.ReleaseRef, pair.Environment, "error.%", postStart, postEnd)
		if err != nil {
			return releaseImpactRow{}, err
		}

		spikeEnd := deployTS.Add(SpikeDetectionHours * time.Hour)
		firstSpike, err = e.reader.FirstFrictionSpike(ctx, orgID, pair.ReleaseRef, pair.Environment, *deployTS, spikeEnd)
		if err != nil {
			return releaseImpactRow{}, err
		}
		if firstSpike != nil {
			hours := firstSpike.Sub(*deployTS).Hours()
			if hours >= 0 {
				timeToFirstIssue = &hours
			}
		}

		windowStart := deployTS.Add(-24 * time.Hour)
		windowEnd := deployTS.Add(24 * time.Hour)
		concurrent, err = e.reader.ConcurrentDeployCount(
			ctx, orgID, pair.ReleaseRef, pair.Environment, windowStart, windowEnd)
		if err != nil {
			return releaseImpactRow{}, err
		}
	}

	totalSessions := frictionSessions + errorSessions

	bucketHours, err := e.reader.BucketHours(ctx, orgID, pair.ReleaseRef, pair.Environment, day)
	if err != nil {
		return releaseImpactRow{}, err
	}
	completeness := DataCompleteness(bucketHours)
	confidence := ComputeConfidence(coverageRatio, totalSessions, concurrent, MinSessionsFriction)
	missing := MissingRequiredFields(frictionDelta, postFrictionRate, errorDelta, postErrorRate)

	return releaseImpactRow{
		OrgID:                 orgID,
		Day:                   day,
		ReleaseRef:            pair.ReleaseRef,
		Environment:           pair.Environment,
		RepoID:                repoID,
		FrictionDelta:         frictionDelta,
		PostFrictionRate:      postFrictionRate,
		ErrorDelta:            errorDelta,
		PostErrorRate:         postErrorRate,
		TimeToFirstIssue:      timeToFirstIssue,
		Confidence:            confidence,
		CoverageRatioTop:      coverageRatio,
		MissingRequiredFields: missing,
		DataCompleteness:      completeness,
		ConcurrentDeployCount: uint32(concurrent),
		ComputedAt:            computedAt,
	}, nil
}

// computeDeltaFor ports _compute_delta's I/O half (release_impact.py:254-301):
// reads the baseline and post windows, then defers to ComputeDelta for the
// null-semantics arithmetic.
func (e *ReleaseImpactExecutor) computeDeltaFor(
	ctx context.Context, orgID string, pair ReleaseEnvPair, deployTS time.Time,
	signalPattern string, minSessions int,
) (*float64, *float64, error) {
	baselineStart := deployTS.Add(-BaselineWindowDays * 24 * time.Hour)
	baselineEnd := deployTS
	postStart := deployTS
	postEnd := deployTS.Add(PostWindowHours * time.Hour)

	preSignals, preSessions, preHadRows, err := e.reader.SignalRateRaw(
		ctx, orgID, pair.ReleaseRef, pair.Environment, signalPattern, baselineStart, baselineEnd)
	if err != nil {
		return nil, nil, err
	}
	postSignals, postSessions, postHadRows, err := e.reader.SignalRateRaw(
		ctx, orgID, pair.ReleaseRef, pair.Environment, signalPattern, postStart, postEnd)
	if err != nil {
		return nil, nil, err
	}

	pre := NewSignalRate(preHadRows, preSignals, preSessions)
	post := NewSignalRate(postHadRows, postSignals, postSessions)
	delta, postRate := ComputeDelta(pre, post, minSessions)
	return delta, postRate, nil
}

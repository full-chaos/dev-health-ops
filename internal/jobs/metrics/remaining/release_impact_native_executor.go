package remaining

import (
	"context"
	"errors"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
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

const releaseImpactRecomputationWindowDays = 7

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
	nowUTC   func() time.Time
}

// NewReleaseImpactExecutor fails closed on a nil connection, matching the
// sibling native executors: a database this code cannot read must refuse the
// kind once and loudly rather than claim partitions and fail each one.
func NewReleaseImpactExecutor(
	conn driver.Conn, observer ReleaseImpactObserver,
) (*ReleaseImpactExecutor, error) {
	if conn == nil {
		return nil, errReleaseImpactUnavailable
	}
	return &ReleaseImpactExecutor{
		reader:   NewReleaseImpactReader(conn),
		observer: observer,
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

// ReportDegraded emits the CHAOS-4258 signal for a day, if the observer wants it.
func (e *ReleaseImpactExecutor) ReportDegraded(orgID string, day time.Time, scope DayScope) error {
	if !scope.DegradedNoTelem || e.observer == nil {
		return nil
	}
	return e.observer.ObserveReleaseImpactDegradedMissingTelemetry(orgID, day, scope.TotalReleases)
}

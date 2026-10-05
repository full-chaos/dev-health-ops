package daily

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jackc/pgx/v5"
)

// CHAOS-8710: the ClickHouse record of the daily metrics run state.
//
// repo_metrics_daily has a row only for a day with activity, so a day with no
// activity and a day that was never computed both read as "no row". The run
// state lives in Postgres (daily_metrics_runs). This file appends it to the
// ClickHouse table daily_metrics_run_marker (migration 105), where a reader
// that only has ClickHouse can learn that a day's run succeeded.
//
// Reader rule (the same text sits in the migration and the metrics docs):
// per (org_id, target_day) take argMax(state, version) over all generations.
// 'succeeded' certifies the day. No row or 'reopened' means unknown, never
// zero. Writers err toward unknown: a failed 'succeeded' append leaves the day
// unknown, and a failed 'reopened' append refuses the reopen.

// RunMarkerState is the state an appended marker row records.
type RunMarkerState string

const (
	// RunMarkerSucceeded: the org-day run reached status and
	// finalization_status 'succeeded'.
	RunMarkerSucceeded RunMarkerState = "succeeded"
	// RunMarkerReopened: the org-day run is no longer succeeded (a redrive or
	// partition recompute reopened it, or its latest run is not succeeded).
	RunMarkerReopened RunMarkerState = "reopened"
)

// RunMarker is one append-only row of daily_metrics_run_marker.
type RunMarker struct {
	OrganizationID string
	TargetDay      time.Time
	Generation     string
	State          RunMarkerState
	FinalizedAt    time.Time
	// Version orders the rows of one (org, day): milliseconds since the epoch.
	Version uint64
}

// RunMarkerWriter appends one marker row.
type RunMarkerWriter interface {
	AppendRunMarker(context.Context, RunMarker) error
}

// RunMarkerReader reads the current marker state of every day in a range for
// one organization. A day with no marker row is absent from the result.
type RunMarkerReader interface {
	RunMarkerStates(ctx context.Context, organizationID string, from, to time.Time) (map[string]RunMarkerState, error)
}

// RunMarkerObserver counts marker appends. Nil is a silent no-op.
type RunMarkerObserver interface {
	ObserveDailyMetricsRunMarker(state, outcome string) error
}

type runMarkerConn interface {
	PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error)
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
}

// ClickHouseRunMarkerStore writes and reads daily_metrics_run_marker.
type ClickHouseRunMarkerStore struct{ conn runMarkerConn }

func NewClickHouseRunMarkerStore(conn runMarkerConn) (*ClickHouseRunMarkerStore, error) {
	if conn == nil {
		return nil, ErrUnavailable
	}
	return &ClickHouseRunMarkerStore{conn: conn}, nil
}

func (store *ClickHouseRunMarkerStore) AppendRunMarker(ctx context.Context, marker RunMarker) error {
	if store == nil || store.conn == nil || !validUUID(marker.OrganizationID) ||
		(marker.State != RunMarkerSucceeded && marker.State != RunMarkerReopened) {
		return ErrInvalidState
	}
	batch, err := store.conn.PrepareBatch(ctx, `INSERT INTO daily_metrics_run_marker (
		org_id, target_day, generation, state, finalized_at, version
	)`)
	if err != nil {
		return fmt.Errorf("prepare daily_metrics_run_marker batch: %w", err)
	}
	if err := batch.Append(
		marker.OrganizationID, marker.TargetDay.UTC(), marker.Generation, string(marker.State),
		marker.FinalizedAt.UTC(), marker.Version,
	); err != nil {
		_ = batch.Abort()
		return fmt.Errorf("append daily_metrics_run_marker row: %w", err)
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send daily_metrics_run_marker batch: %w", err)
	}
	return nil
}

// RunMarkerStatesSQL is the documented reader: the latest state per org-day.
const RunMarkerStatesSQL = `
SELECT toString(target_day), argMax(state, version)
FROM daily_metrics_run_marker
WHERE org_id = ? AND target_day BETWEEN ? AND ?
GROUP BY org_id, target_day`

func (store *ClickHouseRunMarkerStore) RunMarkerStates(
	ctx context.Context, organizationID string, from, to time.Time,
) (map[string]RunMarkerState, error) {
	if store == nil || store.conn == nil || !validUUID(organizationID) {
		return nil, ErrInvalidState
	}
	rows, err := store.conn.Query(ctx, RunMarkerStatesSQL, organizationID, from.UTC(), to.UTC())
	if err != nil {
		return nil, fmt.Errorf("read daily_metrics_run_marker: %w", err)
	}
	defer rows.Close()
	states := make(map[string]RunMarkerState)
	for rows.Next() {
		var day, state string
		if err := rows.Scan(&day, &state); err != nil {
			return nil, fmt.Errorf("scan daily_metrics_run_marker: %w", err)
		}
		states[day] = RunMarkerState(state)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read daily_metrics_run_marker: %w", err)
	}
	return states, nil
}

var (
	_ RunMarkerWriter = (*ClickHouseRunMarkerStore)(nil)
	_ RunMarkerReader = (*ClickHouseRunMarkerStore)(nil)
)

// SetRunMarkerWriter wires the marker writer. Without one the store appends
// nothing and reopen paths do not refuse: production wiring must set it.
func (store *PostgresStore) SetRunMarkerWriter(writer RunMarkerWriter) {
	if store == nil {
		return
	}
	store.markerWriter = writer
}

// SetRunMarkerObserver wires the optional append counter.
func (store *PostgresStore) SetRunMarkerObserver(observer RunMarkerObserver) {
	if store == nil {
		return
	}
	store.markerObserver = observer
}

func (store *PostgresStore) appendMarker(ctx context.Context, orgID, day, generation string, state RunMarkerState) error {
	if store.markerWriter == nil {
		return nil
	}
	targetDay, err := time.Parse(dailyTargetDayLayout, day)
	if err != nil {
		return ErrInvalidState
	}
	now := store.now().UTC()
	// The write happens after a Postgres commit, so a cancelled request
	// context must not be what loses it.
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	err = store.markerWriter.AppendRunMarker(writeCtx, RunMarker{
		OrganizationID: orgID, TargetDay: targetDay, Generation: generation,
		State: state, FinalizedAt: now, Version: markerVersion(now),
	})
	outcome := "ok"
	if err != nil {
		outcome = "failed"
		slog.Error("daily metrics run marker append failed",
			"error", err, "organization_id", orgID, "target_day", day,
			"generation", generation, "state", string(state))
	}
	if store.markerObserver != nil {
		_ = store.markerObserver.ObserveDailyMetricsRunMarker(string(state), outcome)
	}
	return err
}

func markerVersion(now time.Time) uint64 {
	millis := now.UnixMilli()
	if millis < 0 {
		return 0
	}
	return uint64(millis)
}

// markSucceeded appends 'succeeded' after CompleteFinalize committed. It never
// fails the run: on error the day stays unknown (logged and counted) and the
// backfill restores it.
func (store *PostgresStore) markSucceeded(ctx context.Context, run Run) {
	_ = store.appendMarker(ctx, run.OrganizationID, run.TargetDay.UTC().Format(dailyTargetDayLayout),
		run.Generation, RunMarkerSucceeded)
}

// markReopened appends 'reopened' BEFORE a reopen commits, so a failed append
// refuses the reopen (the caller returns ErrUnavailable and rolls back). The
// reverse failure, a committed marker with a reopen that then fails to commit,
// only hides a succeeded day until the next backfill.
func (store *PostgresStore) markReopened(ctx context.Context, orgID, day, generation string) error {
	if err := store.appendMarker(ctx, orgID, day, generation, RunMarkerReopened); err != nil {
		return ErrUnavailable
	}
	return nil
}

// RunMarkerBackfillOutcome reports one backfill pass.
type RunMarkerBackfillOutcome struct {
	DaysExamined int `json:"days_examined"`
	Appended     int `json:"appended"`
	Succeeded    int `json:"appended_succeeded"`
	Reopened     int `json:"appended_reopened"`
	Unchanged    int `json:"unchanged"`
}

// BackfillRunMarkers makes ClickHouse agree with Postgres for every org-day in
// [from, to] that has a run. For each day the latest run by created_at decides:
// status and finalization_status both 'succeeded' means 'succeeded', anything
// else means 'reopened' (not certified). A row is appended only when the
// current ClickHouse state differs, so a second pass appends nothing. A day
// with no run gets no row. no_repositories runs are not marked: the contract
// is status 'succeeded', and an org with no repositories has no repository
// rows to explain.
func (store *PostgresStore) BackfillRunMarkers(
	ctx context.Context, reader RunMarkerReader, orgID string, from, to time.Time, dryRun bool,
) (RunMarkerBackfillOutcome, error) {
	var outcome RunMarkerBackfillOutcome
	if !store.valid() || store.markerWriter == nil || reader == nil || !validUUID(orgID) || to.Before(from) {
		return outcome, ErrUnavailable
	}
	from = from.UTC().Truncate(24 * time.Hour)
	to = to.UTC().Truncate(24 * time.Hour)
	rows, err := store.pool.Query(ctx, `
SELECT DISTINCT ON (run.target_day) run.target_day::text, run.generation,
       (run.status = 'succeeded' AND run.finalization_status = 'succeeded')
FROM public.daily_metrics_runs AS run
WHERE run.org_id = $1::uuid AND run.target_day BETWEEN $2::date AND $3::date
ORDER BY run.target_day, run.created_at DESC, run.id`, orgID, from, to)
	if err != nil {
		return outcome, ErrUnavailable
	}
	type dayState struct {
		generation string
		succeeded  bool
	}
	days := map[string]dayState{}
	for rows.Next() {
		var day, generation string
		var succeeded bool
		if err := rows.Scan(&day, &generation, &succeeded); err != nil {
			rows.Close()
			return outcome, ErrUnavailable
		}
		days[day] = dayState{generation: generation, succeeded: succeeded}
	}
	rowsErr := rows.Err()
	rows.Close()
	if rowsErr != nil && rowsErr != pgx.ErrNoRows {
		return outcome, ErrUnavailable
	}
	current, err := reader.RunMarkerStates(ctx, orgID, from, to)
	if err != nil {
		return outcome, ErrUnavailable
	}
	for day := from; !day.After(to); day = day.AddDate(0, 0, 1) {
		key := day.Format(dailyTargetDayLayout)
		want, ok := days[key]
		if !ok {
			continue
		}
		outcome.DaysExamined++
		desired := RunMarkerReopened
		if want.succeeded {
			desired = RunMarkerSucceeded
		}
		have, hasRow := current[key]
		// A day with no marker row and a run that is not succeeded is already
		// unknown: appending 'reopened' would add nothing.
		if (hasRow && have == desired) || (!hasRow && desired == RunMarkerReopened) {
			outcome.Unchanged++
			continue
		}
		if dryRun {
			outcome.Appended++
			continue
		}
		if err := store.appendMarker(ctx, orgID, key, want.generation, desired); err != nil {
			return outcome, ErrUnavailable
		}
		outcome.Appended++
		if desired == RunMarkerSucceeded {
			outcome.Succeeded++
		} else {
			outcome.Reopened++
		}
	}
	return outcome, nil
}

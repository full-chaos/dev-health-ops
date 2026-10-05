package daily

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
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
// per (org_id, target_day) take the row with the greatest version over all
// generations, 'reopened' winning a tie. 'succeeded' certifies the day. No row
// or 'reopened' means unknown, never zero. Writers err toward unknown: a failed
// 'succeeded' append leaves the day unknown, and a failed 'reopened' append
// refuses the reopen (or the dispatch claim).
//
// The invariant. The marker may say 'succeeded' for (org_id, target_day) only
// when, at the moment of the append, COMMITTED Postgres says the latest
// full-org run of that day is succeeded. Everything below serves it:
//
//   - One lock per (org, day): pg_advisory_xact_lock, taken by every marker
//     writer (lockMarkerDay).
//   - Un-certify paths (the dispatch claim, the finalize-redrive and
//     partition-recompute resets) take the lock in their own Postgres
//     transaction, append 'reopened' BEFORE the commit, and roll back when the
//     append fails.
//   - Certify path: ONE function, markerSync. It takes the lock, reads
//     committed Postgres and appends 'succeeded' only if the day's latest
//     full-org run is succeeded. CompleteFinalize calls it after its commit and
//     the backfill is the same function per day.
//   - Full-org is the run's recorded scope (daily_metrics_runs.full_org, set at
//     creation: no explicit repository list), never the generation text.
//   - One clock: a version is the Postgres clock read in the transaction that
//     decides the append, never the writing host's clock.

// pgClockMillis is the RETURNING expression that reads the Postgres clock in
// milliseconds since the epoch.
const pgClockMillis = `(extract(epoch from clock_timestamp()) * 1000)::bigint`

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

// RunMarkerObservation is the winning row of one org-day.
type RunMarkerObservation struct {
	State   RunMarkerState
	Version uint64
}

// RunMarkerReader reads the current marker of every day in a range for one
// organization. A day with no marker row is absent from the result.
type RunMarkerReader interface {
	RunMarkerStates(ctx context.Context, organizationID string, from, to time.Time) (map[string]RunMarkerObservation, error)
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

// RunMarkerStatesSQL is the documented reader: the winning row per org-day.
// The greatest version wins and 'reopened' wins a tie, so two events in the
// same millisecond never read as succeeded.
const RunMarkerStatesSQL = `
SELECT toString(target_day), argMax(state, (version, state = 'reopened')), max(version)
FROM daily_metrics_run_marker
WHERE org_id = ? AND target_day BETWEEN ? AND ?
GROUP BY org_id, target_day`

func (store *ClickHouseRunMarkerStore) RunMarkerStates(
	ctx context.Context, organizationID string, from, to time.Time,
) (map[string]RunMarkerObservation, error) {
	if store == nil || store.conn == nil || !validUUID(organizationID) {
		return nil, ErrInvalidState
	}
	rows, err := store.conn.Query(ctx, RunMarkerStatesSQL, organizationID, from.UTC(), to.UTC())
	if err != nil {
		return nil, fmt.Errorf("read daily_metrics_run_marker: %w", err)
	}
	defer rows.Close()
	states := make(map[string]RunMarkerObservation)
	for rows.Next() {
		var day, state string
		var version uint64
		if err := rows.Scan(&day, &state, &version); err != nil {
			return nil, fmt.Errorf("scan daily_metrics_run_marker: %w", err)
		}
		states[day] = RunMarkerObservation{State: RunMarkerState(state), Version: version}
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

// SetRunMarkerReader wires the marker reader markerSync compares against.
func (store *PostgresStore) SetRunMarkerReader(reader RunMarkerReader) {
	if store == nil {
		return
	}
	store.markerReader = reader
}

// markerEnabled reports whether the marker is wired (writer and reader).
func (store *PostgresStore) markerEnabled() bool {
	return store.markerWriter != nil && store.markerReader != nil
}

// SetRunMarkerWriter wires the marker writer. Without a writer and a reader the
// store appends nothing and reopen paths do not refuse: production wiring must
// set both.
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

func (store *PostgresStore) appendMarker(
	ctx context.Context, orgID, day, generation string, state RunMarkerState, versionMs int64,
) error {
	if store.markerWriter == nil {
		return nil
	}
	targetDay, err := time.Parse(dailyTargetDayLayout, day)
	if err != nil || versionMs < 0 {
		return ErrInvalidState
	}
	// The write happens after a Postgres commit on some paths, so a cancelled
	// request context must not be what loses it.
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	err = store.markerWriter.AppendRunMarker(writeCtx, RunMarker{
		OrganizationID: orgID, TargetDay: targetDay, Generation: generation,
		State: state, FinalizedAt: time.UnixMilli(versionMs).UTC(), Version: uint64(versionMs),
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

// markReopened appends 'reopened' inside the reopen transaction, after the
// reset statement and before the commit, so a failed append refuses the reopen
// (the caller returns the error and the transaction rolls back). The caller
// already holds the (org, day) lock. A run that is not full-org never certified
// the day, so reopening it writes nothing. The reverse failure, a written marker
// with a reopen that then fails to commit, only hides a succeeded day until
// markerSync heals it.
func (store *PostgresStore) markReopened(ctx context.Context, run Run, day string, versionMs int64) error {
	if !run.FullOrg {
		return nil
	}
	if err := store.appendMarker(ctx, run.OrganizationID, day, run.Generation, RunMarkerReopened, versionMs); err != nil {
		return ErrUnavailable
	}
	return nil
}

// markInFlight appends 'reopened' when a full-org run is claimed for dispatch:
// while a run computes the day it is not certified, even if an earlier run
// succeeded. It runs inside the claim's transaction, under the (org, day) lock,
// before the commit: a failed append rolls the claim back (the job retries).
func (store *PostgresStore) markInFlight(ctx context.Context, run Run, claimedAtMs int64) error {
	if !run.FullOrg {
		return nil
	}
	if err := store.appendMarker(ctx, run.OrganizationID, run.TargetDay.UTC().Format(dailyTargetDayLayout),
		run.Generation, RunMarkerReopened, claimedAtMs); err != nil {
		return ErrUnavailable
	}
	return nil
}

// lockMarkerDay takes the transaction-scoped lock that serializes every marker
// writer of one (org, day). A hash collision only serializes two days.
func lockMarkerDay(ctx context.Context, tx pgx.Tx, orgID, day string) error {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
		"daily_run_marker:"+orgID+":"+day); err != nil {
		return ErrUnavailable
	}
	return nil
}

// lockMarkerDayForRun takes the (org, day) marker lock for the run's day BEFORE
// the caller takes the run's row lock. Every path takes the advisory lock first
// and a run row lock second (the dispatch claim does the same: advisory, then
// its UPDATE), so no two paths can wait on each other in opposite orders. The
// org and day are read without a lock; they never change for a run. A missing
// run takes no lock: the caller's own locking read reports it.
func (store *PostgresStore) lockMarkerDayForRun(ctx context.Context, tx pgx.Tx, runID string) error {
	if !store.markerEnabled() {
		return nil
	}
	var orgID, day string
	err := tx.QueryRow(ctx, `SELECT org_id::text, target_day::text FROM public.daily_metrics_runs WHERE id = $1::uuid`, runID).
		Scan(&orgID, &day)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return ErrUnavailable
	}
	return lockMarkerDay(ctx, tx, orgID, day)
}

// RunMarkerBackfillOutcome reports one backfill pass.
type RunMarkerBackfillOutcome struct {
	DaysExamined int `json:"days_examined"`
	Appended     int `json:"appended"`
	Succeeded    int `json:"appended_succeeded"`
	Reopened     int `json:"appended_reopened"`
	Unchanged    int `json:"unchanged"`
}

// markerSyncResult is what one markerSync pass did.
type markerSyncResult int

const (
	// markerSyncNoRun: the day has no full-org run. No row is written.
	markerSyncNoRun markerSyncResult = iota
	// markerSyncUnchanged: ClickHouse already agrees with Postgres.
	markerSyncUnchanged
	markerSyncSucceeded
	markerSyncReopened
)

// markerSync is the one certify path. In its own transaction it takes the
// (org, day) lock FIRST, then reads ClickHouse, then reads committed Postgres,
// then appends, then commits. The day's latest full-org run (by created_at)
// decides: status and finalization_status both 'succeeded' means 'succeeded',
// anything else 'reopened'. A row is appended only when ClickHouse differs, so
// a second pass appends nothing; a day with no full-org run, or an unfinished
// one with no marker, gets no row. The version is the Postgres clock read under
// the lock, raised to one above the row it observed, so a rolled-back reopen
// heals and a later reopen (which reads the clock after taking the lock)
// outranks it. dryRun reports without appending.
func (store *PostgresStore) markerSync(
	ctx context.Context, orgID string, day time.Time, dryRun bool,
) (markerSyncResult, error) {
	if !store.valid() || !store.markerEnabled() || !validUUID(orgID) {
		return markerSyncNoRun, ErrUnavailable
	}
	key := day.UTC().Format(dailyTargetDayLayout)
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return markerSyncNoRun, ErrUnavailable
	}
	defer func() {
		rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()
	if err := lockMarkerDay(ctx, tx, orgID, key); err != nil {
		return markerSyncNoRun, err
	}
	current, err := store.markerReader.RunMarkerStates(ctx, orgID, day.UTC(), day.UTC())
	if err != nil {
		return markerSyncNoRun, ErrUnavailable
	}
	if store.syncHook != nil {
		store.syncHook("marker_read")
	}
	var generation string
	var succeeded bool
	var clockMs int64
	err = tx.QueryRow(ctx, `
SELECT run.generation,
       (run.status = 'succeeded' AND run.finalization_status = 'succeeded'),
       `+pgClockMillis+`
FROM public.daily_metrics_runs AS run
WHERE run.org_id = $1::uuid AND run.target_day = $2::date AND run.full_org
ORDER BY run.created_at DESC, run.id
LIMIT 1`, orgID, key).Scan(&generation, &succeeded, &clockMs)
	if errors.Is(err, pgx.ErrNoRows) {
		return markerSyncNoRun, nil
	}
	if err != nil {
		return markerSyncNoRun, ErrUnavailable
	}
	if store.syncHook != nil {
		store.syncHook("postgres_read")
	}
	desired := RunMarkerReopened
	if succeeded {
		desired = RunMarkerSucceeded
	}
	have, hasRow := current[key]
	// A day with no marker row and a run that is not succeeded is already
	// unknown: appending 'reopened' would add nothing.
	if (hasRow && have.State == desired) || (!hasRow && desired == RunMarkerReopened) {
		return markerSyncUnchanged, nil
	}
	result := markerSyncReopened
	if desired == RunMarkerSucceeded {
		result = markerSyncSucceeded
	}
	if dryRun {
		return result, nil
	}
	version := clockMs
	if hasRow && int64(have.Version)+1 > version {
		version = int64(have.Version) + 1
	}
	if err := store.appendMarker(ctx, orgID, key, generation, desired, version); err != nil {
		return markerSyncNoRun, ErrUnavailable
	}
	if err := tx.Commit(ctx); err != nil {
		return markerSyncNoRun, ErrUnavailable
	}
	return result, nil
}

// markerSyncAfterCommit is CompleteFinalize's certify call. It never fails the
// run: an error leaves the day unknown (the append failure is already logged
// and counted by appendMarker) and the backfill restores it.
func (store *PostgresStore) markerSyncAfterCommit(ctx context.Context, run Run) {
	if !store.markerEnabled() {
		return
	}
	// The commit is done, so a cancelled request context must not be what
	// loses the sync.
	syncCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	if _, err := store.markerSync(syncCtx, run.OrganizationID, run.TargetDay, false); err != nil {
		slog.Error("daily metrics run marker sync failed",
			"error", err, "organization_id", run.OrganizationID,
			"target_day", run.TargetDay.UTC().Format(dailyTargetDayLayout))
	}
}

// BackfillRunMarkers runs markerSync for every day in [from, to]. It is the
// same function CompleteFinalize calls, so a backfill and a live completion can
// never disagree about what certifies a day.
func (store *PostgresStore) BackfillRunMarkers(
	ctx context.Context, orgID string, from, to time.Time, dryRun bool,
) (RunMarkerBackfillOutcome, error) {
	var outcome RunMarkerBackfillOutcome
	if !store.valid() || !store.markerEnabled() || !validUUID(orgID) || to.Before(from) {
		return outcome, ErrUnavailable
	}
	from = from.UTC().Truncate(24 * time.Hour)
	to = to.UTC().Truncate(24 * time.Hour)
	for day := from; !day.After(to); day = day.AddDate(0, 0, 1) {
		result, err := store.markerSync(ctx, orgID, day, dryRun)
		if err != nil {
			return outcome, err
		}
		switch result {
		case markerSyncNoRun:
		case markerSyncUnchanged:
			outcome.DaysExamined++
			outcome.Unchanged++
		case markerSyncSucceeded:
			outcome.DaysExamined++
			outcome.Appended++
			outcome.Succeeded++
		case markerSyncReopened:
			outcome.DaysExamined++
			outcome.Appended++
			outcome.Reopened++
		}
	}
	return outcome, nil
}

package daily

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
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
// Two rules keep a marker true:
//
//   - Only a full-org run writes markers (isFullOrgGeneration). A run started
//     with an explicit repository list computes only those repositories, so it
//     must never certify the org-day.
//   - One clock. Every version is a Postgres clock reading taken in the
//     transaction that changes the run state (finalized_at at completion, the
//     reset statement at a reopen, the claim statement at dispatch), never the
//     writing host's clock. The backfill replays the stored state's version.

// redriveFullGenerationPrefix is the generation prefix finalize-redrive gives a
// full-org run it resets, so the classification survives the redrive.
const redriveFullGenerationPrefix = "redrive-full:"

// pgClockMillis is the RETURNING expression that reads the Postgres clock in
// milliseconds since the epoch.
const pgClockMillis = `(extract(epoch from clock_timestamp()) * 1000)::bigint`

// isFullOrgGeneration reports whether a run of this generation computes the
// whole organization: the scheduled fan-out and the post-sync run both
// discover the repository set live (RepositoryDiscoveryRequired), and a
// finalize-redriven full-org run keeps its class in redriveFullGenerationPrefix.
// Manual and external-recompute runs can carry an explicit repository list and
// are never treated as full-org.
func isFullOrgGeneration(generation string) bool {
	if strings.HasPrefix(generation, redriveFullGenerationPrefix) && len(generation) <= 64 {
		return true
	}
	base := baseGeneration(generation)
	return isScheduledFanoutGeneration(base) || isPostSyncGeneration(base)
}

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

// markSucceeded appends 'succeeded' after CompleteFinalize committed, with the
// Postgres finalized_at the same transaction stored as its version. Only a
// full-org run certifies its day. It never fails the run: on error the day
// stays unknown (logged and counted) and the backfill restores it.
func (store *PostgresStore) markSucceeded(ctx context.Context, run Run, finalizedAtMs int64) {
	if !isFullOrgGeneration(run.Generation) {
		return
	}
	_ = store.appendMarker(ctx, run.OrganizationID, run.TargetDay.UTC().Format(dailyTargetDayLayout),
		run.Generation, RunMarkerSucceeded, finalizedAtMs)
}

// markReopened appends 'reopened' inside the reopen transaction, after the
// reset statement and before the commit, so a failed append refuses the reopen
// (the caller returns the error and the transaction rolls back). The reverse
// failure, a written marker with a reopen that then fails to commit, only hides
// a succeeded day until the backfill heals it. A partial-scope run never wrote
// 'succeeded', so reopening it writes nothing.
func (store *PostgresStore) markReopened(ctx context.Context, orgID, day, priorGeneration string, versionMs int64) error {
	if !isFullOrgGeneration(priorGeneration) {
		return nil
	}
	if err := store.appendMarker(ctx, orgID, day, priorGeneration, RunMarkerReopened, versionMs); err != nil {
		return ErrUnavailable
	}
	return nil
}

// markInFlight appends 'reopened' when a full-org run is claimed for dispatch:
// while a new generation computes the day it is not certified, even if an
// earlier generation succeeded. The version is the Postgres clock of the claim
// statement. A failed append fails the claim (the job retries), because
// dispatch already needs ClickHouse for repository discovery and a stale
// 'succeeded' would be a false success.
func (store *PostgresStore) markInFlight(ctx context.Context, run Run, claimedAtMs int64) error {
	if !isFullOrgGeneration(run.Generation) {
		return nil
	}
	if err := store.appendMarker(ctx, run.OrganizationID, run.TargetDay.UTC().Format(dailyTargetDayLayout),
		run.Generation, RunMarkerReopened, claimedAtMs); err != nil {
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

// lockMarkerDay takes the transaction-scoped lock that serializes every marker
// writer of one (org, day): the backfill, the reopen resets, and the dispatch
// claim. Two writers of one day therefore never interleave their reads and
// appends, and a reopen that is still inside its transaction blocks a backfill
// until it commits or rolls back. A hash collision only serializes two days.
func lockMarkerDay(ctx context.Context, tx pgx.Tx, orgID, day string) error {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
		"daily_run_marker:"+orgID+":"+day); err != nil {
		return ErrUnavailable
	}
	return nil
}

func (store *PostgresStore) lockMarkerDayIfEnabled(ctx context.Context, tx pgx.Tx, orgID, day string) error {
	if store.markerWriter == nil {
		return nil
	}
	return lockMarkerDay(ctx, tx, orgID, day)
}

// BackfillRunMarkers makes ClickHouse agree with Postgres for every org-day in
// [from, to] that has a full-org run. Per day the latest FULL-ORG run by
// created_at decides (a partial-scope run is ignored): status and
// finalization_status both 'succeeded' means 'succeeded', anything else means
// 'reopened'. A row is appended only when the current ClickHouse state
// differs, so a second pass appends nothing. A day with no full-org run gets no
// row. no_repositories runs are not marked.
//
// Serialization. Each day runs in its own transaction that takes the (org,
// day) marker lock FIRST, then reads ClickHouse, then reads Postgres, then
// appends, then commits. A reopen or dispatch claim of that day holds the same
// lock until it commits, so the backfill reads either the whole state before it
// or the whole state after it, never a half. Versions: a 'succeeded' replay
// carries the run's stored finalized_at (the Postgres clock), raised to one
// above the version it observed so a rolled-back reopen heals; a 'reopened'
// replay carries one above the observed version.
func (store *PostgresStore) BackfillRunMarkers(
	ctx context.Context, reader RunMarkerReader, orgID string, from, to time.Time, dryRun bool,
) (RunMarkerBackfillOutcome, error) {
	var outcome RunMarkerBackfillOutcome
	if !store.valid() || store.markerWriter == nil || reader == nil || !validUUID(orgID) || to.Before(from) {
		return outcome, ErrUnavailable
	}
	from = from.UTC().Truncate(24 * time.Hour)
	to = to.UTC().Truncate(24 * time.Hour)
	for day := from; !day.After(to); day = day.AddDate(0, 0, 1) {
		if err := store.backfillOneDay(ctx, reader, orgID, day, dryRun, &outcome); err != nil {
			return outcome, err
		}
	}
	return outcome, nil
}

func (store *PostgresStore) backfillOneDay(
	ctx context.Context, reader RunMarkerReader, orgID string, day time.Time, dryRun bool,
	outcome *RunMarkerBackfillOutcome,
) error {
	key := day.Format(dailyTargetDayLayout)
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return ErrUnavailable
	}
	defer func() {
		rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()
	if err := lockMarkerDay(ctx, tx, orgID, key); err != nil {
		return err
	}
	current, err := reader.RunMarkerStates(ctx, orgID, day, day)
	if err != nil {
		return ErrUnavailable
	}
	if store.backfillHook != nil {
		store.backfillHook("marker_read")
	}
	rows, err := tx.Query(ctx, `
SELECT run.generation,
       (run.status = 'succeeded' AND run.finalization_status = 'succeeded'),
       (extract(epoch from COALESCE(run.finalized_at, run.updated_at)) * 1000)::bigint
FROM public.daily_metrics_runs AS run
WHERE run.org_id = $1::uuid AND run.target_day = $2::date
ORDER BY run.created_at DESC, run.id`, orgID, day)
	if err != nil {
		return ErrUnavailable
	}
	var (
		found      bool
		generation string
		succeeded  bool
		storedMs   int64
	)
	for rows.Next() && !found {
		var rowGeneration string
		var rowSucceeded bool
		var rowStored int64
		if err := rows.Scan(&rowGeneration, &rowSucceeded, &rowStored); err != nil {
			rows.Close()
			return ErrUnavailable
		}
		if isFullOrgGeneration(rowGeneration) {
			found, generation, succeeded, storedMs = true, rowGeneration, rowSucceeded, rowStored
		}
	}
	rowsErr := rows.Err()
	rows.Close()
	if rowsErr != nil {
		return ErrUnavailable
	}
	if store.backfillHook != nil {
		store.backfillHook("postgres_read")
	}
	if !found {
		return nil
	}
	outcome.DaysExamined++
	desired := RunMarkerReopened
	if succeeded {
		desired = RunMarkerSucceeded
	}
	have, hasRow := current[key]
	// A day with no marker row and a run that is not succeeded is already
	// unknown: appending 'reopened' would add nothing.
	if (hasRow && have.State == desired) || (!hasRow && desired == RunMarkerReopened) {
		outcome.Unchanged++
		return nil
	}
	version := storedMs
	if desired == RunMarkerReopened {
		version = int64(have.Version) + 1
	} else if hasRow && int64(have.Version)+1 > version {
		version = int64(have.Version) + 1
	}
	if dryRun {
		outcome.Appended++
		return nil
	}
	if err := store.appendMarker(ctx, orgID, key, generation, desired, version); err != nil {
		return ErrUnavailable
	}
	if err := tx.Commit(ctx); err != nil {
		return ErrUnavailable
	}
	outcome.Appended++
	if desired == RunMarkerSucceeded {
		outcome.Succeeded++
	} else {
		outcome.Reopened++
	}
	return nil
}

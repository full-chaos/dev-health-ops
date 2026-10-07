package daily

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// TouchedDrainGenerationPrefix identifies a daily-metrics run started by the
// drain of the pending touched days (CHAOS-8846). The generation of one pass
// is this prefix, a trigger letter and the id of the run whose dispatch or end
// started the pass, so a second delivery of the same trigger builds the same
// run ids.
//
// A drain run always carries a repository list, so it has partitions from its
// first write and never enters deferred discovery (MaterializeScheduledFanout
// refuses this prefix on purpose: a drain run without partitions is a defect).
const TouchedDrainGenerationPrefix = "touched-drain:"

// TouchedDaysDrainer starts daily runs for the pending touched days of one
// organization. The dispatch of a nightly run and the end of any daily run
// call it.
//
// It returns nothing: a pass that fails must never fail the run that
// triggered it. An implementation logs and counts its own failures.
type TouchedDaysDrainer interface {
	DrainTouchedDays(ctx context.Context, organizationID, passID string)
}

// SetTouchedDaysDrainer wires the drain pass that the dispatch of a nightly
// run triggers. Nil (the default) means no pass.
func (handler *Dispatcher) SetTouchedDaysDrainer(drainer TouchedDaysDrainer) {
	if handler != nil {
		handler.touchedDrainer = drainer
	}
}

// SetTouchedDaysDrainer wires the drain pass that the end of a daily run
// triggers. Nil (the default) means no pass.
func (handler *FinalizeHandler) SetTouchedDaysDrainer(drainer TouchedDaysDrainer) {
	if handler != nil {
		handler.touchedDrainer = drainer
	}
}

// drainTouchedDaysAfterDispatch is the floor trigger: one pass for each
// nightly run of an organization. Only the nightly run triggers here; the end
// of a run triggers the passes behind it.
func (handler *Dispatcher) drainTouchedDaysAfterDispatch(ctx context.Context, run Run) {
	if handler.touchedDrainer == nil || !isScheduledFanoutGeneration(run.Generation) {
		return
	}
	handler.touchedDrainer.DrainTouchedDays(ctx, run.OrganizationID, "n:"+run.ID)
}

// drainTouchedDaysAfterEnd is the continuation trigger: one pass when a daily
// run reached a terminal state.
func (handler *FinalizeHandler) drainTouchedDaysAfterEnd(ctx context.Context, run Run) {
	if handler.touchedDrainer == nil {
		return
	}
	handler.touchedDrainer.DrainTouchedDays(ctx, run.OrganizationID, "e:"+run.ID)
}

// TouchedDrainRunsInFlightTx counts the drain runs of the organization that
// are not terminal and were created at or after since.
//
// since bounds the count on purpose: a drain run that never ends must not
// stop the drain of its organization for ever. Such a run is the business of
// the blocked-run marker.
func (store *PostgresStore) TouchedDrainRunsInFlightTx(
	ctx context.Context, tx pgx.Tx, organizationID string, since time.Time,
) (int, error) {
	if !store.valid() || tx == nil || !validUUID(organizationID) || since.IsZero() {
		return 0, ErrInvalidState
	}
	var count int
	if err := tx.QueryRow(ctx, `
SELECT count(*)
FROM public.daily_metrics_runs
WHERE org_id = $1::uuid
  AND generation LIKE $2
  AND status IN ('pending', 'running')
  AND created_at >= $3`,
		organizationID, escapeLikePrefix(TouchedDrainGenerationPrefix)+"%", since.UTC(),
	).Scan(&count); err != nil {
		return 0, ErrUnavailable
	}
	return count, nil
}

// TouchedDayFailure is one day whose newest run is a failed touched-day run.
type TouchedDayFailure struct {
	Day      time.Time
	FailedAt time.Time
}

// FailedTouchedDays returns the days of the organization whose NEWEST run (of
// any generation, created at or after since) is a failed run of a post-sync
// fan-out or of the drain. The keys of such a day were marked as dispatched
// when the run started, and the run computed nothing that can be trusted.
//
// A day whose newest run is of another kind, or is not failed, is not
// returned: a later run of the day computes it from stored rows.
func (store *PostgresStore) FailedTouchedDays(
	ctx context.Context, organizationID string, since time.Time,
) ([]TouchedDayFailure, error) {
	if !store.valid() || !validUUID(organizationID) || since.IsZero() {
		return nil, ErrInvalidState
	}
	rows, err := store.pool.Query(ctx, `
SELECT target_day, failed_at
FROM (
    SELECT DISTINCT ON (target_day)
           target_day, status, generation, COALESCE(finalized_at, updated_at) AS failed_at
    FROM public.daily_metrics_runs
    WHERE org_id = $1::uuid AND created_at >= $2
    ORDER BY target_day, created_at DESC, id DESC
) AS newest
WHERE status = 'failed' AND (generation LIKE $3 OR generation LIKE $4)
ORDER BY target_day`,
		organizationID, since.UTC(),
		escapeLikePrefix(postSyncGenerationPrefix)+"%",
		escapeLikePrefix(TouchedDrainGenerationPrefix)+"%",
	)
	if err != nil {
		return nil, ErrUnavailable
	}
	defer rows.Close()
	var failures []TouchedDayFailure
	for rows.Next() {
		var failure TouchedDayFailure
		if err := rows.Scan(&failure.Day, &failure.FailedAt); err != nil {
			return nil, ErrUnavailable
		}
		failure.Day, failure.FailedAt = failure.Day.UTC(), failure.FailedAt.UTC()
		failures = append(failures, failure)
	}
	if rows.Err() != nil {
		return nil, ErrUnavailable
	}
	return failures, nil
}

// DaysWithOnlyFailedRuns returns the days among days whose newest `threshold`
// runs (of any generation) all failed. A day with fewer runs is not returned.
// The map key is the day as 2006-01-02.
func (store *PostgresStore) DaysWithOnlyFailedRuns(
	ctx context.Context, organizationID string, days []time.Time, threshold int,
) (map[string]struct{}, error) {
	if !store.valid() || !validUUID(organizationID) || threshold < 1 {
		return nil, ErrInvalidState
	}
	failed := map[string]struct{}{}
	if len(days) == 0 {
		return failed, nil
	}
	values := make([]string, 0, len(days))
	for _, day := range days {
		values = append(values, day.UTC().Format("2006-01-02"))
	}
	rows, err := store.pool.Query(ctx, `
SELECT target_day::text
FROM (
    SELECT target_day, status,
           row_number() OVER (PARTITION BY target_day ORDER BY created_at DESC, id DESC) AS position
    FROM public.daily_metrics_runs
    WHERE org_id = $1::uuid AND target_day = ANY($2::date[])
) AS ranked
WHERE position <= $3
GROUP BY target_day
HAVING count(*) = $3 AND bool_and(status = 'failed')`,
		uuid.MustParse(organizationID).String(), values, threshold)
	if err != nil {
		return nil, ErrUnavailable
	}
	defer rows.Close()
	for rows.Next() {
		var day string
		if err := rows.Scan(&day); err != nil {
			return nil, ErrUnavailable
		}
		failed[day] = struct{}{}
	}
	if rows.Err() != nil {
		return nil, ErrUnavailable
	}
	return failed, nil
}

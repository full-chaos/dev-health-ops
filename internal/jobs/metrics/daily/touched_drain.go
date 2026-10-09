package daily

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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

// TouchedDaysDrainer returns the drain the dispatch of a nightly run triggers,
// or nil when none is wired.
func (handler *Dispatcher) TouchedDaysDrainer() TouchedDaysDrainer {
	if handler == nil {
		return nil
	}
	return handler.touchedDrainer
}

// TouchedDaysDrainer returns the drain the end of a daily run triggers, or nil
// when none is wired.
func (handler *FinalizeHandler) TouchedDaysDrainer() TouchedDaysDrainer {
	if handler == nil {
		return nil
	}
	return handler.touchedDrainer
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

// DailyRunsStateTx returns a value that is another one after any daily run of
// the organization was created: the number of its runs and the creation time
// of the newest.
//
// The drain reads it before the reads of a pass and again under the lock of
// the pass. The same value twice says that every run that is committed at the
// second read was committed at the first, so what the pass decided from the
// runs is still true.
func (store *PostgresStore) DailyRunsStateTx(ctx context.Context, tx pgx.Tx, organizationID string) (string, error) {
	if !store.valid() || tx == nil || !validUUID(organizationID) {
		return "", ErrInvalidState
	}
	var state string
	if err := tx.QueryRow(ctx, `
SELECT count(*)::text || '|' || COALESCE(max(created_at)::text, '')
FROM public.daily_metrics_runs
WHERE org_id = $1::uuid`, organizationID).Scan(&state); err != nil {
		return "", ErrUnavailable
	}
	return state, nil
}

// touchedRunWithoutResultSQL is true for a run that will give its day no
// result: it is failed or canceled, or it is not ended the number of seconds
// ($%d) after its creation. A run that is not ended after that time is treated
// as one that never ends (a blocked run, a run whose jobs were lost): the same
// time stops such a run from holding back the drain.
//
// Both sides of the comparison are the Postgres clock: created_at is stamped
// by clock_timestamp() in StartRunTx.
const touchedRunWithoutResultSQL = `(status IN ('failed', 'canceled') OR (status IN ('pending', 'running') AND created_at < clock_timestamp() - make_interval(secs => $%d)))`

// touchedMarkingRunSQL is true for a run that a post-sync fan-out or the drain
// started: the only runs whose start marks keys of the touched-day record as
// dispatched. Its two arguments are the LIKE patterns of the two prefixes.
const touchedMarkingRunSQL = `(%[1]s.generation LIKE $%[2]d OR %[1]s.generation LIKE $%[3]d)`

// TouchedRunKeys is one run and keys of the touched-day record that it lists.
type TouchedRunKeys struct {
	RunID string
	Day   time.Time
	// FullOrganization is true for a run of every repository: it lists every
	// key of its day but the ones in ExceptRepositoryIDs.
	FullOrganization bool
	// RepositoryIDs are the listed repositories. Empty for a run of every
	// repository.
	RepositoryIDs []string
	// ExceptRepositoryIDs are, for a run of every repository, the
	// repositories that a newer run lists.
	ExceptRepositoryIDs []string
	// TakenAt is the take time of the run (daily_metrics_runs.touched_take_at):
	// zero when none was recorded.
	TakenAt time.Time
}

// OwnedKeysOfTouchedRunsWithoutResult returns the keys whose owner run ended
// without a result.
//
// The owner of a key (day, repository) is the newest run of a post-sync
// fan-out or of the drain that lists it: the list of a run is its partitions,
// written in the transaction that created the run, and a run of every
// repository lists every key of its day. "Newest" is the order of
// (created_at, id), which only Postgres writes. A run without a result is
// failed, canceled, or not ended notEndedAfter after its creation.
//
// The keys of such an owner were marked as dispatched when it started, and it
// computed nothing that can be trusted. No time of the touched-day record is
// compared with a time of a run: the caller returns exactly these keys.
//
// The read has no bound on the age of a run. It returns at most limit runs,
// newest first; truncated says that more exist. A run that owns no key any
// more (a newer run lists each of its keys) is not returned and does not count
// toward limit, so the runs behind a full read are reached when the keys of
// the returned ones were taken again.
func (store *PostgresStore) OwnedKeysOfTouchedRunsWithoutResult(
	ctx context.Context, organizationID string, notEndedAfter time.Duration, limit int,
) ([]TouchedRunKeys, bool, error) {
	if !store.valid() || !validUUID(organizationID) || notEndedAfter <= 0 || limit < 1 {
		return nil, false, ErrInvalidState
	}
	newer := `newer.org_id = bad.org_id AND newer.target_day = bad.target_day
              AND ` + fmt.Sprintf(touchedMarkingRunSQL, "newer", 2, 3) + `
              AND (newer.created_at, newer.id) > (bad.created_at, bad.id)`
	rows, err := store.pool.Query(ctx, `
WITH bad AS (
    SELECT id, org_id, target_day, full_org, created_at
    FROM public.daily_metrics_runs
    WHERE org_id = $1::uuid
      AND `+fmt.Sprintf(touchedMarkingRunSQL, "daily_metrics_runs", 2, 3)+`
      AND `+fmt.Sprintf(touchedRunWithoutResultSQL, 4)+`
), owned AS (
    SELECT bad.id, bad.target_day, bad.full_org, bad.created_at,
           CASE WHEN bad.full_org THEN ARRAY(
               SELECT DISTINCT listed.repo
               FROM public.daily_metrics_runs AS newer
               JOIN public.daily_metrics_partitions AS part ON part.run_id = newer.id
               CROSS JOIN LATERAL json_array_elements_text(part.repo_ids::json) AS listed(repo)
               WHERE `+newer+`
               ORDER BY listed.repo
           ) ELSE ARRAY(
               SELECT DISTINCT own.repo
               FROM public.daily_metrics_partitions AS part
               CROSS JOIN LATERAL json_array_elements_text(part.repo_ids::json) AS own(repo)
               WHERE part.run_id = bad.id
                 AND NOT EXISTS (
                     SELECT 1
                     FROM public.daily_metrics_runs AS newer
                     WHERE `+newer+`
                       AND (newer.full_org OR EXISTS (
                           SELECT 1 FROM public.daily_metrics_partitions AS newer_part
                           WHERE newer_part.run_id = newer.id
                             AND newer_part.repo_ids::jsonb @> to_jsonb(own.repo)
                       ))
                 )
               ORDER BY own.repo
           ) END AS repos
    FROM bad
    WHERE NOT (bad.full_org AND EXISTS (
        SELECT 1 FROM public.daily_metrics_runs AS newer WHERE `+newer+` AND newer.full_org
    ))
)
SELECT id::text, target_day, full_org, repos
FROM owned
WHERE full_org OR cardinality(repos) > 0
ORDER BY created_at DESC, id DESC
LIMIT $5`,
		uuid.MustParse(organizationID).String(),
		escapeLikePrefix(postSyncGenerationPrefix)+"%",
		escapeLikePrefix(TouchedDrainGenerationPrefix)+"%",
		notEndedAfter.Seconds(), limit+1,
	)
	if err != nil {
		return nil, false, ErrUnavailable
	}
	defer rows.Close()
	var (
		runs      []TouchedRunKeys
		truncated bool
	)
	for rows.Next() {
		var (
			run   TouchedRunKeys
			repos []string
		)
		if err := rows.Scan(&run.RunID, &run.Day, &run.FullOrganization, &repos); err != nil {
			return nil, false, ErrUnavailable
		}
		if len(runs) == limit {
			truncated = true
			break
		}
		run.Day = run.Day.UTC()
		if run.FullOrganization {
			run.ExceptRepositoryIDs = repos
		} else {
			run.RepositoryIDs = repos
		}
		runs = append(runs, run)
	}
	if rows.Err() != nil {
		return nil, false, ErrUnavailable
	}
	return runs, truncated, nil
}

// undefinedColumnCode is the SQLSTATE of a statement that names a column the
// table does not have.
const undefinedColumnCode = "42703"

// ErrTouchedTakeColumnAbsent is returned by StampTouchedTakeTx when
// daily_metrics_runs has no touched_take_at column: a build that runs before
// its migration (a rolling update). The caller goes on without the take time
// and reports the miss; the runs it started are then runs of an unknown take.
var ErrTouchedTakeColumnAbsent = errors.New("daily_metrics_runs.touched_take_at is absent")

// StampTouchedTakeTx records the take time of a pass or a fan-out on every run
// of the generation that has none, in the transaction that created the runs.
//
// The take time is the ClickHouse time read before the pending days that the
// runs were started for. The mark of those runs stamps the keys one
// millisecond before it, so the drain can tell a mark that never landed (a
// listed key still pending, last touched before the take time) from a key that
// was touched again after the take (touched at or after it). A run that has a
// take time keeps it: a second delivery of the same fan-out reads another time
// and does not list the runs of the first.
//
// The UPDATE runs in a savepoint: when the column is absent the statement
// fails, and only the savepoint is rolled back, so the caller's transaction
// still commits its runs.
func (store *PostgresStore) StampTouchedTakeTx(
	ctx context.Context, tx pgx.Tx, organizationID, generation string, takenAt time.Time,
) error {
	if !store.valid() || tx == nil || !validUUID(organizationID) || generation == "" ||
		len(generation) > 64 || takenAt.IsZero() {
		return ErrInvalidState
	}
	nested, err := tx.Begin(ctx)
	if err != nil {
		return ErrUnavailable
	}
	if _, err := nested.Exec(ctx, `
UPDATE public.daily_metrics_runs
SET touched_take_at = $3
WHERE org_id = $1::uuid AND generation = $2 AND touched_take_at IS NULL`,
		organizationID, generation, takenAt.UTC().Truncate(time.Millisecond)); err != nil {
		_ = nested.Rollback(ctx)
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == undefinedColumnCode {
			return ErrTouchedTakeColumnAbsent
		}
		return ErrUnavailable
	}
	if err := nested.Commit(ctx); err != nil {
		return ErrUnavailable
	}
	return nil
}

// TouchedMarkingRunsToCheck returns the runs whose mark the drain checks: the
// run endedRunID (when it is a run that marks keys, whatever its take time) and
// the runs of a post-sync fan-out or of the drain that ended, were created in
// the last window, and have a take time. The run endedRunID comes first and is
// read apart from the others, so it never competes with the limit however many
// newer runs exist; the others are the newest limit, newest first, and the flag
// says that more of them exist.
//
// A run that is not ended is left out on purpose: the mark of a fan-out follows
// its commit by a moment, and a pass in that moment must not read the missing
// mark as lost. A run of any other generation never marks keys and is not
// returned.
//
// The take time is read through to_jsonb of the row, so a database without the
// column answers NULL for it and the caller treats the run as one of an unknown
// take. A run of every repository lists no key: the caller asks for every
// pending key of its day.
func (store *PostgresStore) TouchedMarkingRunsToCheck(
	ctx context.Context, organizationID, endedRunID string, window time.Duration, limit int,
) ([]TouchedRunKeys, bool, error) {
	if !store.valid() || !validUUID(organizationID) || window <= 0 || limit < 1 ||
		(endedRunID != "" && !validUUID(endedRunID)) {
		return nil, false, ErrInvalidState
	}
	org := uuid.MustParse(organizationID).String()
	var runs []TouchedRunKeys
	ended := ""
	if endedRunID != "" {
		ended = uuid.MustParse(endedRunID).String()
		// $5 is named only so that the statement uses every parameter it is given.
		trigger, _, err := store.queryMarkingRuns(ctx, `
  AND run.id = $4::uuid AND $5::float8 IS NOT NULL`, 1, org, ended)
		if err != nil {
			return nil, false, err
		}
		runs = trigger
	}
	newest, truncated, err := store.queryMarkingRuns(ctx, `
  AND run.status NOT IN ('pending', 'running')
  AND run.created_at >= clock_timestamp() - make_interval(secs => $5)
  AND (to_jsonb(run) ->> 'touched_take_at') IS NOT NULL
  AND ($4::uuid IS NULL OR run.id <> $4::uuid)`, limit, org, ended, window.Seconds())
	if err != nil {
		return nil, false, err
	}
	return append(runs, newest...), truncated, nil
}

// queryMarkingRuns reads the runs of the organization that mark keys and match
// the extra condition, newest first, at most limit; the flag says that more
// matched. $1 is the organization, $2 and $3 the generation patterns, $4 the
// run that ended (NULL for none) and $5 the window in seconds.
func (store *PostgresStore) queryMarkingRuns(
	ctx context.Context, condition string, limit int, organizationID, endedRunID string, window ...float64,
) ([]TouchedRunKeys, bool, error) {
	var ended any
	if endedRunID != "" {
		ended = endedRunID
	}
	seconds := 0.0
	if len(window) > 0 {
		seconds = window[0]
	}
	rows, err := store.pool.Query(ctx, `
SELECT run.id::text, run.target_day, run.full_org,
       (to_jsonb(run) ->> 'touched_take_at')::timestamptz,
       CASE WHEN run.full_org THEN ARRAY[]::text[] ELSE ARRAY(
           SELECT DISTINCT listed.repo
           FROM public.daily_metrics_partitions AS part
           CROSS JOIN LATERAL json_array_elements_text(part.repo_ids::json) AS listed(repo)
           WHERE part.run_id = run.id
           ORDER BY listed.repo
       ) END
FROM public.daily_metrics_runs AS run
WHERE run.org_id = $1::uuid
  AND `+fmt.Sprintf(touchedMarkingRunSQL, "run", 2, 3)+condition+`
ORDER BY run.created_at DESC, run.id DESC
LIMIT $6`,
		organizationID,
		escapeLikePrefix(postSyncGenerationPrefix)+"%",
		escapeLikePrefix(TouchedDrainGenerationPrefix)+"%",
		ended, seconds, limit+1,
	)
	if err != nil {
		return nil, false, ErrUnavailable
	}
	defer rows.Close()
	var (
		runs      []TouchedRunKeys
		truncated bool
	)
	for rows.Next() {
		var (
			run     TouchedRunKeys
			takenAt *time.Time
		)
		if err := rows.Scan(&run.RunID, &run.Day, &run.FullOrganization, &takenAt, &run.RepositoryIDs); err != nil {
			return nil, false, ErrUnavailable
		}
		if len(runs) == limit {
			truncated = true
			break
		}
		run.Day = run.Day.UTC()
		if takenAt != nil {
			run.TakenAt = takenAt.UTC()
		}
		runs = append(runs, run)
	}
	if rows.Err() != nil {
		return nil, false, ErrUnavailable
	}
	return runs, truncated, nil
}

// TouchedDaysWithOnlyFailedRuns returns the days among days whose newest
// `threshold` runs (of any generation) all ended without a result: failed,
// canceled, or not ended notEndedAfter after their creation. A day with fewer
// runs is not returned. The map key is the day as 2006-01-02.
//
// The value is true when the newest run of the day was created more than
// retryAfter ago: the day is then due for one more run. Both times are the
// Postgres clock. A run started for the day is its newest run, so the value
// is false again for retryAfter, whatever the end of that run.
func (store *PostgresStore) TouchedDaysWithOnlyFailedRuns(
	ctx context.Context, organizationID string, days []time.Time, threshold int, notEndedAfter, retryAfter time.Duration,
) (map[string]bool, error) {
	if !store.valid() || !validUUID(organizationID) || threshold < 1 || notEndedAfter <= 0 || retryAfter <= 0 {
		return nil, ErrInvalidState
	}
	failed := map[string]bool{}
	if len(days) == 0 {
		return failed, nil
	}
	values := make([]string, 0, len(days))
	for _, day := range days {
		values = append(values, day.UTC().Format("2006-01-02"))
	}
	rows, err := store.pool.Query(ctx, `
SELECT target_day::text, bool_or(position = 1 AND retry_due)
FROM (
    SELECT target_day, `+fmt.Sprintf(touchedRunWithoutResultSQL, 4)+` AS without_result,
           created_at < clock_timestamp() - make_interval(secs => $5) AS retry_due,
           row_number() OVER (PARTITION BY target_day ORDER BY created_at DESC, id DESC) AS position
    FROM public.daily_metrics_runs
    WHERE org_id = $1::uuid AND target_day = ANY($2::date[])
) AS ranked
WHERE position <= $3
GROUP BY target_day
HAVING count(*) = $3 AND bool_and(without_result)`,
		uuid.MustParse(organizationID).String(), values, threshold, notEndedAfter.Seconds(), retryAfter.Seconds())
	if err != nil {
		return nil, ErrUnavailable
	}
	defer rows.Close()
	for rows.Next() {
		var (
			day      string
			retryDue bool
		)
		if err := rows.Scan(&day, &retryDue); err != nil {
			return nil, ErrUnavailable
		}
		failed[day] = retryDue
	}
	if rows.Err() != nil {
		return nil, ErrUnavailable
	}
	return failed, nil
}

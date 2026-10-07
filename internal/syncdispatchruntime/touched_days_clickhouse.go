package syncdispatchruntime

import (
	"context"
	"errors"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// ErrTouchedDaysUnavailable is returned when the touched-day record cannot be
// read or written. A caller never reads it as "no day was touched".
var ErrTouchedDaysUnavailable = errors.New("touched-day record is unavailable")

// ClickHouseTouchedDaysStore keeps the record of the (organization, day,
// repository) keys that stored raw work-item rows touched, in the table
// daily_metrics_touched_days (CHAOS-8813).
//
// Every time in the table is the ClickHouse clock. A key is pending while its
// newest 'touched' event is newer than its newest 'dispatched' event.
type ClickHouseTouchedDaysStore struct {
	conn driver.Conn
}

func NewClickHouseTouchedDaysStore(conn driver.Conn) (*ClickHouseTouchedDaysStore, error) {
	if conn == nil {
		return nil, ErrTouchedDaysUnavailable
	}
	return &ClickHouseTouchedDaysStore{conn: conn}, nil
}

// recordTouchedDaysSQL appends one 'touched' event for each (day, repository)
// that a raw row written at or after the given time belongs to. The server
// does the whole read and write: no row reaches this process.
//
// The days of a work item are the days of created_at, started_at,
// completed_at and closed_at. The day of a transition is the day of
// occurred_at, under the repository of its work item: a transition can carry
// the nil repository while its item carries a real one. A transition whose
// item has no stored row keeps its own repository.
const recordTouchedDaysSQL = `
INSERT INTO daily_metrics_touched_days (org_id, day, repo_id, kind, at)
SELECT ?, day, repo_id, 'touched', fromUnixTimestamp64Milli(toInt64(?), 'UTC')
FROM (
    SELECT repo_id,
           arrayJoin(arrayMap(value -> assumeNotNull(value), arrayFilter(value -> value IS NOT NULL, [
               toDate(created_at, 'UTC'), toDate(started_at, 'UTC'),
               toDate(completed_at, 'UTC'), toDate(closed_at, 'UTC')
           ]))) AS day
    FROM work_items
    WHERE org_id = ? AND last_synced >= fromUnixTimestamp64Milli(toInt64(?), 'UTC')
    UNION ALL
    SELECT if(items.work_item_id = '', transitions.repo_id, items.repo_id) AS repo_id,
           toDate(transitions.occurred_at, 'UTC') AS day
    FROM (
        SELECT work_item_id, repo_id, occurred_at
        FROM work_item_transitions
        WHERE org_id = ? AND last_synced >= fromUnixTimestamp64Milli(toInt64(?), 'UTC')
    ) AS transitions
    LEFT JOIN (
        SELECT DISTINCT work_item_id, repo_id
        FROM work_items
        WHERE org_id = ? AND work_item_id IN (
            SELECT work_item_id
            FROM work_item_transitions
            WHERE org_id = ? AND last_synced >= fromUnixTimestamp64Milli(toInt64(?), 'UTC')
        )
    ) AS items ON items.work_item_id = transitions.work_item_id
)
GROUP BY day, repo_id`

// RecordTouched appends the 'touched' events of the raw rows whose last_synced
// is at or after since, and returns the number of keys it appended.
func (store *ClickHouseTouchedDaysStore) RecordTouched(
	ctx context.Context, organizationID string, since time.Time,
) (uint64, error) {
	if store == nil || store.conn == nil || organizationID == "" || since.IsZero() {
		return 0, ErrTouchedDaysUnavailable
	}
	at, err := store.clock(ctx)
	if err != nil {
		return 0, err
	}
	sinceMillis := since.UTC().UnixMilli()
	if err := store.conn.Exec(ctx, recordTouchedDaysSQL,
		organizationID, at.UnixMilli(),
		organizationID, sinceMillis,
		organizationID, sinceMillis,
		organizationID, organizationID, sinceMillis,
	); err != nil {
		return 0, ErrTouchedDaysUnavailable
	}
	var recorded uint64
	if err := store.conn.QueryRow(ctx, `
SELECT count()
FROM (
    SELECT day, repo_id
    FROM daily_metrics_touched_days
    WHERE org_id = ? AND kind = 'touched' AND at = fromUnixTimestamp64Milli(toInt64(?), 'UTC')
    GROUP BY day, repo_id
)`, organizationID, at.UnixMilli()).Scan(&recorded); err != nil {
		return 0, ErrTouchedDaysUnavailable
	}
	return recorded, nil
}

// pendingTouchedKeysSQL is the reader contract of the table: aggregate for
// each key, never read the rows as they are (merges are eventual).
const pendingTouchedKeysSQL = `
    SELECT day, repo_id
    FROM daily_metrics_touched_days
    WHERE org_id = ?
    GROUP BY day, repo_id
    HAVING maxIf(at, kind = 'touched') > maxIf(at, kind = 'dispatched')`

// PendingDays returns the pending days of the organization, newest first, and
// the ClickHouse time read before them. It reads at most limit days; Truncated
// says that older pending days exist that this read did not return.
func (store *ClickHouseTouchedDaysStore) PendingDays(
	ctx context.Context, organizationID string, limit int,
) (TouchedDaysPending, error) {
	if store == nil || store.conn == nil || organizationID == "" || limit < 1 {
		return TouchedDaysPending{}, ErrTouchedDaysUnavailable
	}
	takenAt, err := store.clock(ctx)
	if err != nil {
		return TouchedDaysPending{}, err
	}
	rows, err := store.conn.Query(ctx, `
SELECT day
FROM (`+pendingTouchedKeysSQL+`
)
GROUP BY day
ORDER BY day DESC
LIMIT ?`, organizationID, limit+1)
	if err != nil {
		return TouchedDaysPending{}, ErrTouchedDaysUnavailable
	}
	defer rows.Close()
	pending := TouchedDaysPending{TakenAt: takenAt}
	for rows.Next() {
		var day time.Time
		if err := rows.Scan(&day); err != nil {
			return TouchedDaysPending{}, ErrTouchedDaysUnavailable
		}
		if len(pending.Days) == limit {
			pending.Truncated = true
			break
		}
		pending.Days = append(pending.Days, utcDay(day))
	}
	if err := rows.Err(); err != nil {
		return TouchedDaysPending{}, ErrTouchedDaysUnavailable
	}
	return pending, nil
}

// PendingRepositories returns, for each of the given days, at most
// limitPerDay of its pending repositories. The map key is the day as
// 2006-01-02. A day with no pending key has no entry.
func (store *ClickHouseTouchedDaysStore) PendingRepositories(
	ctx context.Context, organizationID string, days []time.Time, limitPerDay int,
) (map[string][]string, error) {
	if store == nil || store.conn == nil || organizationID == "" || limitPerDay < 1 {
		return nil, ErrTouchedDaysUnavailable
	}
	repositories := make(map[string][]string, len(days))
	if len(days) == 0 {
		return repositories, nil
	}
	rows, err := store.conn.Query(ctx, `
SELECT toString(day), arraySort(groupUniqArray(?)(toString(repo_id)))
FROM (`+pendingTouchedKeysSQL+`
)
WHERE has(?, toString(day))
GROUP BY day`, limitPerDay, organizationID, touchedDayStrings(days))
	if err != nil {
		return nil, ErrTouchedDaysUnavailable
	}
	defer rows.Close()
	for rows.Next() {
		var (
			day         string
			identifiers []string
		)
		if err := rows.Scan(&day, &identifiers); err != nil {
			return nil, ErrTouchedDaysUnavailable
		}
		repositories[day] = identifiers
	}
	if err := rows.Err(); err != nil {
		return nil, ErrTouchedDaysUnavailable
	}
	return repositories, nil
}

// MarkDispatched appends the 'dispatched' events of one fan-out, all one
// millisecond before the time its read of the pending days was taken. A
// 'touched' event at or after the time of the read stays newer, so its key
// stays pending.
//
// The SELECT of the first statement gives its constants no alias: ClickHouse
// resolves a name to an alias before a column, so "'dispatched' AS kind"
// would make the filter kind = 'touched' false for every row.
//
// fullDays are the days a run of every repository was started for: each of
// their keys touched at or before the time is marked. keys are the exact keys
// of the days a run of listed repositories was started for: a key of such a
// day that the fan-out did not list is not marked.
func (store *ClickHouseTouchedDaysStore) MarkDispatched(
	ctx context.Context, organizationID string, at time.Time, fullDays []time.Time, keys []TouchedDayKey,
) error {
	if store == nil || store.conn == nil || organizationID == "" || at.IsZero() {
		return ErrTouchedDaysUnavailable
	}
	// A 'touched' event written after the read has a time at or after the
	// time of the read (ClickHouse clock), and a key is pending only when its
	// newest 'touched' is strictly newer than its newest 'dispatched'. The
	// mark therefore stamps 'dispatched' one millisecond before the read and
	// ends only events at or before that: an event of the same millisecond as
	// the read, which this fan-out may not have seen, stays pending. The cost
	// is one more recompute of a key touched in that millisecond.
	at = at.Add(-time.Millisecond)
	if len(fullDays) > 0 {
		if err := store.conn.Exec(ctx, `
INSERT INTO daily_metrics_touched_days (org_id, day, repo_id, kind, at)
SELECT org_id, day, repo_id, 'dispatched', fromUnixTimestamp64Milli(toInt64(?), 'UTC')
FROM daily_metrics_touched_days
WHERE org_id = ? AND kind = 'touched' AND has(?, toString(day))
  AND at <= fromUnixTimestamp64Milli(toInt64(?), 'UTC')
GROUP BY org_id, day, repo_id`,
			at.UnixMilli(), organizationID, touchedDayStrings(fullDays), at.UnixMilli()); err != nil {
			return ErrTouchedDaysUnavailable
		}
	}
	if len(keys) == 0 {
		return nil
	}
	batch, err := store.conn.PrepareBatch(ctx,
		`INSERT INTO daily_metrics_touched_days (org_id, day, repo_id, kind, at)`)
	if err != nil {
		return ErrTouchedDaysUnavailable
	}
	defer func() { _ = batch.Abort() }()
	for _, key := range keys {
		repositoryID, parseErr := uuid.Parse(key.RepositoryID)
		if parseErr != nil {
			return ErrTouchedDaysUnavailable
		}
		if err := batch.Append(organizationID, utcDay(key.Day), repositoryID, "dispatched", at.UTC()); err != nil {
			return ErrTouchedDaysUnavailable
		}
	}
	if err := batch.Send(); err != nil {
		return ErrTouchedDaysUnavailable
	}
	return nil
}

// TouchedDaysBacklog is one read of the pending days of an organization for
// the drain (CHAOS-8846).
type TouchedDaysBacklog struct {
	// TakenAt is the time of the store's own clock, read before the days.
	TakenAt time.Time
	// Days are the pending days, newest first.
	Days []time.Time
	// OldestTouchedAt is the oldest of the newest 'touched' events of the
	// pending keys of Days: no pending key of Days has waited less than since
	// this time. The engine keeps only the newest 'touched' event of a key, so
	// a key that was touched again shows the later time. It is zero when Days
	// is empty.
	OldestTouchedAt time.Time
	// Truncated is true when older pending days exist that the read did not
	// return.
	Truncated bool
}

// Backlog returns the pending days of the organization, newest first, at most
// limit of them, with the time of the oldest waiting touch.
func (store *ClickHouseTouchedDaysStore) Backlog(
	ctx context.Context, organizationID string, limit int,
) (TouchedDaysBacklog, error) {
	if store == nil || store.conn == nil || organizationID == "" || limit < 1 {
		return TouchedDaysBacklog{}, ErrTouchedDaysUnavailable
	}
	takenAt, err := store.clock(ctx)
	if err != nil {
		return TouchedDaysBacklog{}, err
	}
	rows, err := store.conn.Query(ctx, `
SELECT day, toUnixTimestamp64Milli(min(touched_at))
FROM (
    SELECT day, repo_id, maxIf(at, kind = 'touched') AS touched_at
    FROM daily_metrics_touched_days
    WHERE org_id = ?
    GROUP BY day, repo_id
    HAVING touched_at > maxIf(at, kind = 'dispatched')
)
GROUP BY day
ORDER BY day DESC
LIMIT ?`, organizationID, limit+1)
	if err != nil {
		return TouchedDaysBacklog{}, ErrTouchedDaysUnavailable
	}
	defer rows.Close()
	backlog := TouchedDaysBacklog{TakenAt: takenAt}
	for rows.Next() {
		var (
			day           time.Time
			touchedMillis int64
		)
		if err := rows.Scan(&day, &touchedMillis); err != nil {
			return TouchedDaysBacklog{}, ErrTouchedDaysUnavailable
		}
		if len(backlog.Days) == limit {
			backlog.Truncated = true
			break
		}
		backlog.Days = append(backlog.Days, utcDay(day))
		touchedAt := time.UnixMilli(touchedMillis).UTC()
		if backlog.OldestTouchedAt.IsZero() || touchedAt.Before(backlog.OldestTouchedAt) {
			backlog.OldestTouchedAt = touchedAt
		}
	}
	if err := rows.Err(); err != nil {
		return TouchedDaysBacklog{}, ErrTouchedDaysUnavailable
	}
	return backlog, nil
}

// touchedKeysToReturnSQL selects the keys of one day that a run was started for
// at or before a time and that are not pending now. Its arguments are the
// organization, the day (2006-01-02) and the time in milliseconds.
const touchedKeysToReturnSQL = `
    SELECT org_id, day, repo_id,
           maxIf(at, kind = 'touched') AS touched_at,
           maxIf(at, kind = 'dispatched') AS dispatched_at
    FROM daily_metrics_touched_days
    WHERE org_id = ? AND toString(day) = ?
    GROUP BY org_id, day, repo_id
    HAVING dispatched_at >= touched_at
       AND dispatched_at <= fromUnixTimestamp64Milli(toInt64(?), 'UTC')`

// ReturnToPending makes every key of the day pending again that was marked as
// dispatched at or before failedAt and that is not pending now. The drain
// calls it for a day with a run that ended without a result at failedAt: the
// keys were marked when the run started, and the run computed nothing. It
// returns false when the day had no such key.
//
// The bound on the mark is what makes a second call for the same failure
// append nothing: the run that is started for the returned keys marks them
// after failedAt. It also leaves alone the keys a later run was started for.
// A key of the day that another run computed before failedAt is returned too
// and is computed once more.
//
// The 'touched' event it appends carries failedAt, or one millisecond after
// the key's newest 'dispatched' event when that is later: the two times come
// from two clocks, and the key must be pending after the append whatever
// their skew. A key that is pending already gets no event, so the time a
// pending key has waited does not move.
//
// The SELECT gives its constant no alias, for the reason MarkDispatched names.
func (store *ClickHouseTouchedDaysStore) ReturnToPending(
	ctx context.Context, organizationID string, day time.Time, failedAt time.Time,
) (bool, error) {
	if store == nil || store.conn == nil || organizationID == "" || day.IsZero() || failedAt.IsZero() {
		return false, ErrTouchedDaysUnavailable
	}
	dayKey, failedMillis := day.UTC().Format("2006-01-02"), failedAt.UTC().UnixMilli()
	var keys uint64
	if err := store.conn.QueryRow(ctx, `SELECT count() FROM (`+touchedKeysToReturnSQL+`)`,
		organizationID, dayKey, failedMillis).Scan(&keys); err != nil {
		return false, ErrTouchedDaysUnavailable
	}
	if keys == 0 {
		return false, nil
	}
	if err := store.conn.Exec(ctx, `
INSERT INTO daily_metrics_touched_days (org_id, day, repo_id, kind, at)
SELECT org_id, day, repo_id, 'touched',
       greatest(fromUnixTimestamp64Milli(toInt64(?), 'UTC'), addMilliseconds(dispatched_at, 1))
FROM (`+touchedKeysToReturnSQL+`)`, failedMillis, organizationID, dayKey, failedMillis); err != nil {
		return false, ErrTouchedDaysUnavailable
	}
	return true, nil
}

// clock reads the ClickHouse clock at millisecond precision.
func (store *ClickHouseTouchedDaysStore) clock(ctx context.Context) (time.Time, error) {
	var millis int64
	if err := store.conn.QueryRow(ctx, `SELECT toUnixTimestamp64Milli(now64(3))`).Scan(&millis); err != nil {
		return time.Time{}, ErrTouchedDaysUnavailable
	}
	return time.UnixMilli(millis).UTC(), nil
}

func touchedDayStrings(days []time.Time) []string {
	values := make([]string, 0, len(days))
	for _, day := range days {
		values = append(values, day.UTC().Format("2006-01-02"))
	}
	return values
}

var _ TouchedDaysStore = (*ClickHouseTouchedDaysStore)(nil)
var _ TouchedDaysDrainStore = (*ClickHouseTouchedDaysStore)(nil)

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

// recordTouchedWindowDaysSQL appends one 'touched' event for each day of the
// given list and each repository that a work item written at or after the
// given time belongs to. The days are the windows of the work-items units of
// the run (PostSyncPlan.WorkItemWindowDays).
const recordTouchedWindowDaysSQL = `
INSERT INTO daily_metrics_touched_days (org_id, day, repo_id, kind, at)
SELECT ?, arrayJoin(arrayMap(value -> toDate(value), ?)) AS day, repo_id, 'touched',
       fromUnixTimestamp64Milli(toInt64(?), 'UTC')
FROM (
    SELECT DISTINCT repo_id
    FROM work_items
    WHERE org_id = ? AND last_synced >= fromUnixTimestamp64Milli(toInt64(?), 'UTC')
)`

// recordTouchedWindowDaysPerStatement bounds the day list of one statement:
// the driver writes the list into the statement text.
const recordTouchedWindowDaysPerStatement = 1000

// RecordTouched appends the 'touched' events of the raw rows whose last_synced
// is at or after since, and one for each day of windowDays and each
// repository of those work items. It returns the number of keys it appended.
// A key that both parts name is one key.
func (store *ClickHouseTouchedDaysStore) RecordTouched(
	ctx context.Context, organizationID string, since time.Time, windowDays []time.Time,
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
	for start := 0; start < len(windowDays); start += recordTouchedWindowDaysPerStatement {
		end := min(start+recordTouchedWindowDaysPerStatement, len(windowDays))
		days := make([]string, 0, end-start)
		for _, day := range windowDays[start:end] {
			if day.IsZero() {
				return 0, ErrTouchedDaysUnavailable
			}
			days = append(days, day.UTC().Format("2006-01-02"))
		}
		if err := store.conn.Exec(ctx, recordTouchedWindowDaysSQL,
			organizationID, days, at.UnixMilli(), organizationID, sinceMillis,
		); err != nil {
			return 0, ErrTouchedDaysUnavailable
		}
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

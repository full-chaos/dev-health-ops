package syncdispatchruntime

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// ErrTouchedDaysUnavailable is returned when the touched-day record cannot be
// read or written. A caller never reads it as "no day was touched".
var ErrTouchedDaysUnavailable = errors.New("touched-day record is unavailable")

// ClickHouseTouchedDaysStore keeps the record of the (organization, day,
// repository) keys that stored raw work-item rows, commits and pull requests
// touched, in the table daily_metrics_touched_days (CHAOS-8813, CHAOS-9169).
//
// Every time in the table is the ClickHouse clock. A key is pending while its
// newest 'touched' event is newer than its newest 'dispatched' event.
type ClickHouseTouchedDaysStore struct {
	conn driver.Conn
	// lane is the pair of event kinds the pending, mark, return and check
	// reads and writes use. The zero value is the touched-day lane.
	lane touchedKinds
}

// touchedKinds is the pair of event kinds of one lane of the record.
type touchedKinds struct{ touched, dispatched string }

// The lanes of the record. The touched-day lane holds the days raw rows
// touched (every family of the day is computed again); the testops window
// lane holds the days after a retraction whose trailing window holds it
// (only the families that read it are computed again).
var (
	touchedDaysKinds    = touchedKinds{touched: "touched", dispatched: "dispatched"}
	testopsWindowKinds  = touchedKinds{touched: "touched_testops_window", dispatched: "dispatched_testops_window"}
	touchedKindReplacer = func(kinds touchedKinds) *strings.Replacer {
		return strings.NewReplacer("{touched}", "'"+kinds.touched+"'", "{dispatched}", "'"+kinds.dispatched+"'")
	}
)

// TestopsWindowLane returns the store of the testops window lane: the same
// table, the kinds 'touched_testops_window' and 'dispatched_testops_window'.
func (store *ClickHouseTouchedDaysStore) TestopsWindowLane() *ClickHouseTouchedDaysStore {
	if store == nil {
		return nil
	}
	return &ClickHouseTouchedDaysStore{conn: store.conn, lane: testopsWindowKinds}
}

func (store *ClickHouseTouchedDaysStore) kinds() touchedKinds {
	if store.lane == (touchedKinds{}) {
		return touchedDaysKinds
	}
	return store.lane
}

// laneSQL writes the kinds of the store's lane into a statement: {touched}
// and {dispatched} are the quoted constants of the lane, never input.
func (store *ClickHouseTouchedDaysStore) laneSQL(query string) string {
	return touchedKindReplacer(store.kinds()).Replace(query)
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

// recordTouchedRangeSQL appends one 'touched' event for every day from the day
// of a NEW event to the day its row was written (CHAOS-8855). The days between
// show the item in the wrong state until the row is written: an item that
// completed on 08-15 and is written on 08-20 was counted as open on 08-16 to
// 08-19, and the event day alone does not touch them.
//
// An event is new when its time is later than the previous record of its
// repository: the newest 'touched' event of the repository older than the lower
// bound of this read (the start of the sync run), so the record of a run that
// started later never hides the late event of a run that started earlier. An
// old item that is only written again makes nothing new known and adds no day.
// A repository with no previous record in the last 366 days has no new event
// (its first sync computes its days by the windows of its units). The range
// covers at most 366 day keys, the write day and the 365 days before it, the
// same bound as a backfill unit (WorkItemsUnitWindowDays refuses a 367th day).
// The previous record is found by its time `at`, not by the day it touched: a
// sync that wrote only old items leaves a record with a recent `at` and an old
// day, and it is the previous record all the same. The read is bounded by the
// organization, the kind and the 366 days of `at` before the lower bound of the
// read; the sort key is (org, day, repo, kind), so it reads the 'touched' rows
// of the organization and not the whole table.
//
// The days of the event itself are the part of recordTouchedDaysSQL; this
// statement adds the days after it. A transition is read under the repository
// of its work item, as there.
const recordTouchedRangeSQL = `
INSERT INTO daily_metrics_touched_days (org_id, day, repo_id, kind, at)
SELECT ?, day, repo_id, 'touched', fromUnixTimestamp64Milli(toInt64(?), 'UTC')
FROM (
    SELECT events.repo_id AS repo_id,
           arrayJoin(arrayMap(value -> toDate(value),
               range(toUInt32(greatest(toDate(events.event_time, 'UTC'), events.write_day - 365)),
                     toUInt32(events.write_day) + 1))) AS day
    FROM (
        SELECT repo_id,
               arrayJoin(arrayMap(value -> assumeNotNull(value), arrayFilter(value -> value IS NOT NULL,
                   [toDateTime64(created_at, 3, 'UTC'), toDateTime64(started_at, 3, 'UTC'),
                    toDateTime64(completed_at, 3, 'UTC'), toDateTime64(closed_at, 3, 'UTC')]))) AS event_time,
               toDate(last_synced, 'UTC') AS write_day
        FROM work_items
        WHERE org_id = ? AND last_synced >= fromUnixTimestamp64Milli(toInt64(?), 'UTC')
        UNION ALL
        SELECT if(items.work_item_id = '', transitions.repo_id, items.repo_id) AS repo_id,
               toDateTime64(transitions.occurred_at, 3, 'UTC') AS event_time,
               toDate(transitions.last_synced, 'UTC') AS write_day
        FROM (
            SELECT work_item_id, repo_id, occurred_at, last_synced
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
    ) AS events
    INNER JOIN (
        SELECT repo_id, max(at) AS previous_at
        FROM daily_metrics_touched_days
        WHERE org_id = ? AND kind = 'touched'
          AND at >= fromUnixTimestamp64Milli(toInt64(?), 'UTC') - INTERVAL 366 DAY
          AND at < fromUnixTimestamp64Milli(toInt64(?), 'UTC')
        GROUP BY repo_id
    ) AS previous ON previous.repo_id = events.repo_id
    WHERE events.event_time > previous.previous_at
      AND toDate(events.event_time, 'UTC') < events.write_day
)`

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

// recordTouchedWindowReposSQL counts the repositories the window statement
// multiplies the days by.
const recordTouchedWindowReposSQL = `
SELECT uniqExact(repo_id)
FROM work_items
WHERE org_id = ? AND last_synced >= fromUnixTimestamp64Milli(toInt64(?), 'UTC')`

// recordTouchedWindowDaysPerStatement bounds the day list of one statement:
// the driver writes the list into the statement text.
const recordTouchedWindowDaysPerStatement = 1000

// TouchedWindowKeysBound is the number of window marks (days x repositories)
// that one record is expected to stay within: a 366-day backfill of 1000
// repositories, 366,000 rows. Above it every mark is still recorded (a dropped
// mark is a lost day) and the fan-out logs an error.
const TouchedWindowKeysBound = 366 * 1000

// TouchedWindowReport is the cost of the window marks of one RecordTouched.
type TouchedWindowReport struct {
	Days  int
	Repos int
	// Keys is days x repositories, the rows the window statement appends
	// before any merge collapses repeats.
	Keys int
}

// OverBound reports whether the window marks are above TouchedWindowKeysBound.
func (report TouchedWindowReport) OverBound() bool { return report.Keys > TouchedWindowKeysBound }

type touchedWindowSinkKey struct{}

// WithTouchedWindowSink returns a context that makes RecordTouched store the
// cost of its window marks in sink.
func WithTouchedWindowSink(ctx context.Context, sink *TouchedWindowReport) context.Context {
	return context.WithValue(ctx, touchedWindowSinkKey{}, sink)
}

// RecordTouched appends the 'touched' events of the raw rows whose last_synced
// is at or after since, and one for each day of windowDays and each
// repository of those work items. It returns the number of keys it appended.
// A key that both parts name is one key.
//
// A retry of the same fan-out appends the window marks again with a newer
// `at`, so keys already dispatched are pending once more: one extra daily
// recompute, never a lost day. The write is not skipped for a key that is
// pending already: a concurrent dispatch can take that key at a time older
// than this run's rows, and the skipped mark would then be lost. Repeated rows
// of a key collapse in the table (ReplacingMergeTree), so the row count does
// not grow after a merge. The cost of one record is days x repositories rows,
// reported through WithTouchedWindowSink.
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
	if err := store.conn.Exec(ctx, recordTouchedRangeSQL,
		organizationID, at.UnixMilli(),
		organizationID, sinceMillis,
		organizationID, sinceMillis,
		organizationID, organizationID, sinceMillis,
		organizationID, sinceMillis, sinceMillis,
	); err != nil {
		return 0, ErrTouchedDaysUnavailable
	}
	if len(windowDays) > 0 {
		var repos uint64
		if err := store.conn.QueryRow(ctx, recordTouchedWindowReposSQL, organizationID, sinceMillis).Scan(&repos); err != nil {
			return 0, ErrTouchedDaysUnavailable
		}
		if sink, ok := ctx.Value(touchedWindowSinkKey{}).(*TouchedWindowReport); ok && sink != nil {
			*sink = TouchedWindowReport{Days: len(windowDays), Repos: int(repos), Keys: len(windowDays) * int(repos)}
		}
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

// recordTouchedGitSQL appends one 'touched' event for each (day, repository)
// that a commit, a pull request, a pull request review or the stats of a commit
// written at or after the given time belongs to (CHAOS-9169, CHAOS-14673). A late or old row marks its own day however far the day lies
// from the window of its sync: no window of the sync's units is needed.
//
// The days are the ones the daily run reads the rows under, not every date the
// row carries: a commit is read by its committer date (repouser, wellbeing), a
// pull request by its creation day and, when merged, its merge day. The author
// date of a rebased commit is not one of them, and a day no run reads the row
// under would only start a run that computes nothing new. A review is read by
// its own submission day, not by the day of its pull request: a review of day D
// on an older open pull request marks D, and the pull request row re-written
// with it marks only its own days. The line counts of a commit are read under
// the committer day of the commit, and a later sync can write them after the
// commit: a stats row written in the run marks the committer day of its commit
// (the commit row is looked up by the stats keys, whatever its own last_synced).
const recordTouchedGitSQL = `
INSERT INTO daily_metrics_touched_days (org_id, day, repo_id, kind, at)
SELECT ?, day, repo_id, 'touched', fromUnixTimestamp64Milli(toInt64(?), 'UTC')
FROM (
    SELECT repo_id, toDate(committer_when, 'UTC') AS day
    FROM git_commits
    WHERE org_id = ? AND last_synced >= fromUnixTimestamp64Milli(toInt64(?), 'UTC')
    UNION ALL
    SELECT repo_id,
           arrayJoin(arrayMap(value -> assumeNotNull(value), arrayFilter(value -> value IS NOT NULL, [
               toDate(created_at, 'UTC'), toDate(merged_at, 'UTC')
           ]))) AS day
    FROM git_pull_requests
    WHERE org_id = ? AND last_synced >= fromUnixTimestamp64Milli(toInt64(?), 'UTC')
    UNION ALL
    SELECT repo_id, toDate(submitted_at, 'UTC') AS day
    FROM git_pull_request_reviews
    WHERE org_id = ? AND last_synced >= fromUnixTimestamp64Milli(toInt64(?), 'UTC')
    UNION ALL
    SELECT repo_id, toDate(committer_when, 'UTC') AS day
    FROM git_commits
    WHERE org_id = ? AND (repo_id, hash) IN (
        SELECT repo_id, commit_hash
        FROM git_commit_stats
        WHERE org_id = ? AND last_synced >= fromUnixTimestamp64Milli(toInt64(?), 'UTC')
    )
)
GROUP BY day, repo_id`

// RecordTouchedGit appends the 'touched' events of the commits, pull requests,
// pull request reviews and commit stats whose last_synced is at or after since. It returns the number of
// keys it appended.
func (store *ClickHouseTouchedDaysStore) RecordTouchedGit(
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
	if err := store.conn.Exec(ctx, recordTouchedGitSQL,
		organizationID, at.UnixMilli(),
		organizationID, sinceMillis,
		organizationID, sinceMillis,
		organizationID, sinceMillis,
		organizationID, organizationID, sinceMillis,
	); err != nil {
		return 0, ErrTouchedDaysUnavailable
	}
	return store.recordedAt(ctx, organizationID, at)
}

// recordedAt counts the keys of the store's lane one record call appended at
// its clock at.
func (store *ClickHouseTouchedDaysStore) recordedAt(ctx context.Context, organizationID string, at time.Time) (uint64, error) {
	var recorded uint64
	if err := store.conn.QueryRow(ctx, store.laneSQL(`
SELECT count()
FROM (
    SELECT day, repo_id
    FROM daily_metrics_touched_days
    WHERE org_id = ? AND kind = {touched} AND at = fromUnixTimestamp64Milli(toInt64(?), 'UTC')
    GROUP BY day, repo_id
)`), organizationID, at.UnixMilli()).Scan(&recorded); err != nil {
		return 0, ErrTouchedDaysUnavailable
	}
	return recorded, nil
}

// recordTouchedTestsSQL touches the day of every test suite row of each
// workflow run (repo_id, run_id) that had a suite row written at or after the
// bound: every attempt, every report slot and the legacy rows of the run. The
// daily run counts only the newest attempt, so a re-run that moves a suite to
// another day changes the day it left as well; that day has only rows written
// before the bound. A suite's day is the UTC day of coalesce(started_at,
// finished_at), the day the daily run reads it under. No FINAL: an older
// version of a re-written key names a day of the same run.
const recordTouchedTestsSQL = `
INSERT INTO daily_metrics_touched_days (org_id, day, repo_id, kind, at)
SELECT ?, day, repo_id, 'touched', fromUnixTimestamp64Milli(toInt64(?), 'UTC')
FROM (
    SELECT repo_id, toDate(coalesce(started_at, finished_at), 'UTC') AS day
    FROM test_suite_results
    WHERE org_id = ? AND coalesce(started_at, finished_at) IS NOT NULL
      AND (repo_id, run_id) IN (
          SELECT repo_id, run_id
          FROM test_suite_results
          WHERE org_id = ? AND last_synced >= fromUnixTimestamp64Milli(toInt64(?), 'UTC')
      )
)
GROUP BY day, repo_id`

// recordTouchedTestopsWindowSQL appends one 'touched_testops_window' event for
// each of the 29 days after every suite day of each run (repo_id, run_id) that
// had a retraction row (retracted = 1) written at or after the bound, up to the
// day of the record. The recurrence of day X reads the history [X-29, X), so a
// retired report slot on day D changes the days D+1..D+29 and no other: only
// the repositories and runs the retraction rows name are marked. The base days
// are every suite day of the run, not the day of the retraction row: a minimal
// retraction row keeps the day of the slot's first suite only, and the readers
// retire the whole slot. No FINAL, as in recordTouchedTestsSQL. A day after the
// record has no stored row to correct: its own run reads the retracted rows.
const recordTouchedTestopsWindowSQL = `
INSERT INTO daily_metrics_touched_days (org_id, day, repo_id, kind, at)
SELECT ?, day, repo_id, 'touched_testops_window', fromUnixTimestamp64Milli(toInt64(?), 'UTC')
FROM (
    SELECT repo_id, addDays(run_day, day_offset) AS day
    FROM (
        SELECT DISTINCT repo_id, toDate(coalesce(started_at, finished_at), 'UTC') AS run_day
        FROM test_suite_results
        WHERE org_id = ? AND coalesce(started_at, finished_at) IS NOT NULL
          AND (repo_id, run_id) IN (
              SELECT repo_id, run_id
              FROM test_suite_results
              WHERE org_id = ? AND retracted = 1 AND last_synced >= fromUnixTimestamp64Milli(toInt64(?), 'UTC')
          )
    )
    ARRAY JOIN range(1, 30) AS day_offset
)
WHERE day <= toDate(fromUnixTimestamp64Milli(toInt64(?), 'UTC'), 'UTC')
GROUP BY day, repo_id`

// TestopsWindowKeysBound is the number of testops window keys one record is
// expected to stay within: the window of one base day for each of the
// repositories one run can list. Above it every key is still recorded (a
// dropped key is a stale day) and the fan-out logs a warning.
const TestopsWindowKeysBound = 29 * 1000

// TestopsWindowReport is what the record of one fan-out wrote to the testops
// window lane.
type TestopsWindowReport struct {
	// Keys is the number of (day, repository) keys the record appended.
	Keys uint64
}

// OverBound reports whether the record is above TestopsWindowKeysBound.
func (report TestopsWindowReport) OverBound() bool { return report.Keys > TestopsWindowKeysBound }

type testopsWindowSinkKey struct{}

// WithTestopsWindowSink returns a context that makes RecordTouchedTests store
// what it wrote to the testops window lane in sink.
func WithTestopsWindowSink(ctx context.Context, sink *TestopsWindowReport) context.Context {
	return context.WithValue(ctx, testopsWindowSinkKey{}, sink)
}

// RecordTouchedTests appends the 'touched' events of the test suite days of
// the runs written at or after since, and the testops window events of the
// runs a retraction row was written for (recordTouchedTestopsWindowSQL). It
// returns the number of 'touched' keys it appended.
func (store *ClickHouseTouchedDaysStore) RecordTouchedTests(
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
	if err := store.conn.Exec(ctx, recordTouchedTestsSQL,
		organizationID, at.UnixMilli(),
		organizationID,
		organizationID, sinceMillis,
	); err != nil {
		return 0, ErrTouchedDaysUnavailable
	}
	if err := store.conn.Exec(ctx, recordTouchedTestopsWindowSQL,
		organizationID, at.UnixMilli(),
		organizationID,
		organizationID, sinceMillis,
		at.UnixMilli(),
	); err != nil {
		return 0, ErrTouchedDaysUnavailable
	}
	if sink, ok := ctx.Value(testopsWindowSinkKey{}).(*TestopsWindowReport); ok && sink != nil {
		window, err := store.TestopsWindowLane().recordedAt(ctx, organizationID, at)
		if err != nil {
			return 0, err
		}
		*sink = TestopsWindowReport{Keys: window}
	}
	return store.recordedAt(ctx, organizationID, at)
}

// pendingTouchedKeysSQL is the reader contract of the table: aggregate for
// each key, never read the rows as they are (merges are eventual).
const pendingTouchedKeysSQL = `
    SELECT day, repo_id
    FROM daily_metrics_touched_days
    WHERE org_id = ? AND kind IN ({touched}, {dispatched})
    GROUP BY day, repo_id
    HAVING maxIf(at, kind = {touched}) > maxIf(at, kind = {dispatched})`

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
	rows, err := store.conn.Query(ctx, store.laneSQL(`
SELECT day, count()
FROM (`+pendingTouchedKeysSQL+`
)
GROUP BY day
ORDER BY day DESC
LIMIT ?`), organizationID, limit+1)
	if err != nil {
		return TouchedDaysPending{}, ErrTouchedDaysUnavailable
	}
	defer rows.Close()
	pending := TouchedDaysPending{TakenAt: takenAt, Keys: map[string]int{}}
	for rows.Next() {
		var (
			day  time.Time
			keys uint64
		)
		if err := rows.Scan(&day, &keys); err != nil {
			return TouchedDaysPending{}, ErrTouchedDaysUnavailable
		}
		if len(pending.Days) == limit {
			pending.Truncated = true
			break
		}
		pending.Days = append(pending.Days, utcDay(day))
		pending.Keys[utcDay(day).Format("2006-01-02")] = int(keys)
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
	rows, err := store.conn.Query(ctx, store.laneSQL(`
SELECT toString(day), arraySort(groupUniqArray(?)(toString(repo_id)))
FROM (`+pendingTouchedKeysSQL+`
)
WHERE has(?, toString(day))
GROUP BY day`), limitPerDay, organizationID, touchedDayStrings(days))
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
		if err := store.conn.Exec(ctx, store.laneSQL(`
INSERT INTO daily_metrics_touched_days (org_id, day, repo_id, kind, at)
SELECT org_id, day, repo_id, {dispatched}, fromUnixTimestamp64Milli(toInt64(?), 'UTC')
FROM daily_metrics_touched_days
WHERE org_id = ? AND kind = {touched} AND has(?, toString(day))
  AND at <= fromUnixTimestamp64Milli(toInt64(?), 'UTC')
GROUP BY org_id, day, repo_id`),
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
		if err := batch.Append(organizationID, utcDay(key.Day), repositoryID, store.kinds().dispatched, at.UTC()); err != nil {
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
	// Keys are the pending keys of each day of Days (2006-01-02), at the read.
	Keys map[string]int
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
	rows, err := store.conn.Query(ctx, store.laneSQL(`
SELECT day, toUnixTimestamp64Milli(min(touched_at)), count()
FROM (
    SELECT day, repo_id, maxIf(at, kind = {touched}) AS touched_at
    FROM daily_metrics_touched_days
    WHERE org_id = ? AND kind IN ({touched}, {dispatched})
    GROUP BY day, repo_id
    HAVING touched_at > maxIf(at, kind = {dispatched})
)
GROUP BY day
ORDER BY day DESC
LIMIT ?`), organizationID, limit+1)
	if err != nil {
		return TouchedDaysBacklog{}, ErrTouchedDaysUnavailable
	}
	defer rows.Close()
	backlog := TouchedDaysBacklog{TakenAt: takenAt, Keys: map[string]int{}}
	for rows.Next() {
		var (
			day           time.Time
			touchedMillis int64
			keys          uint64
		)
		if err := rows.Scan(&day, &touchedMillis, &keys); err != nil {
			return TouchedDaysBacklog{}, ErrTouchedDaysUnavailable
		}
		if len(backlog.Days) == limit {
			backlog.Truncated = true
			break
		}
		backlog.Days = append(backlog.Days, utcDay(day))
		backlog.Keys[utcDay(day).Format("2006-01-02")] = int(keys)
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

// TouchedRunKeys is one daily run and the keys of the touched-day record it
// lists, as the run state (Postgres) holds them.
type TouchedRunKeys struct {
	RunID string
	Day   time.Time
	// FullOrganization is true for a run of every repository: it lists every
	// key of its day but the ones of ExceptRepositoryIDs.
	FullOrganization bool
	// RepositoryIDs are the listed repositories of a run that is not a run of
	// every repository.
	RepositoryIDs []string
	// ExceptRepositoryIDs are the repositories a run of every repository does
	// not list, because a newer run lists them.
	ExceptRepositoryIDs []string
	// TakenAt is the take time of the run: the ClickHouse time read before the
	// pending days the run was started for, recorded with the run. Zero when
	// the run has none.
	TakenAt time.Time
}

// touchedReturnKeysPerStatement bounds the keys one statement of
// ReturnToPending names.
const touchedReturnKeysPerStatement = 10000

// touchedMarkedKeysSQL selects the keys of the organization that are not
// pending now (a run was started for them), among the given days. %s is one
// more condition on the key. Its arguments are the organization, the days
// (2006-01-02) and the arguments of the condition.
//
// A key with no touch of the lane is never selected (touched_at > 0): a key
// of another lane only is no key of this one, and with no event of the lane
// both times read as zero.
const touchedMarkedKeysSQL = `
    SELECT org_id, day, repo_id,
           maxIf(at, kind = {touched}) AS touched_at,
           maxIf(at, kind = {dispatched}) AS dispatched_at
    FROM daily_metrics_touched_days
    WHERE org_id = ? AND kind IN ({touched}, {dispatched}) AND has(?, toString(day))
    GROUP BY org_id, day, repo_id
    HAVING touched_at > toDateTime64(0, 3, 'UTC') AND dispatched_at >= touched_at AND %s`

// ReturnToPending makes the given keys pending again that are not pending
// now, and returns the number of days it did that for. The drain calls it with
// the keys whose owner run ended without a result: they were marked when that
// run started, and the run computed nothing.
//
// The keys are named by the caller, from the list of the run. No time of a run
// is an argument, so no clock of the run state is compared with a time of this
// table. The organization is a condition of every statement: the keys of
// another organization with the same day and repository are never written.
//
// The 'touched' event it appends carries the ClickHouse time of the append, or
// one millisecond after the key's newest 'dispatched' event when that is
// later: the key must be pending after the append also when its mark carries a
// time ahead of the clock. A key that is pending already gets no event, so the
// time a pending key has waited does not move.
//
// A second call for the same run appends nothing while its keys are pending.
// When a run was started for them again, that run is their owner and the
// caller does not name them for the first one.
//
// The SELECT gives its constant no alias, for the reason MarkDispatched names.
func (store *ClickHouseTouchedDaysStore) ReturnToPending(
	ctx context.Context, organizationID string, runs []TouchedRunKeys,
) (int, error) {
	if store == nil || store.conn == nil || organizationID == "" {
		return 0, ErrTouchedDaysUnavailable
	}
	returned := map[string]struct{}{}
	var (
		days []string
		keys []string
	)
	flush := func() error {
		if len(keys) == 0 {
			return nil
		}
		err := store.returnMarkedKeys(ctx, organizationID, days,
			`has(?, concat(toString(day), '|', toString(repo_id)))`, keys, returned)
		days, keys = nil, nil
		return err
	}
	for _, run := range runs {
		if run.Day.IsZero() {
			return 0, ErrTouchedDaysUnavailable
		}
		day := run.Day.UTC().Format("2006-01-02")
		if run.FullOrganization {
			except := run.ExceptRepositoryIDs
			if except == nil {
				except = []string{}
			}
			if err := store.returnMarkedKeys(ctx, organizationID, []string{day},
				`NOT has(?, toString(repo_id))`, except, returned); err != nil {
				return 0, err
			}
			continue
		}
		for _, repositoryID := range run.RepositoryIDs {
			parsed, err := uuid.Parse(repositoryID)
			if err != nil {
				return 0, ErrTouchedDaysUnavailable
			}
			keys = append(keys, day+"|"+parsed.String())
		}
		days = append(days, day)
		if len(keys) >= touchedReturnKeysPerStatement {
			if err := flush(); err != nil {
				return 0, err
			}
		}
	}
	if err := flush(); err != nil {
		return 0, err
	}
	return len(returned), nil
}

// returnMarkedKeys appends one 'touched' event for each key of days that is
// not pending and that condition (with its one array argument) selects, and
// adds the days of those keys to returned.
func (store *ClickHouseTouchedDaysStore) returnMarkedKeys(
	ctx context.Context, organizationID string, days []string, condition string, argument []string,
	returned map[string]struct{},
) error {
	marked := store.laneSQL(fmt.Sprintf(touchedMarkedKeysSQL, condition))
	rows, err := store.conn.Query(ctx, `SELECT DISTINCT toString(day) FROM (`+marked+`)`,
		organizationID, days, argument)
	if err != nil {
		return ErrTouchedDaysUnavailable
	}
	var found []string
	for rows.Next() {
		var day string
		if err := rows.Scan(&day); err != nil {
			_ = rows.Close()
			return ErrTouchedDaysUnavailable
		}
		found = append(found, day)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return ErrTouchedDaysUnavailable
	}
	if len(found) == 0 {
		return nil
	}
	if err := store.conn.Exec(ctx, store.laneSQL(`
INSERT INTO daily_metrics_touched_days (org_id, day, repo_id, kind, at)
SELECT org_id, day, repo_id, {touched},
       greatest(now64(3, 'UTC'), addMilliseconds(dispatched_at, 1))
FROM (`)+marked+`)`, organizationID, days, argument); err != nil {
		return ErrTouchedDaysUnavailable
	}
	for _, day := range found {
		returned[day] = struct{}{}
	}
	return nil
}

// DaysPendingSinceBeforeTake returns the days (UTC, ascending, distinct) of
// the keys that the given runs list and that are pending although they were
// last touched strictly before the take time of the run that lists them.
//
// The mark of a run ends every key it lists that was touched before its take
// time (it stamps one millisecond before it), so such a key says that the mark
// did not reach the table. A key last touched at or after the take time was
// touched again while the run ran: it is pending for a reason of its own. A
// run of every repository lists every key of its day. A run with no take time
// is skipped.
//
// Two statements read the table once each: the first for the days of the runs
// of every repository (the oldest pending touch of each), the second for the
// keys of the runs of listed repositories. The comparison with the take times
// is made here, where it can be read.
func (store *ClickHouseTouchedDaysStore) DaysPendingSinceBeforeTake(
	ctx context.Context, organizationID string, runs []TouchedRunKeys,
) ([]time.Time, error) {
	if store == nil || store.conn == nil || organizationID == "" {
		return nil, ErrTouchedDaysUnavailable
	}
	var (
		fullDayTakes = map[string]int64{}
		listedTakes  = map[string][]int64{}
		fullDays     []string
		listedDays   []string
		listedKeys   []string
	)
	for _, run := range runs {
		if run.TakenAt.IsZero() {
			continue
		}
		day, take := run.Day.UTC().Format("2006-01-02"), run.TakenAt.UnixMilli()
		if run.FullOrganization {
			if newest, seen := fullDayTakes[day]; !seen {
				fullDays = append(fullDays, day)
				fullDayTakes[day] = take
			} else if take > newest {
				fullDayTakes[day] = take
			}
			continue
		}
		for _, repositoryID := range run.RepositoryIDs {
			parsed, err := uuid.Parse(repositoryID)
			if err != nil {
				return nil, ErrTouchedDaysUnavailable
			}
			key := day + "|" + parsed.String()
			if _, seen := listedTakes[key]; !seen {
				listedKeys = append(listedKeys, key)
				listedDays = append(listedDays, day)
			}
			listedTakes[key] = append(listedTakes[key], take)
		}
	}
	found := map[string]struct{}{}
	if len(fullDays) > 0 {
		rows, err := store.conn.Query(ctx, store.laneSQL(`
SELECT toString(day), toUnixTimestamp64Milli(min(touched_at))
FROM (
    SELECT day, repo_id, maxIf(at, kind = {touched}) AS touched_at
    FROM daily_metrics_touched_days
    WHERE org_id = ? AND kind IN ({touched}, {dispatched}) AND has(?, toString(day))
    GROUP BY day, repo_id
    HAVING touched_at > maxIf(at, kind = {dispatched})
)
GROUP BY day`), organizationID, fullDays)
		if err != nil {
			return nil, ErrTouchedDaysUnavailable
		}
		for rows.Next() {
			var (
				day         string
				touchedMill int64
			)
			if err := rows.Scan(&day, &touchedMill); err != nil {
				_ = rows.Close()
				return nil, ErrTouchedDaysUnavailable
			}
			if touchedMill < fullDayTakes[day] {
				found[day] = struct{}{}
			}
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			return nil, ErrTouchedDaysUnavailable
		}
	}
	if len(listedKeys) > 0 {
		rows, err := store.conn.Query(ctx, store.laneSQL(`
SELECT toString(day), toString(repo_id), toUnixTimestamp64Milli(touched_at)
FROM (
    SELECT day, repo_id, maxIf(at, kind = {touched}) AS touched_at
    FROM daily_metrics_touched_days
    WHERE org_id = ? AND kind IN ({touched}, {dispatched}) AND has(?, toString(day))
      AND has(?, concat(toString(day), '|', toString(repo_id)))
    GROUP BY day, repo_id
    HAVING touched_at > maxIf(at, kind = {dispatched})
)`), organizationID, listedDays, listedKeys)
		if err != nil {
			return nil, ErrTouchedDaysUnavailable
		}
		for rows.Next() {
			var (
				day, repo   string
				touchedMill int64
			)
			if err := rows.Scan(&day, &repo, &touchedMill); err != nil {
				_ = rows.Close()
				return nil, ErrTouchedDaysUnavailable
			}
			for _, take := range listedTakes[day+"|"+repo] {
				if touchedMill < take {
					found[day] = struct{}{}
					break
				}
			}
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			return nil, ErrTouchedDaysUnavailable
		}
	}
	days := make([]time.Time, 0, len(found))
	for day := range found {
		parsed, err := time.Parse("2006-01-02", day)
		if err != nil {
			return nil, ErrTouchedDaysUnavailable
		}
		days = append(days, parsed.UTC())
	}
	sort.Slice(days, func(left, right int) bool { return days[left].Before(days[right]) })
	return days, nil
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

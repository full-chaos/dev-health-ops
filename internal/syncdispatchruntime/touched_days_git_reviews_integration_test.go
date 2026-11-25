//go:build integration

package syncdispatchruntime

import (
	"context"
	"crypto/sha256"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

type touchedTestReview struct {
	org      string
	repo     uuid.UUID
	number   uint32
	id       string
	reviewer string
	at       time.Time
	synced   time.Time
}

func insertTouchedTestReviews(t *testing.T, ctx context.Context, conn driver.Conn, reviews ...touchedTestReview) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO git_pull_request_reviews (
		repo_id, number, review_id, reviewer, state, submitted_at, org_id, last_synced)`)
	if err != nil {
		t.Fatal(err)
	}
	for _, review := range reviews {
		if err := batch.Append(review.repo, review.number, review.id, review.reviewer, "APPROVED", review.at, review.org, review.synced); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
}

// recordedTouchedTestKeys reads the keys one record appended, whatever their
// pending state, sorted, as "2006-01-02|repository".
func recordedTouchedTestKeys(t *testing.T, ctx context.Context, conn driver.Conn, org string) []string {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT DISTINCT toString(day), toString(repo_id) FROM daily_metrics_touched_days
		WHERE org_id = ? AND kind = 'touched'`, org)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	keys := []string{}
	for rows.Next() {
		var day, repo string
		if err := rows.Scan(&day, &repo); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, day+"|"+repo)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	sort.Strings(keys)
	return keys
}

// CHAOS-14673: a review marks its OWN submission day, however old its pull
// request is and whatever days the re-written pull request row marks. A review
// written before the lower bound of the read, and a review of another
// organization, mark nothing; several reviews of one day mark it once.
func TestTouchedDaysRecordHoldsTheSubmissionDayOfTheReviewsOfTheRun(t *testing.T) {
	ctx, conn := newReadbackIntegrationConn(t)
	store, err := NewClickHouseTouchedDaysStore(conn)
	if err != nil {
		t.Fatal(err)
	}
	inRun := touchedTestSince.Add(time.Hour)
	beforeRun := touchedTestSince.Add(-time.Millisecond)
	// The pull request is old and open; its row is re-written in this run and
	// marks its creation day only.
	insertTouchedTestPullRequests(t, ctx, conn,
		touchedTestPullRequest{org: touchedTestOrg, repo: touchedTestRepoA, number: 7, created: touchedTestDay(time.June, 2), synced: inRun},
	)
	insertTouchedTestReviews(t, ctx, conn,
		touchedTestReview{org: touchedTestOrg, repo: touchedTestRepoA, number: 7, id: "r1", reviewer: "dev-a@example.com", at: touchedTestDay(time.August, 20), synced: inRun},
		touchedTestReview{org: touchedTestOrg, repo: touchedTestRepoA, number: 7, id: "r2", reviewer: "dev-c@example.com", at: touchedTestDay(time.August, 20).Add(time.Hour), synced: inRun},
		touchedTestReview{org: touchedTestOrg, repo: touchedTestRepoB, number: 3, id: "r3", reviewer: "dev-a@example.com", at: touchedTestDay(time.August, 21), synced: touchedTestSince},
		touchedTestReview{org: touchedTestOrg, repo: touchedTestRepoB, number: 3, id: "r4", reviewer: "dev-a@example.com", at: touchedTestDay(time.August, 22), synced: beforeRun},
		touchedTestReview{org: touchedTestOtherOrg, repo: touchedTestRepoA, number: 7, id: "r5", reviewer: "dev-a@example.com", at: touchedTestDay(time.August, 23), synced: inRun},
	)
	want := []string{
		touchedTestKey(time.June, 2, touchedTestRepoA),
		touchedTestKey(time.August, 20, touchedTestRepoA),
		touchedTestKey(time.August, 21, touchedTestRepoB),
	}
	sort.Strings(want)
	if _, err := store.RecordTouchedGit(ctx, touchedTestOrg, touchedTestSince, false); err != nil {
		t.Fatal(err)
	}
	if got := pendingTouchedTestKeys(t, ctx, conn, touchedTestOrg); !reflect.DeepEqual(got, want) {
		t.Fatalf("pending keys\n got %v\nwant %v", got, want)
	}
	if got := pendingTouchedTestKeys(t, ctx, conn, touchedTestOtherOrg); len(got) != 0 {
		t.Fatalf("the other organization was not recorded, so it has no key; got %v", got)
	}
}

type touchedTestStat struct {
	org    string
	repo   uuid.UUID
	hash   string
	file   string
	synced time.Time
}

func insertTouchedTestStats(t *testing.T, ctx context.Context, conn driver.Conn, stats ...touchedTestStat) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO git_commit_stats (
		repo_id, commit_hash, file_path, additions, deletions, org_id, last_synced)`)
	if err != nil {
		t.Fatal(err)
	}
	for _, stat := range stats {
		if err := batch.Append(stat.repo, stat.hash, stat.file, int32(3), int32(1), stat.org, stat.synced); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
}

// CHAOS-14673: the stats of a commit written in the run mark the COMMITTER day of
// their commit, however old the commit row is: the commit row's own last_synced
// is no part of the read. Stats written before the lower bound, stats of another
// organization (even for the same repository and hash), and stats whose commit
// is not in the organization's commit table mark nothing; several files of one
// commit mark its day once.
func TestTouchedDaysRecordHoldsTheCommitDayOfTheStatsOfTheRun(t *testing.T) {
	ctx, conn := newReadbackIntegrationConn(t)
	store, err := NewClickHouseTouchedDaysStore(conn)
	if err != nil {
		t.Fatal(err)
	}
	inRun := touchedTestSince.Add(time.Hour)
	beforeRun := touchedTestSince.Add(-time.Millisecond)
	longAgo := touchedTestSince.Add(-48 * time.Hour)
	insertTouchedTestCommits(t, ctx, conn,
		// Old commit rows (written long before the run): only their stats are written in the run.
		touchedTestCommit{org: touchedTestOrg, repo: touchedTestRepoA, hash: "c-in-run", author: touchedTestDay(time.July, 5), committer: touchedTestDay(time.July, 5), synced: longAgo},
		touchedTestCommit{org: touchedTestOrg, repo: touchedTestRepoB, hash: "c-at-bound", author: touchedTestDay(time.July, 6), committer: touchedTestDay(time.July, 6), synced: longAgo},
		touchedTestCommit{org: touchedTestOrg, repo: touchedTestRepoA, hash: "c-stats-old", author: touchedTestDay(time.July, 7), committer: touchedTestDay(time.July, 7), synced: longAgo},
		touchedTestCommit{org: touchedTestOrg, repo: touchedTestRepoA, hash: "c-other-org-stats", author: touchedTestDay(time.July, 8), committer: touchedTestDay(time.July, 8), synced: longAgo},
		// Rebased: the stats mark the committer day, never the author day.
		touchedTestCommit{org: touchedTestOrg, repo: touchedTestRepoB, hash: "c-rebased", author: touchedTestDay(time.January, 2), committer: touchedTestDay(time.July, 9), synced: longAgo},
	)
	insertTouchedTestStats(t, ctx, conn,
		touchedTestStat{org: touchedTestOrg, repo: touchedTestRepoA, hash: "c-in-run", file: "a.go", synced: inRun},
		touchedTestStat{org: touchedTestOrg, repo: touchedTestRepoA, hash: "c-in-run", file: "b.go", synced: inRun},
		touchedTestStat{org: touchedTestOrg, repo: touchedTestRepoB, hash: "c-at-bound", file: "a.go", synced: touchedTestSince},
		touchedTestStat{org: touchedTestOrg, repo: touchedTestRepoA, hash: "c-stats-old", file: "a.go", synced: beforeRun},
		touchedTestStat{org: touchedTestOtherOrg, repo: touchedTestRepoA, hash: "c-other-org-stats", file: "a.go", synced: inRun},
		touchedTestStat{org: touchedTestOrg, repo: touchedTestRepoB, hash: "c-rebased", file: "a.go", synced: inRun},
		// Stats of a commit that is not in the organization's commit table.
		touchedTestStat{org: touchedTestOrg, repo: touchedTestRepoA, hash: "c-unknown", file: "a.go", synced: inRun},
	)
	want := []string{
		touchedTestKey(time.July, 5, touchedTestRepoA),
		touchedTestKey(time.July, 6, touchedTestRepoB),
		touchedTestKey(time.July, 9, touchedTestRepoB),
	}
	sort.Strings(want)
	// A sync that wrote no commit stats does not read the stats table: its stats rows mark nothing.
	if _, err := store.RecordTouchedGit(ctx, touchedTestOrg, touchedTestSince, false); err != nil {
		t.Fatal(err)
	}
	if got := pendingTouchedTestKeys(t, ctx, conn, touchedTestOrg); len(got) != 0 {
		t.Fatalf("a record without commit stats marked %v, want nothing (control)", got)
	}
	if _, err := store.RecordTouchedGit(ctx, touchedTestOrg, touchedTestSince, true); err != nil {
		t.Fatal(err)
	}
	if got := pendingTouchedTestKeys(t, ctx, conn, touchedTestOrg); !reflect.DeepEqual(got, want) {
		t.Fatalf("pending keys\n got %v\nwant %v", got, want)
	}
	if got := pendingTouchedTestKeys(t, ctx, conn, touchedTestOtherOrg); len(got) != 0 {
		t.Fatalf("the other organization has no commit row, so it has no key; got %v", got)
	}
}

// recordTouchedGitSelectAtMain is the SELECT of recordTouchedGitSQL at ops
// 57ac6d4a5613, before the commit stats arm (commits, pull requests, reviews).
const recordTouchedGitSelectAtMain = `
SELECT day, repo_id
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
)
GROUP BY day, repo_id`

// CHAOS-14673 equality control: a sync that wrote commits, pull requests and
// reviews but no commit stats at or after the bound records the same days as the
// record at main, byte for byte, although the store holds older stats and the
// stats of another organization written in the run (for the hash of a commit of
// this organization: a stats lookup that is not scoped by organization would
// mark its day).
func TestTouchedDaysRecordWithoutLateStatsEqualsTheRecordAtMain(t *testing.T) {
	ctx, conn := newReadbackIntegrationConn(t)
	store, err := NewClickHouseTouchedDaysStore(conn)
	if err != nil {
		t.Fatal(err)
	}
	inRun := touchedTestSince.Add(time.Hour)
	beforeRun := touchedTestSince.Add(-time.Millisecond)
	merged := touchedTestDay(time.June, 15)
	insertTouchedTestCommits(t, ctx, conn,
		touchedTestCommit{org: touchedTestOrg, repo: touchedTestRepoA, hash: "late", author: touchedTestDay(time.May, 3), committer: touchedTestDay(time.May, 3), synced: inRun},
		touchedTestCommit{org: touchedTestOrg, repo: touchedTestRepoA, hash: "rebased", author: touchedTestDay(time.March, 1), committer: touchedTestDay(time.May, 4), synced: inRun},
		touchedTestCommit{org: touchedTestOrg, repo: touchedTestRepoA, hash: "older", author: touchedTestDay(time.May, 9), committer: touchedTestDay(time.May, 9), synced: beforeRun},
	)
	insertTouchedTestPullRequests(t, ctx, conn,
		touchedTestPullRequest{org: touchedTestOrg, repo: touchedTestRepoB, number: 7, created: touchedTestDay(time.June, 2), merged: &merged, synced: inRun},
		touchedTestPullRequest{org: touchedTestOrg, repo: touchedTestRepoB, number: 8, created: touchedTestDay(time.June, 3), synced: inRun},
	)
	insertTouchedTestReviews(t, ctx, conn,
		touchedTestReview{org: touchedTestOrg, repo: touchedTestRepoB, number: 7, id: "old", reviewer: "dev-a@example.com", at: touchedTestDay(time.June, 10), synced: beforeRun},
		touchedTestReview{org: touchedTestOrg, repo: touchedTestRepoB, number: 7, id: "late", reviewer: "dev-a@example.com", at: touchedTestDay(time.June, 12), synced: inRun},
		touchedTestReview{org: touchedTestOtherOrg, repo: touchedTestRepoB, number: 7, id: "other", reviewer: "dev-a@example.com", at: touchedTestDay(time.June, 11), synced: inRun},
	)
	insertTouchedTestStats(t, ctx, conn,
		touchedTestStat{org: touchedTestOrg, repo: touchedTestRepoA, hash: "older", file: "a.go", synced: beforeRun},
		touchedTestStat{org: touchedTestOtherOrg, repo: touchedTestRepoA, hash: "older", file: "a.go", synced: inRun},
	)

	legacy := []string{}
	rows, err := conn.Query(ctx, `SELECT toString(day), toString(repo_id) FROM (`+recordTouchedGitSelectAtMain+`)`, touchedTestOrg, touchedTestSince.UnixMilli(), touchedTestOrg, touchedTestSince.UnixMilli(), touchedTestOrg, touchedTestSince.UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var day, repo string
		if err := rows.Scan(&day, &repo); err != nil {
			t.Fatal(err)
		}
		legacy = append(legacy, day+"|"+repo)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	sort.Strings(legacy)
	if len(legacy) != 6 {
		t.Fatalf("the record at main holds %d keys, want 6 (control): %v", len(legacy), legacy)
	}

	if _, err := store.RecordTouchedGit(ctx, touchedTestOrg, touchedTestSince, true); err != nil {
		t.Fatal(err)
	}
	got := recordedTouchedTestKeys(t, ctx, conn, touchedTestOrg)
	if gotSum, wantSum := sha256.Sum256([]byte(strings.Join(got, "\n"))), sha256.Sum256([]byte(strings.Join(legacy, "\n"))); gotSum != wantSum {
		t.Fatalf("the record without late stats differs from the record at main\n got %v\nwant %v", got, legacy)
	}
}

// CHAOS-14673 cost cell: on a synthetic set of 40 commits, 30 pull requests, 60
// reviews and 50 commit stats rows (half of each written in the run), a record call
// without commit stats is exactly 3 statements (the clock, the INSERT ... SELECT,
// the key count) and reads at most the rows of the three git tables once; a call
// with commit stats is exactly 4 statements (one more INSERT ... SELECT) and that
// statement reads at most the stats rows and the commit rows once (one pass, so no
// per-row lookup). The stats table is read by no statement of the first call.
func TestRecordTouchedGitReadsBoundedRowsInThreeOrFourStatements(t *testing.T) {
	ctx, conn := newReadbackIntegrationConn(t)
	store, err := NewClickHouseTouchedDaysStore(conn)
	if err != nil {
		t.Fatal(err)
	}
	const commits, pullRequests, reviews, stats = 40, 30, 60, 50
	inRun := touchedTestSince.Add(time.Hour)
	beforeRun := touchedTestSince.Add(-time.Hour)
	synced := func(i int) time.Time {
		if i%2 == 0 {
			return inRun
		}
		return beforeRun
	}
	var commitRows []touchedTestCommit
	for i := 0; i < commits; i++ {
		when := touchedTestDay(time.May, 1+i%20)
		commitRows = append(commitRows, touchedTestCommit{org: touchedTestOrg, repo: touchedTestRepoA, hash: fmt.Sprintf("c%03d", i), author: when, committer: when, synced: synced(i)})
	}
	insertTouchedTestCommits(t, ctx, conn, commitRows...)
	var prRows []touchedTestPullRequest
	for i := 0; i < pullRequests; i++ {
		prRows = append(prRows, touchedTestPullRequest{org: touchedTestOrg, repo: touchedTestRepoB, number: uint32(1 + i), created: touchedTestDay(time.June, 1+i%20), synced: synced(i)})
	}
	insertTouchedTestPullRequests(t, ctx, conn, prRows...)
	var reviewRows []touchedTestReview
	for i := 0; i < reviews; i++ {
		reviewRows = append(reviewRows, touchedTestReview{org: touchedTestOrg, repo: touchedTestRepoB, number: uint32(1 + i%pullRequests), id: fmt.Sprintf("r%03d", i),
			reviewer: "dev-a@example.com", at: touchedTestDay(time.July, 1+i%25), synced: synced(i)})
	}
	insertTouchedTestReviews(t, ctx, conn, reviewRows...)
	var statRows []touchedTestStat
	for i := 0; i < stats; i++ {
		statRows = append(statRows, touchedTestStat{org: touchedTestOrg, repo: touchedTestRepoA, hash: fmt.Sprintf("c%03d", i%commits), file: fmt.Sprintf("f%d.go", i), synced: synced(i)})
	}
	insertTouchedTestStats(t, ctx, conn, statRows...)

	var database string
	if err := conn.QueryRow(ctx, `SELECT currentDatabase()`).Scan(&database); err != nil {
		t.Fatal(err)
	}
	// The harness may give every test a private scratch database on one shared server
	// (containers/harness.go, paths 1 and 2): the filter names this connection's database.
	measure := func(commitStats bool) (statements, readRows, statsRows uint64) {
		t.Helper()
		var startMicros int64
		if err := conn.QueryRow(ctx, `SELECT toUnixTimestamp64Micro(now64(6))`).Scan(&startMicros); err != nil {
			t.Fatal(err)
		}
		if _, err := store.RecordTouchedGit(ctx, touchedTestOrg, touchedTestSince, commitStats); err != nil {
			t.Fatal(err)
		}
		if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
			t.Fatalf("the query log is not readable by this user: %v", err)
		}
		if err := conn.QueryRow(ctx, `SELECT count(),
			sumIf(read_rows, position(query, 'INSERT INTO daily_metrics_touched_days') > 0),
			sumIf(read_rows, position(query, 'git_commit_stats') > 0)
			FROM system.query_log
			WHERE type = 'QueryFinish' AND is_initial_query AND current_database = ? AND toUnixTimestamp64Micro(event_time_microseconds) >= ?
			  AND position(query, 'system.query_log') = 0
			  AND (position(query, 'daily_metrics_touched_days') > 0 OR position(query, 'toUnixTimestamp64Milli(now64(3))') > 0)`,
			database, startMicros).Scan(&statements, &readRows, &statsRows); err != nil {
			t.Fatalf("the query log has no row for the record: %v", err)
		}
		return statements, readRows, statsRows
	}
	statements, readRows, statsRows := measure(false)
	t.Logf("record without commit stats: statements=%d insert_select_read_rows=%d stats_statement_read_rows=%d", statements, readRows, statsRows)
	if statements != 3 {
		t.Fatalf("a record call without commit stats ran %d statements, want 3", statements)
	}
	if statsRows != 0 {
		t.Fatalf("a record call without commit stats read %d rows through the stats table, want 0", statsRows)
	}
	if readRows == 0 || readRows > commits+pullRequests+reviews {
		t.Fatalf("the INSERT ... SELECT read %d rows, want within (0, %d]: each git table once", readRows, commits+pullRequests+reviews)
	}
	statements, readRows, statsRows = measure(true)
	t.Logf("record with commit stats: statements=%d insert_select_read_rows=%d stats_statement_read_rows=%d", statements, readRows, statsRows)
	if statements != 4 {
		t.Fatalf("a record call with commit stats ran %d statements, want 4", statements)
	}
	if statsRows == 0 || statsRows > stats+commits {
		t.Fatalf("the stats statement read %d rows, want within (0, %d]: the stats once and the commits once", statsRows, stats+commits)
	}
	if readRows > commits+pullRequests+reviews+stats+commits {
		t.Fatalf("the INSERT ... SELECT statements read %d rows, want at most %d", readRows, commits+pullRequests+reviews+stats+commits)
	}
	if got := recordedTouchedTestKeys(t, ctx, conn, touchedTestOrg); len(got) == 0 {
		t.Fatal("the record appended no key (control)")
	}
}

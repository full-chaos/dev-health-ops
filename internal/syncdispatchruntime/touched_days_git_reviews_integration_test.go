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
	if _, err := store.RecordTouchedGit(ctx, touchedTestOrg, touchedTestSince); err != nil {
		t.Fatal(err)
	}
	if got := pendingTouchedTestKeys(t, ctx, conn, touchedTestOrg); !reflect.DeepEqual(got, want) {
		t.Fatalf("pending keys\n got %v\nwant %v", got, want)
	}
	if got := pendingTouchedTestKeys(t, ctx, conn, touchedTestOtherOrg); len(got) != 0 {
		t.Fatalf("the other organization was not recorded, so it has no key; got %v", got)
	}
}

// recordTouchedGitSelectAtMain is the SELECT of recordTouchedGitSQL at ops
// 5dd4758412f2, before the review arm.
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
)
GROUP BY day, repo_id`

// CHAOS-14673 equality control: a sync that wrote commits and pull requests but
// no review at or after the bound records the same days as the record at main,
// byte for byte, although the store holds older reviews and the reviews of
// another organization written in the run.
func TestTouchedDaysRecordWithoutALateReviewEqualsTheRecordAtMain(t *testing.T) {
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
	)
	insertTouchedTestPullRequests(t, ctx, conn,
		touchedTestPullRequest{org: touchedTestOrg, repo: touchedTestRepoB, number: 7, created: touchedTestDay(time.June, 2), merged: &merged, synced: inRun},
		touchedTestPullRequest{org: touchedTestOrg, repo: touchedTestRepoB, number: 8, created: touchedTestDay(time.June, 3), synced: inRun},
	)
	insertTouchedTestReviews(t, ctx, conn,
		touchedTestReview{org: touchedTestOrg, repo: touchedTestRepoB, number: 7, id: "old", reviewer: "dev-a@example.com", at: touchedTestDay(time.June, 10), synced: beforeRun},
		touchedTestReview{org: touchedTestOtherOrg, repo: touchedTestRepoB, number: 7, id: "other", reviewer: "dev-a@example.com", at: touchedTestDay(time.June, 11), synced: inRun},
	)

	legacy := []string{}
	rows, err := conn.Query(ctx, `SELECT toString(day), toString(repo_id) FROM (`+recordTouchedGitSelectAtMain+`)`, touchedTestOrg, touchedTestSince.UnixMilli(), touchedTestOrg, touchedTestSince.UnixMilli())
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
	if len(legacy) != 5 {
		t.Fatalf("the record at main holds %d keys, want 5 (control): %v", len(legacy), legacy)
	}

	if _, err := store.RecordTouchedGit(ctx, touchedTestOrg, touchedTestSince); err != nil {
		t.Fatal(err)
	}
	got := recordedTouchedTestKeys(t, ctx, conn, touchedTestOrg)
	if gotSum, wantSum := sha256.Sum256([]byte(strings.Join(got, "\n"))), sha256.Sum256([]byte(strings.Join(legacy, "\n"))); gotSum != wantSum {
		t.Fatalf("the record without a late review differs from the record at main\n got %v\nwant %v", got, legacy)
	}
}

// CHAOS-14673 cost cell: on a synthetic set of 40 commits, 30 pull requests and
// 60 reviews (half of each written in the run), one record call is exactly 3
// statements (the clock, the INSERT ... SELECT, the key count) and the
// INSERT ... SELECT reads each of the three tables once: no more rows than the
// three tables hold, so the review arm adds no per-pull-request lookup.
func TestRecordTouchedGitReadsEachTableOnceInThreeStatements(t *testing.T) {
	ctx, conn := newReadbackIntegrationConn(t)
	store, err := NewClickHouseTouchedDaysStore(conn)
	if err != nil {
		t.Fatal(err)
	}
	const commits, pullRequests, reviews = 40, 30, 60
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

	var startMicros int64
	if err := conn.QueryRow(ctx, `SELECT toUnixTimestamp64Micro(now64(6))`).Scan(&startMicros); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordTouchedGit(ctx, touchedTestOrg, touchedTestSince); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatalf("the query log is not readable by this user: %v", err)
	}
	// The connection is idle between the two marks, so the statements of the call are
	// the finished initial queries that are the INSERT, the clock read or a read of the record table.
	var statements, readRows uint64
	if err := conn.QueryRow(ctx, `SELECT count(), sumIf(read_rows, query_kind = 'Insert') FROM system.query_log
		WHERE type = 'QueryFinish' AND is_initial_query AND toUnixTimestamp64Micro(event_time_microseconds) >= ?
		  AND position(query, 'system.query_log') = 0
		  AND (query_kind = 'Insert' OR position(query, 'daily_metrics_touched_days') > 0 OR position(query, 'toUnixTimestamp64Milli(now64(3))') > 0)`,
		startMicros).Scan(&statements, &readRows); err != nil {
		t.Fatalf("the query log has no row for the record: %v", err)
	}
	t.Logf("record of the git days: statements=%d insert_select_read_rows=%d (tables hold %d)", statements, readRows, commits+pullRequests+reviews)
	if statements != 3 {
		t.Fatalf("one record call ran %d statements, want 3", statements)
	}
	if readRows == 0 || readRows > commits+pullRequests+reviews {
		t.Fatalf("the INSERT ... SELECT read %d rows, want within (0, %d]: each table once", readRows, commits+pullRequests+reviews)
	}
	if got := recordedTouchedTestKeys(t, ctx, conn, touchedTestOrg); len(got) == 0 {
		t.Fatal("the record appended no key (control)")
	}
}

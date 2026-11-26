package syncdispatchruntime

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type gitRecordFakeConn struct {
	driver.Conn
	execs    []string
	execArgs [][]any
	queries  int
}

func (conn *gitRecordFakeConn) QueryRow(_ context.Context, query string, _ ...any) driver.Row {
	conn.queries++
	if strings.Contains(query, "now64") {
		return windowFakeRow{value: int64(1_700_000_000_000)}
	}
	return windowFakeRow{value: uint64(0)}
}

func (conn *gitRecordFakeConn) Exec(_ context.Context, query string, args ...any) error {
	conn.execs = append(conn.execs, query)
	conn.execArgs = append(conn.execArgs, args)
	return nil
}

// CHAOS-14673: the git record is ONE INSERT ... SELECT with one arm for each of
// the three tables (commits, pull requests, reviews), and every arm is bounded by
// the same last_synced lower bound of the run. An arm that runs as a second
// statement, or without that bound, fails here. A sync that wrote no commit stats
// never names the stats table.
func TestRecordTouchedGitIsOneStatementWithOneBoundedArmPerTable(t *testing.T) {
	conn := &gitRecordFakeConn{}
	store, err := NewClickHouseTouchedDaysStore(conn)
	if err != nil {
		t.Fatal(err)
	}
	since := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	if _, err := store.RecordTouchedGit(context.Background(), "org", since, false); err != nil {
		t.Fatal(err)
	}
	if len(conn.execs) != 1 {
		t.Fatalf("RecordTouchedGit ran %d statements, want 1", len(conn.execs))
	}
	if conn.queries != 2 {
		t.Fatalf("RecordTouchedGit ran %d reads (the clock and the key count), want 2", conn.queries)
	}
	query := conn.execs[0]
	for _, table := range []string{"git_commits", "git_pull_requests", "git_pull_request_reviews"} {
		if got := strings.Count(query, "FROM "+table+"\n"); got != 1 {
			t.Fatalf("the statement reads %s %d times, want 1 arm", table, got)
		}
	}
	if strings.Contains(query, "git_commit_stats") {
		t.Fatal("a sync that wrote no commit stats must not read the stats table")
	}
	if got := strings.Count(query, "UNION ALL"); got != 2 {
		t.Fatalf("the statement has %d UNION ALL, want 2 (three arms)", got)
	}
	if got := strings.Count(query, "last_synced >= fromUnixTimestamp64Milli(toInt64(?), 'UTC')"); got != 3 {
		t.Fatalf("the statement bounds %d arms by last_synced, want 3", got)
	}
	if !strings.Contains(query, "toDate(submitted_at, 'UTC') AS day") {
		t.Fatal("the review arm must key on the review's own submission day")
	}
	args := conn.execArgs[0]
	if len(args) != 8 {
		t.Fatalf("the statement has %d arguments, want 8 (org, at, then org and bound for each of 3 arms)", len(args))
	}
	for arm := 0; arm < 3; arm++ {
		org, bound := args[2+2*arm], args[3+2*arm]
		if org != "org" || bound != since.UnixMilli() {
			t.Fatalf("arm %d arguments = (%v, %v), want (org, %d)", arm, org, bound, since.UnixMilli())
		}
	}
}

// CHAOS-14673: a sync that wrote commit stats runs ONE more statement, the stats
// statement: the committer day of the commits whose stats rows the run wrote,
// bounded by the same lower bound and the same organization on the stats table
// and on the commit lookup.
func TestRecordTouchedGitRunsOneMoreBoundedStatementForCommitStats(t *testing.T) {
	conn := &gitRecordFakeConn{}
	store, err := NewClickHouseTouchedDaysStore(conn)
	if err != nil {
		t.Fatal(err)
	}
	since := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	if _, err := store.RecordTouchedGit(context.Background(), "org", since, true); err != nil {
		t.Fatal(err)
	}
	if len(conn.execs) != 2 {
		t.Fatalf("RecordTouchedGit with commit stats ran %d statements, want 2", len(conn.execs))
	}
	if conn.queries != 2 {
		t.Fatalf("RecordTouchedGit ran %d reads (the clock and the key count), want 2", conn.queries)
	}
	if conn.execs[0] != recordTouchedGitSQL {
		t.Fatal("the first statement must be the git record, unchanged by the stats guard")
	}
	query := conn.execs[1]
	if query != recordTouchedGitStatsSQL {
		t.Fatal("the second statement must be the stats statement")
	}
	if !strings.Contains(query, "toDate(committer_when, 'UTC') AS day\n    FROM git_commits\n    WHERE org_id = ? AND (repo_id, hash) IN (") ||
		!strings.Contains(query, "FROM git_commit_stats\n        WHERE org_id = ? AND last_synced >= fromUnixTimestamp64Milli(toInt64(?), 'UTC')") {
		t.Fatal("the stats statement must key on the committer day and bound the stats table by organization and last_synced")
	}
	want := []any{"org", conn.execArgs[1][1], "org", "org", since.UnixMilli()}
	got := conn.execArgs[1]
	if len(got) != len(want) {
		t.Fatalf("the stats statement has %d arguments, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("stats argument %d = %v, want %v (arguments %v)", i, got[i], want[i], got)
		}
	}
	if conn.execArgs[0][1] != got[1] {
		t.Fatalf("the two statements stamp different events: %v and %v", conn.execArgs[0][1], got[1])
	}
}

// CHAOS-14673: the plan flag reaches the store: a plan with CommitStats asks for
// the stats statement, a plan without it does not.
func TestRecordPlanTouchedDaysPassesTheCommitStatsFlagToTheGitRecord(t *testing.T) {
	started := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	for _, commitStats := range []bool{false, true} {
		store := &recordingTouchedDaysStore{}
		plan := &PostSyncPlan{OrganizationID: "org", Git: true, CommitStats: commitStats, RunStartedAt: started}
		if err := recordPlanTouchedDays(context.Background(), store, plan, &touchedDaysTake{}); err != nil {
			t.Fatal(err)
		}
		if len(store.gitCommitStats) != 1 || store.gitCommitStats[0] != commitStats {
			t.Fatalf("CommitStats=%t: RecordTouchedGit commitStats arguments = %v", commitStats, store.gitCommitStats)
		}
	}
}

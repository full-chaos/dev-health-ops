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
// the four tables (commits, pull requests, reviews, commit stats), and every arm
// is bounded by the same last_synced lower bound of the run. An arm that runs as
// a second statement, or without that bound, fails here.
func TestRecordTouchedGitIsOneStatementWithOneBoundedArmPerTable(t *testing.T) {
	conn := &gitRecordFakeConn{}
	store, err := NewClickHouseTouchedDaysStore(conn)
	if err != nil {
		t.Fatal(err)
	}
	since := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	if _, err := store.RecordTouchedGit(context.Background(), "org", since); err != nil {
		t.Fatal(err)
	}
	if len(conn.execs) != 1 {
		t.Fatalf("RecordTouchedGit ran %d statements, want 1", len(conn.execs))
	}
	if conn.queries != 2 {
		t.Fatalf("RecordTouchedGit ran %d reads (the clock and the key count), want 2", conn.queries)
	}
	query := conn.execs[0]
	// git_commits is read twice: by its own arm, and as the lookup of the commit day of the stats rows.
	for table, want := range map[string]int{"git_commits": 2, "git_pull_requests": 1, "git_pull_request_reviews": 1, "git_commit_stats": 1} {
		if got := strings.Count(query, "FROM "+table+"\n"); got != want {
			t.Fatalf("the statement reads %s %d times, want %d", table, got, want)
		}
	}
	if got := strings.Count(query, "UNION ALL"); got != 3 {
		t.Fatalf("the statement has %d UNION ALL, want 3 (four arms)", got)
	}
	if got := strings.Count(query, "last_synced >= fromUnixTimestamp64Milli(toInt64(?), 'UTC')"); got != 4 {
		t.Fatalf("the statement bounds %d arms by last_synced, want 4", got)
	}
	if !strings.Contains(query, "toDate(submitted_at, 'UTC') AS day") {
		t.Fatal("the review arm must key on the review's own submission day")
	}
	if !strings.Contains(query, "toDate(committer_when, 'UTC') AS day\n    FROM git_commits\n    WHERE org_id = ? AND (repo_id, hash) IN (") {
		t.Fatal("the stats arm must key on the committer day of the commit its stats belong to")
	}
	args := conn.execArgs[0]
	want := []any{"org", args[1], "org", since.UnixMilli(), "org", since.UnixMilli(), "org", since.UnixMilli(), "org", "org", since.UnixMilli()}
	if len(args) != len(want) {
		t.Fatalf("the statement has %d arguments, want %d (org, at, then org and bound for three arms, then org, org, bound for the stats arm)", len(args), len(want))
	}
	for i := range want {
		if args[i] != want[i] {
			t.Fatalf("argument %d = %v, want %v (arguments %v)", i, args[i], want[i], args)
		}
	}
}

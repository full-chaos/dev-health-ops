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
// the three tables (commits, pull requests, reviews), and every arm is bounded
// by the same last_synced lower bound of the run. A review arm that runs as a
// second statement, or without that bound, fails here.
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
	for _, table := range []string{"git_commits", "git_pull_requests", "git_pull_request_reviews"} {
		if got := strings.Count(query, "FROM "+table+"\n"); got != 1 {
			t.Fatalf("the statement reads %s %d times, want 1 arm", table, got)
		}
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

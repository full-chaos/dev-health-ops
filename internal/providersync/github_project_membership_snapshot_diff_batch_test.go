package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

type snapshotPriorConn struct {
	driver.Conn
	queries int
	args    [][]any
	rows    [][4]string
}

func (conn *snapshotPriorConn) Query(_ context.Context, _ string, args ...any) (driver.Rows, error) {
	conn.queries++
	conn.args = append(conn.args, args)
	return &snapshotPriorRows{rows: conn.rows}, nil
}

type snapshotPriorRows struct {
	driver.Rows
	rows [][4]string
	at   int
}

func (rows *snapshotPriorRows) Next() bool { rows.at++; return rows.at <= len(rows.rows) }
func (rows *snapshotPriorRows) Scan(dest ...any) error {
	row := rows.rows[rows.at-1]
	*(dest[0].(*string)) = row[0]
	*(dest[1].(*string)) = row[1]
	*(dest[2].(*uuid.UUID)) = uuid.MustParse(row[2])
	*(dest[3].(*string)) = row[3]
	return nil
}
func (rows *snapshotPriorRows) Close() error { return nil }
func (rows *snapshotPriorRows) Err() error   { return nil }

func TestGitHubProjectV2SnapshotDiffReadsAllBoardsInOneQuery(t *testing.T) {
	conn := &snapshotPriorConn{rows: [][4]string{
		{"ghprojv2:acme#1", "work_item", "c7198fbc-1945-3717-05d8-eb78866b4e79", "gh:acme/api#1"},
		{"ghprojv2:acme#2", "work_item", "c7198fbc-1945-3717-05d8-eb78866b4e79", "gh:acme/api#2"},
		{"ghprojv2:acme#2", "pull_request", "c7198fbc-1945-3717-05d8-eb78866b4e79", "gh:acme/api#9"},
	}}
	claim := nativeTestClaim("github", "work_items")
	at := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	snapshots := []githubProjectV2BoardSnapshot{
		{ProjectScopeID: "ghprojv2:acme#1", Complete: true},
		{ProjectScopeID: "ghprojv2:acme#2", Complete: true},
		{ProjectScopeID: "ghprojv2:acme#3", Complete: true},
	}
	rows, err := resolveGitHubProjectV2SnapshotDiff(
		context.Background(), GitHubProjectV2SnapshotDiffClickHouseReader{Conn: conn}, claim, snapshots, at)
	if err != nil {
		t.Fatal(err)
	}
	if conn.queries != 1 {
		t.Fatalf("queries=%d want 1 for 3 boards", conn.queries)
	}
	if got := len(conn.args[0][0].([]string)); got != 3 {
		t.Fatalf("project list=%d want 3", got)
	}
	// Boards 1 and 2 are empty now, so each prior subject of its own board
	// retires; board 3 has no prior and nothing current: no row.
	if len(rows) != 3 {
		t.Fatalf("rows=%+v want 3 removals", rows)
	}
	for _, row := range rows {
		if row.FromProjectID == "" || row.ToProjectID != "" {
			t.Fatalf("not a removal: %+v", row)
		}
	}
}

func TestGitHubProjectV2SnapshotDiffReaderRefusesBlankProjectAndSkipsEmptyList(t *testing.T) {
	conn := &snapshotPriorConn{}
	reader := GitHubProjectV2SnapshotDiffClickHouseReader{Conn: conn}
	if _, err := reader.PriorActiveSubjects(context.Background(), "org", []string{"a", " "}); err == nil {
		t.Fatal("blank project id accepted")
	}
	if _, err := resolveGitHubProjectV2SnapshotDiff(context.Background(), reader, nativeTestClaim("github", "work_items"), nil, time.Now()); err != nil || conn.queries != 0 {
		t.Fatalf("empty snapshots: err=%v queries=%d", err, conn.queries)
	}
}

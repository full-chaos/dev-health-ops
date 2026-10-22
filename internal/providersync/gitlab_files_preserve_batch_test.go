package providersync

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type gitLabFilesReadbackConn struct {
	driver.Conn
	existing map[string]*string
	queries  int
	args     [][]any
}

func (conn *gitLabFilesReadbackConn) Query(_ context.Context, _ string, args ...any) (driver.Rows, error) {
	conn.queries++
	conn.args = append(conn.args, args)
	paths, _ := args[2].([]string)
	rows := &gitLabFilesReadbackRows{}
	for _, path := range paths {
		if contents, ok := conn.existing[path]; ok {
			rows.rows = append(rows.rows, [2]any{path, contents})
		}
	}
	return rows, nil
}

type gitLabFilesReadbackRows struct {
	driver.Rows
	rows [][2]any
	at   int
}

func (rows *gitLabFilesReadbackRows) Next() bool {
	rows.at++
	return rows.at <= len(rows.rows)
}

func (rows *gitLabFilesReadbackRows) Scan(dest ...any) error {
	row := rows.rows[rows.at-1]
	*(dest[0].(*string)) = row[0].(string)
	*(dest[1].(**string)) = row[1].(*string)
	return nil
}
func (rows *gitLabFilesReadbackRows) Close() error { return nil }
func (rows *gitLabFilesReadbackRows) Err() error   { return nil }

func TestGitLabFilesPreserveExistingContentsBatchesQueries(t *testing.T) {
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	const repoA, repoB = "c7198fbc-1945-3717-05d8-eb78866b4e79", "d7198fbc-1945-3717-05d8-eb78866b4e79"
	known := "package known\n"
	fresh := "fresh\n"
	conn := &gitLabFilesReadbackConn{existing: map[string]*string{"p/0": &known, "p/7": &known, "p/null": nil}}
	var rows []gitFileRow
	for i := 0; i < 200; i++ {
		rows = append(rows, gitFileRow{RepoID: repoA, Path: fmt.Sprintf("p/%d", i), LastSynced: now, OrgID: "org-a"})
	}
	rows = append(rows,
		gitFileRow{RepoID: repoA, Path: "p/null", LastSynced: now, OrgID: "org-a"},
		gitFileRow{RepoID: repoA, Path: "p/3", Contents: &fresh, LastSynced: now, OrgID: "org-a"},
		gitFileRow{RepoID: repoB, Path: "p/0", LastSynced: now, OrgID: "org-a"},
	)
	sink := GitLabFilesClickHouseEffects{Conn: conn}
	if err := sink.preserveExistingContents(context.Background(), nativeTestClaim("gitlab", "files"), rows); err != nil {
		t.Fatal(err)
	}
	if conn.queries != 2 {
		t.Fatalf("queries=%d want 2 (one per repo)", conn.queries)
	}
	if rows[0].Contents != &known || rows[7].Contents != &known || rows[1].Contents != nil {
		t.Fatalf("hydration wrong: %v %v %v", rows[0].Contents, rows[7].Contents, rows[1].Contents)
	}
	if rows[200].Contents != nil {
		t.Fatalf("null existing contents must stay nil")
	}
	if rows[201].Contents != &fresh {
		t.Fatalf("fresh contents must stay authoritative")
	}
	if rows[202].Contents != &known {
		t.Fatalf("repo B row must hydrate from its own repo query")
	}
}

func TestGitLabFilesPreserveExistingContentsChunksLongPathLists(t *testing.T) {
	conn := &gitLabFilesReadbackConn{existing: map[string]*string{}}
	long := strings.Repeat("x", 4000)
	var rows []gitFileRow
	for i := 0; i < 100; i++ {
		rows = append(rows, gitFileRow{RepoID: "r", Path: fmt.Sprintf("%s/%d", long, i), OrgID: "o"})
	}
	sink := GitLabFilesClickHouseEffects{Conn: conn}
	if err := sink.preserveExistingContents(context.Background(), nativeTestClaim("gitlab", "files"), rows); err != nil {
		t.Fatal(err)
	}
	if conn.queries < 2 || conn.queries > 8 {
		t.Fatalf("queries=%d want a few byte-bounded chunks", conn.queries)
	}
	total := 0
	for _, args := range conn.args {
		total += len(args[2].([]string))
	}
	if total != 100 {
		t.Fatalf("paths queried=%d want 100", total)
	}
}

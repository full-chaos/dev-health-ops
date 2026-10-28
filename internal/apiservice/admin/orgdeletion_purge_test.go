package admin

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// countingConn is a ClickHouse connection whose count() answers per table and
// that records every statement it executes.
type countingConn struct {
	driver.Conn
	mu     sync.Mutex
	counts map[string]uint64
	execs  []string
}

type countRow struct{ value uint64 }

func (r countRow) Err() error { return nil }
func (r countRow) Scan(dest ...any) error {
	*(dest[0].(*uint64)) = r.value
	return nil
}
func (r countRow) ScanStruct(any) error { return nil }

func (c *countingConn) QueryRow(_ context.Context, query string, _ ...any) driver.Row {
	for table, n := range c.counts {
		if strings.Contains(query, "`"+table+"`") {
			return countRow{n}
		}
	}
	return countRow{0}
}

func (c *countingConn) Exec(_ context.Context, query string, _ ...any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.execs = append(c.execs, query)
	return nil
}

// A row policy can hide an organization's rows from SELECT while ALTER DELETE
// still reaches them, so a count of 0 proves nothing: a real delete issues ALTER
// DELETE on every discovered table, whatever the count; a dry run issues none.
func TestOrgDeletionPurgeDeletesEveryTableEvenWhenItCountsZero(t *testing.T) {
	tables := []ChTable{{Name: "hidden_rows", OrgIDType: "String"}, {Name: "visible_rows", OrgIDType: "UUID"}}
	for _, dryRun := range []bool{false, true} {
		conn := &countingConn{counts: map[string]uint64{"hidden_rows": 0, "visible_rows": 3}}
		result := &orgDeletionResult{clickhouseCount: map[string]int64{}}
		h := &handlers{}
		if err := h.orgDeletionPurgeClickHouse(context.Background(), &orgDeletionClickHouse{conn: conn, tables: tables}, "aaaaaaaa-0000-4000-8000-00000000000a", dryRun, result); err != nil {
			t.Fatalf("dry run %v: %v", dryRun, err)
		}
		want := []string{
			"ALTER TABLE `hidden_rows` DELETE WHERE org_id = ?",
			"ALTER TABLE `visible_rows` DELETE WHERE org_id = toUUID(?)",
		}
		if dryRun {
			want = nil
		}
		if strings.Join(conn.execs, "\n") != strings.Join(want, "\n") {
			t.Errorf("dry run %v: executed\n%s\nwant\n%s", dryRun, strings.Join(conn.execs, "\n"), strings.Join(want, "\n"))
		}
		if result.clickhouseTotal != 3 || result.clickhouseCount["hidden_rows"] != 0 || result.clickhouseCount["visible_rows"] != 3 {
			t.Errorf("dry run %v: counts total %d %v, want 3 {hidden_rows:0 visible_rows:3}", dryRun, result.clickhouseTotal, result.clickhouseCount)
		}
	}
}

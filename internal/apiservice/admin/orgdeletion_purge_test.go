package admin

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// countingConn is a ClickHouse connection whose count() answers per table and
// that records every statement it executes.
type countingConn struct {
	driver.Conn
	mu        sync.Mutex
	counts    map[string]uint64
	countErrs map[string]error
	execErrs  map[string]error
	execs     []string
}

type countRow struct {
	value uint64
	err   error
}

func (r countRow) Err() error { return r.err }
func (r countRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	*(dest[0].(*uint64)) = r.value
	return nil
}
func (r countRow) ScanStruct(any) error { return nil }

func (c *countingConn) QueryRow(_ context.Context, query string, _ ...any) driver.Row {
	for table, err := range c.countErrs {
		if strings.Contains(query, "`"+table+"`") {
			return countRow{err: err}
		}
	}
	for table, n := range c.counts {
		if strings.Contains(query, "`"+table+"`") {
			return countRow{value: n}
		}
	}
	return countRow{}
}

func (c *countingConn) Exec(_ context.Context, query string, _ ...any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.execs = append(c.execs, query)
	for table, err := range c.execErrs {
		if strings.Contains(query, "`"+table+"`") {
			return err
		}
	}
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

// A failing count or ALTER DELETE never yields success: the table comes back
// in an *orgDeletionIncompleteError (the route's 500, the CLI's exit 1). A
// failing count does not skip the table's ALTER DELETE on a real delete; a dry
// run issues no ALTER DELETE at all.
func TestOrgDeletionPurgeReportsEveryFailureWithoutSkippingTheDelete(t *testing.T) {
	tables := []ChTable{{Name: "count_fails", OrgIDType: "String"}, {Name: "alter_fails", OrgIDType: "String"}, {Name: "fine", OrgIDType: "String"}}
	alter := func(table string) string { return "ALTER TABLE `" + table + "` DELETE WHERE org_id = ?" }
	for _, tc := range []struct {
		name       string
		countErrs  map[string]error
		execErrs   map[string]error
		dryRun     bool
		wantExecs  []string
		wantFailed []string
	}{
		{"count error: the ALTER DELETE still runs, incomplete",
			map[string]error{"count_fails": errors.New("count refused")}, nil, false,
			[]string{alter("count_fails"), alter("alter_fails"), alter("fine")}, []string{"count_fails"}},
		{"ALTER DELETE error alone: not done",
			nil, map[string]error{"alter_fails": errors.New("mutation refused")}, false,
			[]string{alter("count_fails"), alter("alter_fails"), alter("fine")}, []string{"alter_fails"}},
		{"count and ALTER DELETE of one table fail: listed once",
			map[string]error{"count_fails": errors.New("count refused")}, map[string]error{"count_fails": errors.New("mutation refused")}, false,
			[]string{alter("count_fails"), alter("alter_fails"), alter("fine")}, []string{"count_fails"}},
		{"dry run with a count error: no ALTER DELETE, not done",
			map[string]error{"count_fails": errors.New("count refused")}, nil, true,
			nil, []string{"count_fails"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := &countingConn{counts: map[string]uint64{"alter_fails": 1, "fine": 2}, countErrs: tc.countErrs, execErrs: tc.execErrs}
			result := &orgDeletionResult{clickhouseCount: map[string]int64{}}
			err := (&handlers{}).orgDeletionPurgeClickHouse(context.Background(), &orgDeletionClickHouse{conn: conn, tables: tables}, "aaaaaaaa-0000-4000-8000-00000000000a", tc.dryRun, result)
			var incomplete *orgDeletionIncompleteError
			if !errors.As(err, &incomplete) {
				t.Fatalf("err = %v, want an *orgDeletionIncompleteError", err)
			}
			if strings.Join(incomplete.tables, ",") != strings.Join(tc.wantFailed, ",") {
				t.Errorf("failed tables %v, want %v", incomplete.tables, tc.wantFailed)
			}
			if strings.Join(conn.execs, "\n") != strings.Join(tc.wantExecs, "\n") {
				t.Errorf("executed\n%s\nwant\n%s", strings.Join(conn.execs, "\n"), strings.Join(tc.wantExecs, "\n"))
			}
		})
	}
}

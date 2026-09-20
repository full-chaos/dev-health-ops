package reports

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type call struct {
	sql  string
	args []any
}

// fakePG answers each statement, in order, with the next canned result and
// records every statement and argument it was given.
type fakePG struct {
	calls   []call
	results []fakeResult
}

type fakeResult struct {
	rows [][]any
	err  error
	// rowsErr is returned by Rows.Err after iteration.
	rowsErr error
}

func (f *fakePG) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	f.calls = append(f.calls, call{sql, args})
	if len(f.results) == 0 {
		return nil, errors.New("fakePG: no canned result")
	}
	r := f.results[0]
	f.results = f.results[1:]
	if r.err != nil {
		return nil, r.err
	}
	return &fakeRows{rows: r.rows, err: r.rowsErr, idx: -1}, nil
}

type fakeRows struct {
	rows [][]any
	err  error
	idx  int
}

func (r *fakeRows) Close()                                       {}
func (r *fakeRows) Err() error                                   { return r.err }
func (r *fakeRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *fakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *fakeRows) Values() ([]any, error)                       { return nil, nil }
func (r *fakeRows) RawValues() [][]byte                          { return nil }
func (r *fakeRows) Conn() *pgx.Conn                              { return nil }
func (r *fakeRows) Next() bool                                   { r.idx++; return r.idx < len(r.rows) }
func (r *fakeRows) Scan(dest ...any) error {
	row := r.rows[r.idx]
	if len(dest) != len(row) {
		return fmt.Errorf("fakeRows: %d destinations for %d columns", len(dest), len(row))
	}
	for i, d := range dest {
		dv := reflect.ValueOf(d).Elem()
		if row[i] == nil {
			dv.Set(reflect.Zero(dv.Type()))
			continue
		}
		v := reflect.ValueOf(row[i])
		if v.Type().AssignableTo(dv.Type()) {
			dv.Set(v)
		} else if dv.Kind() == reflect.Ptr {
			p := reflect.New(dv.Type().Elem())
			p.Elem().Set(v)
			dv.Set(p)
		} else {
			dv.Set(v)
		}
	}
	return nil
}

const rid = "01234567-89ab-cdef-0123-456789abcdef"

func str(s string) *string { return &s }

var ts = time.Date(2026, 3, 4, 5, 6, 7, 0, time.FixedZone("x", 3600))

func reportRow(plan, params any) []any {
	return []any{rid, "org-1", "Weekly", nil, plan, true, nil, params, nil, true, nil, nil, ts, ts, nil}
}

func TestList_StatementsAndArguments(t *testing.T) {
	pg := &fakePG{results: []fakeResult{
		{rows: [][]any{{int64(7)}}},
		{rows: [][]any{reportRow(str(`{"a":1}`), nil)}},
	}}
	got, err := (&Reader{Postgres: pg}).List(context.Background(), "org-1", 50, 10)
	if err != nil {
		t.Fatal(err)
	}
	if got.Total != 7 || len(got.Items) != 1 {
		t.Fatalf("total=%d items=%d", got.Total, len(got.Items))
	}
	if len(pg.calls) != 2 {
		t.Fatalf("calls = %d", len(pg.calls))
	}
	if pg.calls[0].sql != savedReportsCountSQL || !reflect.DeepEqual(pg.calls[0].args, []any{"org-1"}) {
		t.Errorf("count call = %+v", pg.calls[0])
	}
	if pg.calls[1].sql != savedReportsPageSQL || !reflect.DeepEqual(pg.calls[1].args, []any{"org-1", 10, 50}) {
		t.Errorf("page call = %+v", pg.calls[1])
	}
	for _, sql := range []string{savedReportsCountSQL, savedReportsPageSQL} {
		if !strings.Contains(sql, "s.org_id = $1") {
			t.Errorf("statement lacks the org predicate: %s", sql)
		}
	}
	if !strings.Contains(savedReportsPageSQL, "ORDER BY s.updated_at DESC") {
		t.Error("page is not ordered by updated_at DESC")
	}
	if got.Items[0].CreatedAt.Location() != time.UTC {
		t.Error("created_at is not UTC")
	}
}

func TestList_EmptyPageIsAnEmptyListNotNil(t *testing.T) {
	pg := &fakePG{results: []fakeResult{{rows: [][]any{{int64(0)}}}, {}}}
	got, err := (&Reader{Postgres: pg}).List(context.Background(), "org-1", 50, 0)
	if err != nil || got.Items == nil || len(got.Items) != 0 || got.Total != 0 {
		t.Fatalf("got %+v, %v", got, err)
	}
}

func TestGet_StatementIdNormalizedAndOrgBound(t *testing.T) {
	pg := &fakePG{results: []fakeResult{{rows: [][]any{reportRow(str(`{}`), str(`{"p":1}`))}}}}
	got, err := (&Reader{Postgres: pg}).Get(context.Background(), "org-1", strings.ToUpper(strings.ReplaceAll(rid, "-", "")))
	if err != nil || got == nil {
		t.Fatalf("got %v, %v", got, err)
	}
	if pg.calls[0].sql != savedReportSQL || !reflect.DeepEqual(pg.calls[0].args, []any{rid, "org-1"}) {
		t.Errorf("call = %+v", pg.calls[0])
	}
	if !strings.Contains(savedReportSQL, "s.org_id = $2") {
		t.Error("statement lacks the org predicate")
	}
	if string(got.Parameters) != `{"p":1}` || string(got.ReportPlan) != `{}` {
		t.Errorf("json = %s / %s", got.ReportPlan, got.Parameters)
	}
}

func TestGet_NoRowIsNil(t *testing.T) {
	pg := &fakePG{results: []fakeResult{{}}}
	got, err := (&Reader{Postgres: pg}).Get(context.Background(), "org-1", rid)
	if err != nil || got != nil {
		t.Fatalf("got %v, %v", got, err)
	}
}

func TestGet_MalformedIDIsAnErrorAndReadsNothing(t *testing.T) {
	pg := &fakePG{}
	if _, err := (&Reader{Postgres: pg}).Get(context.Background(), "org-1", "nope"); err == nil {
		t.Fatal("malformed id accepted")
	}
	if len(pg.calls) != 0 {
		t.Fatal("statement issued for a malformed id")
	}
}

func TestGet_NullReportPlanIsAnError(t *testing.T) {
	for _, plan := range []*string{nil, str("null")} {
		pg := &fakePG{results: []fakeResult{{rows: [][]any{reportRow(plan, nil)}}}}
		if _, err := (&Reader{Postgres: pg}).Get(context.Background(), "org-1", rid); err == nil {
			t.Errorf("plan %v accepted", plan)
		}
	}
}

func TestRuns_StatementsAndMapping(t *testing.T) {
	started := ts
	row := []any{rid, rid, "success", started, nil, 1.5, "md", nil, str(`[{"a":1,"a":2}]`), nil, "api", ts}
	pg := &fakePG{results: []fakeResult{{rows: [][]any{{int64(3)}}}, {rows: [][]any{row}}}}
	got, err := (&Reader{Postgres: pg}).Runs(context.Background(), "org-1", rid, 20)
	if err != nil {
		t.Fatal(err)
	}
	if got.Total != 3 || len(got.Items) != 1 {
		t.Fatalf("total=%d items=%d", got.Total, len(got.Items))
	}
	if pg.calls[0].sql != reportRunsCountSQL || !reflect.DeepEqual(pg.calls[0].args, []any{"org-1", rid}) {
		t.Errorf("count call = %+v", pg.calls[0])
	}
	if pg.calls[1].sql != reportRunsPageSQL || !reflect.DeepEqual(pg.calls[1].args, []any{"org-1", rid, 20}) {
		t.Errorf("page call = %+v", pg.calls[1])
	}
	for _, sql := range []string{reportRunsCountSQL, reportRunsPageSQL} {
		if !strings.Contains(sql, "s.org_id = $1") || !strings.Contains(sql, "JOIN saved_reports s ON s.id = r.report_id") {
			t.Errorf("statement lacks the org-carrying join: %s", sql)
		}
	}
	if !strings.Contains(reportRunsPageSQL, "ORDER BY r.created_at DESC") {
		t.Error("runs are not ordered by created_at DESC")
	}
	item := got.Items[0]
	if string(item.ProvenanceRecords) != `[{"a":2}]` || item.StartedAt == nil || item.CompletedAt != nil || *item.DurationSeconds != 1.5 {
		t.Errorf("item = %+v", item)
	}
	if item.StartedAt.Location() != time.UTC {
		t.Error("started_at is not UTC")
	}
}

func TestInt32Range(t *testing.T) {
	pg := &fakePG{}
	r := &Reader{Postgres: pg}
	for _, n := range []int{1 << 31, -(1 << 31) - 1} {
		if _, err := r.List(context.Background(), "o", n, 0); err == nil {
			t.Errorf("limit %d accepted", n)
		}
		if _, err := r.List(context.Background(), "o", 1, n); err == nil {
			t.Errorf("offset %d accepted", n)
		}
		if _, err := r.Runs(context.Background(), "o", rid, n); err == nil {
			t.Errorf("runs limit %d accepted", n)
		}
	}
	if len(pg.calls) != 0 {
		t.Fatal("statement issued for an out-of-range Int")
	}
	// The bounds themselves are accepted and passed through to the database.
	pg.results = []fakeResult{{rows: [][]any{{int64(0)}}}, {}}
	if _, err := r.List(context.Background(), "o", 1<<31-1, -(1 << 31)); err != nil {
		t.Errorf("boundary rejected: %v", err)
	}
}

func TestFailuresAreErrorsNeverPartialData(t *testing.T) {
	boom := errors.New("boom")
	cases := map[string]func(pg *fakePG) error{
		"list count": func(pg *fakePG) error {
			_, err := (&Reader{Postgres: pg}).List(context.Background(), "o", 1, 0)
			return err
		},
		"get": func(pg *fakePG) error {
			_, err := (&Reader{Postgres: pg}).Get(context.Background(), "o", rid)
			return err
		},
		"runs count": func(pg *fakePG) error {
			_, err := (&Reader{Postgres: pg}).Runs(context.Background(), "o", rid, 1)
			return err
		},
	}
	for name, run := range cases {
		if err := run(&fakePG{results: []fakeResult{{err: boom}}}); !errors.Is(err, boom) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
	// A failure on the page after a good count is still an error.
	pg := &fakePG{results: []fakeResult{{rows: [][]any{{int64(1)}}}, {err: boom}}}
	if _, err := (&Reader{Postgres: pg}).List(context.Background(), "o", 1, 0); !errors.Is(err, boom) {
		t.Errorf("list page: %v", err)
	}
	pg = &fakePG{results: []fakeResult{{rows: [][]any{{int64(1)}}}, {rowsErr: boom}}}
	if _, err := (&Reader{Postgres: pg}).List(context.Background(), "o", 1, 0); !errors.Is(err, boom) {
		t.Errorf("list rows: %v", err)
	}
	pg = &fakePG{results: []fakeResult{{rows: [][]any{{int64(1)}}}, {err: boom}}}
	if _, err := (&Reader{Postgres: pg}).Runs(context.Background(), "o", rid, 1); !errors.Is(err, boom) {
		t.Errorf("runs page: %v", err)
	}
}

func TestNoReaderIsAnError(t *testing.T) {
	r := &Reader{}
	if _, err := r.List(context.Background(), "o", 1, 0); !errors.Is(err, ErrUnavailable) {
		t.Errorf("list: %v", err)
	}
	if _, err := r.Get(context.Background(), "o", rid); !errors.Is(err, ErrUnavailable) {
		t.Errorf("get: %v", err)
	}
	if _, err := r.Runs(context.Background(), "o", rid, 1); !errors.Is(err, ErrUnavailable) {
		t.Errorf("runs: %v", err)
	}
}

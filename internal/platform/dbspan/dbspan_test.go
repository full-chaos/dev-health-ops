package dbspan_test

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/full-chaos/dev-health-ops/internal/platform/dbspan"
)

func provider(sampler sdktrace.Sampler) (*sdktrace.TracerProvider, *tracetest.SpanRecorder) {
	rec := tracetest.NewSpanRecorder()
	return sdktrace.NewTracerProvider(sdktrace.WithSampler(sampler), sdktrace.WithSpanProcessor(rec)), rec
}

func children(rec *tracetest.SpanRecorder, parentName string) []sdktrace.ReadOnlySpan {
	var out []sdktrace.ReadOnlySpan
	for _, s := range rec.Ended() {
		if s.Name() != parentName {
			out = append(out, s)
		}
	}
	return out
}

func keys(s sdktrace.ReadOnlySpan) []string {
	var k []string
	for _, a := range s.Attributes() {
		k = append(k, string(a.Key))
	}
	sort.Strings(k)
	return k
}

func TestNoParentNoSpan(t *testing.T) {
	_, rec := provider(sdktrace.AlwaysSample())
	e := dbspan.NewEmitter(dbspan.SystemPostgres, "domain").WithMin(0)
	p := e.Start(context.Background())
	if p.Active() {
		t.Fatal("probe active without a parent span")
	}
	p.Finish(context.Background(), "SELECT 1", "", errors.New("boom"), "")
	if n := len(rec.Ended()); n != 0 {
		t.Fatalf("spans = %d, want 0", n)
	}
}

func TestNonRecordingParentNoSpan(t *testing.T) {
	tp, rec := provider(sdktrace.NeverSample())
	ctx, parent := tp.Tracer("t").Start(context.Background(), "parent")
	defer parent.End()
	e := dbspan.NewEmitter(dbspan.SystemPostgres, "").WithMin(0)
	p := e.Start(ctx)
	p.Finish(ctx, "SELECT 1", "", errors.New("boom"), "")
	if p.Active() || len(rec.Ended()) != 0 {
		t.Fatal("span emitted under a non-recording parent")
	}
}

func TestFastCallNoSpanSlowCallSpan(t *testing.T) {
	tp, rec := provider(sdktrace.AlwaysSample())
	ctx, parent := tp.Tracer("t").Start(context.Background(), "parent")
	e := dbspan.NewEmitter(dbspan.SystemPostgres, "domain").WithMin(30 * time.Millisecond)

	p := e.Start(ctx)
	p.Finish(ctx, "SELECT 1", "", nil, "")
	if len(rec.Ended()) != 0 {
		t.Fatal("fast call emitted a span")
	}

	p = e.Start(ctx)
	time.Sleep(40 * time.Millisecond)
	p.Finish(ctx, "SELECT id FROM public.work_items WHERE x = $1", "", nil, "")
	parent.End()

	spans := children(rec, "parent")
	if len(spans) != 1 {
		t.Fatalf("spans = %d, want 1", len(spans))
	}
	s := spans[0]
	if s.SpanKind() != trace.SpanKindClient || s.Name() != "SELECT public.work_items" || s.Parent().SpanID() != parent.SpanContext().SpanID() {
		t.Fatalf("kind=%v name=%q parent ok=%v", s.SpanKind(), s.Name(), s.Parent().SpanID() == parent.SpanContext().SpanID())
	}
	// The EXACT key set: no statement hash (removed by design), no text.
	want := []string{"db.operation.name", "db.query.summary", "db.system", "db.system.name", "dh.db.pool"}
	got := keys(s)
	if len(got) != len(want) {
		t.Fatalf("attribute keys = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("attribute keys = %v, want %v", got, want)
		}
	}
	if d := s.EndTime().Sub(s.StartTime()); d < 40*time.Millisecond {
		t.Fatalf("span duration %v does not cover the call", d)
	}
	if s.Status().Code != codes.Unset {
		t.Fatalf("status = %v", s.Status().Code)
	}
}

func TestErrorEmitsFastSpanWithBoundedClass(t *testing.T) {
	tp, rec := provider(sdktrace.AlwaysSample())
	ctx, parent := tp.Tracer("t").Start(context.Background(), "parent")
	e := dbspan.NewEmitter(dbspan.SystemClickHouse, "").WithMin(time.Hour)
	p := e.Start(ctx)
	p.Finish(ctx, "INSERT INTO t VALUES ('secret')", "", errors.New("value 'secret' invalid"), "")
	parent.End()
	spans := children(rec, "parent")
	if len(spans) != 1 {
		t.Fatalf("spans = %d", len(spans))
	}
	s := spans[0]
	if s.Status().Code != codes.Error || s.Status().Description != "" {
		t.Fatalf("status = %+v", s.Status())
	}
	var typ string
	for _, a := range s.Attributes() {
		if a.Key == "error.type" {
			typ = a.Value.AsString()
		}
		if a.Key == "dh.db.pool" {
			t.Fatal("empty pool must be omitted")
		}
	}
	if typ != "driver_error" {
		t.Fatalf("error.type = %q", typ)
	}
}

func TestNoRowsIsNotAnError(t *testing.T) {
	for _, err := range []error{sql.ErrNoRows, pgx.ErrNoRows} {
		tp, rec := provider(sdktrace.AlwaysSample())
		ctx, parent := tp.Tracer("t").Start(context.Background(), "parent")
		p := dbspan.NewEmitter(dbspan.SystemPostgres, "").WithMin(time.Hour).Start(ctx)
		p.Finish(ctx, "SELECT 1", "", err, "")
		parent.End()
		if n := len(children(rec, "parent")); n != 0 {
			t.Fatalf("%v emitted %d spans", err, n)
		}
	}
}

func TestCancellationIsClassedNotErrored(t *testing.T) {
	tp, rec := provider(sdktrace.AlwaysSample())
	ctx, parent := tp.Tracer("t").Start(context.Background(), "parent")
	e := dbspan.NewEmitter(dbspan.SystemPostgres, "").WithMin(time.Hour)
	p := e.Start(ctx)
	p.Finish(ctx, "SELECT 1", "", context.Canceled, "")
	if len(children(rec, "parent")) != 0 {
		t.Fatal("fast cancellation emitted a span")
	}
	e = e.WithMin(0)
	p = e.Start(ctx)
	p.Finish(ctx, "SELECT 1", "", context.Canceled, "")
	parent.End()
	spans := children(rec, "parent")
	if len(spans) != 1 || spans[0].Status().Code != codes.Unset {
		t.Fatalf("slow cancellation: spans=%d", len(spans))
	}
	var typ string
	for _, a := range spans[0].Attributes() {
		if a.Key == "error.type" {
			typ = a.Value.AsString()
		}
	}
	if typ != "canceled" {
		t.Fatalf("error.type = %q", typ)
	}
}

func TestParseMin(t *testing.T) {
	cases := []struct {
		in string
		d  time.Duration
		ok bool
	}{
		{"", dbspan.DefaultMin, true},
		{"0", 0, true},
		{" 120 ", 120 * time.Millisecond, true},
		{"-1", dbspan.DefaultMin, false},
		{"abc", dbspan.DefaultMin, false},
		{"1.5", dbspan.DefaultMin, false},
		{"99999999999999", dbspan.DefaultMin, false},
	}
	for _, c := range cases {
		d, ok := dbspan.ParseMin(c.in)
		if d != c.d || ok != c.ok {
			t.Errorf("ParseMin(%q) = %v,%v want %v,%v", c.in, d, ok, c.d, c.ok)
		}
	}
}

func TestOperationAndSummary(t *testing.T) {
	cases := []struct{ stmt, op, sum string }{
		{"SELECT * FROM public.t WHERE a = 1", "SELECT", "SELECT public.t"},
		{"  /* c */ -- x\n insert into t (a) values (1)", "INSERT", "INSERT t"},
		{"UPDATE jobs SET a = 1", "UPDATE", "UPDATE jobs"},
		{"WITH x AS (SELECT 1) SELECT * FROM (SELECT 1)", "WITH", "WITH"},
		{"SELECT * FROM \"Quoted\"", "SELECT", "SELECT"},
		{"SELECT 1", "SELECT", "SELECT"},
		{"VACUUM t", "OTHER", "OTHER"},
		{"", "OTHER", "OTHER"},
		{"ALTER TABLE db.t ADD COLUMN c Int8", "ALTER", "ALTER db.t"},
		{"SELECT * FROM t;DROP TABLE u", "SELECT", "SELECT t"},
		{"SELECT * FROM 1bad", "SELECT", "SELECT"},
	}
	for _, c := range cases {
		op := dbspan.Operation(c.stmt)
		if op != c.op {
			t.Errorf("Operation(%q) = %q want %q", c.stmt, op, c.op)
		}
		if got := dbspan.Summary(c.stmt, op); got != c.sum {
			t.Errorf("Summary(%q) = %q want %q", c.stmt, got, c.sum)
		}
	}
}

func TestInvalidEnv(t *testing.T) {
	t.Setenv(dbspan.EnvMinMS, "abc")
	if !dbspan.InvalidEnv() {
		t.Fatal("abc must be invalid")
	}
	t.Setenv(dbspan.EnvMinMS, "0")
	if dbspan.InvalidEnv() {
		t.Fatal("0 is valid")
	}
}

func TestBatchSpanCarriesStatementCountAndExactKeys(t *testing.T) {
	tp, rec := provider(sdktrace.AlwaysSample())
	ctx, parent := tp.Tracer("t").Start(context.Background(), "parent")
	e := dbspan.NewEmitter(dbspan.SystemPostgres, "").WithMin(0)
	e.Start(ctx).WithBatchSize(3).Finish(ctx, "", dbspan.OpBatch, nil, "")
	e.Start(ctx).Finish(ctx, "SELECT 1", "", nil, "")
	parent.End()
	spans := children(rec, "parent")
	if len(spans) != 2 {
		t.Fatalf("spans = %d, want 2", len(spans))
	}
	wants := map[string][]string{
		"BATCH":  {"db.operation.batch.size", "db.operation.name", "db.query.summary", "db.system", "db.system.name"},
		"SELECT": {"db.operation.name", "db.query.summary", "db.system", "db.system.name"},
	}
	for _, s := range spans {
		want, ok := wants[s.Name()]
		if !ok {
			t.Fatalf("unexpected span %q", s.Name())
		}
		if got := keys(s); strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("%s attribute keys = %v, want exactly %v", s.Name(), got, want)
		}
		for _, kv := range s.Attributes() {
			if kv.Key == "db.operation.batch.size" && kv.Value.AsInt64() != 3 {
				t.Fatalf("batch size = %d, want 3", kv.Value.AsInt64())
			}
		}
	}
}

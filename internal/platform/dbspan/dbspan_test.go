package dbspan_test

import (
	"context"
	"database/sql"
	"errors"
	"sort"
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
	want := []string{"db.operation.name", "db.query.fingerprint", "db.query.summary", "db.system", "db.system.name", "dh.db.pool"}
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
	if f := dbspan.Fingerprint("SELECT 1"); len(f) != 16 {
		t.Errorf("fingerprint %q", f)
	}
}

func TestFingerprintGroupsTemplatesAndHidesLiterals(t *testing.T) {
	a := dbspan.Fingerprint("SELECT * FROM t WHERE email = 'secret@x.y' AND n = 42 AND k = $1")
	b := dbspan.Fingerprint("SELECT * FROM t WHERE email = 'other''s' AND n = 7 AND k = $1")
	if a != b {
		t.Fatalf("same template, different literals: %s != %s", a, b)
	}
	if dbspan.Fingerprint("SELECT col1 FROM t1") == dbspan.Fingerprint("SELECT col2 FROM t1") {
		t.Fatal("digits inside identifiers must not be normalized away")
	}
	if dbspan.Fingerprint("SELECT $$secret$$") != dbspan.Fingerprint("SELECT $tag$other$tag$") {
		t.Fatal("dollar-quoted bodies must normalize")
	}
	if len(a) != 16 {
		t.Fatalf("fingerprint %q", a)
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

func TestFingerprintDropsCommentsAndBooleanLiterals(t *testing.T) {
	base := dbspan.Fingerprint("SELECT 1 FROM t WHERE a = ? AND b = ?")
	for _, stmt := range []string{
		"SELECT 1 FROM t WHERE a = TRUE AND b = null",
		"SELECT 1 FROM t WHERE a = false AND b = NULL",
		"SELECT /* alice@example.com */ 1 FROM t WHERE a = ? -- bob@example.com\n AND b = ?",
	} {
		if got := dbspan.Fingerprint(stmt); got != base {
			t.Errorf("%q fingerprint %s != template %s", stmt, got, base)
		}
	}
	if dbspan.Fingerprint("SELECT nullable, trueish FROM t") == dbspan.Fingerprint("SELECT ?, ? FROM t") {
		t.Error("identifiers that merely start with a keyword must be kept")
	}
}

package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	chdriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/full-chaos/dev-health-ops/internal/platform/dbspan"
)

type fakeConn struct {
	driver.Conn
	err   error
	sleep time.Duration
	batch *fakeBatch
}

func (f *fakeConn) work() error                                       { time.Sleep(f.sleep); return f.err }
func (f *fakeConn) Select(context.Context, any, string, ...any) error { return f.work() }
func (f *fakeConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	return nil, f.work()
}
func (f *fakeConn) QueryRow(context.Context, string, ...any) driver.Row { return fakeRow{f.work()} }
func (f *fakeConn) Exec(context.Context, string, ...any) error          { return f.work() }
func (f *fakeConn) AsyncInsert(context.Context, string, bool, ...any) error {
	return f.work()
}
func (f *fakeConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	return f.batch, nil
}

type fakeRow struct{ err error }

func (r fakeRow) Err() error           { return r.err }
func (r fakeRow) Scan(...any) error    { return r.err }
func (r fakeRow) ScanStruct(any) error { return r.err }

type fakeBatch struct {
	driver.Batch
	sendErr error
	sleep   time.Duration
}

func (b *fakeBatch) Send() error  { time.Sleep(b.sleep); return b.sendErr }
func (b *fakeBatch) Abort() error { return nil }

func recCtx(sampler sdktrace.Sampler) (context.Context, *tracetest.SpanRecorder, func()) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sampler), sdktrace.WithSpanProcessor(rec))
	ctx, parent := tp.Tracer("t").Start(context.Background(), "parent")
	return ctx, rec, func() { parent.End() }
}

func chSpans(rec *tracetest.SpanRecorder) []sdktrace.ReadOnlySpan {
	var out []sdktrace.ReadOnlySpan
	for _, s := range rec.Ended() {
		if s.Name() != "parent" {
			out = append(out, s)
		}
	}
	return out
}

func wrap(f *fakeConn, min time.Duration) driver.Conn {
	return newSpanConn(f, dbspan.NewEmitter(dbspan.SystemClickHouse, "").WithMin(min))
}

func TestSpanConnEveryMethodEmitsOneSlowSpan(t *testing.T) {
	ctx, rec, end := recCtx(sdktrace.AlwaysSample())
	c := wrap(&fakeConn{sleep: 15 * time.Millisecond, batch: &fakeBatch{sleep: 15 * time.Millisecond}}, 10*time.Millisecond)
	const q = "SELECT a FROM metrics.daily WHERE org = {org:String}"
	_ = c.Select(ctx, nil, q)
	_, _ = c.Query(ctx, q)
	_ = c.QueryRow(ctx, q)
	_ = c.Exec(ctx, "INSERT INTO metrics.daily SELECT 1")
	_ = c.AsyncInsert(ctx, "INSERT INTO metrics.daily VALUES", true)
	b, _ := c.PrepareBatch(ctx, "INSERT INTO metrics.daily")
	_ = b.Send()
	_ = b.Send() // second Send must not emit a second span
	end()
	spans := chSpans(rec)
	if len(spans) != 6 {
		t.Fatalf("spans = %d, want 6", len(spans))
	}
	if spans[5].Name() != "BATCH metrics.daily" {
		t.Fatalf("batch span name = %q", spans[5].Name())
	}
	for _, s := range spans {
		for _, a := range s.Attributes() {
			if a.Key == "db.system" && a.Value.AsString() != "clickhouse" {
				t.Fatalf("db.system = %q", a.Value.AsString())
			}
		}
		if len(s.Attributes()) != 5 {
			t.Fatalf("attribute count = %d, want 5", len(s.Attributes()))
		}
	}
}

func TestSpanConnNoSpanWithoutRecordingParentOrWhenFast(t *testing.T) {
	// no parent
	c := wrap(&fakeConn{batch: &fakeBatch{}}, 0)
	_ = c.Exec(context.Background(), "SELECT 1")
	b, err := c.PrepareBatch(context.Background(), "INSERT INTO t")
	if err != nil {
		t.Fatal(err)
	}
	if _, wrapped := b.(*spanBatch); wrapped {
		t.Fatal("batch wrapped without a recording parent")
	}
	// non-recording parent
	ctx, rec, end := recCtx(sdktrace.NeverSample())
	_ = c.Exec(ctx, "SELECT 1")
	end()
	// fast under threshold
	ctx2, rec2, end2 := recCtx(sdktrace.AlwaysSample())
	_ = wrap(&fakeConn{}, time.Hour).Exec(ctx2, "SELECT 1")
	end2()
	if len(chSpans(rec)) != 0 || len(chSpans(rec2)) != 0 {
		t.Fatal("span emitted")
	}
}

func TestSpanConnErrorSpanUsesExceptionCodeOnly(t *testing.T) {
	ctx, rec, end := recCtx(sdktrace.AlwaysSample())
	exc := &chdriver.Exception{Code: 60, Message: "Table secret@x.y doesn't exist"}
	_ = wrap(&fakeConn{err: exc}, time.Hour).Exec(ctx, "SELECT 1")
	end()
	s := chSpans(rec)[0]
	if s.Status().Code != codes.Error || s.Status().Description != "" {
		t.Fatalf("status %+v", s.Status())
	}
	var typ string
	for _, a := range s.Attributes() {
		if a.Key == "error.type" {
			typ = a.Value.AsString()
		}
	}
	if typ != "ch_60" {
		t.Fatalf("error.type = %q", typ)
	}
}

func TestSpanConnBatchSendErrorAndAbort(t *testing.T) {
	ctx, rec, end := recCtx(sdktrace.AlwaysSample())
	c := wrap(&fakeConn{batch: &fakeBatch{sendErr: errors.New("boom secret")}}, time.Hour)
	b, _ := c.PrepareBatch(ctx, "INSERT INTO t")
	_ = b.Send()
	c2 := wrap(&fakeConn{batch: &fakeBatch{}}, time.Hour)
	b2, _ := c2.PrepareBatch(ctx, "INSERT INTO t")
	_ = b2.Abort() // fast abort: no span
	end()
	spans := chSpans(rec)
	if len(spans) != 1 || spans[0].Status().Code != codes.Error {
		t.Fatalf("spans = %d", len(spans))
	}
}

// The leak test: statement literals, arguments and error text reach nothing.
func TestSpanConnNeverCarriesStatementTextArgsOrErrorText(t *testing.T) {
	secrets := []string{"secret@x.y", "sk-abc", "arg-secret-1"}
	stmt := "SELECT * FROM t WHERE email = 'secret@x.y' AND token = 'sk-abc'"
	args := []any{"arg-secret-1"}
	errs := []error{
		nil,
		errors.New("bad value secret@x.y sk-abc arg-secret-1"),
		&chdriver.Exception{Code: 62, Message: "Syntax error near 'sk-abc' secret@x.y"},
	}
	ctx, rec, end := recCtx(sdktrace.AlwaysSample())
	for _, e := range errs {
		c := wrap(&fakeConn{err: e, batch: &fakeBatch{sendErr: e}}, 0)
		_ = c.Select(ctx, nil, stmt, args...)
		_, _ = c.Query(ctx, stmt, args...)
		_ = c.QueryRow(ctx, stmt, args...)
		_ = c.Exec(ctx, stmt, args...)
		_ = c.AsyncInsert(ctx, stmt, false, args...)
		b, _ := c.PrepareBatch(ctx, stmt)
		_ = b.Send()
	}
	end()
	spans := chSpans(rec)
	if len(spans) != 18 {
		t.Fatalf("spans = %d, want 18", len(spans))
	}
	for _, s := range spans {
		var dump strings.Builder
		fmt.Fprint(&dump, s.Name(), "|", s.Status().Description)
		for _, a := range s.Attributes() {
			fmt.Fprint(&dump, "|", a.Key, "=", a.Value.Emit())
		}
		for _, ev := range s.Events() {
			fmt.Fprint(&dump, "|", ev.Name)
			for _, a := range ev.Attributes {
				fmt.Fprint(&dump, "|", a.Key, "=", a.Value.Emit())
			}
		}
		for _, secret := range secrets {
			if strings.Contains(dump.String(), secret) {
				t.Fatalf("span leaks %q: %s", secret, dump.String())
			}
		}
		if strings.Contains(dump.String(), "email =") {
			t.Fatalf("span carries statement text: %s", dump.String())
		}
	}
}

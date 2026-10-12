package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/full-chaos/dev-health-ops/internal/platform/dbspan"
)

// dbTracer is the part of both tracers the spans use.
type dbTracer interface {
	pgx.QueryTracer
	pgx.BatchTracer
}

func tracers() map[string]dbTracer {
	pool := newPoolAcquireTracer("domain")
	pool.spans = pool.spans.WithMin(0)
	return map[string]dbTracer{
		"poolAcquireTracer": pool,
		"spanTracer":        spanTracer{spans: dbspan.NewEmitter(dbspan.SystemPostgres, "").WithMin(0)},
	}
}

func recordingCtx() (context.Context, *tracetest.SpanRecorder, func()) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	ctx, parent := tp.Tracer("t").Start(context.Background(), "parent")
	return ctx, rec, func() { parent.End() }
}

func dbSpans(rec *tracetest.SpanRecorder) []sdktrace.ReadOnlySpan {
	var out []sdktrace.ReadOnlySpan
	for _, s := range rec.Ended() {
		if s.Name() != "parent" {
			out = append(out, s)
		}
	}
	return out
}

func runQuery(tr dbTracer, ctx context.Context, sql string, args []any, err error) {
	ctx = tr.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{SQL: sql, Args: args})
	tr.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{Err: err})
}

func TestPostgresSlowCallSpan(t *testing.T) {
	for name, tr := range tracers() {
		t.Run(name, func(t *testing.T) {
			ctx, rec, end := recordingCtx()
			runQuery(tr, ctx, "SELECT id FROM work_items WHERE id = $1", []any{"x"}, nil)
			bctx := tr.TraceBatchStart(ctx, nil, pgx.TraceBatchStartData{})
			tr.TraceBatchQuery(bctx, nil, pgx.TraceBatchQueryData{SQL: "INSERT INTO t VALUES ($1)"})
			tr.TraceBatchEnd(bctx, nil, pgx.TraceBatchEndData{})
			end()
			spans := dbSpans(rec)
			if len(spans) != 2 {
				t.Fatalf("spans = %d, want 2", len(spans))
			}
			if spans[0].Name() != "SELECT work_items" || spans[1].Name() != "BATCH" {
				t.Fatalf("names = %q, %q", spans[0].Name(), spans[1].Name())
			}
			wantKeys := 5
			if name == "poolAcquireTracer" {
				wantKeys = 6 // dh.db.pool
			}
			if got := len(spans[0].Attributes()); got != wantKeys {
				t.Fatalf("attribute count = %d, want %d", got, wantKeys)
			}
		})
	}
}

func TestPostgresNoParentAndFastCallEmitNothing(t *testing.T) {
	for name, tr := range tracers() {
		t.Run(name, func(t *testing.T) {
			runQuery(tr, context.Background(), "SELECT 1", nil, errors.New("x")) // no parent: must not panic or emit
		})
	}
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	ctx, parent := tp.Tracer("t").Start(context.Background(), "parent")
	fast := spanTracer{spans: dbspan.NewEmitter(dbspan.SystemPostgres, "").WithMin(time.Hour)}
	runQuery(fast, ctx, "SELECT 1", nil, nil)
	parent.End()
	if len(dbSpans(rec)) != 0 {
		t.Fatal("fast call emitted a span")
	}
}

func TestPostgresErrorSpanUsesSQLStateOnly(t *testing.T) {
	for name, tr := range tracers() {
		t.Run(name, func(t *testing.T) {
			ctx, rec, end := recordingCtx()
			runQuery(tr, ctx, "INSERT INTO t (a) VALUES ($1)", nil,
				&pgconn.PgError{Code: "23505", Message: "duplicate key value (a)=(secret@x.y)", Detail: "Key (a)=(secret@x.y) exists"})
			end()
			s := dbSpans(rec)[0]
			if s.Status().Code != codes.Error || s.Status().Description != "" {
				t.Fatalf("status = %+v", s.Status())
			}
			var typ string
			for _, a := range s.Attributes() {
				if a.Key == "error.type" {
					typ = a.Value.AsString()
				}
			}
			if typ != "pg_23505" {
				t.Fatalf("error.type = %q", typ)
			}
		})
	}
}

// The leak test: literals in the statement, arguments and error text must
// reach no attribute, name, status description or event.
func TestPostgresSpansNeverCarryStatementTextArgsOrErrorText(t *testing.T) {
	secrets := []string{"secret@x.y", "sk-abc", "arg-secret-1", "arg-secret-2"}
	stmt := "SELECT * FROM t WHERE email = 'secret@x.y' AND token = 'sk-abc'"
	args := []any{"arg-secret-1", 42, "arg-secret-2"}
	errText := errors.New("duplicate value secret@x.y token sk-abc arg-secret-1")
	for name, tr := range tracers() {
		t.Run(name, func(t *testing.T) {
			ctx, rec, end := recordingCtx()
			runQuery(tr, ctx, stmt, args, nil)
			runQuery(tr, ctx, stmt, args, errText)
			runQuery(tr, ctx, stmt, args, &pgconn.PgError{Code: "42P01", Message: "relation secret@x.y", Detail: "sk-abc"})
			bctx := tr.TraceBatchStart(ctx, nil, pgx.TraceBatchStartData{})
			tr.TraceBatchQuery(bctx, nil, pgx.TraceBatchQueryData{SQL: stmt, Args: args})
			tr.TraceBatchEnd(bctx, nil, pgx.TraceBatchEndData{Err: errText})
			end()
			spans := dbSpans(rec)
			if len(spans) != 4 {
				t.Fatalf("spans = %d, want 4", len(spans))
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
		})
	}
}

func TestNewWithoutTracerInstallsSpanTracer(t *testing.T) {
	cfg := DefaultConfig("postgres://u:p@127.0.0.1:1/db")
	pool, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, ok := pool.Config().ConnConfig.Tracer.(spanTracer); !ok {
		t.Fatalf("tracer = %T, want spanTracer", pool.Config().ConnConfig.Tracer)
	}
	cfg.Tracer = NewPhaseTracer("domain")
	pool2, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool2.Close()
	if _, ok := pool2.Config().ConnConfig.Tracer.(*poolAcquireTracer); !ok {
		t.Fatalf("explicit tracer replaced: %T", pool2.Config().ConnConfig.Tracer)
	}
}

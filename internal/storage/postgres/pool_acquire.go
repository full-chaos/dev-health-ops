package postgres

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/dbphase"
	"github.com/full-chaos/dev-health-ops/internal/platform/dbspan"
)

// PoolAcquireObserver records pgx pool acquisition latency by bounded pool
// name ("domain"|"queue_control") and result
// ("acquired"|"timeout"|"cancelled"|"error") — the exact vocabulary
// jobruntime.MetricsCollector.ObserveDatabasePoolAcquire enforces.
// Implementations must be safe for concurrent use and must never see a DSN,
// credential, or query text: only the pool label and a duration.
type PoolAcquireObserver interface {
	ObserveDatabasePoolAcquire(pool, result string, duration time.Duration) error
}

type poolAcquireStartKey struct{}

type (
	phaseAcquireKey   struct{}
	phaseStatementKey struct{}
)

// poolAcquireTracer is both a pgx.QueryTracer and a pgxpool.AcquireTracer.
// It times Acquire, feeds dbphase and emits slow-call client spans
// (CHAOS-9364). pgxpool only honors an AcquireTracer that is reachable
// through ConnConfig.Tracer (which is typed pgx.QueryTracer).
//
// The observer is attached after construction (attach) rather than passed to
// the constructor, because pgxpool freezes its AcquireTracer at
// pgxpool.NewWithConfig — before the process's MetricsCollector, which is
// built later in internal/workerservice, exists.
type poolAcquireTracer struct {
	pool  string
	spans dbspan.Emitter

	mu       sync.RWMutex
	observer PoolAcquireObserver
}

func newPoolAcquireTracer(pool string) *poolAcquireTracer {
	return &poolAcquireTracer{pool: pool, spans: dbspan.NewEmitter(dbspan.SystemPostgres, pool)}
}

func (t *poolAcquireTracer) attach(observer PoolAcquireObserver) {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.observer = observer
	t.mu.Unlock()
}

func (t *poolAcquireTracer) TraceAcquireStart(ctx context.Context, _ *pgxpool.Pool, _ pgxpool.TraceAcquireStartData) context.Context {
	ctx = context.WithValue(ctx, poolAcquireStartKey{}, time.Now())
	// The phase trace (CHAOS-6936) is independent of the metric observer: a
	// pool with no observer attached (the coordinator pool, or a pool before
	// AttachPoolAcquireObserver) still tells a failed stage that its budget
	// went to waiting for a connection.
	return context.WithValue(ctx, phaseAcquireKey{}, dbphase.Start(ctx, dbphase.KindAcquire, t.pool, ""))
}

func (t *poolAcquireTracer) TraceAcquireEnd(ctx context.Context, _ *pgxpool.Pool, data pgxpool.TraceAcquireEndData) {
	if handle, ok := ctx.Value(phaseAcquireKey{}).(dbphase.Handle); ok {
		handle.End(data.Err)
	}
	t.mu.RLock()
	observer := t.observer
	t.mu.RUnlock()
	if observer == nil {
		return
	}
	started, ok := ctx.Value(poolAcquireStartKey{}).(time.Time)
	if !ok {
		return
	}
	_ = observer.ObserveDatabasePoolAcquire(t.pool, poolAcquireResult(data.Err), time.Since(started))
}

func poolAcquireResult(err error) string {
	switch {
	case err == nil:
		return "acquired"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "cancelled"
	default:
		return "error"
	}
}

// TraceQueryStart/TraceQueryEnd record each statement round trip (BEGIN and
// COMMIT included -- pgx issues them as statements) into the dbphase.Trace the
// call's context carries, if any (CHAOS-6936), and into a slow-call client span
// when the context carries a recording span (CHAOS-9364). They never feed the
// acquire metric and never record arguments: pgx hands the SQL text only, and
// the span carries a summary and a hash, never the text.
func (t *poolAcquireTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return traceStatementStart(ctx, t.pool, t.spans, data.SQL)
}

func (t *poolAcquireTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	traceStatementEnd(ctx, data.Err)
}

// TraceBatchStart/Query/End cover pgx.SendBatch: one BATCH span per round
// trip. Individual batch queries are not spanned.
func (t *poolAcquireTracer) TraceBatchStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceBatchStartData) context.Context {
	return traceBatchStart(ctx, t.spans)
}

func (t *poolAcquireTracer) TraceBatchQuery(ctx context.Context, _ *pgx.Conn, _ pgx.TraceBatchQueryData) {
}

func (t *poolAcquireTracer) TraceBatchEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceBatchEndData) {
	traceBatchEnd(ctx, data.Err)
}

// spanKey carries the span probe and statement from Start to End.
type spanKey struct{}

type spanState struct {
	probe dbspan.Probe
	sql   string
	op    string
}

func traceStatementStart(ctx context.Context, pool string, spans dbspan.Emitter, sql string) context.Context {
	if probe := spans.Start(ctx); probe.Active() {
		ctx = context.WithValue(ctx, spanKey{}, spanState{probe: probe, sql: sql})
	}
	if dbphase.From(ctx) == nil {
		return ctx
	}
	return context.WithValue(ctx, phaseStatementKey{}, dbphase.Start(ctx, dbphase.KindStatement, pool, sql))
}

func traceStatementEnd(ctx context.Context, err error) {
	if handle, ok := ctx.Value(phaseStatementKey{}).(dbphase.Handle); ok {
		handle.End(err)
	}
	finishSpan(ctx, err)
}

func traceBatchStart(ctx context.Context, spans dbspan.Emitter) context.Context {
	if probe := spans.Start(ctx); probe.Active() {
		return context.WithValue(ctx, spanKey{}, spanState{probe: probe, op: dbspan.OpBatch})
	}
	return ctx
}

func traceBatchEnd(ctx context.Context, err error) { finishSpan(ctx, err) }

func finishSpan(ctx context.Context, err error) {
	if st, ok := ctx.Value(spanKey{}).(spanState); ok {
		st.probe.Finish(ctx, st.sql, st.op, err, pgErrorClass(err))
	}
}

// pgErrorClass is a bounded class: the five-character SQLSTATE of a server
// error ("pg_23505"), never its message, or the generic class.
func pgErrorClass(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code != "" && len(pgErr.Code) <= 5 {
		return "pg_" + pgErr.Code
	}
	return dbspan.ErrorClass(err)
}

// spanTracer is the span-only tracer factory.New installs on a pool that was
// built without one, so every pool the process owns produces slow-call spans.
type spanTracer struct{ spans dbspan.Emitter }

func (t spanTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return traceStatementStart(ctx, "", t.spans, data.SQL)
}

func (t spanTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	traceStatementEnd(ctx, data.Err)
}

func (t spanTracer) TraceBatchStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceBatchStartData) context.Context {
	return traceBatchStart(ctx, t.spans)
}

func (t spanTracer) TraceBatchQuery(context.Context, *pgx.Conn, pgx.TraceBatchQueryData) {}

func (t spanTracer) TraceBatchEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceBatchEndData) {
	traceBatchEnd(ctx, data.Err)
}

var (
	_ pgxpool.AcquireTracer = (*poolAcquireTracer)(nil)
	_ pgx.QueryTracer       = (*poolAcquireTracer)(nil)
	_ pgx.BatchTracer       = (*poolAcquireTracer)(nil)
	_ pgx.QueryTracer       = spanTracer{}
	_ pgx.BatchTracer       = spanTracer{}
)

// NewPhaseTracer returns the tracer the runtime pools carry, with no metric
// observer, for a pool built outside NewRuntimePools that must still feed the
// dbphase.Trace its callers' contexts carry (CHAOS-6936). Pass it as
// Config.Tracer.
func NewPhaseTracer(pool string) pgx.QueryTracer {
	return newPoolAcquireTracer(pool)
}

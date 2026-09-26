package postgres

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/dbphase"
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
// Query tracing is an intentional no-op: this exists solely to time
// Acquire, and pgxpool only honors an AcquireTracer that is reachable
// through ConnConfig.Tracer (which is typed pgx.QueryTracer).
//
// The observer is attached after construction (attach) rather than passed to
// the constructor, because pgxpool freezes its AcquireTracer at
// pgxpool.NewWithConfig — before the process's MetricsCollector, which is
// built later in internal/workerservice, exists.
type poolAcquireTracer struct {
	pool string

	mu       sync.RWMutex
	observer PoolAcquireObserver
}

func newPoolAcquireTracer(pool string) *poolAcquireTracer {
	return &poolAcquireTracer{pool: pool}
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
// call's context carries, if any (CHAOS-6936). They never feed the acquire
// metric and never see arguments: pgx hands the SQL text only.
func (t *poolAcquireTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if dbphase.From(ctx) == nil {
		return ctx
	}
	return context.WithValue(ctx, phaseStatementKey{}, dbphase.Start(ctx, dbphase.KindStatement, t.pool, data.SQL))
}

func (t *poolAcquireTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	if handle, ok := ctx.Value(phaseStatementKey{}).(dbphase.Handle); ok {
		handle.End(data.Err)
	}
}

var (
	_ pgxpool.AcquireTracer = (*poolAcquireTracer)(nil)
	_ pgx.QueryTracer       = (*poolAcquireTracer)(nil)
)

// NewPhaseTracer returns the tracer the runtime pools carry, with no metric
// observer, for a pool built outside NewRuntimePools that must still feed the
// dbphase.Trace its callers' contexts carry (CHAOS-6936). Pass it as
// Config.Tracer.
func NewPhaseTracer(pool string) pgx.QueryTracer {
	return newPoolAcquireTracer(pool)
}

package postgres

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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

// poolAcquireTracer is both a pgx.QueryTracer and a pgxpool.AcquireTracer.
// Query tracing is an intentional no-op: this exists solely to time
// Acquire, and pgxpool only honors an AcquireTracer that is reachable
// through ConnConfig.Tracer (which is typed pgx.QueryTracer).
//
// The observer is attached after construction (attach) rather than passed to
// the constructor, because pgxpool freezes its AcquireTracer at
// pgxpool.NewWithConfig — before the process's MetricsCollector, which is
// built later in cmd/dev-health-worker, exists.
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
	return context.WithValue(ctx, poolAcquireStartKey{}, time.Now())
}

func (t *poolAcquireTracer) TraceAcquireEnd(ctx context.Context, _ *pgxpool.Pool, data pgxpool.TraceAcquireEndData) {
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

// TraceQueryStart/TraceQueryEnd are required to satisfy pgx.QueryTracer (the
// type of ConnConfig.Tracer) but intentionally do nothing: this tracer only
// instruments Acquire.
func (t *poolAcquireTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	return ctx
}

func (t *poolAcquireTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

var (
	_ pgxpool.AcquireTracer = (*poolAcquireTracer)(nil)
	_ pgx.QueryTracer       = (*poolAcquireTracer)(nil)
)

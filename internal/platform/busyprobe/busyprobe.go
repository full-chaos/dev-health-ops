// Package busyprobe is the readiness rule "a work pool fully acquired by
// progressing work is BUSY, not BROKEN" (CHAOS-6771), shared by every runtime
// that self-probes its domain pool (worker, scheduler, reconciler).
//
// A readiness transaction probe (selfprobe.TxOpener.Begin) that has to queue
// for a pool connection behind the process's own work runs into its deadline
// without ever reaching the database, and reads as an unavailable database.
// Opener tolerates that in one narrow shape only:
//
//   - the acquire ran into ITS OWN deadline while WAITING FOR A POOL
//     CONNECTION (a selfprobe.AcquireError: no statement was sent, so nothing
//     says the database is broken; a deadline on the BEGIN itself, after a
//     connection was acquired, is a stalled database and stays a failure),
//   - the pool really is fully acquired right now,
//   - the caller's progress guard is green (work is provably moving), and
//   - the caller's own deadline still has time left, because the health
//     registry gives up on a check at ITS budget: a pass that only arrives
//     afterwards is counted but never seen (CHAOS-6771 r1 P1).
//
// Everything else stays a failure, and every tolerated probe is loud: a
// rate-limited log line and a counter.
package busyprobe

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/selfprobe"
)

const (
	// GuardTimeout bounds the progress-guard read, and is the time reserved
	// for it inside the probe's own deadline (see acquireContext).
	GuardTimeout = 2 * time.Second
	// AcquireWait is how long a probe waits for a connection when the pool is
	// ALREADY fully acquired: a healthy pool frees one in milliseconds, so
	// waiting out the whole check budget only delays the (busy) answer to the
	// kubelet. When the pool is not saturated the probe keeps the full budget.
	AcquireWait = time.Second
	// LogInterval limits the busy log line per check; the counter counts all.
	LogInterval = 30 * time.Second
)

// Counter counts probes that passed as busy, by check name, and rate-limits
// their log line. It is a health.MetricsSource.
type Counter struct {
	metric string
	checks []string
	logger *slog.Logger
	now    func() time.Time

	mu      sync.Mutex
	counts  map[string]uint64
	lastLog map[string]time.Time
}

// NewCounter builds a Counter exposing metric{check=...} for the named checks.
// logger may be nil.
func NewCounter(metric string, checks []string, logger *slog.Logger) *Counter {
	return &Counter{
		metric: metric, checks: checks, logger: logger, now: time.Now,
		counts: map[string]uint64{}, lastLog: map[string]time.Time{},
	}
}

// SetClock replaces the clock; for tests.
func (c *Counter) SetClock(now func() time.Time) { c.now = now }

// Record counts one busy pass and logs it at most once per LogInterval.
func (c *Counter) Record(ctx context.Context, check string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.counts[check]++
	count := c.counts[check]
	now := c.now()
	logNow := now.Sub(c.lastLog[check]) >= LogInterval
	if logNow {
		c.lastLog[check] = now
	}
	c.mu.Unlock()
	if logNow && c.logger != nil {
		c.logger.WarnContext(ctx, "readiness probe passed as busy: the domain pool is fully acquired by progressing work",
			"check", check, "busy_probes_total", count)
	}
}

// WritePrometheus implements health.MetricsSource.
func (c *Counter) WritePrometheus(w io.Writer) error {
	if _, err := fmt.Fprintf(w, "# HELP %s Readiness probes that timed out acquiring a connection from a fully acquired domain pool while the progress guard was green, and passed as busy (CHAOS-6771).\n", c.metric); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "# TYPE %s counter\n", c.metric); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, check := range c.checks {
		if _, err := fmt.Fprintf(w, "%s{check=%q} %d\n", c.metric, check, c.counts[check]); err != nil {
			return err
		}
	}
	return nil
}

// Opener wraps a domain-pool TxOpener for one named check.
type Opener struct {
	Inner selfprobe.TxOpener
	Check string
	// Saturated reports the domain pool is fully acquired right now.
	Saturated func() bool
	// Progress is the progress guard; nil error = work is provably moving.
	Progress func(context.Context) error
	Counter  *Counter
}

type busyTx struct{}

func (busyTx) Rollback(context.Context) error { return nil }

// acquireContext bounds the acquire so time is left inside the caller's own
// deadline for the progress guard and the busy pass to reach the caller. The
// reserve is GuardTimeout, capped at half of what remains so a short budget
// still gets an acquire; a saturated pool waits at most AcquireWait. A context
// with no deadline is used as is (unless saturated).
func acquireContext(ctx context.Context, saturated bool) (context.Context, context.CancelFunc) {
	deadline, ok := ctx.Deadline()
	if !ok {
		if saturated {
			return context.WithTimeout(ctx, AcquireWait)
		}
		return ctx, func() {}
	}
	reserve := GuardTimeout
	if half := time.Until(deadline) / 2; reserve > half {
		reserve = half
	}
	acquireBy := deadline.Add(-reserve)
	if saturated {
		if capped := time.Now().Add(AcquireWait); capped.Before(acquireBy) {
			acquireBy = capped
		}
	}
	return context.WithDeadline(ctx, acquireBy)
}

// Begin implements selfprobe.TxOpener.
func (o Opener) Begin(ctx context.Context) (selfprobe.Tx, error) {
	acquireCtx, cancelAcquire := acquireContext(ctx, o.Saturated != nil && o.Saturated())
	defer cancelAcquire()
	tx, err := o.Inner.Begin(acquireCtx)
	if err == nil {
		return tx, nil
	}
	// The caller's own deadline is gone too: there is no time left to prove
	// progress, and the caller has already given up on this probe.
	if ctx.Err() != nil {
		return nil, err
	}
	if !errors.Is(err, context.DeadlineExceeded) || !selfprobe.IsAcquireError(err) || o.Saturated == nil || o.Progress == nil || !o.Saturated() {
		return nil, err
	}
	guardCtx, cancelGuard := context.WithTimeout(ctx, GuardTimeout)
	defer cancelGuard()
	if o.Progress(guardCtx) != nil {
		return nil, err
	}
	o.Counter.Record(ctx, o.Check)
	return busyTx{}, nil
}

// Saturated reports whether every connection of the pool is acquired.
func Saturated(pool *pgxpool.Pool) bool {
	if pool == nil {
		return false
	}
	stat := pool.Stat()
	return stat != nil && stat.MaxConns() > 0 && stat.AcquiredConns() >= stat.MaxConns()
}

// PoolProgress is a progress guard derived from the pool itself, for runtimes
// with no queue-level claim signal: work is moving when connections keep being
// acquired. A pool wedged by stuck work completes no acquires, so its count
// stops advancing and, after the window, the guard goes red.
type PoolProgress struct {
	acquireCount func() int64
	window       time.Duration
	now          func() time.Time

	mu        sync.Mutex
	lastCount int64
	lastMove  time.Time
}

// NewPoolProgress observes pool's completed-acquire count MINUS ownAcquires
// (the probe's own successful acquires, selfprobe.OwnAcquires; nil = none): the
// probe acquiring a connection is not evidence that the pool's work is moving,
// and counting it would let a pool wedged by stuck work read as progressing for
// as long as probes keep succeeding at acquiring (CHAOS-6771 r1). The clock
// starts at construction: a new process gets one full window before "no
// acquires" can read as wedged. A nil pool is never ready.
func NewPoolProgress(pool *pgxpool.Pool, window time.Duration, ownAcquires func() int64) *PoolProgress {
	if pool == nil {
		return newPoolProgress(nil, window, time.Now)
	}
	return newPoolProgress(func() int64 {
		count := pool.Stat().AcquireCount()
		if ownAcquires != nil {
			count -= ownAcquires()
		}
		return count
	}, window, time.Now)
}

func newPoolProgress(acquireCount func() int64, window time.Duration, now func() time.Time) *PoolProgress {
	return &PoolProgress{acquireCount: acquireCount, window: window, now: now, lastMove: now()}
}

// Ready is the guard: nil while an acquire completed within the window.
func (p *PoolProgress) Ready(context.Context) error {
	if p == nil || p.acquireCount == nil {
		return errors.New("busyprobe: no pool to observe")
	}
	count := p.acquireCount()
	now := p.now()
	p.mu.Lock()
	defer p.mu.Unlock()
	if count != p.lastCount {
		p.lastCount, p.lastMove = count, now
		return nil
	}
	if now.Sub(p.lastMove) <= p.window {
		return nil
	}
	return errors.New("busyprobe: no acquire completed within the progress window")
}

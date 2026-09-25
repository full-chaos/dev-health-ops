package workerservice

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/selfprobe"
)

// CHAOS-6771: readiness answers "can this replica do its work", and a work
// pool fully acquired by progressing jobs is BUSY, not BROKEN. go-sync sat 0/1
// while its sync backlog drained: its four domain-pool connections were held
// by claimed jobs, so the transaction probes (idempotency_backend and the
// execution-liveness monitor) queued for a connection, ran into their deadline
// and read as an unavailable database, although 13.7k acquires had succeeded
// and the backlog was draining. The tolerance below is deliberately narrow:
//
//   - only an acquire that ran into ITS OWN deadline (the driver never got a
//     transaction, so nothing says the database is broken),
//   - only while the domain pool really is fully acquired right now,
//   - only while the claim-liveness guard is green (a real job was claimed
//     recently, or the queues are confirmed empty or at capacity) -- a pool
//     wedged by stuck jobs has no recent claims and still fails,
//
// and every tolerated probe is loud: a log line and a counter.
const (
	// busyGuardTimeout bounds the claim-liveness read made after the probe's own
	// deadline has already passed; it runs on a fresh context for that reason.
	busyGuardTimeout = 2 * time.Second
	// busyLogInterval limits the busy log line per check; the counter counts all.
	busyLogInterval = 30 * time.Second
)

// readinessBusy counts probes that passed as busy, by check name.
type readinessBusy struct {
	mu       sync.Mutex
	counts   map[string]uint64
	lastLog  map[string]time.Time
	now      func() time.Time
	logger   *slog.Logger
	sourceID string
}

func newReadinessBusy(logger *slog.Logger) *readinessBusy {
	return &readinessBusy{
		counts: map[string]uint64{}, lastLog: map[string]time.Time{}, now: time.Now, logger: logger,
	}
}

func (b *readinessBusy) record(ctx context.Context, check string) {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.counts[check]++
	count := b.counts[check]
	now := b.now()
	logNow := now.Sub(b.lastLog[check]) >= busyLogInterval
	if logNow {
		b.lastLog[check] = now
	}
	b.mu.Unlock()
	if logNow && b.logger != nil {
		b.logger.WarnContext(ctx, "worker readiness probe passed as busy: the domain pool is fully acquired by progressing work",
			"check", check, "busy_probes_total", count)
	}
}

// WritePrometheus makes readinessBusy a health.MetricsSource.
func (b *readinessBusy) WritePrometheus(w io.Writer) error {
	if _, err := fmt.Fprintln(w, "# HELP worker_readiness_busy_total Readiness probes that timed out acquiring a connection from a fully acquired domain pool while claim liveness was green, and passed as busy (CHAOS-6771)."); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "# TYPE worker_readiness_busy_total counter"); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, check := range []string{"idempotency_backend", "execution_liveness"} {
		if _, err := fmt.Fprintf(w, "worker_readiness_busy_total{check=%q} %d\n", check, b.counts[check]); err != nil {
			return err
		}
	}
	return nil
}

// busyTolerantOpener wraps the domain pool's TxOpener for one named check.
type busyTolerantOpener struct {
	inner selfprobe.TxOpener
	check string
	// saturated reports the domain pool is fully acquired right now.
	saturated func() bool
	// progress is the claim-liveness guard; nil error = work is progressing.
	progress func(context.Context) error
	busy     *readinessBusy
}

type busyTx struct{}

func (busyTx) Rollback(context.Context) error { return nil }

func (o busyTolerantOpener) Begin(ctx context.Context) (selfprobe.Tx, error) {
	tx, err := o.inner.Begin(ctx)
	if err == nil {
		return tx, nil
	}
	if !errors.Is(err, context.DeadlineExceeded) || o.saturated == nil || o.progress == nil || !o.saturated() {
		return nil, err
	}
	guardCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), busyGuardTimeout)
	defer cancel()
	if o.progress(guardCtx) != nil {
		return nil, err
	}
	o.busy.record(ctx, o.check)
	return busyTx{}, nil
}

// Package poolstat exposes a work pool's saturation as a metric and a loud log
// line, and NOTHING ELSE (CHAOS-6800, lead D2588).
//
// Readiness answers "can this replica run its own checks on its own probe
// pool". Whether the shared WORK pool happens to be fully acquired right now is
// contention, not health: three review rounds each found a new hole in every
// heuristic that tried to read it as busy-versus-broken, so readiness never
// consults it. This source is the operator's view of that contention; it never
// feeds a readiness check.
package poolstat

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// LogInterval limits the saturation log line; the gauge is exact on every scrape.
const LogInterval = 30 * time.Second

// SampleInterval is how often the running Source looks at the pool to log (a var so
// tests of the services that wire it can shrink it). The
// warning must not depend on a metrics scrape: with scraping absent or broken,
// sustained saturation would otherwise leave no trace at all (CHAOS-6800 r4).
var SampleInterval = 10 * time.Second

// Source is a health.MetricsSource for one work pool AND a lifecycle component
// whose ticker logs saturation independently of any scrape.
type Source struct {
	metric string
	pool   *pgxpool.Pool
	logger *slog.Logger
	now    func() time.Time
	every  time.Duration

	mu      sync.Mutex
	lastLog time.Time

	startOnce sync.Once
	stopOnce  sync.Once
	done      chan struct{}
	stopped   chan struct{}
	running   bool
}

// New builds a Source writing metric{pool="domain"} (acquired / max, 0..1) and
// metric_acquired_conns / metric_max_conns companions. logger may be nil; a nil
// pool reports zeros.
func New(metric string, pool *pgxpool.Pool, logger *slog.Logger) *Source {
	return &Source{
		metric: metric, pool: pool, logger: logger, now: time.Now, every: SampleInterval,
		done: make(chan struct{}), stopped: make(chan struct{}),
	}
}

// SetClock replaces the clock; for tests.
func (s *Source) SetClock(now func() time.Time) { s.now = now }

// SetSampleInterval replaces the ticker interval; for tests. Call before Start.
func (s *Source) SetSampleInterval(every time.Duration) {
	if every > 0 {
		s.every = every
	}
}

func stat(pool *pgxpool.Pool) (statistics *pgxpool.Stat) {
	defer func() {
		if recover() != nil {
			statistics = nil
		}
	}()
	if pool == nil {
		return nil
	}
	return pool.Stat()
}

func (s *Source) numbers() (acquired, max int32) {
	if statistics := stat(s.pool); statistics != nil {
		return statistics.AcquiredConns(), statistics.MaxConns()
	}
	return 0, 0
}

// Name implements lifecycle.Component.
func (s *Source) Name() string { return "pool-saturation-" + s.metric }

// Start implements lifecycle.Component: it starts the sampler and returns.
func (s *Source) Start(context.Context) error {
	s.startOnce.Do(func() {
		s.running = true
		go s.loop()
	})
	return nil
}

// Shutdown implements lifecycle.Component.
func (s *Source) Shutdown(ctx context.Context) error {
	if !s.running {
		return nil
	}
	s.stopOnce.Do(func() { close(s.done) })
	select {
	case <-s.stopped:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Source) loop() {
	defer close(s.stopped)
	ticker := time.NewTicker(s.every)
	defer ticker.Stop()
	s.Sample()
	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			s.Sample()
		}
	}
}

// Sample looks at the pool once and, when it is fully acquired, logs (at most
// once per LogInterval) that readiness does not depend on it, so an operator
// reading the line is not sent chasing a readiness failure that this is not.
func (s *Source) Sample() {
	acquired, max := s.numbers()
	if max <= 0 || acquired < max || s.logger == nil {
		return
	}
	s.mu.Lock()
	now := s.now()
	logNow := now.Sub(s.lastLog) >= LogInterval
	if logNow {
		s.lastLog = now
	}
	s.mu.Unlock()
	if logNow {
		s.logger.Warn("domain work pool is fully acquired; readiness does not depend on it (informational)",
			"metric", s.metric, "acquired_conns", acquired, "max_conns", max)
	}
}

// WritePrometheus implements health.MetricsSource. It only reports the gauge; the
// log line is the sampler's job, so it never depends on a scrape.
func (s *Source) WritePrometheus(w io.Writer) error {
	acquired, max := s.numbers()
	ratio := 0.0
	if max > 0 {
		ratio = float64(acquired) / float64(max)
	}
	for _, line := range []string{
		fmt.Sprintf("# HELP %s Fraction of the domain WORK pool's connections currently acquired. Informational: readiness does not depend on it (CHAOS-6800).\n# TYPE %s gauge\n%s{pool=\"domain\"} %g\n", s.metric, s.metric, s.metric, ratio),
		fmt.Sprintf("# HELP %s_acquired_conns Domain work pool connections currently acquired.\n# TYPE %s_acquired_conns gauge\n%s_acquired_conns{pool=\"domain\"} %d\n", s.metric, s.metric, s.metric, acquired),
		fmt.Sprintf("# HELP %s_max_conns Domain work pool configured maximum connections.\n# TYPE %s_max_conns gauge\n%s_max_conns{pool=\"domain\"} %d\n", s.metric, s.metric, s.metric, max),
	} {
		if _, err := io.WriteString(w, line); err != nil {
			return err
		}
	}
	return nil
}

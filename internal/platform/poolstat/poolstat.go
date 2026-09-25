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
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// LogInterval limits the saturation log line; the gauge is exact on every scrape.
const LogInterval = 30 * time.Second

// Source is a health.MetricsSource for one work pool.
type Source struct {
	metric string
	pool   *pgxpool.Pool
	logger *slog.Logger
	now    func() time.Time

	mu      sync.Mutex
	lastLog time.Time
}

// New builds a Source writing metric{pool="domain"} (acquired / max, 0..1) and
// metric_acquired_conns / metric_max_conns companions. logger may be nil; a nil
// pool reports zeros.
func New(metric string, pool *pgxpool.Pool, logger *slog.Logger) *Source {
	return &Source{metric: metric, pool: pool, logger: logger, now: time.Now}
}

// SetClock replaces the clock; for tests.
func (s *Source) SetClock(now func() time.Time) { s.now = now }

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

// WritePrometheus implements health.MetricsSource. When the pool is fully
// acquired it also logs (at most once per LogInterval) that readiness does NOT
// depend on it, so an operator reading the line is not sent chasing a readiness
// failure that this is not.
func (s *Source) WritePrometheus(w io.Writer) error {
	var acquired, max int32
	if statistics := stat(s.pool); statistics != nil {
		acquired, max = statistics.AcquiredConns(), statistics.MaxConns()
	}
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
	if max > 0 && acquired >= max && s.logger != nil {
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
	return nil
}

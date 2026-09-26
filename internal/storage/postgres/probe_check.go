package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// LazyProbeCheck is a readiness check whose steady-state probe never waits on a connection pool, on
// the database, or on a slow query (CHAOS-6934). The zero value is ready to use.
//
// The queue-control and coordinator work pools are small (2 connections in
// deploy/go-workers/deployment.json) and sit behind session-mode PgBouncer, so a dedicated probe
// pool per role does not fit the declared connection budget. Instead the check runs as a
// CachedPostureCheck: the query itself runs in the BACKGROUND (single-flight, RunTimeout, on the work
// pool, where queueing behind other work harms nothing) and a probe is answered by CheckNoWait from
// the last answer: a pass younger than MaxStale is a pass (refreshed in the background after TTL), a
// definitive refusal is served until replaced, and past MaxStale with no answer the probe REFUSES at
// once, naming why.
//
// Warm starts the first execution at process construction so the process proves its roles before
// Ready. The one exception to "never waits" is a probe that arrives before the very first execution
// has completed (a cold start that was not warmed, e.g. a test): it waits for that single execution,
// bounded by its own context, exactly as CachedPostureCheck.Check does; from then on it never waits.
type LazyProbeCheck struct {
	once  sync.Once
	check *CachedPostureCheck
}

// Warm builds the check and starts its first background execution now. It never waits.
func (l *LazyProbeCheck) Warm(name string, run func(context.Context) error, options PostureCheckOptions) {
	l.build(name, run, options)
}

func (l *LazyProbeCheck) build(name string, run func(context.Context) error, options PostureCheckOptions) {
	l.once.Do(func() {
		l.check = NewCachedRunCheck(name, run, options)
		l.check.Warm()
	})
}

// Answered reports whether the check has produced any state a probe can be answered from.
func (l *LazyProbeCheck) Answered() bool { return l.check != nil && l.check.Answered() }

// Check answers a probe. name and run are those of the first call (or of Warm).
func (l *LazyProbeCheck) Check(ctx context.Context, name string, run func(context.Context) error) error {
	return l.CheckWith(ctx, name, run, PostureCheckOptions{})
}

// CheckWith is Check with tuned freshness/timeouts; zero fields take the defaults. Only the options of
// the FIRST call (or Warm) are kept.
func (l *LazyProbeCheck) CheckWith(ctx context.Context, name string, run func(context.Context) error, options PostureCheckOptions) error {
	l.build(name, run, options)
	var err error
	if l.check.Answered() {
		err = l.check.CheckNoWait()
	} else {
		err = l.check.Check(ctx) // the single cold wait, bounded by the probe's own context
	}
	if err == nil || errors.Is(err, ErrPostureRefused) {
		return err
	}
	// No usable answer: classify by the real cause of the last unanswered execution (a deadline stays
	// a deadline, a genuine failure stays one); with no execution answered yet, the probe's budget ran
	// out without an answer, which is a timeout.
	if cause := l.check.LastUnanswered(); cause != nil {
		if errors.Is(err, cause) {
			return err
		}
		return fmt.Errorf("%w: %w", err, cause)
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return err
	}
	return fmt.Errorf("%w: %w", err, context.DeadlineExceeded)
}

package postgres

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/dialclass"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/platform/timeoutclass"
)

const (
	// FirstConnectBudget bounds the wait of a one-shot process (a hook Job, an
	// init container) for its first connect. A new pod's first connect can be
	// refused or dropped while its NetworkPolicy rules are still programmed.
	FirstConnectBudget = 60 * time.Second
	// FirstConnectAttemptTimeout bounds one attempt, so a dropped SYN is
	// retried instead of holding the whole budget.
	FirstConnectAttemptTimeout = 10 * time.Second
)

// FirstConnectRetry is one failed attempt that is retried. It carries no
// error text: the text can name the host and the login.
type FirstConnectRetry struct {
	Attempt  int
	Elapsed  time.Duration
	Wait     time.Duration
	Category string
}

// FirstConnect waits for a store's first connect: DialRetry over Budget,
// each attempt under AttemptTimeout. A failure that is not a transient dial
// failure (authentication, permission, TLS, configuration) ends it at once.
type FirstConnect struct {
	Budget         time.Duration
	AttemptTimeout time.Duration
	Report         func(FirstConnectRetry)
	Now            func() time.Time
	Sleep          func(context.Context, time.Duration)
}

func DefaultFirstConnect() FirstConnect {
	return FirstConnect{Budget: FirstConnectBudget, AttemptTimeout: FirstConnectAttemptTimeout}
}

// WithReport returns f with report as its Report.
func (f FirstConnect) WithReport(report func(FirstConnectRetry)) FirstConnect {
	f.Report = report
	return f
}

// TextReport writes one line per retry: the verb, the store, the attempt, the
// elapsed time, the failure class and the pause.
func TextReport(w io.Writer, verb, store string) func(FirstConnectRetry) {
	return func(r FirstConnectRetry) {
		_, _ = fmt.Fprintf(w, "%s waiting: %s endpoint not reachable (attempt %d, %s elapsed, %s, next in %s)\n",
			verb, store, r.Attempt, r.Elapsed.Round(100*time.Millisecond), r.Category, r.Wait.Round(100*time.Millisecond))
	}
}

// Do runs attempt until it succeeds, fails with a final error, or Budget is
// spent, and returns the last attempt's error unchanged. An attempt that runs
// out its own time (AttemptTimeout, or the driver's connect timeout) while ctx
// is still live counts as a transient failure.
func (f FirstConnect) Do(ctx context.Context, attempt func(context.Context) error) error {
	var last error
	err := DialRetry{
		Budget: f.Budget,
		Now:    f.Now,
		Sleep:  f.Sleep,
		OnRetry: func(n int, elapsed, wait time.Duration) {
			if f.Report != nil {
				f.Report(FirstConnectRetry{Attempt: n, Elapsed: elapsed, Wait: wait, Category: dialclass.Category(last)})
			}
		},
	}.Do(ctx, func(ctx context.Context) error {
		attemptCtx, cancel := ctx, context.CancelFunc(func() {})
		if f.AttemptTimeout > 0 {
			attemptCtx, cancel = context.WithTimeout(ctx, f.AttemptTimeout)
		}
		defer cancel()
		last = attempt(attemptCtx)
		if last != nil && ctx.Err() == nil && timeoutclass.IsTimeout(last) {
			return timeoutclass.ErrTimeout
		}
		return last
	})
	if err == nil {
		return nil
	}
	return last
}

// OpenWaiting is Open with its Ping under wait: the same pool, and on failure
// the same redacted ErrUnavailable error Open returns.
func OpenWaiting(ctx context.Context, config Config, wait FirstConnect) (*pgxpool.Pool, error) {
	pool, err := New(ctx, config)
	if err != nil {
		return nil, err
	}
	if err := wait.Do(ctx, pool.Ping); err != nil {
		resolved := resolvedCredentials(pool.Config())
		pool.Close()
		return nil, secrets.WithRedactedCauseAlso(ErrUnavailable, config.URI, err, resolved...)
	}
	return pool, nil
}

// ConnectWaiting is pgx.Connect under wait. It returns the last attempt's
// error unchanged; the caller redacts it as it redacts a pgx.Connect error.
func ConnectWaiting(ctx context.Context, dsn string, wait FirstConnect) (*pgx.Conn, error) {
	var conn *pgx.Conn
	err := wait.Do(ctx, func(ctx context.Context) error {
		c, err := pgx.Connect(ctx, dsn)
		if err != nil {
			return err
		}
		conn = c
		return nil
	})
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// OpenRuntimePoolsWaiting is OpenRuntimePools with its Ping under wait: on a
// failed wait it returns ErrUnavailable, as OpenRuntimePools does.
func OpenRuntimePoolsWaiting(ctx context.Context, runtimeConfig RuntimeConfig, wait FirstConnect) (*RuntimePools, error) {
	pools, err := NewRuntimePools(ctx, runtimeConfig)
	if err != nil {
		return nil, err
	}
	if err := wait.Do(ctx, pools.pingEach); err != nil {
		pools.Close()
		return nil, ErrUnavailable
	}
	return pools, nil
}

// pingEach pings the pools Ping pings, in the same order, and returns the
// driver's error so the wait can classify it.
func (p *RuntimePools) pingEach(ctx context.Context) error {
	if p == nil || p.Domain == nil || p.QueueControl == nil {
		return ErrUnavailable
	}
	if err := p.Domain.Ping(ctx); err != nil {
		return err
	}
	if err := p.QueueControl.Ping(ctx); err != nil {
		return err
	}
	if p.Coordinator != nil {
		return p.Coordinator.Ping(ctx)
	}
	return nil
}

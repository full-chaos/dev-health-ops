package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Defaults for CachedPostureCheck (CHAOS-6765).
//
// rolePostureQuery sweeps every relation in the public and River schemas with
// has_*_privilege calls. On the production catalog that takes 1.4-1.9 s, and
// the readiness registry bounds each check at 2 s and cancels it at the
// deadline -- so a check that ran per probe both flapped go-api readiness and
// could never finish once the catalog grew (a cancelled query caches nothing).
// A role's grants only change at provisioning time, never per request, so the
// answer is safe to reuse for a bounded window.
const (
	// defaultPostureTTL is how long a passing answer is served as fresh.
	defaultPostureTTL = 30 * time.Second
	// defaultPostureMaxStale is how long a passing answer may still be served
	// while a background refresh runs or keeps failing to answer. Past it the
	// probe must see a live answer (or fail): drift cannot hide indefinitely.
	defaultPostureMaxStale = 5 * time.Minute
	// defaultPostureRefusalTTL keeps concurrent probes from each re-running the
	// heavy query, and is deliberately SHORTER than the shortest readiness
	// probe period in deploy/values.prod.yaml (goApi readinessProbe: 5 s), so a
	// failing answer is never served across two probes: the next probe after a
	// GRANT/REVOKE fix runs the query again.
	defaultPostureRefusalTTL = 2 * time.Second
	// defaultPostureRunTimeout bounds one real execution. It is detached from
	// the probe's own deadline on purpose, so a probe that gives up still lets
	// the query finish and fill the cache for the next probe.
	defaultPostureRunTimeout = 30 * time.Second
)

// PostureCheckOptions tunes a CachedPostureCheck. Zero values take the
// defaults above; Logger may be nil.
type PostureCheckOptions struct {
	Logger     *slog.Logger
	TTL        time.Duration
	MaxStale   time.Duration
	RefusalTTL time.Duration
	RunTimeout time.Duration
}

// CachedPostureCheck is CheckRolePosture for a readiness probe: bounded,
// single-flight and cached.
//
//   - A passing answer younger than TTL is returned without touching the
//     database. One older than TTL but younger than MaxStale is still returned
//     immediately, and a background refresh (single-flight, RunTimeout) starts.
//   - With no usable passing answer the caller waits for the single in-flight
//     execution, bounded by its own context. If the caller gives up the
//     execution still completes and the next probe reads its answer.
//   - A refusal (ErrPostureRefused) invalidates the passing answer at once and
//     is served for RefusalTTL; it names the first mismatched privilege.
//   - A query that never answered (connection error, deadline) is not cached
//     as an answer; a prior passing answer stays valid until MaxStale.
//
// Each real execution logs its duration and outcome. Use CheckRolePosture
// directly where a live answer every call is the point (startup gates,
// tests that flip grants).
type CachedPostureCheck struct {
	run        func(context.Context) error
	role       string
	logger     *slog.Logger
	ttl        time.Duration
	maxStale   time.Duration
	refusalTTL time.Duration
	runTimeout time.Duration
	now        func() time.Time

	mu        sync.Mutex
	okAt      time.Time
	refused   error
	refusedAt time.Time
	flight    *postureFlight
}

type postureFlight struct {
	done chan struct{}
	err  error
}

// NewCachedPostureCheck binds one role's declared posture to a pool. Invalid
// identifiers surface as ErrUnavailable from Check, as CheckRolePosture does.
func NewCachedPostureCheck(
	pool *pgxpool.Pool, expectedRole, riverSchema string, posture RolePosture, options PostureCheckOptions,
) *CachedPostureCheck {
	return newCachedPostureCheck(expectedRole, func(ctx context.Context) error {
		return CheckRolePosture(ctx, pool, expectedRole, riverSchema, posture)
	}, options)
}

func newCachedPostureCheck(role string, run func(context.Context) error, options PostureCheckOptions) *CachedPostureCheck {
	pick := func(value, fallback time.Duration) time.Duration {
		if value > 0 {
			return value
		}
		return fallback
	}
	return &CachedPostureCheck{
		run:        run,
		role:       role,
		logger:     options.Logger,
		ttl:        pick(options.TTL, defaultPostureTTL),
		maxStale:   pick(options.MaxStale, defaultPostureMaxStale),
		refusalTTL: pick(options.RefusalTTL, defaultPostureRefusalTTL),
		runTimeout: pick(options.RunTimeout, defaultPostureRunTimeout),
		now:        time.Now,
	}
}

// Check reports whether the role's live grants match its declared posture,
// from cache where the rules on CachedPostureCheck allow.
func (c *CachedPostureCheck) Check(ctx context.Context) error {
	c.mu.Lock()
	now := c.now()
	if !c.okAt.IsZero() {
		age := now.Sub(c.okAt)
		if age < c.ttl {
			c.mu.Unlock()
			return nil
		}
		if age < c.maxStale {
			c.startFlightLocked()
			c.mu.Unlock()
			return nil
		}
	}
	if c.refused != nil && now.Sub(c.refusedAt) < c.refusalTTL {
		err := c.refused
		c.mu.Unlock()
		return err
	}
	flight := c.startFlightLocked()
	c.mu.Unlock()

	select {
	case <-flight.done:
		return flight.err
	case <-ctx.Done():
		return fmt.Errorf("%w: role posture check still running: %w", ErrUnavailable, ctx.Err())
	}
}

// startFlightLocked returns the in-flight execution, starting one if none.
// The caller holds c.mu.
func (c *CachedPostureCheck) startFlightLocked() *postureFlight {
	if c.flight != nil {
		return c.flight
	}
	flight := &postureFlight{done: make(chan struct{})}
	c.flight = flight
	go c.execute(flight)
	return flight
}

func (c *CachedPostureCheck) execute(flight *postureFlight) {
	ctx, cancel := context.WithTimeout(context.Background(), c.runTimeout)
	defer cancel()
	started := c.now()
	err := c.protectedRun(ctx)
	elapsed := c.now().Sub(started)

	c.mu.Lock()
	finished := c.now()
	switch {
	case err == nil:
		c.okAt, c.refused = finished, nil
	case errors.Is(err, ErrPostureRefused):
		c.okAt, c.refused, c.refusedAt = time.Time{}, err, finished
	}
	flight.err = err
	c.flight = nil
	close(flight.done)
	c.mu.Unlock()

	if c.logger == nil {
		return
	}
	attrs := []slog.Attr{
		slog.String("role", c.role),
		slog.Int64("duration_ms", elapsed.Milliseconds()),
	}
	switch {
	case err == nil:
		attrs = append(attrs, slog.String("outcome", "ok"))
		c.logger.LogAttrs(ctx, slog.LevelInfo, "role posture check", attrs...)
	case errors.Is(err, ErrPostureRefused):
		attrs = append(attrs, slog.String("outcome", "refused"), slog.String("detail", err.Error()))
		c.logger.LogAttrs(ctx, slog.LevelError, "role posture check", attrs...)
	default:
		attrs = append(attrs, slog.String("outcome", "unanswered"), slog.String("detail", err.Error()))
		c.logger.LogAttrs(ctx, slog.LevelWarn, "role posture check", attrs...)
	}
}

// protectedRun turns a panic in the check into an error, so a panicking run
// still releases waiters and clears the in-flight slot.
func (c *CachedPostureCheck) protectedRun(ctx context.Context) (err error) {
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("%w: role posture check panicked", ErrUnavailable)
		}
	}()
	return c.run(ctx)
}

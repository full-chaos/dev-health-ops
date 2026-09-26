package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Defaults for CachedPostureCheck (CHAOS-6765, CHAOS-6937).
//
// rolePostureQuery swept every relation in the public and River schemas with
// has_*_privilege calls. On the production catalog that took 1.4-1.9 s (up to
// 14 s under load), and the readiness registry bounds each check at 2 s and
// cancels it at the deadline -- so a check that ran per probe both flapped
// go-api readiness and could never finish once the catalog grew (a cancelled
// query caches nothing). CHAOS-6937 reads the ACL columns of the governed
// schemas instead, which removes most of that cost; the cadence below is what
// bounds what is left: on prod the check held 10-30% of a 4-connection domain
// pool for seconds at a time, every ~10 s per replica, so other stages ran
// into their budgets waiting for a connection.
//
// A role's grants only change at provisioning time, never per request, so the
// answer is safe to reuse for a bounded window.
const (
	// defaultPostureTTL is how long a passing answer is served as fresh: the
	// interval between refreshes of a healthy role. Together with
	// defaultPostureJitter it is the longest a grant change can go unseen by a
	// running replica (a replica that has just started proves its posture at
	// startup, before it is ready).
	defaultPostureTTL = 5 * time.Minute
	// defaultPostureJitter shortens each pass's TTL by a random fraction of up
	// to this much, drawn afresh per pass, so replicas that started together do
	// not refresh in lockstep and load the pooler at the same instant. It only
	// ever shortens: TTL stays the upper bound on staleness.
	defaultPostureJitter = 0.2
	// defaultRunCheckTTL and defaultRunCheckMaxStale are the freshness of a cached check that is NOT a
	// role-posture statement (NewCachedRunCheck): the pre-CHAOS-6937 values.
	defaultRunCheckTTL      = 30 * time.Second
	defaultRunCheckMaxStale = 5 * time.Minute
	// defaultPostureMaxStale is how long a passing answer may still be served
	// while a background refresh runs or keeps failing to answer. Past it the
	// probe must see a live answer (or fail): drift cannot hide indefinitely.
	// It is three TTLs, so two consecutive refreshes may go unanswered (a busy
	// pool, a slow pooler) before readiness is affected.
	defaultPostureMaxStale = 3 * defaultPostureTTL
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
	// Jitter is the largest fraction of TTL a pass may be shortened by. Zero
	// takes the default; a negative value disables jitter.
	Jitter float64
	// Rand returns a value in [0, 1) for the per-pass jitter. Nil uses
	// math/rand/v2; tests inject a fixed one.
	Rand func() float64
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
	jitter     float64
	rand       func() float64
	now        func() time.Time

	mu   sync.Mutex
	okAt time.Time
	// passTTL is THIS pass's freshness window: ttl shortened by a random
	// fraction of at most jitter, drawn when the pass was recorded.
	passTTL   time.Duration
	refused   error
	refusedAt time.Time
	// unanswered is the error of the last execution that neither passed nor was a definitive
	// refusal (a deadline, a dropped connection, ...); cleared by a pass. LazyProbeCheck surfaces it
	// so a probe classifies the failure by its real cause (CHAOS-6934).
	unanswered error
	flight     *postureFlight
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

// NewCachedRunCheck binds ANY bounded read-only readiness query (queue authorization, the River
// schema, contract versions, ...) to the same machinery as NewCachedPostureCheck: single-flight,
// cached, run in the background with its own timeout, and answered to a probe by CheckNoWait
// without the probe ever waiting on a connection pool (CHAOS-6934). name is a checked-in label for
// the log line. run should wrap a DEFINITIVE "no" in ErrPostureRefused (it invalidates the cached
// pass at once); any other error is an unanswered query, not an answer.
//
// Its freshness defaults are the generic ones (30 s fresh, 5 min stale): what a run checks (the
// River schema, the queued contract versions) can change after a pass, unlike provisioned role
// grants. A run that IS a role-posture statement (the queue and coordinator checks) opts into the
// role-posture cadence with AsRolePosture.
func NewCachedRunCheck(name string, run func(context.Context) error, options PostureCheckOptions) *CachedPostureCheck {
	if options.TTL <= 0 {
		options.TTL = defaultRunCheckTTL
		if options.MaxStale <= 0 {
			options.MaxStale = defaultRunCheckMaxStale
		}
	}
	return newCachedPostureCheck(name, run, options)
}

// AsRolePosture returns options whose freshness is the role-posture cadence (defaultPostureTTL,
// defaultPostureMaxStale, CHAOS-6937) unless the caller set its own. Use it for a run that is a
// role's posture statement; every other run keeps NewCachedRunCheck's generic defaults.
func AsRolePosture(options PostureCheckOptions) PostureCheckOptions {
	if options.TTL <= 0 {
		options.TTL = defaultPostureTTL
	}
	if options.MaxStale <= 0 {
		options.MaxStale = defaultPostureMaxStale
	}
	return options
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
		jitter:     postureJitter(options.Jitter),
		rand:       options.Rand,
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
		if age < c.freshFor() {
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

// Warm starts the first background execution now, so a process can begin
// proving its role at startup instead of on its first probe. It never waits.
func (c *CachedPostureCheck) Warm() {
	c.mu.Lock()
	c.startFlightLocked()
	c.mu.Unlock()
}

// CheckNoWait is Check for a probe that must NEVER wait on the expensive query
// (CHAOS-6804; the readiness contract: a probe answers within its budget from
// state it already holds, it does not run a live check). It returns at once
// with the last answer and starts the background execution that will replace
// it:
//
//   - a passing answer younger than MaxStale is a pass (a refresh runs in the
//     background once it is older than TTL);
//   - a refusal is served, with its age, until a newer answer replaces it (a
//     re-run starts once it is older than RefusalTTL), so a GRANT/REVOKE fix is
//     picked up one run later without any probe waiting for it;
//   - no usable answer at all (never proven, or the last pass older than
//     MaxStale and the refreshes since have not answered) is ErrUnavailable,
//     fail closed, stating why and the last pass's age: absence of an answer is
//     never read as a pass.
func (c *CachedPostureCheck) CheckNoWait() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	if !c.okAt.IsZero() {
		age := now.Sub(c.okAt)
		if age < c.freshFor() {
			return nil
		}
		if age < c.maxStale {
			c.startFlightLocked()
			return nil
		}
	}
	if c.refused != nil {
		age := now.Sub(c.refusedAt)
		if age >= c.refusalTTL {
			c.startFlightLocked()
		}
		return fmt.Errorf("%w (answered %s ago)", c.refused, age.Round(time.Second))
	}
	c.startFlightLocked()
	if c.okAt.IsZero() {
		return fmt.Errorf("%w: role posture not yet proven (the check runs in the background)", ErrUnavailable)
	}
	return fmt.Errorf("%w: role posture last proven %s ago, older than the %s bound (the check runs in the background)",
		ErrUnavailable, now.Sub(c.okAt).Round(time.Second), c.maxStale)
}

// Answered reports whether any execution has completed (a pass, a definitive refusal or an
// unanswered query), i.e. whether the check has ever produced state a probe can be answered from.
func (c *CachedPostureCheck) Answered() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.okAt.IsZero() || c.refused != nil || c.unanswered != nil
}

// LastUnanswered returns the error of the last execution that neither passed nor was a definitive
// refusal, or nil.
func (c *CachedPostureCheck) LastUnanswered() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.unanswered
}

// freshFor is how long the current passing answer is served without a
// refresh. The caller holds c.mu.
func (c *CachedPostureCheck) freshFor() time.Duration {
	if c.passTTL > 0 {
		return c.passTTL
	}
	return c.ttl
}

// jitteredTTL draws one pass's freshness window: ttl minus a random fraction of
// at most jitter. The caller holds c.mu.
func (c *CachedPostureCheck) jitteredTTL() time.Duration {
	if c.jitter <= 0 {
		return c.ttl
	}
	draw := rand.Float64
	if c.rand != nil {
		draw = c.rand
	}
	fraction := draw()
	if fraction < 0 || fraction >= 1 {
		fraction = 0
	}
	return time.Duration(float64(c.ttl) * (1 - c.jitter*fraction))
}

// postureJitter resolves the option: zero takes the default, negative turns
// jitter off, and anything above 1 is clamped (a window cannot go negative).
func postureJitter(value float64) float64 {
	switch {
	case value == 0:
		return defaultPostureJitter
	case value < 0:
		return 0
	case value > 1:
		return 1
	default:
		return value
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
		c.okAt, c.refused, c.unanswered = finished, nil, nil
		c.passTTL = c.jitteredTTL()
	case errors.Is(err, ErrPostureRefused):
		c.okAt, c.refused, c.refusedAt, c.unanswered = time.Time{}, err, finished, nil
	default:
		c.unanswered = err
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

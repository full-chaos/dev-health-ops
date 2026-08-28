// Package selfprobe implements a periodic, DB-backed execution-liveness
// signal for River-consuming processes (CHAOS-4029).
//
// The problem it closes: every existing readiness check in this repo proves a
// DEPENDENCY is reachable (domain_postgres, queue_postgres, river_schema,
// ...). None of them prove that THIS PROCESS is still doing the thing it
// exists to do -- claim and execute work. On 2026-08-20 every Go worker
// answered /readyz 200 for two hours while discarding every job at Begin(),
// because the domain pool moved seventeen seconds after startup admission and
// nothing re-observed the process's own execution path afterward.
//
// Monitor closes that gap with an independent, ticking self-probe: it opens
// and immediately rolls back a real transaction against the same pool the
// production write path uses, on its own goroutine, on a fixed interval, and
// remembers only the last SUCCESS. Readiness asks "how long ago did this
// process last prove it can begin a transaction," not "did a dependency
// answer a query I asked ten milliseconds ago from the HTTP handler's own
// goroutine." That distinction matters: a synchronous per-request probe run
// from a healthy HTTP handler can succeed even when the process's own
// background scheduler is wedged, starved, or deadlocked -- the ticker proves
// the process's OWN loop is still alive and pumping, not just that this one
// request could reach the database.
//
// Idle-queue safety (explicit ticket requirement): the probe never depends on
// real job traffic. It runs on its own clock regardless of queue depth, so a
// quiet queue never reads as unhealthy and a wedged claim path is never
// masked by "nothing to claim right now."
//
// Pre-seeded fail-closed: Stale reports true, with reason "never_proven",
// until the first sample completes -- absence of a signal is never silently
// read as healthy. Start runs the first sample SYNCHRONOUSLY and bounded, so
// readiness has a real answer by the time the process opens for admission
// (see Registry.Gate / SetReady sequencing), not an unproven placeholder.
package selfprobe

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TxOpener is the narrow capability Monitor depends on: begin a transaction
// and roll it back. Narrowed to an interface (rather than depending on
// *pgxpool.Pool directly) so tests can prove failure handling without a live
// database. Use NewPool to wrap a real *pgxpool.Pool.
type TxOpener interface {
	Begin(ctx context.Context) (Tx, error)
}

// Tx is the narrow rollback capability the probe needs from a began
// transaction.
type Tx interface {
	Rollback(ctx context.Context) error
}

// poolOpener adapts *pgxpool.Pool to TxOpener. pgx.Tx (the concrete Begin
// return type) declares Rollback(context.Context) error, so it satisfies Tx
// structurally -- no wrapping of the transaction itself is needed.
type poolOpener struct{ pool *pgxpool.Pool }

// NewPool wraps a real *pgxpool.Pool as a TxOpener. A nil pool is preserved
// as a nil TxOpener so New's nil-checks compose correctly rather than
// wrapping a nil pool in a non-nil interface value.
func NewPool(pool *pgxpool.Pool) TxOpener {
	if pool == nil {
		return nil
	}
	return poolOpener{pool: pool}
}

func (o poolOpener) Begin(ctx context.Context) (Tx, error) {
	return o.pool.Begin(ctx)
}

// Once runs a single, synchronous Begin+Rollback round trip against opener
// and returns a bounded, sanitized error -- never the underlying driver text
// -- suitable for direct use as a health.CheckFunc. It is the immediate,
// per-poll counterpart to Monitor's ticking staleness check: cheap enough to
// run on every /readyz request, so a dependency that drops between two
// samples of the ticking monitor is still visible within one poll instead of
// waiting up to the staleness window.
//
// A nil opener (an unconstructed dependency) fails closed rather than
// panicking, matching every other CheckFunc in this repo.
func Once(ctx context.Context, opener TxOpener) (err error) {
	// health.Registry's own CheckFunc executor already recovers a panicking
	// check (registry.go's requiredCheck.execute), but Once is an exported
	// function other callers may use directly, so it carries its own
	// recovery rather than depending on every caller having that same
	// protection -- the same reasoning Monitor.safeAttempt documents.
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("execution liveness probe panicked")
		}
	}()
	if opener == nil {
		return fmt.Errorf("execution liveness probe target is unconfigured")
	}
	tx, beginErr := opener.Begin(ctx)
	if beginErr != nil {
		return fmt.Errorf("begin probe transaction: unavailable")
	}
	if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
		return fmt.Errorf("rollback probe transaction: unavailable")
	}
	return nil
}

const (
	// DefaultInterval is how often the probe samples. It is intentionally
	// short relative to the staleness window below so a real outage is
	// visible within one interval, not one staleness window.
	DefaultInterval = 20 * time.Second
	// DefaultStalenessMultiple sizes the staleness window as a multiple of
	// the sampling interval rather than an independent constant, so the two
	// cannot silently drift apart the way two unrelated tunables would
	// (CHAOS-3938's lesson applied here). Three misses in a row is required
	// before readiness flips, which absorbs one slow sample or one
	// transient timeout without flapping while still bounding detection
	// latency to a small, fixed multiple of the interval.
	DefaultStalenessMultiple = 3
	// sampleTimeout bounds a single probe attempt. It must stay well under
	// the health registry's own per-check timeout (2s default,
	// health.NewRegistry) since the readiness check reads Monitor's held
	// state rather than blocking on a fresh sample -- this timeout only
	// protects the background loop from a single hung attempt piling up
	// goroutines.
	sampleTimeout = 5 * time.Second
)

// FailureReason is a bounded, metric-safe label. Never derived from a raw
// driver error: the underlying error may carry a DSN or credential fragment
// (see health.Registry's doc comment on the same discipline), so only these
// pre-declared categories ever reach a log line or a Prometheus label.
type FailureReason string

const (
	ReasonNone           FailureReason = ""
	ReasonBeginFailed    FailureReason = "begin_failed"
	ReasonRollbackFailed FailureReason = "rollback_failed"
	ReasonTimeout        FailureReason = "timeout"
	ReasonUnconfigured   FailureReason = "unconfigured"
	ReasonPanicked       FailureReason = "panicked"
)

// Monitor is a lifecycle.Component (Name/Start/Shutdown by structural typing,
// avoiding an import of the lifecycle package from this dependency-light
// leaf) that periodically self-probes a pool's ability to begin and roll
// back a transaction, and answers a bounded staleness question for a
// health.CheckFunc to wrap.
type Monitor struct {
	name     string
	opener   TxOpener
	interval time.Duration
	stale    time.Duration
	logger   *slog.Logger

	mu            sync.RWMutex
	lastSuccessAt time.Time
	everSucceeded bool
	lastReason    FailureReason

	failures map[FailureReason]uint64

	now func() time.Time

	start   sync.Once
	stop    sync.Once
	running atomic.Bool
	done    chan struct{}
	stopped chan struct{}
}

// New constructs a Monitor. opener/logger nil returns nil, matching this
// repo's convention for optional lifecycle components (see
// newQueueHealthMonitor) -- callers compose the returned *Monitor into their
// component list only when non-nil.
func New(name string, opener TxOpener, logger *slog.Logger) *Monitor {
	if opener == nil || logger == nil || name == "" {
		return nil
	}
	return &Monitor{
		name:     name,
		opener:   opener,
		interval: DefaultInterval,
		stale:    DefaultStalenessMultiple * DefaultInterval,
		logger:   logger,
		failures: make(map[FailureReason]uint64),
		now:      time.Now,
		done:     make(chan struct{}),
		stopped:  make(chan struct{}),
	}
}

func (m *Monitor) Name() string { return "self-probe-" + m.name }

// SetInterval overrides the sampling interval from DefaultInterval. It must
// be called before Start -- changing it afterward races the running ticker
// goroutine and is unsupported. This exists so a caller composing a Monitor
// for a latency-sensitive deployment (or a test proving staleness detection
// in real wall-clock time, rather than waiting out the production interval)
// can tune it without a second constructor.
func (m *Monitor) SetInterval(interval time.Duration) {
	if interval > 0 {
		m.interval = interval
	}
}

// SetStaleness overrides the staleness window directly, in place of the
// DefaultStalenessMultiple*interval derivation. Same before-Start
// restriction as SetInterval.
func (m *Monitor) SetStaleness(stale time.Duration) {
	if stale > 0 {
		m.stale = stale
	}
}

// Start runs one bounded sample SYNCHRONOUSLY before returning, so this
// component never reports "never_proven" purely because its background loop
// had not scheduled yet by the time readiness opened -- see the package doc.
// A failed first sample is not itself a Start error: exactly like
// queueHealthMonitor, this component must never block process startup on a
// dependency that already has its own required check (idempotency_backend /
// domain_postgres); its job is to keep sampling and let the readiness check
// this feeds report the failure.
func (m *Monitor) Start(ctx context.Context) error {
	m.start.Do(func() {
		m.running.Store(true)
		m.Probe(ctx)
		go m.loop()
	})
	return nil
}

// Probe runs one bounded sample synchronously and is safe to call any number
// of times, including before Start (callers that construct a Monitor and
// need Ready to reflect a real result immediately -- e.g. a readiness check
// registered in the same call that constructs the monitor, before the
// lifecycle runtime has run Start on anything -- call this directly rather
// than waiting for Start's own first sample). Start calls this internally,
// so calling both is harmless: the second call simply re-samples.
func (m *Monitor) Probe(ctx context.Context) { m.sample(ctx) }

func (m *Monitor) Shutdown(ctx context.Context) error {
	if !m.running.Load() {
		return nil
	}
	m.stop.Do(func() { close(m.done) })
	select {
	case <-m.stopped:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Monitor) loop() {
	defer close(m.stopped)
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		select {
		case <-m.done:
			return
		case <-ticker.C:
			m.sample(context.Background())
		}
	}
}

func (m *Monitor) sample(parent context.Context) {
	ctx, cancel := context.WithTimeout(parent, sampleTimeout)
	defer cancel()

	// A panicking attempt fails the sample instead of crashing the caller --
	// the same discipline health.Registry's requiredCheck.execute applies to
	// every readiness CheckFunc ("Any failed, missing, timed-out, or
	// panicking check fails readiness", registry.go's doc comment). This
	// matters doubly here: sample also runs on the background ticker
	// goroutine (loop), where an unrecovered panic would crash the whole
	// process, not just fail one check.
	reason := m.safeAttempt(ctx)
	now := m.now()

	m.mu.Lock()
	m.lastReason = reason
	if reason == ReasonNone {
		m.lastSuccessAt = now
		m.everSucceeded = true
	} else {
		m.failures[reason]++
	}
	m.mu.Unlock()

	if reason != ReasonNone {
		// Bounded reason only -- never the underlying driver error, which can
		// carry a DSN (see health.Registry's identical discipline).
		m.logger.WarnContext(ctx, "execution_liveness_probe_failed",
			"probe", m.name, "reason", string(reason))
	}
}

// safeAttempt wraps attempt with a panic recovery, converting a panicking
// TxOpener/Tx implementation into ReasonPanicked instead of propagating the
// panic. A zero-value or misconfigured pool implementation panicking on
// Begin/Rollback is exactly the kind of "dependency is broken" state this
// package exists to surface as a readiness failure, not a process crash.
func (m *Monitor) safeAttempt(ctx context.Context) (reason FailureReason) {
	defer func() {
		if recover() != nil {
			reason = ReasonPanicked
		}
	}()
	return m.attempt(ctx)
}

func (m *Monitor) attempt(ctx context.Context) FailureReason {
	if m.opener == nil {
		return ReasonUnconfigured
	}
	tx, err := m.opener.Begin(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return ReasonTimeout
		}
		return ReasonBeginFailed
	}
	if err := tx.Rollback(ctx); err != nil {
		if ctx.Err() != nil {
			return ReasonTimeout
		}
		return ReasonRollbackFailed
	}
	return ReasonNone
}

// Ready is a health.CheckFunc-compatible readiness probe. It answers from
// held state -- it never itself performs I/O -- so it is cheap to run on
// every /readyz poll alongside the synchronous dependency checks. It fails
// closed in two cases: no sample has ever succeeded ("never_proven", the
// pre-seeded state so absence of a signal cannot read as healthy), or the
// last success is older than the staleness window (the process's own
// execution loop has stopped proving itself, whatever the reason).
func (m *Monitor) Ready(context.Context) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !m.everSucceeded {
		return fmt.Errorf("execution liveness probe %q has never succeeded", m.name)
	}
	if age := m.now().Sub(m.lastSuccessAt); age > m.stale {
		return fmt.Errorf("execution liveness probe %q is stale (last success %s ago, threshold %s)",
			m.name, age, m.stale)
	}
	return nil
}

// snapshot is an internally consistent read of the fields WritePrometheus
// renders, taken under one lock acquisition so the exposition below cannot
// mix a pre- and post-mutation view of related fields.
type snapshot struct {
	everSucceeded bool
	lastSuccessAt time.Time
	lastReason    FailureReason
	failures      map[FailureReason]uint64
	now           time.Time
}

func (m *Monitor) snapshot() snapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	failures := make(map[FailureReason]uint64, len(m.failures))
	for reason, count := range m.failures {
		failures[reason] = count
	}
	return snapshot{
		everSucceeded: m.everSucceeded,
		lastSuccessAt: m.lastSuccessAt,
		lastReason:    m.lastReason,
		failures:      failures,
		now:           m.now(),
	}
}

// WritePrometheus implements health.MetricsSource. It is the telemetry half
// of this check: an operator (or an alert) can see not just that
// execution_liveness failed but WHY, and how long ago the process last
// proved it could execute, independent of whether /readyz is even being
// polled right now.
func (m *Monitor) WritePrometheus(output io.Writer) error {
	snap := m.snapshot()
	var buffer bytes.Buffer
	fmt.Fprintf(&buffer,
		"# HELP dev_health_execution_liveness_seconds_since_success Seconds since the named self-probe last proved it could begin a transaction. -1 if it has never succeeded.\n"+
			"# TYPE dev_health_execution_liveness_seconds_since_success gauge\n")
	secondsSinceSuccess := -1.0
	if snap.everSucceeded {
		secondsSinceSuccess = snap.now.Sub(snap.lastSuccessAt).Seconds()
	}
	fmt.Fprintf(&buffer, "dev_health_execution_liveness_seconds_since_success{probe=%s} %s\n",
		strconv.Quote(m.name), strconv.FormatFloat(secondsSinceSuccess, 'f', 3, 64))

	fmt.Fprintf(&buffer,
		"# HELP dev_health_execution_liveness_probe_failures_total Self-probe attempts that did not end in a committed round-trip, by bounded reason.\n"+
			"# TYPE dev_health_execution_liveness_probe_failures_total counter\n")
	// Every declared reason gets a series, even at zero, for the same
	// "alert on absence, not presence" reason dev_health_runtime_check_failed
	// pre-registers every check name (health/server.go).
	for _, reason := range []FailureReason{ReasonBeginFailed, ReasonRollbackFailed, ReasonTimeout, ReasonUnconfigured, ReasonPanicked} {
		fmt.Fprintf(&buffer, "dev_health_execution_liveness_probe_failures_total{probe=%s,reason=%s} %d\n",
			strconv.Quote(m.name), strconv.Quote(string(reason)), snap.failures[reason])
	}
	_, err := output.Write(buffer.Bytes())
	return err
}

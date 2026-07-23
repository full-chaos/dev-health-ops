package joboutbox

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

const (
	minReconcilerPollInterval = 10 * time.Millisecond
	maxReconcilerPollInterval = 15 * time.Minute
	minReconcilerLimit        = 1
	maxReconcilerLimit        = 100
)

var (
	// ErrReconcilerLoopAlreadyStarted prevents a second owner from claiming the
	// same outbox reconciler instance.
	ErrReconcilerLoopAlreadyStarted = errors.New("worker outbox reconciler loop already started")
	errReconcilerLoopNotReady       = errors.New("worker outbox reconciler loop has not completed a successful step")
)

// RelayStepper is the bounded, transactional unit of reconciliation. Relay
// implements this interface; keeping the loop on this small seam makes its
// lifecycle independently testable.
type RelayStepper interface {
	Step(context.Context, time.Time, int) (StepResult, error)
}

// ReconcilerLoopConfig bounds the polling work owned by ReconcilerLoop. The
// registry is required so the loop can fail readiness closed until its first
// successful step and export a stable operator metric fragment.
type ReconcilerLoopConfig struct {
	PollInterval time.Duration
	Limit        int
	Registry     *health.Registry
}

func DefaultReconcilerLoopConfig(registry *health.Registry) ReconcilerLoopConfig {
	return ReconcilerLoopConfig{
		PollInterval: time.Second,
		Limit:        100,
		Registry:     registry,
	}
}

func (config ReconcilerLoopConfig) validate() error {
	if config.Registry == nil ||
		config.PollInterval < minReconcilerPollInterval || config.PollInterval > maxReconcilerPollInterval ||
		config.Limit < minReconcilerLimit || config.Limit > maxReconcilerLimit {
		return ErrInvalidConfiguration
	}
	return nil
}

type reconcilerClock interface {
	Now() time.Time
	NewTicker(time.Duration) reconcilerTicker
}

type reconcilerTicker interface {
	Chan() <-chan time.Time
	Stop()
}

type systemReconcilerClock struct{}

func (systemReconcilerClock) Now() time.Time { return time.Now() }

func (systemReconcilerClock) NewTicker(interval time.Duration) reconcilerTicker {
	return systemReconcilerTicker{Ticker: time.NewTicker(interval)}
}

type systemReconcilerTicker struct{ *time.Ticker }

func (ticker systemReconcilerTicker) Chan() <-chan time.Time { return ticker.C }

// ReconcilerLoop owns one polling goroutine around bounded Relay steps. A
// successful empty step is a valid database/connectivity proof and opens its
// required readiness dependency. Step errors are fatal: callers should use
// Errors with lifecycle.Runtime so the process exits rather than silently
// serving stale control-plane state.
type ReconcilerLoop struct {
	stepper RelayStepper
	config  ReconcilerLoopConfig
	clock   reconcilerClock

	ready atomic.Bool

	mu       sync.Mutex
	started  bool
	stopping bool
	cancel   context.CancelFunc
	done     chan struct{}
	ticker   reconcilerTicker

	claimed   uint64
	delivered uint64
	retried   uint64
	dead      uint64
	leaseLost uint64
	lastOK    time.Time
	up        bool

	errors chan error
}

// NewReconcilerLoop constructs a fail-closed lifecycle component. The
// readiness and metrics registrations happen at composition time, before the
// shell opens its global readiness gate.
func NewReconcilerLoop(stepper RelayStepper, config ReconcilerLoopConfig) (*ReconcilerLoop, error) {
	return newReconcilerLoop(stepper, config, systemReconcilerClock{})
}

func newReconcilerLoop(
	stepper RelayStepper,
	config ReconcilerLoopConfig,
	clock reconcilerClock,
) (*ReconcilerLoop, error) {
	if stepper == nil || clock == nil || config.validate() != nil {
		return nil, ErrInvalidConfiguration
	}
	loop := &ReconcilerLoop{
		stepper: stepper,
		config:  config,
		clock:   clock,
		errors:  make(chan error, 1),
	}
	if err := config.Registry.RegisterRequired("reconciler_loop", loop.readiness); err != nil {
		return nil, fmt.Errorf("register reconciler readiness: %w", err)
	}
	if err := config.Registry.RegisterMetrics("outbox_reconciler", loop); err != nil {
		return nil, fmt.Errorf("register reconciler metrics: %w", err)
	}
	return loop, nil
}

func (*ReconcilerLoop) Name() string { return "outbox-reconciler-loop" }

// Start performs one immediate bounded step before beginning periodic polling.
// This means the lifecycle runtime never advertises a healthy reconciler based
// only on goroutine creation.
func (loop *ReconcilerLoop) Start(ctx context.Context) error {
	if loop == nil || ctx == nil {
		return ErrInvalidConfiguration
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	loop.mu.Lock()
	if loop.started {
		loop.mu.Unlock()
		return ErrReconcilerLoopAlreadyStarted
	}
	loop.started = true
	loop.mu.Unlock()

	if err := loop.step(ctx, loop.clock.Now()); err != nil {
		loop.setFailed()
		return fmt.Errorf("initial outbox reconciliation: %w", err)
	}

	loopCtx, cancel := context.WithCancel(ctx)
	ticker := loop.clock.NewTicker(loop.config.PollInterval)
	done := make(chan struct{})
	loop.mu.Lock()
	if loop.stopping {
		loop.mu.Unlock()
		ticker.Stop()
		cancel()
		loop.setFailed()
		return context.Canceled
	}
	loop.cancel = cancel
	loop.ticker = ticker
	loop.done = done
	loop.mu.Unlock()
	go loop.run(loopCtx, ticker, done)
	return nil
}

func (loop *ReconcilerLoop) run(ctx context.Context, ticker reconcilerTicker, done chan struct{}) {
	defer close(done)
	for {
		select {
		case <-ctx.Done():
			return
		case now, open := <-ticker.Chan():
			if !open {
				return
			}
			if err := loop.step(ctx, now); err != nil {
				loop.setFailed()
				select {
				case loop.errors <- fmt.Errorf("outbox reconciliation step: %w", err):
				case <-ctx.Done():
				}
				return
			}
		}
	}
}

func (loop *ReconcilerLoop) step(ctx context.Context, now time.Time) error {
	result, err := loop.stepper.Step(ctx, now, loop.config.Limit)
	if err != nil {
		return err
	}
	loop.mu.Lock()
	loop.claimed += nonNegativeUint(result.Claimed)
	loop.delivered += nonNegativeUint(result.Delivered)
	loop.retried += nonNegativeUint(result.Retried)
	loop.dead += nonNegativeUint(result.Dead)
	loop.leaseLost += nonNegativeUint(result.LeaseLost)
	loop.lastOK = now
	loop.up = true
	loop.mu.Unlock()
	loop.ready.Store(true)
	return nil
}

func nonNegativeUint(value int) uint64 {
	if value < 0 {
		return 0
	}
	return uint64(value)
}

func (loop *ReconcilerLoop) setFailed() {
	loop.ready.Store(false)
	loop.mu.Lock()
	loop.up = false
	loop.mu.Unlock()
}

func (loop *ReconcilerLoop) readiness(context.Context) error {
	if loop != nil && loop.ready.Load() {
		return nil
	}
	return errReconcilerLoopNotReady
}

// Errors reports periodic fatal reconciliation errors to lifecycle.Runtime.
// It is intentionally buffered so the loop can stop before the runtime's
// error watcher is scheduled.
func (loop *ReconcilerLoop) Errors() <-chan error {
	if loop == nil {
		return nil
	}
	return loop.errors
}

// Shutdown closes readiness before cancelling its polling goroutine, then
// waits only as long as the lifecycle shutdown context permits.
func (loop *ReconcilerLoop) Shutdown(ctx context.Context) error {
	if loop == nil || ctx == nil {
		return ErrInvalidConfiguration
	}
	loop.setFailed()

	loop.mu.Lock()
	loop.stopping = true
	cancel := loop.cancel
	ticker := loop.ticker
	done := loop.done
	loop.mu.Unlock()
	if ticker != nil {
		ticker.Stop()
	}
	if cancel != nil {
		cancel()
	}
	if done == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// WritePrometheus exports only process-wide, low-cardinality counters and
// gauges. It deliberately contains no job kind, organization, payload, or
// dynamic error text labels.
func (loop *ReconcilerLoop) WritePrometheus(output io.Writer) error {
	if loop == nil || output == nil {
		return errors.New("Prometheus output is required")
	}
	loop.mu.Lock()
	claimed := loop.claimed
	delivered := loop.delivered
	retried := loop.retried
	dead := loop.dead
	leaseLost := loop.leaseLost
	lastOK := loop.lastOK
	up := loop.up
	now := loop.clock.Now()
	loop.mu.Unlock()

	lastSuccessAge := 0.0
	if up && !lastOK.IsZero() && !now.Before(lastOK) {
		lastSuccessAge = now.Sub(lastOK).Seconds()
	}
	var text strings.Builder
	writeReconcilerCounter(&text, "worker_outbox_reconciler_claimed_total", "Outbox rows claimed by the reconciler.", claimed)
	writeReconcilerCounter(&text, "worker_outbox_reconciler_delivered_total", "Outbox rows delivered to River by the reconciler.", delivered)
	writeReconcilerCounter(&text, "worker_outbox_reconciler_retried_total", "Outbox rows scheduled for relay retry by the reconciler.", retried)
	writeReconcilerCounter(&text, "worker_outbox_reconciler_dead_total", "Outbox rows terminalized by the reconciler.", dead)
	writeReconcilerCounter(&text, "worker_outbox_reconciler_lease_lost_total", "Outbox claims lost before reconciliation completed.", leaseLost)
	fmt.Fprint(&text, "# HELP worker_outbox_reconciler_up Whether the reconciler loop is currently healthy.\n# TYPE worker_outbox_reconciler_up gauge\nworker_outbox_reconciler_up ")
	if up {
		text.WriteString("1\n")
	} else {
		text.WriteString("0\n")
	}
	fmt.Fprintf(&text, "# HELP worker_outbox_reconciler_last_success_age_seconds Age of the last successful reconciler step.\n# TYPE worker_outbox_reconciler_last_success_age_seconds gauge\nworker_outbox_reconciler_last_success_age_seconds %s\n", strconv.FormatFloat(lastSuccessAge, 'g', -1, 64))
	_, err := io.WriteString(output, text.String())
	return err
}

func writeReconcilerCounter(output *strings.Builder, name, help string, value uint64) {
	fmt.Fprintf(output, "# HELP %s %s\n# TYPE %s counter\n%s %d\n", name, help, name, name, value)
}

package sync

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
	minLoopPollInterval = 10 * time.Millisecond
	maxLoopPollInterval = 15 * time.Minute
	minLoopStepTimeout  = 10 * time.Millisecond
	maxLoopStepTimeout  = 30 * time.Second
	defaultLoopTimeout  = 2 * time.Second
)

var (
	ErrLoopAlreadyStarted = errors.New("sync scheduler loop already started")
	errLoopNotReady       = errors.New("sync scheduler loop has not completed a successful handoff window")
)

// HandoffStepper keeps the scheduler lifecycle independent of PostgreSQL
// construction. Repository implements it through HandoffDueResult.
type HandoffStepper interface {
	HandoffDueResult(context.Context, time.Time, int, Coordinator) (HandoffResult, error)
}

// LoopConfig bounds each scheduler transaction and its retry cadence. A
// timeout is an error, never a successful empty handoff window.
type LoopConfig struct {
	PollInterval time.Duration
	StepTimeout  time.Duration
	MaxBackoff   time.Duration
	Limit        int
	Registry     *health.Registry
}

func DefaultLoopConfig(registry *health.Registry) LoopConfig {
	return LoopConfig{
		PollInterval: time.Second,
		StepTimeout:  defaultLoopTimeout,
		MaxBackoff:   time.Minute,
		Limit:        maximumSnapshotLimit,
		Registry:     registry,
	}
}

func (config LoopConfig) validate() error {
	if config.Registry == nil ||
		config.PollInterval < minLoopPollInterval || config.PollInterval > maxLoopPollInterval ||
		config.StepTimeout < minLoopStepTimeout || config.StepTimeout > maxLoopStepTimeout ||
		config.MaxBackoff < config.PollInterval || config.MaxBackoff > maxLoopPollInterval ||
		config.Limit < minimumSnapshotLimit || config.Limit > maximumSnapshotLimit {
		return ErrInvalidTransactionRequest
	}
	return nil
}

type loopClock interface {
	Now() time.Time
	NewTicker(time.Duration) loopTicker
}

type loopTicker interface {
	Chan() <-chan time.Time
	Stop()
}

type systemLoopClock struct{}

func (systemLoopClock) Now() time.Time { return time.Now() }
func (systemLoopClock) NewTicker(interval time.Duration) loopTicker {
	return systemLoopTicker{Ticker: time.NewTicker(interval)}
}

type systemLoopTicker struct{ *time.Ticker }

func (ticker systemLoopTicker) Chan() <-chan time.Time { return ticker.C }

// Loop performs one bounded transactional handoff window at a time. Failures
// close readiness and use a capped exponential retry delay; a later successful
// window reopens readiness. Unsupported and invalid cron forms are counted as
// explicit no-write fallback, leaving Celery able to evaluate them.
type Loop struct {
	stepper     HandoffStepper
	coordinator Coordinator
	config      LoopConfig
	clock       loopClock

	ready atomic.Bool

	mu       sync.Mutex
	started  bool
	stopping bool
	cancel   context.CancelFunc
	done     chan struct{}
	ticker   loopTicker

	cycles          uint64
	handoffs        uint64
	unsupportedCron uint64
	invalidCron     uint64
	failures        uint64
	consecutive     uint64
	lastOK          time.Time
	up              bool
}

func NewLoop(stepper HandoffStepper, coordinator Coordinator, config LoopConfig) (*Loop, error) {
	return newLoop(stepper, coordinator, config, systemLoopClock{})
}

func newLoop(stepper HandoffStepper, coordinator Coordinator, config LoopConfig, clock loopClock) (*Loop, error) {
	if stepper == nil || coordinator == nil || clock == nil || config.validate() != nil {
		return nil, ErrInvalidTransactionRequest
	}
	loop := &Loop{stepper: stepper, coordinator: coordinator, config: config, clock: clock}
	if err := config.Registry.RegisterRequired("scheduler_loop", loop.readiness); err != nil {
		return nil, fmt.Errorf("register scheduler loop readiness: %w", err)
	}
	if err := config.Registry.RegisterMetrics("sync_scheduler", loop); err != nil {
		return nil, fmt.Errorf("register scheduler loop metrics: %w", err)
	}
	return loop, nil
}

func (*Loop) Name() string { return "sync-scheduler-loop" }

func (loop *Loop) Start(ctx context.Context) error {
	if loop == nil || ctx == nil {
		return ErrInvalidTransactionRequest
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	loopCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	loop.mu.Lock()
	if loop.started {
		loop.mu.Unlock()
		cancel()
		return ErrLoopAlreadyStarted
	}
	if loop.stopping {
		loop.mu.Unlock()
		cancel()
		return context.Canceled
	}
	loop.started, loop.cancel, loop.done = true, cancel, done
	loop.mu.Unlock()

	if err := loop.step(loopCtx, loop.clock.Now()); err != nil {
		loop.setFailed()
		cancel()
		close(done)
		return fmt.Errorf("initial scheduler handoff: %w", err)
	}
	ticker := loop.clock.NewTicker(loop.config.PollInterval)
	loop.mu.Lock()
	if loop.stopping || loopCtx.Err() != nil {
		loop.mu.Unlock()
		ticker.Stop()
		cancel()
		loop.setFailed()
		close(done)
		if err := loopCtx.Err(); err != nil {
			return err
		}
		return context.Canceled
	}
	loop.ticker = ticker
	loop.mu.Unlock()
	go loop.run(loopCtx, ticker, done)
	return nil
}

func (loop *Loop) run(ctx context.Context, ticker loopTicker, done chan struct{}) {
	defer close(done)
	defer ticker.Stop()
	var nextEligible time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case now, open := <-ticker.Chan():
			if !open {
				loop.setFailed()
				return
			}
			if !nextEligible.IsZero() && now.Before(nextEligible) {
				continue
			}
			if err := loop.step(ctx, now); err != nil {
				loop.setFailed()
				// Measure retry delay from failure completion. A slow or timed
				// out step may leave old ticker values buffered; those must not
				// collapse the intended backoff.
				nextEligible = loop.clock.Now().Add(loop.backoff())
				continue
			}
			nextEligible = time.Time{}
		}
	}
}

func (loop *Loop) step(parent context.Context, now time.Time) error {
	stepCtx, cancel := context.WithTimeout(parent, loop.config.StepTimeout)
	defer cancel()
	result, err := loop.stepper.HandoffDueResult(stepCtx, now.UTC(), loop.config.Limit, loop.coordinator)
	if stepCtx.Err() != nil {
		return stepCtx.Err()
	}
	if result.UnsupportedCron > 0 || result.InvalidCron > 0 {
		loop.mu.Lock()
		loop.unsupportedCron += uint64(result.UnsupportedCron)
		loop.invalidCron += uint64(result.InvalidCron)
		loop.mu.Unlock()
		if err == nil {
			return ErrSchedulerFallbackRequired
		}
	}
	if err != nil {
		return err
	}
	loop.mu.Lock()
	if loop.stopping {
		loop.mu.Unlock()
		return context.Canceled
	}
	loop.cycles++
	loop.handoffs += uint64(len(result.HandedOff))
	loop.unsupportedCron += uint64(result.UnsupportedCron)
	loop.invalidCron += uint64(result.InvalidCron)
	loop.consecutive = 0
	loop.lastOK, loop.up = now.UTC(), true
	loop.ready.Store(true)
	loop.mu.Unlock()
	return nil
}

func (loop *Loop) backoff() time.Duration {
	loop.mu.Lock()
	defer loop.mu.Unlock()
	loop.failures++
	loop.consecutive++
	delay := loop.config.PollInterval
	for failures := uint64(1); failures < loop.consecutive && delay < loop.config.MaxBackoff; failures++ {
		if delay > loop.config.MaxBackoff/2 {
			delay = loop.config.MaxBackoff
			break
		}
		delay *= 2
	}
	if delay > loop.config.MaxBackoff {
		return loop.config.MaxBackoff
	}
	return delay
}

func (loop *Loop) setFailed() {
	loop.ready.Store(false)
	loop.mu.Lock()
	loop.up = false
	loop.mu.Unlock()
}

func (loop *Loop) readiness(context.Context) error {
	if loop != nil && loop.ready.Load() {
		return nil
	}
	return errLoopNotReady
}

func (loop *Loop) Shutdown(ctx context.Context) error {
	if loop == nil || ctx == nil {
		return ErrInvalidTransactionRequest
	}
	loop.setFailed()
	loop.mu.Lock()
	loop.stopping = true
	cancel, ticker, done := loop.cancel, loop.ticker, loop.done
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

func (loop *Loop) WritePrometheus(output io.Writer) error {
	if loop == nil || output == nil {
		return errors.New("Prometheus output is required")
	}
	loop.mu.Lock()
	cycles, handoffs := loop.cycles, loop.handoffs
	unsupported, invalid := loop.unsupportedCron, loop.invalidCron
	failures, consecutive := loop.failures, loop.consecutive
	lastOK, up, now := loop.lastOK, loop.up, loop.clock.Now()
	loop.mu.Unlock()

	age := 0.0
	if !lastOK.IsZero() && !now.Before(lastOK) {
		age = now.Sub(lastOK).Seconds()
	}
	var text strings.Builder
	writeLoopCounter(&text, "sync_scheduler_windows_total", "Successful bounded scheduler handoff windows.", cycles)
	writeLoopCounter(&text, "sync_scheduler_handoffs_total", "Scheduled occurrences durably handed off before marker advancement.", handoffs)
	writeLoopCounter(&text, "sync_scheduler_unsupported_cron_fallback_total", "Unsupported cron candidates left for the existing scheduler owner.", unsupported)
	writeLoopCounter(&text, "sync_scheduler_invalid_cron_total", "Invalid cron candidates left without marker mutation.", invalid)
	writeLoopCounter(&text, "sync_scheduler_failures_total", "Failed bounded scheduler handoff windows.", failures)
	fmt.Fprintf(&text, "# HELP sync_scheduler_consecutive_failures Consecutive failed handoff windows.\n# TYPE sync_scheduler_consecutive_failures gauge\nsync_scheduler_consecutive_failures %d\n", consecutive)
	fmt.Fprint(&text, "# HELP sync_scheduler_up Whether the scheduler has completed a current successful handoff window.\n# TYPE sync_scheduler_up gauge\nsync_scheduler_up ")
	if up {
		text.WriteString("1\n")
	} else {
		text.WriteString("0\n")
	}
	fmt.Fprintf(&text, "# HELP sync_scheduler_last_success_age_seconds Age of the last successful handoff window.\n# TYPE sync_scheduler_last_success_age_seconds gauge\nsync_scheduler_last_success_age_seconds %s\n", strconv.FormatFloat(age, 'g', -1, 64))
	_, err := io.WriteString(output, text.String())
	return err
}

func writeLoopCounter(output *strings.Builder, name, help string, value uint64) {
	fmt.Fprintf(output, "# HELP %s %s\n# TYPE %s counter\n%s %d\n", name, help, name, name, value)
}

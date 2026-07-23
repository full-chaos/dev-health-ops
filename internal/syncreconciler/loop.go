package syncreconciler

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
	minPollInterval           = 10 * time.Millisecond
	maxPollInterval           = 15 * time.Minute
	minObservationTimeout     = 10 * time.Millisecond
	maxObservationTimeout     = 30 * time.Second
	defaultObservationTimeout = 2 * time.Second
)

var (
	ErrLoopAlreadyStarted = errors.New("sync dispatch observer loop already started")
	ErrTickerClosed       = errors.New("sync dispatch observer ticker closed unexpectedly")
	errLoopNotReady       = errors.New("sync dispatch observer loop has not completed a successful observation")
)

// Stepper is kept small so loop lifecycle has no database dependency in its
// tests. Observer satisfies this interface.
type Stepper interface {
	Step(context.Context, time.Time, int) (Observation, error)
}

type LoopConfig struct {
	PollInterval       time.Duration
	ObservationTimeout time.Duration
	Limit              int
	Registry           *health.Registry
	// Recorder is optional and caller-owned. Implementations must honor the
	// non-blocking TryRecord contract; Loop additionally permits only one
	// recorder call in flight and drops while busy. Shutdown never waits for
	// this caller-owned dependency, so a contract-violating recorder can strand
	// at most one goroutine without holding readiness or process shutdown.
	Recorder ObservationRecorder
}

// DefaultLoopConfig allows two seconds for one indexed read of at most 101
// candidates. This is deliberately conservative relative to the bounded query
// while still failing readiness promptly on database stalls.
func DefaultLoopConfig(registry *health.Registry) LoopConfig {
	return LoopConfig{
		PollInterval:       time.Second,
		ObservationTimeout: defaultObservationTimeout,
		Limit:              maximumStepLimit,
		Registry:           registry,
	}
}

func (config LoopConfig) validate() error {
	if config.Registry == nil || config.PollInterval < minPollInterval || config.PollInterval > maxPollInterval ||
		config.ObservationTimeout < minObservationTimeout || config.ObservationTimeout > maxObservationTimeout ||
		config.Limit < minimumStepLimit || config.Limit > maximumStepLimit {
		return ErrInvalidConfiguration
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

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }
func (systemClock) NewTicker(interval time.Duration) loopTicker {
	return systemTicker{Ticker: time.NewTicker(interval)}
}

type systemTicker struct{ *time.Ticker }

func (ticker systemTicker) Chan() <-chan time.Time { return ticker.C }

// Loop owns periodic observation and publishes only the latest successful
// snapshot. Later observation errors are fatal so readiness cannot advertise
// stale database state.
type Loop struct {
	stepper Stepper
	config  LoopConfig
	clock   loopClock

	ready atomic.Bool

	mu       sync.Mutex
	started  bool
	stopping bool
	cancel   context.CancelFunc
	done     chan struct{}
	ticker   loopTicker

	observation  Observation
	lastOK       time.Time
	up           bool
	errors       chan error
	recorderBusy chan struct{}
}

func NewLoop(stepper Stepper, config LoopConfig) (*Loop, error) {
	return newLoop(stepper, config, systemClock{})
}

func newLoop(stepper Stepper, config LoopConfig, clock loopClock) (*Loop, error) {
	if stepper == nil || clock == nil || config.validate() != nil {
		return nil, ErrInvalidConfiguration
	}
	loop := &Loop{stepper: stepper, config: config, clock: clock, errors: make(chan error, 1)}
	if config.Recorder != nil {
		loop.recorderBusy = make(chan struct{}, 1)
	}
	if err := config.Registry.RegisterRequired("sync_dispatch_observer", loop.readiness); err != nil {
		return nil, fmt.Errorf("register sync dispatch observer readiness: %w", err)
	}
	if err := config.Registry.RegisterMetrics("sync_dispatch_observer", loop); err != nil {
		return nil, fmt.Errorf("register sync dispatch observer metrics: %w", err)
	}
	return loop, nil
}

func (*Loop) Name() string { return "sync-dispatch-observer-loop" }

func (loop *Loop) Start(ctx context.Context) error {
	if loop == nil || ctx == nil {
		return ErrInvalidConfiguration
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
	loop.started = true
	loop.cancel = cancel
	loop.done = done
	loop.mu.Unlock()

	if err := loop.step(loopCtx, loop.clock.Now()); err != nil {
		loop.setFailed()
		cancel()
		close(done)
		return fmt.Errorf("initial sync dispatch observation: %w", err)
	}
	ticker := loop.clock.NewTicker(loop.config.PollInterval)
	loop.mu.Lock()
	if loop.stopping || loopCtx.Err() != nil {
		startErr := loopCtx.Err()
		if startErr == nil {
			startErr = context.Canceled
		}
		loop.mu.Unlock()
		ticker.Stop()
		cancel()
		loop.setFailed()
		close(done)
		return startErr
	}
	loop.ticker = ticker
	loop.mu.Unlock()
	go loop.run(loopCtx, ticker, done)
	if err := loopCtx.Err(); err != nil {
		loop.setFailed()
		return err
	}
	return nil
}

func (loop *Loop) run(ctx context.Context, ticker loopTicker, done chan struct{}) {
	var fatal error
	defer close(done)
	defer ticker.Stop()
	defer func() {
		if fatal != nil {
			loop.reportError(ctx, fatal)
		}
	}()
	defer loop.setFailed()
	for {
		select {
		case <-ctx.Done():
			return
		case now, open := <-ticker.Chan():
			if !open {
				if ctx.Err() == nil {
					fatal = ErrTickerClosed
				}
				return
			}
			if err := loop.step(ctx, now); err != nil {
				if isContextError(err) && (ctx.Err() != nil || loop.isStopping()) {
					return
				}
				fatal = fmt.Errorf("sync dispatch observation step: %w", err)
				return
			}
		}
	}
}

func (loop *Loop) reportError(ctx context.Context, err error) {
	select {
	case loop.errors <- err:
	case <-ctx.Done():
	}
}

func (loop *Loop) step(ctx context.Context, now time.Time) error {
	stepCtx, cancel := context.WithTimeout(ctx, loop.config.ObservationTimeout)
	defer cancel()
	observation, err := loop.stepper.Step(stepCtx, now, loop.config.Limit)
	if contextErr := stepCtx.Err(); contextErr != nil {
		return contextErr
	}
	if err != nil {
		// Unknown stored kinds are a failed observation, but their bounded total
		// is still valuable operator evidence. Keep it as a gauge while the
		// readiness failure prevents this process from being considered healthy.
		if errors.Is(err, ErrUnknownKind) {
			offer := false
			loop.mu.Lock()
			if !loop.stopping && stepCtx.Err() == nil {
				loop.observation = copyObservation(observation)
				offer = true
			}
			loop.mu.Unlock()
			if offer {
				loop.offerObservation(observation)
			}
		}
		return err
	}
	if err := stepCtx.Err(); err != nil {
		return err
	}
	loop.mu.Lock()
	if loop.stopping {
		loop.mu.Unlock()
		return context.Canceled
	}
	if err := stepCtx.Err(); err != nil {
		loop.mu.Unlock()
		return err
	}
	loop.observation = copyObservation(observation)
	loop.lastOK = now
	loop.up = true
	loop.ready.Store(true)
	loop.mu.Unlock()
	loop.offerObservation(observation)
	return nil
}

func (loop *Loop) offerObservation(observation Observation) {
	if loop == nil || loop.config.Recorder == nil || loop.recorderBusy == nil {
		return
	}
	select {
	case loop.recorderBusy <- struct{}{}:
	default:
		return
	}
	observation = copyObservation(observation)
	go func() {
		defer func() {
			_ = recover()
			<-loop.recorderBusy
		}()
		_ = loop.config.Recorder.TryRecord(observation)
	}()
}

func (loop *Loop) isStopping() bool {
	loop.mu.Lock()
	defer loop.mu.Unlock()
	return loop.stopping
}

func isContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func copyObservation(observation Observation) Observation {
	observation.Kinds = append([]KindObservation(nil), observation.Kinds...)
	return observation
}

func (loop *Loop) setFailed() {
	loop.mu.Lock()
	loop.up = false
	loop.ready.Store(false)
	loop.mu.Unlock()
}

func (loop *Loop) readiness(context.Context) error {
	if loop != nil && loop.ready.Load() {
		return nil
	}
	return errLoopNotReady
}

func (loop *Loop) Errors() <-chan error {
	if loop == nil {
		return nil
	}
	return loop.errors
}

func (loop *Loop) Shutdown(ctx context.Context) error {
	if loop == nil || ctx == nil {
		return ErrInvalidConfiguration
	}
	loop.mu.Lock()
	loop.stopping = true
	loop.up = false
	loop.ready.Store(false)
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

// WritePrometheus emits current observations as gauges. It never accumulates
// snapshots: counters would misrepresent current queued work after rows are
// dispatched or claims expire.
func (loop *Loop) WritePrometheus(output io.Writer) error {
	if loop == nil || output == nil {
		return errors.New("Prometheus output is required")
	}
	loop.mu.Lock()
	observation := copyObservation(loop.observation)
	lastOK := loop.lastOK
	up := loop.up
	now := loop.clock.Now()
	loop.mu.Unlock()

	var text strings.Builder
	text.WriteString("# HELP sync_dispatch_observer_due_pending Due pending rows in the bounded Python claim-order window by fixed kind.\n# TYPE sync_dispatch_observer_due_pending gauge\n")
	for _, kind := range fixedMetricKinds(observation) {
		fmt.Fprintf(&text, "sync_dispatch_observer_due_pending{kind=%q} %d\n", kind.Kind, kind.DuePending)
	}
	text.WriteString("# HELP sync_dispatch_observer_expired_claims Expired claims among due rows in the bounded Python claim-order window by fixed kind.\n# TYPE sync_dispatch_observer_expired_claims gauge\n")
	for _, kind := range fixedMetricKinds(observation) {
		fmt.Fprintf(&text, "sync_dispatch_observer_expired_claims{kind=%q} %d\n", kind.Kind, kind.ExpiredClaims)
	}
	fmt.Fprintf(&text, "# HELP sync_dispatch_observer_unknown_kinds Unknown-kind rows in the bounded Python claim-order window.\n# TYPE sync_dispatch_observer_unknown_kinds gauge\nsync_dispatch_observer_unknown_kinds %d\n", observation.UnknownKindCount)
	fmt.Fprintf(&text, "# HELP sync_dispatch_observer_celery_due_pending Due pending rows routed to Celery in the bounded Python claim-order window.\n# TYPE sync_dispatch_observer_celery_due_pending gauge\nsync_dispatch_observer_celery_due_pending %d\n", observation.CeleryDuePending)
	fmt.Fprintf(&text, "# HELP sync_dispatch_observer_river_due_pending Due pending rows routed to River in the bounded Python claim-order window.\n# TYPE sync_dispatch_observer_river_due_pending gauge\nsync_dispatch_observer_river_due_pending %d\n", observation.RiverDuePending)
	fmt.Fprintf(&text, "# HELP sync_dispatch_observer_sampled_candidates Due rows sampled from the bounded Python claim-order window.\n# TYPE sync_dispatch_observer_sampled_candidates gauge\nsync_dispatch_observer_sampled_candidates %d\n", observation.SampledCandidates)
	text.WriteString("# HELP sync_dispatch_observer_truncated Whether an extra due row proved the bounded Python claim-order window was truncated.\n# TYPE sync_dispatch_observer_truncated gauge\nsync_dispatch_observer_truncated ")
	if observation.Truncated {
		text.WriteString("1\n")
	} else {
		text.WriteString("0\n")
	}
	text.WriteString("# HELP sync_dispatch_observer_up Whether the observer loop is currently healthy.\n# TYPE sync_dispatch_observer_up gauge\nsync_dispatch_observer_up ")
	if up {
		text.WriteString("1\n")
	} else {
		text.WriteString("0\n")
	}
	if !lastOK.IsZero() {
		lastSuccessAge := 0.0
		if !now.Before(lastOK) {
			lastSuccessAge = now.Sub(lastOK).Seconds()
		}
		fmt.Fprintf(&text, "# HELP sync_dispatch_observer_last_success_age_seconds Age of the last successful sync-dispatch observation.\n# TYPE sync_dispatch_observer_last_success_age_seconds gauge\nsync_dispatch_observer_last_success_age_seconds %s\n", strconv.FormatFloat(lastSuccessAge, 'g', -1, 64))
	}
	_, err := io.WriteString(output, text.String())
	return err
}

// fixedMetricKinds prevents an accidental or test-only Stepper from creating
// an unbounded Prometheus label series. Missing fixed kinds are exported as
// zero until the next valid observer snapshot.
func fixedMetricKinds(observation Observation) []KindObservation {
	byKind := make(map[string]KindObservation, len(observation.Kinds))
	for _, kind := range observation.Kinds {
		if kind.DuePending < 0 || kind.ExpiredClaims < 0 {
			continue
		}
		byKind[kind.Kind] = kind
	}
	result := make([]KindObservation, 0, len(frozenKinds))
	for _, name := range frozenKinds {
		kind := byKind[name]
		kind.Kind = name
		result = append(result, kind)
	}
	return result
}

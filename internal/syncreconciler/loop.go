package syncreconciler

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
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
	// Logger names why an observation step failed. It is optional so tests and
	// embedders need not supply one; a nil Logger discards. Without it this
	// loop could flip readiness closed forever and emit nothing at all
	// (CHAOS-3907).
	Logger *slog.Logger
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

	observation Observation
	// exhaustedRecoveries accumulates across steps. Every other quantity here
	// is a snapshot of current queued work, where a counter would misreport
	// state after rows dispatch; a recovery is an event that already happened,
	// so a snapshot is what would misreport it -- the next step's zero would
	// erase the only evidence that a delivery had to be reclaimed at all.
	exhaustedRecoveries uint64
	// wakeupReportFailures and unreclaimableTerminalized accumulate for the
	// same reason exhaustedRecoveries does: both are EVENTS that already
	// happened, so the next step's zero would erase the only evidence they
	// occurred. The two gauge quantities beside them (runaway wakeups over
	// the threshold, sweep candidates) stay in the observation snapshot,
	// because for those a total that kept climbing after the condition
	// cleared is precisely what an operator must not be shown.
	wakeupReportFailures       uint64
	unreclaimableTerminalized  uint64
	unreclaimableSweepFailures uint64
	lastOK                     time.Time
	up                         bool
	errors                     chan error
	recorderBusy               chan struct{}
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
		loop.logger().ErrorContext(ctx, "sync dispatch observer initial step failed", "error", err.Error())
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
		loop.logger().ErrorContext(ctx, "sync dispatch observer stopped before its polling loop started", "error", startErr.Error())
		close(done)
		return startErr
	}
	loop.ticker = ticker
	loop.mu.Unlock()
	go loop.run(loopCtx, ticker, done)
	if err := loopCtx.Err(); err != nil {
		loop.setFailed()
		loop.logger().ErrorContext(ctx, "sync dispatch observer context failed after start", "error", err.Error())
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
			loop.logger().ErrorContext(ctx, "sync dispatch observer step failed", "error", fatal.Error())
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
	// Recoveries are committed by the repair stage before anything downstream
	// can fail, so they are counted here -- ahead of every error, timeout, and
	// shutdown branch below. Counting them only on the success path would drop
	// exactly the reclaims that happened during a degraded step, which is when
	// a cycling delivery is most likely to be what is degrading it.
	loop.mu.Lock()
	loop.accumulateRecoveriesLocked(observation)
	loop.mu.Unlock()
	if contextErr := stepCtx.Err(); contextErr != nil {
		return contextErr
	}
	if err != nil {
		// The CHAOS-4097 gauges survive a failed pass, on the same principle
		// the ErrUnknownKind branch below already established: a failed
		// observation can still carry bounded operator evidence, and readiness
		// is what says the process is unhealthy.
		//
		// It matters most in exactly the case that motivated it. The pipeline
		// carries these figures through every return precisely because the
		// sweep and the report commit before later stages run, so a kernel or
		// observer fault after a pass measured 83 runaway runs must not leave
		// the dashboard showing the last healthy snapshot. The degraded pass
		// IS the incident (review finding).
		//
		// ONLY these two fields are merged, never the whole observation: the
		// observer's own queue gauges are unreliable on a failed pass, and
		// publishing them would trade one stale number for several wrong ones.
		//
		// And only when the pass MEASURED them. Overwriting a real 83 with a
		// zero the pass never took would clear a gauge-based alert at exactly
		// the moment measurement became unavailable -- the opposite failure
		// from dropping the evidence, and just as bad. An unmeasured pass
		// leaves the last measured value standing; the failure counters above
		// and readiness below are what mark it stale.
		loop.mu.Lock()
		if !loop.stopping {
			if observation.RunawayMeasured {
				loop.observation.RunawayDispatchWakeups = observation.RunawayDispatchWakeups
			}
			if observation.UnreclaimableMeasured {
				loop.observation.UnreclaimableCandidates = observation.UnreclaimableCandidates
			}
		}
		loop.mu.Unlock()
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

// accumulateRecoveriesLocked adds one step's exhausted-delivery recoveries to
// the process total. A negative count cannot be represented in the metric and a
// wrapped total would read as a drop, so both are refused rather than exported.
func (loop *Loop) accumulateRecoveriesLocked(observation Observation) {
	loop.exhaustedRecoveries = accumulateCount(
		loop.exhaustedRecoveries, observation.ExhaustedDeliveriesRecovered,
	)
	loop.wakeupReportFailures = accumulateCount(
		loop.wakeupReportFailures, observation.WakeupReportFailures,
	)
	loop.unreclaimableTerminalized = accumulateCount(
		loop.unreclaimableTerminalized, observation.UnreclaimableTerminalized,
	)
	loop.unreclaimableSweepFailures = accumulateCount(
		loop.unreclaimableSweepFailures, observation.UnreclaimableSweepFailures,
	)
}

// accumulateCount adds one step's events to a process total, refusing both a
// negative count -- which the metric cannot represent -- and an overflow,
// which would wrap and read as a DROP. A counter that goes backwards is worse
// than one that stops: every rate() over it reports a spike that never
// happened. Refusing leaves the total flat, which is at least honest about
// having stopped counting.
func accumulateCount(total uint64, delta int64) uint64 {
	if delta <= 0 {
		return total
	}
	if total > math.MaxUint64-uint64(delta) {
		return total
	}
	return total + uint64(delta)
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

// logger is nil-safe: an unset Config.Logger discards rather than panicking,
// and never falls back to slog.Default(), so an embedder cannot be surprised
// by observer output appearing on a logger it did not choose.
func (loop *Loop) logger() *slog.Logger {
	if loop == nil || loop.config.Logger == nil {
		return slog.New(slog.DiscardHandler)
	}
	return loop.config.Logger
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
	exhaustedRecoveries := loop.exhaustedRecoveries
	wakeupReportFailures := loop.wakeupReportFailures
	unreclaimableTerminalized := loop.unreclaimableTerminalized
	unreclaimableSweepFailures := loop.unreclaimableSweepFailures
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
	fmt.Fprintf(&text, "# HELP sync_dispatch_exhausted_delivery_recoveries_total Coordinator deliveries reclaimed after River spent their whole attempt budget.\n# TYPE sync_dispatch_exhausted_delivery_recoveries_total counter\nsync_dispatch_exhausted_delivery_recoveries_total %d\n", exhaustedRecoveries)
	// CHAOS-4097. The two gauges are the alertable pair: a runaway count above
	// zero says a run is looping, and a failure counter that climbs says the
	// thing which would have told you is broken. Reading either one alone can
	// mislead -- a quiet gauge is meaningless while the detector is down --
	// which is why both are exported rather than only the interesting one.
	//
	// The runaway threshold is stated in the HELP text rather than left in the
	// Go source, because the number is what an operator needs to interpret the
	// series and they are not reading materializer.go at 3am. It is derived
	// from the production distribution (healthy p99 43-211 attempts against
	// 6499 and 72601 for the CHAOS-4093 rows), and it decides nothing: it
	// selects what is reported, never what is written.
	fmt.Fprintf(&text, "# HELP sync_dispatch_runaway_dispatch_wakeups Non-terminal runs whose dispatch wakeup has exceeded %d attempts. Exact, not sampled. Unproven while sync_dispatch_wakeup_report_failures_total is climbing: read the pair, never this alone.\n# TYPE sync_dispatch_runaway_dispatch_wakeups gauge\nsync_dispatch_runaway_dispatch_wakeups %d\n", runawayDispatchAttempts, observation.RunawayDispatchWakeups)
	fmt.Fprintf(&text, "# HELP sync_dispatch_wakeup_report_failures_total Passes on which the runaway dispatch-wakeup report did not run at all -- whether the report faulted or the pass ended before reaching it. While this climbs, the gauge above is a stale zero rather than a measurement.\n# TYPE sync_dispatch_wakeup_report_failures_total counter\nsync_dispatch_wakeup_report_failures_total %d\n", wakeupReportFailures)
	fmt.Fprintf(&text, "# HELP sync_dispatch_unreclaimable_candidates Units the unreclaimable sweep selected on its last pass. Non-zero in shadow mode means work it WOULD have terminalized. Unproven while sync_dispatch_unreclaimable_sweep_failures_total is climbing.\n# TYPE sync_dispatch_unreclaimable_candidates gauge\nsync_dispatch_unreclaimable_candidates %d\n", observation.UnreclaimableCandidates)
	fmt.Fprintf(&text, "# HELP sync_dispatch_unreclaimable_terminalized_total Units the unreclaimable sweep destroyed. Always zero in shadow mode; a persistent gap against the candidate gauge means writes are being refused.\n# TYPE sync_dispatch_unreclaimable_terminalized_total counter\nsync_dispatch_unreclaimable_terminalized_total %d\n", unreclaimableTerminalized)
	fmt.Fprintf(&text, "# HELP sync_dispatch_unreclaimable_sweep_failures_total Passes on which the unreclaimable sweep could not run. The candidate gauge above reads zero on those passes, identically to a healthy idle system, so read the pair.\n# TYPE sync_dispatch_unreclaimable_sweep_failures_total counter\nsync_dispatch_unreclaimable_sweep_failures_total %d\n", unreclaimableSweepFailures)
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

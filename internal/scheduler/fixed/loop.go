package fixed

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

const (
	minLoopPollInterval = time.Second
	maxLoopPollInterval = 15 * time.Minute
	minLoopStepTimeout  = 100 * time.Millisecond
	maxLoopStepTimeout  = 5 * time.Minute
)

var (
	// ErrLoopAlreadyStarted identifies a second Start on one loop.
	ErrLoopAlreadyStarted = errors.New("fixed schedule loop already started")
	errLoopNotReady       = errors.New("fixed schedule loop has not completed a successful window")
	errScheduleOverdue    = errors.New("fixed schedule occurrence is overdue")
)

// Stepper is the engine seam the loop drives.
type Stepper interface {
	Step(context.Context, time.Time) (WindowResult, error)
	Schedules() []Schedule
}

// LoopConfig bounds one window and its retry cadence.
type LoopConfig struct {
	PollInterval time.Duration
	StepTimeout  time.Duration
	MaxBackoff   time.Duration
	Registry     *health.Registry
}

// DefaultLoopConfig is the production cadence. The poll interval is far
// shorter than the shortest declared cadence so a due occurrence is claimed
// promptly, and the duplicate claim path makes over-polling free.
func DefaultLoopConfig(registry *health.Registry) LoopConfig {
	return LoopConfig{
		PollInterval: 15 * time.Second,
		StepTimeout:  2 * time.Minute,
		MaxBackoff:   5 * time.Minute,
		Registry:     registry,
	}
}

func (config LoopConfig) validate() error {
	if config.Registry == nil ||
		config.PollInterval < minLoopPollInterval || config.PollInterval > maxLoopPollInterval ||
		config.StepTimeout < minLoopStepTimeout || config.StepTimeout > maxLoopStepTimeout ||
		config.MaxBackoff < config.PollInterval || config.MaxBackoff > maxLoopPollInterval {
		return ErrEngineUnavailable
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

type scheduleState struct {
	claimed    uint64
	duplicate  uint64
	handoffs   uint64
	skipped    uint64
	failures   uint64
	overdue    bool
	missingFor time.Duration
	// degraded names a bounded no-work condition, for example an installation
	// with no active organizations. It is exported so an operator can
	// distinguish "nothing to do" from "producing work".
	degraded string
	// coldStarts counts baseline records, which are expected exactly once per
	// schedule on a new deployment and never again.
	coldStarts uint64
}

// Loop runs the fixed-schedule engine on a bounded cadence and owns its
// readiness and telemetry.
//
// Readiness closes on a failed window and on any schedule whose newest
// occurrence is older than its declared alert threshold. The second condition
// is what turns "this producer silently never ran" into an operator-visible
// failure rather than a quiet gap in the product.
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

	windows     uint64
	failures    uint64
	consecutive uint64
	lastOK      time.Time
	up          bool
	schedules   map[string]*scheduleState
	overdue     []string
}

// NewLoop constructs the production loop.
func NewLoop(stepper Stepper, config LoopConfig) (*Loop, error) {
	return newLoop(stepper, config, systemLoopClock{})
}

func newLoop(stepper Stepper, config LoopConfig, clock loopClock) (*Loop, error) {
	if stepper == nil || clock == nil || config.validate() != nil {
		return nil, ErrEngineUnavailable
	}
	if len(stepper.Schedules()) == 0 {
		return nil, ErrEngineUnavailable
	}
	loop := &Loop{
		stepper:   stepper,
		config:    config,
		clock:     clock,
		schedules: make(map[string]*scheduleState),
	}
	for _, schedule := range stepper.Schedules() {
		loop.schedules[schedule.ID] = &scheduleState{}
	}
	// Readiness names are deliberately NOT registered here.
	//
	// The health registry rejects duplicate names and offers no unregister, so
	// any constructor that registers a name and then fails leaves the caller
	// unable to register a fallback under that name. That turned a fixed-loop
	// construction failure into a failure of the whole scheduler process,
	// stranding pending occurrences — the precise coupling the optional fixed
	// loop exists to prevent. Ownership of the two readiness names therefore
	// sits with the composition root, which registers them exactly once and
	// unconditionally, before anything fallible runs.
	//
	// Metrics registration stays here because it is the last fallible step and
	// owns a name nothing else claims, so failing it leaves no partial state.
	if err := config.Registry.RegisterMetrics("fixed_scheduler", loop); err != nil {
		return nil, fmt.Errorf("register fixed scheduler metrics: %w", err)
	}
	return loop, nil
}

func (*Loop) Name() string { return "fixed-schedule-loop" }

func (loop *Loop) Start(ctx context.Context) error {
	if loop == nil || ctx == nil {
		return ErrEngineUnavailable
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

// run drives every window, including the first.
//
// The first window runs here rather than in Start for two reasons. Its result
// must not gate startup: window failures close readiness and back off wherever
// they occur, so treating the first one as fatal made startup permanently
// unrecoverable for failures the steady-state path is built to retry — a due
// occurrence carrying a bad operator override could then only be repaired by
// restarting the process. Its duration must not gate startup either: a window
// may run for up to the configured step timeout, and blocking Start for that
// long delays every component sequenced after it, for a result nobody waits on.
//
// Running it on entry rather than on the first tick keeps the cadence honest:
// the loop still evaluates due work immediately at startup, it just does so
// without holding the caller.
func (loop *Loop) run(ctx context.Context, ticker loopTicker, done chan struct{}) {
	defer close(done)
	defer ticker.Stop()
	var nextEligible time.Time
	if ctx.Err() == nil {
		if err := loop.step(ctx, loop.clock.Now()); err != nil {
			loop.setFailed()
			nextEligible = loop.clock.Now().Add(loop.backoff())
		}
	}
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
	result, err := loop.stepper.Step(stepCtx, now.UTC())
	if stepCtx.Err() != nil && parent.Err() == nil {
		return fmt.Errorf("fixed schedule window timed out: %w", stepCtx.Err())
	}
	if err != nil {
		return err
	}
	loop.record(result, now)
	if result.Failed() {
		return result.Err()
	}
	if overdue := loop.overdueSchedules(); len(overdue) > 0 {
		return fmt.Errorf("%w: %s", errScheduleOverdue, strings.Join(overdue, ", "))
	}
	loop.mu.Lock()
	loop.windows++
	loop.consecutive = 0
	loop.lastOK, loop.up = now.UTC(), true
	loop.mu.Unlock()
	loop.ready.Store(true)
	return nil
}

func (loop *Loop) record(result WindowResult, now time.Time) {
	thresholds := make(map[string]time.Duration, len(loop.schedules))
	for _, schedule := range loop.stepper.Schedules() {
		thresholds[schedule.ID] = schedule.AlertThreshold
	}
	loop.mu.Lock()
	defer loop.mu.Unlock()
	overdue := make([]string, 0, len(result.Schedules))
	for _, schedule := range result.Schedules {
		state, ok := loop.schedules[schedule.ScheduleID]
		if !ok {
			state = &scheduleState{}
			loop.schedules[schedule.ScheduleID] = state
		}
		state.claimed += uint64(schedule.Claimed)
		state.duplicate += uint64(schedule.Duplicate)
		state.handoffs += uint64(schedule.Handoffs)
		state.skipped += uint64(schedule.Skipped)
		state.missingFor = schedule.MissingFor
		// Only a window that actually evaluated this schedule may change its
		// degraded verdict. The loop polls far more often than any schedule is due,
		// so overwriting unconditionally cleared a live reason on the very next
		// poll: a permanent fault stayed visible for one poll interval and was
		// missed by any realistic scrape.
		if schedule.Evaluated {
			state.degraded = schedule.Degraded
		}
		if schedule.ColdStart {
			state.coldStarts++
		}
		if schedule.Err != nil {
			state.failures++
		}
		threshold, present := thresholds[schedule.ScheduleID]
		state.overdue = present && schedule.MissingFor > threshold
		if state.overdue {
			overdue = append(overdue, schedule.ScheduleID)
		}
	}
	sort.Strings(overdue)
	loop.overdue = overdue
	_ = now
}

func (loop *Loop) overdueSchedules() []string {
	loop.mu.Lock()
	defer loop.mu.Unlock()
	return append([]string(nil), loop.overdue...)
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

// Readiness reports whether the loop has completed a current successful window.
// The composition root binds it to the fixed_scheduler_loop check.
func (loop *Loop) Readiness(context.Context) error {
	if loop != nil && loop.ready.Load() {
		return nil
	}
	return errLoopNotReady
}

// Coverage keeps readiness closed while the checked schedule inventory does
// not fully account for the legacy Beat inventory. Unknown schedule ownership
// is a deployment error, not a runtime degradation. The composition root binds
// it to the fixed_schedule_coverage check.
func (loop *Loop) Coverage(context.Context) error {
	if loop == nil {
		return ErrEngineUnavailable
	}
	if err := ValidateInventory(); err != nil {
		return err
	}
	declared, err := Schedules()
	if err != nil {
		return err
	}
	operated := make(map[string]struct{}, len(loop.stepper.Schedules()))
	for _, schedule := range loop.stepper.Schedules() {
		operated[schedule.ID] = struct{}{}
	}
	missing := make([]string, 0, len(declared))
	for _, schedule := range declared {
		if _, ok := operated[schedule.ID]; !ok {
			missing = append(missing, schedule.ID)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Errorf(
			"%w: declared schedules are not operated by this process: %s",
			ErrProducerUnavailable, strings.Join(missing, ", "),
		)
	}
	return nil
}

func (loop *Loop) Shutdown(ctx context.Context) error {
	if loop == nil || ctx == nil {
		return ErrEngineUnavailable
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

// WritePrometheus exports the occurrence lifecycle signals TRD section 15
// requires: due, inserted, completed, missed, and retried per schedule.
func (loop *Loop) WritePrometheus(output io.Writer) error {
	if loop == nil || output == nil {
		return errors.New("Prometheus output is required")
	}
	loop.mu.Lock()
	windows, failures, consecutive := loop.windows, loop.failures, loop.consecutive
	lastOK, up, now := loop.lastOK, loop.up, loop.clock.Now()
	identifiers := make([]string, 0, len(loop.schedules))
	snapshot := make(map[string]scheduleState, len(loop.schedules))
	for identifier, state := range loop.schedules {
		identifiers = append(identifiers, identifier)
		snapshot[identifier] = *state
	}
	loop.mu.Unlock()
	sort.Strings(identifiers)

	age := 0.0
	if !lastOK.IsZero() && !now.Before(lastOK) {
		age = now.Sub(lastOK).Seconds()
	}
	var text strings.Builder
	writeCounter(&text, "fixed_scheduler_windows_total", "Successful bounded fixed-schedule windows.", windows)
	writeCounter(&text, "fixed_scheduler_failures_total", "Failed bounded fixed-schedule windows.", failures)
	fmt.Fprintf(&text, "# HELP fixed_scheduler_consecutive_failures Consecutive failed windows.\n# TYPE fixed_scheduler_consecutive_failures gauge\nfixed_scheduler_consecutive_failures %d\n", consecutive)
	fmt.Fprint(&text, "# HELP fixed_scheduler_up Whether the fixed scheduler completed a current successful window.\n# TYPE fixed_scheduler_up gauge\nfixed_scheduler_up ")
	if up {
		text.WriteString("1\n")
	} else {
		text.WriteString("0\n")
	}
	fmt.Fprintf(&text, "# HELP fixed_scheduler_last_success_age_seconds Age of the last successful window.\n# TYPE fixed_scheduler_last_success_age_seconds gauge\nfixed_scheduler_last_success_age_seconds %s\n", strconv.FormatFloat(age, 'g', -1, 64))

	text.WriteString("# HELP fixed_scheduler_occurrences_total Fixed-schedule occurrences by outcome.\n# TYPE fixed_scheduler_occurrences_total counter\n")
	for _, identifier := range identifiers {
		state := snapshot[identifier]
		writeLabeled(&text, "fixed_scheduler_occurrences_total", identifier, "claimed", state.claimed)
		writeLabeled(&text, "fixed_scheduler_occurrences_total", identifier, "duplicate", state.duplicate)
		writeLabeled(&text, "fixed_scheduler_occurrences_total", identifier, "skipped", state.skipped)
		writeLabeled(&text, "fixed_scheduler_occurrences_total", identifier, "failed", state.failures)
	}
	text.WriteString("# HELP fixed_scheduler_handoffs_total Durable job handoffs produced by fixed schedules.\n# TYPE fixed_scheduler_handoffs_total counter\n")
	for _, identifier := range identifiers {
		fmt.Fprintf(&text, "fixed_scheduler_handoffs_total{schedule=%q} %d\n", identifier, snapshot[identifier].handoffs)
	}
	text.WriteString("# HELP fixed_scheduler_cold_starts_total Baseline occurrences recorded for a schedule with no history.\n# TYPE fixed_scheduler_cold_starts_total counter\n")
	for _, identifier := range identifiers {
		fmt.Fprintf(&text, "fixed_scheduler_cold_starts_total{schedule=%q} %d\n", identifier, snapshot[identifier].coldStarts)
	}
	// A degraded schedule is not a failure and must not close readiness, but it
	// is also not healthy work. Exporting the reason as a labelled gauge lets an
	// operator alert on, for example, an installation whose organization table
	// is empty and whose nightly fan-outs therefore schedule nothing.
	text.WriteString("# HELP fixed_scheduler_schedule_degraded Whether a schedule produced no work for a bounded, named reason.\n# TYPE fixed_scheduler_schedule_degraded gauge\n")
	for _, identifier := range identifiers {
		reason := snapshot[identifier].degraded
		value := 0
		if reason != "" {
			value = 1
		} else {
			reason = "none"
		}
		fmt.Fprintf(&text, "fixed_scheduler_schedule_degraded{schedule=%q,reason=%q} %d\n", identifier, reason, value)
	}
	text.WriteString("# HELP fixed_scheduler_missing_occurrence_seconds How long the newest occurrence has lagged its due time.\n# TYPE fixed_scheduler_missing_occurrence_seconds gauge\n")
	for _, identifier := range identifiers {
		fmt.Fprintf(
			&text,
			"fixed_scheduler_missing_occurrence_seconds{schedule=%q} %s\n",
			identifier,
			strconv.FormatFloat(snapshot[identifier].missingFor.Seconds(), 'g', -1, 64),
		)
	}
	_, err := io.WriteString(output, text.String())
	return err
}

func writeCounter(output *strings.Builder, name, help string, value uint64) {
	fmt.Fprintf(output, "# HELP %s %s\n# TYPE %s counter\n%s %d\n", name, help, name, name, value)
}

func writeLabeled(output *strings.Builder, name, schedule, result string, value uint64) {
	fmt.Fprintf(output, "%s{schedule=%q,result=%q} %d\n", name, schedule, result, value)
}

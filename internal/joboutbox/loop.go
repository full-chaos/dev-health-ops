package joboutbox

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
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
	// Logger names why a step failed. It is optional so tests and embedders
	// need not supply one; a nil Logger discards. Without it this loop could
	// flip readiness closed forever and emit nothing at all (CHAOS-3907).
	Logger *slog.Logger
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

// consecutiveStepFailureDegradeThreshold mirrors chris's CHAOS-4239 readiness
// ruling (internal/syncreconciler/pipeline.go's
// consecutiveFailureDegradeThreshold) for this loop's own Relay.Step: a
// single transient failure -- a lock wait, a ctx deadline exceeded under
// load, any of the bounded ErrUnavailable/ErrLeaseLost conditions Relay/
// Repository already classify -- must not flap readiness (the false-alarm
// failure mode), but a step failing on every tick must still be visible on
// readyz. Three consecutive failures is the line; one success clears it
// immediately.
const consecutiveStepFailureDegradeThreshold = 3

// ReconcilerLoop owns one polling goroutine around bounded Relay steps. A
// successful empty step is a valid database/connectivity proof and opens its
// required readiness dependency.
//
// CHAOS-4429: a periodic step failure is NO LONGER fatal to the process.
// Before this fix, ANY single Relay.Step error -- a transient Postgres blip
// indistinguishable in kind from the ones syncreconciler.MutationPipeline's
// stages already absorb -- was reported through Errors() and, via
// lifecycle.Runtime, treated as fatal to the WHOLE reconciler binary: it took
// down the materializer, kernel, and lease-repair stages along with the
// relay, even though CHAOS-4239 had already fixed exactly this defect shape
// one component over. Every error Relay.Step can return is one of its own
// package's bounded, already-classified sentinels (ErrUnavailable,
// ErrLeaseLost, ErrInvalidConfiguration) -- never an arbitrary/unclassified
// failure -- so absorbing all of them here, the same way the mutation
// pipeline absorbs its own stage errors, is safe: log + count + reflect on
// readyz after consecutiveStepFailureDegradeThreshold consecutive misses,
// keep ticking, self-heal the instant one step succeeds. The process staying
// up with the relay degraded is strictly better than the outbox-reconciler-
// loop crash-looping the entire container on a blip that clears on its own.
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

	recovered                    uint64
	postRepairContractRejections uint64
	strandsRearmed               uint64
	strandJobsSkippedLive        uint64
	strandClaimsLive             uint64
	strandClaimsSettled          uint64
	strandRaceLost               uint64
	claimed                      uint64
	delivered                    uint64
	retried                      uint64
	dead                         uint64
	leaseLost                    uint64
	lastOK                       time.Time
	up                           bool

	// consecutiveFailures and stepFailures are the CHAOS-4429 telemetry pair:
	// consecutiveFailures drives the readiness threshold above (reset to zero
	// the instant a step succeeds), stepFailures is the lifetime counter an
	// operator alerts on regardless of whether any single streak ever crossed
	// the degrade threshold.
	consecutiveFailures int
	stepFailures        uint64

	errors chan error

	// stepObserved is a test-only synchronization hook (codex review
	// finding): production never sets it, and run's nil check is a no-op in
	// that case. It fires once run has finished ALL bookkeeping for a tick
	// (recordStepFailure or the success path, whichever ran) so a test can
	// wait on it instead of racing run's own goroutine off a signal sent
	// from inside the stepper -- the stepper returning is not the same
	// instant as readiness/metrics reflecting that return.
	stepObserved func()
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
		loop.logger().ErrorContext(ctx, "outbox reconciler initial step failed", "error", err.Error())
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
		loop.logger().ErrorContext(ctx, "outbox reconciler stopped before its polling loop started")
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
				// A step in flight when Shutdown fires observes its own
				// cancellation as a Relay.Step error -- that is the process
				// stopping on purpose, not an operational failure, and must
				// never count against step_failures_total or the degrade
				// streak (codex review finding, round 2).
				//
				// Deliberately NOT gated on the error's own type (e.g.
				// errors.Is(err, context.Canceled)): Repository's methods
				// (claimDueExcept, Dispatch, recordFailure, ...) collapse
				// EVERY pg failure -- a cancellation-induced one included --
				// into the package's own ErrUnavailable sentinel before it
				// ever reaches here (internal/joboutbox/repository.go), so a
				// type check on err would never match the very case it
				// exists to catch (codex round-1 fix looked right in
				// isolation and was wrong for exactly this reason). ctx here
				// is loopCtx, cancelled ONLY by Shutdown (never a per-step
				// timeout -- Relay.Step is called with it directly, no
				// derived deadline), so checking the context alone is both
				// necessary and sufficient.
				if ctx.Err() != nil || loop.isStopping() {
					return
				}
				loop.recordStepFailure(ctx, err)
				if loop.stepObserved != nil {
					loop.stepObserved()
				}
				continue
			}
			if loop.stepObserved != nil {
				loop.stepObserved()
			}
		}
	}
}

func (loop *ReconcilerLoop) isStopping() bool {
	loop.mu.Lock()
	defer loop.mu.Unlock()
	return loop.stopping
}

func (loop *ReconcilerLoop) step(ctx context.Context, now time.Time) error {
	result, err := loop.stepper.Step(ctx, now, loop.config.Limit)
	// CHAOS-4429 (codex review finding): accumulate the committed result
	// BEFORE branching on err. Relay.Step returns a non-zero StepResult
	// alongside a non-nil error on most of its own mid-pass failure paths
	// (a claim already delivered before a later claim's dispatch faults,
	// say) -- those actions already committed to Postgres. Discarding the
	// result here undercounts Prometheus and, now that a failing tick is
	// retried forever instead of crashing the process on the first miss,
	// would silently lose that evidence on every retry rather than just once
	// before a crash-loop restart reset it anyway.
	loop.mu.Lock()
	loop.recovered += nonNegativeUint(result.Recovered)
	loop.postRepairContractRejections += nonNegativeUint(result.PostRepairContractRejectionsRecovered)
	loop.strandsRearmed += nonNegativeUint(result.StrandsRearmed)
	loop.strandJobsSkippedLive += nonNegativeUint(result.StrandJobsSkippedLive)
	loop.strandClaimsLive += nonNegativeUint(result.StrandClaimsLive)
	loop.strandClaimsSettled += nonNegativeUint(result.StrandClaimsSettled)
	loop.strandRaceLost += nonNegativeUint(result.StrandRaceLost)
	loop.claimed += nonNegativeUint(result.Claimed)
	loop.delivered += nonNegativeUint(result.Delivered)
	loop.retried += nonNegativeUint(result.Retried)
	loop.dead += nonNegativeUint(result.Dead)
	loop.leaseLost += nonNegativeUint(result.LeaseLost)
	if err == nil {
		loop.lastOK = now
		loop.up = true
		// CHAOS-4429: one success clears the streak immediately, matching
		// stageTelemetry.recordSuccess -- a relay that recovers is never held
		// degraded on readyz by a stale streak.
		loop.consecutiveFailures = 0
	}
	loop.mu.Unlock()
	if err == nil {
		loop.ready.Store(true)
	}
	return err
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

// recordStepFailure is CHAOS-4429's replacement for treating every periodic
// Relay.Step error as fatal. It logs and counts the failure unconditionally,
// and closes readiness ONLY once consecutiveFailures has crossed
// consecutiveStepFailureDegradeThreshold -- exactly stageTelemetry's
// recordFailure/isDegraded shape one component over, and for the same
// reason: a lone blip must not flap readyz, but a relay stuck failing every
// tick must be visible by name rather than only in a Prometheus counter.
// It deliberately never sends on loop.errors / returns from run -- the
// process itself is no longer at risk from a Relay.Step failure of any kind
// (see the ReconcilerLoop doc comment for why every error Step can return is
// safe to absorb here).
func (loop *ReconcilerLoop) recordStepFailure(ctx context.Context, err error) {
	loop.mu.Lock()
	loop.stepFailures++
	loop.consecutiveFailures++
	consecutive := loop.consecutiveFailures
	degraded := consecutive >= consecutiveStepFailureDegradeThreshold
	if degraded {
		loop.up = false
	}
	loop.mu.Unlock()
	if degraded {
		loop.ready.Store(false)
	}
	loop.logger().ErrorContext(
		ctx, "outbox reconciler step failed",
		"error", err.Error(),
		"consecutive_failures", consecutive,
		"degraded", degraded,
	)
}

// logger is nil-safe: an unset Config.Logger discards rather than panicking,
// and never falls back to slog.Default(), so an embedder cannot be surprised
// by reconciler output appearing on a logger it did not choose.
func (loop *ReconcilerLoop) logger() *slog.Logger {
	if loop == nil || loop.config.Logger == nil {
		return slog.New(slog.DiscardHandler)
	}
	return loop.config.Logger
}

func (loop *ReconcilerLoop) readiness(context.Context) error {
	if loop != nil && loop.ready.Load() {
		return nil
	}
	return errReconcilerLoopNotReady
}

// Errors satisfies lifecycle.ErrorSource. CHAOS-4429: run's per-tick
// failures no longer send on this channel -- see recordStepFailure and the
// ReconcilerLoop doc comment -- so in normal operation nothing is ever sent
// here. The channel is kept (rather than returning nil) as the seam a future
// genuinely-fatal condition would use; it is intentionally buffered so the
// loop can stop before the runtime's error watcher is scheduled.
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
	recovered := loop.recovered
	postRepairContractRejections := loop.postRepairContractRejections
	strandsRearmed := loop.strandsRearmed
	strandJobsSkippedLive := loop.strandJobsSkippedLive
	strandClaimsLive := loop.strandClaimsLive
	strandClaimsSettled := loop.strandClaimsSettled
	strandRaceLost := loop.strandRaceLost
	claimed := loop.claimed
	delivered := loop.delivered
	retried := loop.retried
	dead := loop.dead
	leaseLost := loop.leaseLost
	lastOK := loop.lastOK
	up := loop.up
	stepFailures := loop.stepFailures
	degraded := loop.consecutiveFailures >= consecutiveStepFailureDegradeThreshold
	now := loop.clock.Now()
	loop.mu.Unlock()

	lastSuccessAge := 0.0
	if up && !lastOK.IsZero() && !now.Before(lastOK) {
		lastSuccessAge = now.Sub(lastOK).Seconds()
	}
	var text strings.Builder
	writeReconcilerCounter(&text, "worker_outbox_reconciler_terminal_deliveries_recovered_total", "Terminal River deliveries rearmed by the reconciler.", recovered)
	writeReconcilerCounter(&text, "worker_outbox_reconciler_post_repair_contract_rejections_recovered_total", "Post-repair provider contract rejections recovered by the reconciler.", postRepairContractRejections)
	writeReconcilerCounter(&text, "worker_outbox_reconciler_strands_rearmed_total", "Stranded daily-metrics and work-graph deliveries rearmed by the reconciler.", strandsRearmed)
	// Exported even though it is a refusal rather than an action: a skip count
	// that climbs while nothing is ever rearmed is the signature of a River
	// rescuer that has stopped running, which would otherwise be
	// indistinguishable from "no strands exist".
	writeReconcilerCounter(&text, "worker_outbox_reconciler_strand_jobs_skipped_live_total", "Strand candidates left alone because their River delivery was not terminal.", strandJobsSkippedLive)
	writeReconcilerCounter(&text, "worker_outbox_reconciler_strand_claims_live_total", "Strand candidates left alone because their idempotency claim was still live.", strandClaimsLive)
	// Settled claims are a DIFFERENT problem from live ones: the work will
	// never be re-driven by rearming, because a fresh delivery is ACKed as a
	// duplicate. A non-zero value here means rows need a remedy this sweep
	// does not provide, which is worth seeing rather than folding into the
	// live-claim count.
	writeReconcilerCounter(&text, "worker_outbox_reconciler_strand_claims_settled_total", "Strand candidates left alone because their idempotency claim had already settled.", strandClaimsSettled)
	// A race loss is a refusal like any other. Without it the loser of a
	// two-replica contest reports a successful zero pass and the contention
	// is invisible.
	writeReconcilerCounter(&text, "worker_outbox_reconciler_strand_race_lost_total", "Strand candidates that no longer matched under the phase-3 lock.", strandRaceLost)
	writeReconcilerCounter(&text, "worker_outbox_reconciler_claimed_total", "Outbox rows claimed by the reconciler.", claimed)
	writeReconcilerCounter(&text, "worker_outbox_reconciler_delivered_total", "Outbox rows delivered to River by the reconciler.", delivered)
	writeReconcilerCounter(&text, "worker_outbox_reconciler_retried_total", "Outbox rows scheduled for relay retry by the reconciler.", retried)
	writeReconcilerCounter(&text, "worker_outbox_reconciler_dead_total", "Outbox rows terminalized by the reconciler.", dead)
	writeReconcilerCounter(&text, "worker_outbox_reconciler_lease_lost_total", "Outbox claims lost before reconciliation completed.", leaseLost)
	// CHAOS-4429: the lifetime failure count survives even a streak that
	// never crossed the degrade threshold below -- an operator alerting on
	// this sees every absorbed blip, not only the ones that flipped readyz.
	writeReconcilerCounter(&text, "worker_outbox_reconciler_step_failures_total", "Relay.Step failures absorbed by the reconciler loop, including ones that never crossed the readiness degrade threshold.", stepFailures)
	fmt.Fprint(&text, "# HELP worker_outbox_reconciler_degraded Whether the reconciler loop has failed its last consecutive Relay.Step calls past the readiness threshold. One success clears it.\n# TYPE worker_outbox_reconciler_degraded gauge\nworker_outbox_reconciler_degraded ")
	if degraded {
		text.WriteString("1\n")
	} else {
		text.WriteString("0\n")
	}
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

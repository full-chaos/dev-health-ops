package syncreconciler

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
)

const (
	defaultMutationLeaseDuration    = 5 * time.Minute
	maximumMutationStaleDispatchAge = 24 * time.Hour
)

// ErrDegradedStage marks a MutationPipeline.Step error as CHAOS-4239's
// stage-scoped failure, not a fatal one. Loop.run unwraps it with errors.Is
// and keeps ticking instead of tearing the whole process down -- the failing
// stage already recorded itself loudly (syncreconciler.stage_failed,
// sync_reconciler_stage_failures_total) before this ever reaches Loop.
//
// Only the observer stage's own failure is ever wrapped this way. The other
// five stages absorb their own errors internally (see the stage
// classification comment on MutationPipeline.Step) precisely so that a
// repair/sweep/terminal-repair/materializer/kernel hiccup never needs Loop's
// cooperation to survive; the observer is different because Loop trusts a
// nil Step error to mean "the returned Observation is fresh," and a failed
// observer call cannot honor that promise (see carryUnmeasuredGaugesLocked).
var ErrDegradedStage = errors.New("syncreconciler: pipeline stage degraded")

// sweepReportSample bounds the identifier list on the selection log line. A
// pass can select up to `limit` units, and a log line carrying a hundred UUIDs
// every tick is a different kind of unreadable from a log line carrying none.
// The COUNT is always exact; only the sample is cut, and the line says when it
// was.
const sweepReportSample = 10

// sampleIdentifiers returns at most sweepReportSample entries. It never sorts
// or dedupes: the caller's order is selection order, which is the order an
// operator would page through.
func sampleIdentifiers(identifiers []string) []string {
	if len(identifiers) <= sweepReportSample {
		return identifiers
	}
	return identifiers[:sweepReportSample]
}

// LeaseRepairStepper is the bounded expired-lease repair seam used by the
// command-owned mutation pipeline.
type LeaseRepairStepper interface {
	Step(context.Context, time.Time, int) (LeaseRepairResult, error)
}

// TerminalDeliveryRepairStepper is the queue-side recovery seam for a River
// maintenance discard that occurred before the authoritative domain work ran.
type TerminalDeliveryRepairStepper interface {
	Step(context.Context, time.Time, int) (TerminalDeliveryRepairResult, error)
}

// MaterializerStepper is the bounded wakeup materialization seam used by the
// command-owned mutation pipeline.
type MaterializerStepper interface {
	Step(context.Context, time.Time, time.Time, int) (MaterializerResult, error)
}

// KernelStepper is the transport claim-and-delivery seam used by the
// command-owned mutation pipeline.
type KernelStepper interface {
	Step(
		context.Context,
		time.Time,
		int,
		time.Duration,
		AtLeastOncePublisher,
		PostSyncHandoff,
	) (KernelResult, error)
}

// MutationPipelineConfig keeps compatibility policy explicit at composition
// time. The defaults match the Python reconciler's stale-dispatch and claim
// lease behavior.
type MutationPipelineConfig struct {
	StaleDispatchAge time.Duration
	LeaseDuration    time.Duration
	// StageBudgets replaces one flat deadline for the whole pipeline call
	// with one bounded sub-context per stage (CHAOS-4239). Must name exactly
	// the stages MutationPipeline.Step runs -- see StageBudgets.validate.
	StageBudgets StageBudgets
	// Registry is optional. When set, NewMutationPipeline self-registers a
	// metrics fragment carrying the per-stage failure counters and duration
	// gauges added for CHAOS-4239, the same way syncreconciler.Loop already
	// registers its own. Nil skips registration -- most unit tests construct
	// a MutationPipeline without a process-wide registry at all.
	Registry *health.Registry
}

func DefaultMutationPipelineConfig() MutationPipelineConfig {
	return MutationPipelineConfig{
		// StaleDispatchAge is the same value the Celery sync-provider
		// quiescer uses to decide whether a DISPATCHING row is still live
		// (internal/jobroute's PostgresCelerySyncProviderQuiescer) -- both
		// call syncdispatchcontract.DispatchStaleAge so the two never drift
		// apart, and both pick up an operator's SYNC_UNIT_DISPATCH_STALE_SECONDS
		// override the same way Python does (CHAOS-3929).
		StaleDispatchAge: syncdispatchcontract.DispatchStaleAge(),
		LeaseDuration:    defaultMutationLeaseDuration,
		StageBudgets:     DefaultStageBudgets(),
	}
}

func (config MutationPipelineConfig) valid() bool {
	return config.StaleDispatchAge > 0 &&
		config.StaleDispatchAge <= maximumMutationStaleDispatchAge &&
		config.LeaseDuration >= minimumLeaseDuration &&
		config.LeaseDuration <= maximumLeaseDuration &&
		config.StageBudgets.validate() == nil
}

// MutationPipeline composes the already-reviewed repair, materialization, and
// transport kernels into one bounded reconciler step. It observes after all
// committed mutation stages so the existing lifecycle loop and parity metrics
// describe the resulting database state.
//
// Construction and execution do not change transport routes. River routes
// fail closed unless command composition supplies a concrete publisher.
type MutationPipeline struct {
	repair       LeaseRepairStepper
	terminal     TerminalDeliveryRepairStepper
	materializer MaterializerStepper
	kernel       KernelStepper
	observer     Stepper
	publish      AtLeastOncePublisher
	postSync     PostSyncHandoff
	sweep        UnreclaimableSweepStepper
	config       MutationPipelineConfig

	stages stageTelemetry
}

// UnreclaimableSweepStepper is the CHAOS-4005 safety net. It is a positional
// constructor parameter rather than a config field or a setter ON PURPOSE:
// this whole ticket exists because a component was present in the tree and
// silently never wired, so "did you decide about the sweep?" is a compile-time
// question at every construction site. Passing nil is a valid answer -- a
// deployment that has not declared its worker topology runs without it -- but
// it has to be an answer.
type UnreclaimableSweepStepper interface {
	Step(context.Context, time.Time, int) (UnreclaimableSweepResult, error)
}

func NewMutationPipeline(
	repair LeaseRepairStepper,
	terminal TerminalDeliveryRepairStepper,
	materializer MaterializerStepper,
	kernel KernelStepper,
	observer Stepper,
	publish AtLeastOncePublisher,
	postSync PostSyncHandoff,
	sweep UnreclaimableSweepStepper,
	config MutationPipelineConfig,
) (*MutationPipeline, error) {
	if repair == nil || terminal == nil || materializer == nil || kernel == nil || observer == nil || !config.valid() {
		return nil, ErrInvalidConfiguration
	}
	pipeline := &MutationPipeline{
		repair:       repair,
		terminal:     terminal,
		materializer: materializer,
		kernel:       kernel,
		observer:     observer,
		publish:      publish,
		postSync:     postSync,
		sweep:        sweep,
		config:       config,
		stages:       newStageTelemetry(),
	}
	if config.Registry != nil {
		if err := config.Registry.RegisterMetrics("sync_reconciler_pipeline", pipeline); err != nil {
			return nil, fmt.Errorf("register sync reconciler pipeline metrics: %w", err)
		}
	}
	return pipeline, nil
}

// runStage bounds one stage call to its own budget instead of letting it
// share the whole tick's envelope with its five siblings (CHAOS-4239), and
// records the outcome unconditionally: a stage that runs to completion inside
// its budget still gets its duration recorded, so the gauge is never silently
// empty for a healthy stage.
//
// A failure caused by the PARENT context (loop shutdown, not this stage's own
// tighter deadline) is deliberately excluded from the stage_failed telemetry
// and left for the caller to detect via ctx.Err() and propagate -- it is not
// a stage degrading, it is the process stopping.
func (pipeline *MutationPipeline) runStage(
	ctx context.Context, stage StageName, fn func(context.Context) error,
) error {
	budget := pipeline.config.StageBudgets[stage]
	stageCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()

	start := time.Now()
	err := fn(stageCtx)
	elapsed := time.Since(start)
	pipeline.stages.recordDuration(stage, elapsed)

	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		// Shutdown, not a stage failure -- the caller decides whether to
		// propagate ctx.Err() itself.
		return err
	}
	pipeline.stages.recordFailure(stage)
	slog.Error(
		"syncreconciler.stage_failed",
		"stage", string(stage),
		"budget_ms", budget.Milliseconds(),
		"elapsed_ms", elapsed.Milliseconds(),
		"error", err.Error(),
	)
	return err
}

func (pipeline *MutationPipeline) Step(
	ctx context.Context,
	now time.Time,
	limit int,
) (observation Observation, err error) {
	if pipeline == nil || ctx == nil || now.IsZero() ||
		limit < minimumStepLimit || limit > maximumStepLimit ||
		!pipeline.config.valid() {
		return Observation{}, ErrInvalidConfiguration
	}
	if err := ctx.Err(); err != nil {
		return Observation{}, err
	}
	now = now.UTC()

	// ONE ACCOUNTING POINT FOR "THE REPORT DID NOT RUN", not one per return.
	//
	// This is a class fix, and it is written this way because the per-site
	// version failed three times. Each round of review found another exit that
	// prevented the report while leaving the counter at zero -- first the
	// materializer's own error, then the swallowed non-fatal failures, then
	// repair failure, sweep cancellation and terminal-repair failure. Every
	// one was the same bug, and every fix patched the site in front of me
	// rather than the reason there were sites at all.
	//
	// A deferred accounting on the NAMED return covers every path that exists
	// and every path added later, including ones whose author never reads this
	// file. The invariant it enforces is exactly what the counter's HELP text
	// promises: if this pass did not deliver a report, it says so.
	//
	// It is installed AFTER the validation returns above on purpose. A call
	// rejected for a bad limit never began a pass, so counting it as a
	// detector outage would be its own kind of false signal.
	reportStep := runawayReportStepUpstream
	reportDelivered := false
	defer func() {
		if reportDelivered {
			return
		}
		observation.WakeupReportFailures = 1
		slog.Error(
			"syncreconciler.dispatch_wakeup_report_unavailable",
			"step", reportStep,
		)
	}()

	// STAGE-SCOPED FAILURE (CHAOS-4239). Every stage below runs under its own
	// budget via runStage, which logs+counts a failure regardless of what
	// happens next. What happens next is a per-stage classification, not one
	// shared rule:
	//
	//   - LeaseRepair and Kernel are the two stages a failure aborts the rest
	//     of THIS TICK'S mutation work for (not the process -- see below).
	//     Repair is first and everything after it assumes expired leases were
	//     already freed; Kernel is the actual claim-and-publish stage, and
	//     nothing mutation-shaped follows it anyway. Skipping straight to the
	//     Observer preserves exactly the ordering guarantee the pre-CHAOS-4239
	//     code already had (a repair failure already skipped sweep, terminal
	//     repair, materializer and kernel every time) -- the only change is
	//     that the tick no longer ends there, and the process no longer dies.
	//   - UnreclaimableSweep, TerminalDeliveryRepair and Materializer are
	//     read-adjacent safety nets and repair passes over largely disjoint
	//     tables (see the pool-composition comment on buildSyncMutationPipeline
	//     in cmd/dev-health-reconciler/dependencies.go); a stall in one buys
	//     nothing by blocking the others, so each failure is absorbed and the
	//     pipeline continues. Sweep already worked this way before this
	//     ticket; TerminalDeliveryRepair and Materializer are upgraded from
	//     "abort the tick" to "continue" here, matching sweep's precedent.
	//   - Observer always runs last, REGARDLESS of what happened above -- it
	//     is read-only and independent, and it is also the stage whose output
	//     Loop trusts wholesale on a nil Step error. Because Loop cannot tell
	//     a fresh Observation from a stale one except by trusting Step's
	//     return, an Observer failure is the one case this function still
	//     returns as an error (wrapped in ErrDegradedStage so Loop.run knows
	//     to keep ticking instead of tearing the process down). Every other
	//     stage's failure is fully absorbed here and never reaches Loop as an
	//     error at all.
	swept := Observation{}
	aborted := false

	if err := pipeline.runStage(ctx, StageLeaseRepair, func(stageCtx context.Context) error {
		_, stepErr := pipeline.repair.Step(stageCtx, now, limit)
		return stepErr
	}); err != nil {
		if ctx.Err() != nil {
			return swept, err
		}
		aborted = true
	}

	// CHAOS-4005: the never-leased strand. Lease repair above reaches only a
	// RUNNING unit whose lease expired; a unit stuck in 'dispatching' holds no
	// lease, so nothing else in this pass can free it. Runs BEFORE the
	// materializer so a run this sweep just unblocked gets its finalize wakeup
	// in the same pass rather than waiting a full cycle.
	//
	// A sweep failure is deliberately NOT fatal to the pass: it is a safety
	// net, and taking lease repair down with it would trade a bounded strand
	// for an unbounded one.
	if !aborted && pipeline.sweep != nil {
		var sweepResult UnreclaimableSweepResult
		sweepErr := pipeline.runStage(ctx, StageUnreclaimableSweep, func(stageCtx context.Context) error {
			var stepErr error
			sweepResult, stepErr = pipeline.sweep.Step(stageCtx, now, limit)
			return stepErr
		})
		// A fenced decline is not an error -- the sweep correctly refused to
		// write against a route that moved underneath it -- but it must not be
		// invisible either. Without this line an operator sees a healthy pass
		// while selected strands are abandoned every tick, which is the same
		// class of silence that let CHAOS-3990 sit unnoticed and CHAOS-4035
		// ship. It is logged BEFORE the error branch below because the two are
		// mutually exclusive outcomes of the same call.
		if sweepErr == nil && sweepResult.DeclinedRouteChange {
			slog.Warn(
				"syncreconciler.unreclaimable_sweep_declined_route_change",
				"candidates", sweepResult.Candidates,
			)
		}
		// SHADOW MODE HAD NO OUTPUT AT ALL (adversarial review finding).
		//
		// Shadow is the DEFAULT, and the sweep's own documentation justifies
		// that default by saying "every deployment gets would-terminalize
		// observability at zero write risk". Nothing implemented the
		// observability half: this pipeline consumed only DeclinedRouteChange
		// and threw Candidates, UnitIDs and Pairs away, so a shadow deployment
		// could identify the same strand on every tick for weeks and emit not
		// one line. That is the same unimplemented-claim shape the sweep file
		// already records twice, and CHAOS-4097 widens what shadow selects, so
		// leaving it silent would have made a new population invisible too.
		//
		// Logged for BOTH modes. In active mode the terminalized rows carry
		// their own durable reason, so the line is a convenience; in shadow
		// mode it is the only record that exists, which is why it is emitted
		// on selection rather than on write.
		if sweepErr == nil && sweepResult.Candidates > 0 {
			slog.Warn(
				"syncreconciler.unreclaimable_sweep_selected",
				"mode", string(sweepResult.Mode),
				"candidates", sweepResult.Candidates,
				"terminalized", sweepResult.Terminalized,
				"runs", len(sweepResult.RunIDs),
				// Pairs are few by construction -- production carried 23
				// across the whole CHAOS-4093 population -- and they are the
				// field an operator groups by first, so they are not sampled.
				"pairs", sweepResult.Pairs,
				// Unit ids are up to `limit` per pass, so they ARE sampled.
				// The count above is the truth; this is a handle for going
				// and looking at one.
				"unit_id_sample", sampleIdentifiers(sweepResult.UnitIDs),
				"unit_id_sample_truncated", len(sweepResult.UnitIDs) > sweepReportSample,
			)
		}
		if sweepErr == nil {
			swept.UnreclaimableCandidates = int64(sweepResult.Candidates)
			swept.UnreclaimableTerminalized = int64(sweepResult.Terminalized)
			// The sweep ran and answered, so its zero -- if it is a zero -- is
			// a finding rather than an absence.
			swept.UnreclaimableMeasured = true
		}
		if sweepErr != nil {
			if ctx.Err() != nil {
				return swept, sweepErr
			}
			// But it must never fail SILENTLY. Swallowing this without a word
			// reports a healthy pass while the strand the sweep exists to
			// clear is still there -- the same invisibility that let
			// CHAOS-3990 sit unnoticed for sixteen hours (review finding).
			slog.Warn(
				"syncreconciler.unreclaimable_sweep_failed",
				"error", sweepErr.Error(),
			)
			// ...and a log line is not an alertable signal. The candidate
			// gauge reads zero on a failed pass, which is exactly what a
			// healthy idle system reads, so without this counter a sweep that
			// has stopped working entirely is invisible to every dashboard.
			// That is not hypothetical: CHAOS-4035 was this component
			// answering 42501 once a second from its first deploy.
			swept.UnreclaimableSweepFailures = 1
		}
	}

	var terminal TerminalDeliveryRepairResult
	terminalRan := false
	if !aborted {
		terminalErr := pipeline.runStage(ctx, StageTerminalDeliveryRepair, func(stageCtx context.Context) error {
			var stepErr error
			terminal, stepErr = pipeline.terminal.Step(stageCtx, now, limit)
			return stepErr
		})
		if terminalErr != nil && ctx.Err() != nil {
			return swept, terminalErr
		}
		terminalRan = terminalErr == nil
	}

	// The repair commits its own transaction before anything below runs, so its
	// recoveries are already durable no matter how this step ends. Every return
	// from here on carries the count: an observation reporting zero recoveries
	// after rows were in fact reclaimed would let a cycling delivery stay under
	// its own alert threshold, which is the failure the counter exists to catch.
	recovered := swept
	if terminalRan {
		// A repair cannot recover more rows than its own bounded window.
		// Treating an out-of-range count as a failed step keeps a
		// miscounting repair from quietly inflating the recovery metric
		// operators alert on. This is a data-integrity guard, not a timeout:
		// it deliberately reports nothing rather than a count it cannot
		// stand behind, exactly as before CHAOS-4239.
		if terminal.ExhaustedRecovered < 0 || terminal.ExhaustedRecovered > terminal.Recovered ||
			terminal.RescueOnlyCancelsRecovered < 0 ||
			terminal.RescueOnlyCancelsRecovered > terminal.Recovered ||
			terminal.ExhaustedRecovered+terminal.RescueOnlyCancelsRecovered > terminal.Recovered ||
			terminal.Recovered > limit {
			slog.Error(
				"syncreconciler.stage_failed",
				"stage", string(StageTerminalDeliveryRepair),
				"error", ErrUnavailable.Error(),
				"reason", "recovered count outside its own bounded window",
			)
			pipeline.stages.recordFailure(StageTerminalDeliveryRepair)
		} else {
			recovered.ExhaustedDeliveriesRecovered = int64(terminal.ExhaustedRecovered)
			// A rescue-only cancel is a registry or queue-routing fault: the job
			// was inserted onto a queue whose client does not execute that kind.
			// Unlike the other two recoveries it will repeat deterministically
			// until someone changes a deployment, so it is logged rather than
			// only counted (CHAOS-4097).
			if terminal.RescueOnlyCancelsRecovered > 0 {
				slog.Warn(
					"syncreconciler.rescue_only_cancel_recovered",
					"recovered", terminal.RescueOnlyCancelsRecovered,
				)
			}
		}
	}

	var materialized MaterializerResult
	var materializerErr error
	if !aborted {
		materializerErr = pipeline.runStage(ctx, StageMaterializer, func(stageCtx context.Context) error {
			var stepErr error
			materialized, stepErr = pipeline.materializer.Step(
				stageCtx, now, now.Add(-pipeline.config.StaleDispatchAge), limit,
			)
			return stepErr
		})
		if materializerErr != nil && ctx.Err() != nil {
			return recovered, materializerErr
		}
	}
	// CHAOS-4097: one sync_dispatch_outbox row reached attempts = 72601 in
	// production, generating roughly 1500 no-op River jobs a minute for
	// twenty-two hours, and nothing anywhere said a word. Counters do not
	// export from this deployment (CHAOS-4094), so the durable column is read
	// and reported directly. ERROR, not WARN: a run re-arming four figures of
	// times is not degraded, it is looping, and it will not stop on its own.
	//
	// Reported even when the step failed, because the report is taken before
	// the commit and a materialization failure does not make a looping run
	// less true. It is emitted per row rather than as a total so the log line
	// names something an operator can go and look at.
	for _, wakeup := range materialized.Runaway {
		slog.Error(
			"syncreconciler.dispatch_wakeup_attempts_exceeded",
			"sync_run_id", wakeup.SyncRunID,
			"attempts", wakeup.Attempts,
			"threshold", runawayDispatchAttempts,
			"truncated", materialized.RunawayTruncated,
		)
	}
	// A BROKEN DETECTOR MUST NOT READ AS A CLEAN ONE (adversarial review
	// finding). An empty Runaway above means one of two opposite things: no
	// run is looping, or the statement that would have said so did not run.
	// Without this line the second is indistinguishable from the first, and a
	// permission or schema fault would reproduce exactly the silence this
	// report was added to end. ERROR for the same reason the report itself is
	// ERROR: the measurement layer failing is not a degraded state, it is a
	// blind one.
	// The one place that can say the report DID arrive. Everything else is the
	// deferred accounting above, which assumes it did not.
	//
	// The distinction the log preserves: a named step means the report itself
	// faulted, the upstream code means the pass never reached it. Those want
	// different first questions even though the counter merges them.
	if materialized.RunawayReportStep != "" {
		reportStep = materialized.RunawayReportStep
	}
	// !aborted guards this the same way the rest of the accounting does: when
	// repair already aborted the tick, the materializer never ran at all, and
	// its zero-value MaterializerResult{} has an empty RunawayReportStep for
	// the same reason a genuinely successful, nothing-to-report pass would --
	// without this guard the two are indistinguishable and a repair outage
	// would read as a delivered report.
	reportDelivered = !aborted && materialized.RunawayReportStep == "" && materializerErr == nil
	// THE EXACT TOTAL, never len(Runaway) (review finding). Runaway is a
	// sample capped at runawayDispatchScan; CHAOS-4093 held 83 stuck runs, so
	// a gauge fed from the sample would have reported 20 for an incident more
	// than four times that size. Understating scope is the specific way a
	// scope metric fails, and it fails silently.
	//
	// A pass where the report did not run publishes zero here, and that zero
	// is NOT a claim -- it is unproven, and the failure counter above is what
	// says so. The two are only meaningful read together, which is why the
	// HELP text on both says so.
	recovered.RunawayDispatchWakeups = materialized.RunawayTotal
	// Measured only when the report actually delivered. Anything else leaves
	// the previous value standing rather than overwriting a real count with a
	// zero nobody took (review finding).
	recovered.RunawayMeasured = reportDelivered

	if !aborted {
		if err := pipeline.runStage(ctx, StageKernel, func(stageCtx context.Context) error {
			_, stepErr := pipeline.kernel.Step(
				stageCtx, now, limit, pipeline.config.LeaseDuration, pipeline.publish, pipeline.postSync,
			)
			return stepErr
		}); err != nil {
			if ctx.Err() != nil {
				return recovered, err
			}
			aborted = true
		}
	}

	// Observer always runs, whatever happened above -- it is read-only,
	// independent of every earlier stage, and it is the stage whose success
	// or failure this function ultimately reports to Loop (see the stage
	// classification comment near the top of this method).
	if err := ctx.Err(); err != nil {
		return recovered, err
	}
	var observed Observation
	observerErr := pipeline.runStage(ctx, StageObserver, func(stageCtx context.Context) error {
		var stepErr error
		observed, stepErr = pipeline.observer.Step(stageCtx, now, limit)
		return stepErr
	})
	// The read-only Observer leaves every pipeline-authored field zero, so
	// they are copied across rather than merged. One assignment per field,
	// spelled out: a struct-level copy would silently drop the observer's own
	// queue snapshot, and a loop over reflection would hide the next field
	// somebody forgets to carry.
	observation = observed
	observation.ExhaustedDeliveriesRecovered = recovered.ExhaustedDeliveriesRecovered
	observation.RunawayDispatchWakeups = recovered.RunawayDispatchWakeups
	observation.UnreclaimableCandidates = recovered.UnreclaimableCandidates
	observation.UnreclaimableTerminalized = recovered.UnreclaimableTerminalized
	observation.UnreclaimableSweepFailures = recovered.UnreclaimableSweepFailures
	observation.RunawayMeasured = recovered.RunawayMeasured
	observation.UnreclaimableMeasured = recovered.UnreclaimableMeasured
	if observerErr != nil {
		if ctx.Err() != nil {
			return observation, observerErr
		}
		// The one stage whose failure this function still reports as an
		// error -- see the classification comment above. ErrDegradedStage
		// tells Loop.run this is CHAOS-4239's stage-scoped failure, not a
		// fatal one: log it, keep ticking, let the next tick self-heal.
		return observation, fmt.Errorf("observer stage: %w: %w", ErrDegradedStage, observerErr)
	}
	return observation, nil
}

// stageTelemetry is the CHAOS-4239 metrics fragment: a failure counter and a
// last-observed-duration gauge per stage, keyed the same way
// sync_dispatch_observer's own WritePrometheus keys its fixed kinds -- every
// known stage always exports a line, zero until it first runs or fails, so a
// stage that has never failed is visibly zero rather than absent.
type stageTelemetry struct {
	mu        sync.Mutex
	failures  map[StageName]uint64
	durations map[StageName]time.Duration
}

func newStageTelemetry() stageTelemetry {
	return stageTelemetry{
		failures:  make(map[StageName]uint64, len(orderedStages)),
		durations: make(map[StageName]time.Duration, len(orderedStages)),
	}
}

func (telemetry *stageTelemetry) recordFailure(stage StageName) {
	telemetry.mu.Lock()
	defer telemetry.mu.Unlock()
	telemetry.failures[stage]++
}

func (telemetry *stageTelemetry) recordDuration(stage StageName, elapsed time.Duration) {
	telemetry.mu.Lock()
	defer telemetry.mu.Unlock()
	telemetry.durations[stage] = elapsed
}

func (telemetry *stageTelemetry) snapshot() (map[StageName]uint64, map[StageName]time.Duration) {
	telemetry.mu.Lock()
	defer telemetry.mu.Unlock()
	failures := make(map[StageName]uint64, len(telemetry.failures))
	for stage, count := range telemetry.failures {
		failures[stage] = count
	}
	durations := make(map[StageName]time.Duration, len(telemetry.durations))
	for stage, elapsed := range telemetry.durations {
		durations[stage] = elapsed
	}
	return failures, durations
}

// WritePrometheus satisfies health.MetricsSource. It is registered only when
// MutationPipelineConfig.Registry is set.
func (pipeline *MutationPipeline) WritePrometheus(output io.Writer) error {
	if pipeline == nil || output == nil {
		return errors.New("Prometheus output is required")
	}
	failures, durations := pipeline.stages.snapshot()
	budgets := pipeline.config.StageBudgets

	stages := make([]string, 0, len(orderedStages))
	for _, stage := range orderedStages {
		stages = append(stages, string(stage))
	}
	sort.Strings(stages)

	var text strings.Builder
	text.WriteString("# HELP sync_reconciler_stage_failures_total Pipeline stage passes that exceeded their own budget or errored (CHAOS-4239). The pipeline continues past every stage except lease_repair and kernel; a stage climbing here degrades only its own work, never the process.\n# TYPE sync_reconciler_stage_failures_total counter\n")
	for _, name := range stages {
		fmt.Fprintf(&text, "sync_reconciler_stage_failures_total{stage=%q} %d\n", name, failures[StageName(name)])
	}
	text.WriteString("# HELP sync_reconciler_stage_duration_seconds Wall-clock time the most recent pass spent in this stage, whether it succeeded or not.\n# TYPE sync_reconciler_stage_duration_seconds gauge\n")
	for _, name := range stages {
		fmt.Fprintf(&text, "sync_reconciler_stage_duration_seconds{stage=%q} %s\n", name, formatSeconds(durations[StageName(name)]))
	}
	text.WriteString("# HELP sync_reconciler_stage_budget_seconds The bounded sub-context this stage runs under. Static per deployment; exported so elapsed and budget can be read from the same dashboard without checking the source.\n# TYPE sync_reconciler_stage_budget_seconds gauge\n")
	for _, name := range stages {
		fmt.Fprintf(&text, "sync_reconciler_stage_budget_seconds{stage=%q} %s\n", name, formatSeconds(budgets[StageName(name)]))
	}
	_, err := io.WriteString(output, text.String())
	return err
}

func formatSeconds(duration time.Duration) string {
	return strconv.FormatFloat(duration.Seconds(), 'g', -1, 64)
}

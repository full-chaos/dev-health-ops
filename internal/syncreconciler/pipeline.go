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
	"github.com/jackc/pgx/v5/pgconn"
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
	outboxClose  TerminalOutboxCloseStepper
	config       MutationPipelineConfig

	stages stageTelemetry
	// rollupBumps reuses dev_health_sync_run_rollup_bumped_total (CHAOS-4559,
	// widened CHAOS-4586) for THIS pipeline's own two terminal-status
	// mechanisms -- UnreclaimableSweep.terminalize and LeaseRepair.Step's
	// markExpiredLeaseFailed both now write through the shared
	// internal/syncrunrollup.Bump seam, and a write with no matching counter
	// bump is exactly the "measurement never happened" gap the standing
	// order this counters. Same metric NAME as internal/providerfoundation's
	// own RecordSyncRunRollupBumped in the dev-health-worker binary -- two
	// independent processes/label sources under one Prometheus family, not
	// one shared Go instance.
	rollupBumps rollupBumpCounts
}

// rollupBumpCounts is this package's own minimal counter for
// dev_health_sync_run_rollup_bumped_total{outcome,path} -- deliberately not
// a dependency on internal/providerfoundation (this binary, dev-health-
// reconciler, has no other reason to import it, and CHAOS-4239 already
// established the pattern of a pipeline-local counter type for its own
// stage metrics rather than reaching for a shared Metrics type).
type rollupBumpCounts struct {
	mu     sync.Mutex
	counts map[[2]string]uint64 // [outcome, path] -> count
}

func newRollupBumpCounts() rollupBumpCounts {
	return rollupBumpCounts{counts: make(map[[2]string]uint64, 2)}
}

// record is a no-op for n<=0 -- callers pass a result count that is
// legitimately zero on a quiet pass, and a zero-add must never be
// distinguished from "never called" in the underlying map (both read back
// as zero either way, but a stray zero-key entry would grow the map
// forever across ticks for no reason).
func (counts *rollupBumpCounts) record(outcome, path string, n int) {
	if counts == nil || n <= 0 {
		return
	}
	counts.mu.Lock()
	defer counts.mu.Unlock()
	counts.counts[[2]string{outcome, path}] += uint64(n)
}

func (counts *rollupBumpCounts) snapshot() map[[2]string]uint64 {
	counts.mu.Lock()
	defer counts.mu.Unlock()
	snapshot := make(map[[2]string]uint64, len(counts.counts))
	for key, value := range counts.counts {
		snapshot[key] = value
	}
	return snapshot
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
	outboxClose TerminalOutboxCloseStepper,
	config MutationPipelineConfig,
) (*MutationPipeline, error) {
	// outboxClose is REQUIRED, unlike sweep -- see TerminalOutboxCloseStepper's
	// doc comment for why CHAOS-4583's closer carries no staged-rollout risk
	// the way the sweep's terminalization did.
	if repair == nil || terminal == nil || materializer == nil || kernel == nil ||
		observer == nil || outboxClose == nil || !config.valid() {
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
		outboxClose:  outboxClose,
		config:       config,
		stages:       newStageTelemetry(),
		rollupBumps:  newRollupBumpCounts(),
	}
	if config.Registry != nil {
		if err := config.Registry.RegisterMetrics("sync_reconciler_pipeline", pipeline); err != nil {
			return nil, fmt.Errorf("register sync reconciler pipeline metrics: %w", err)
		}
		// One required readiness check per stage, healthy by default (a
		// stage that has not yet run, or has not yet failed
		// consecutiveFailureDegradeThreshold times, must never hold up
		// startup readiness on its own). CheckStatus.Name is a bounded,
		// pre-registered identifier -- the same safety property every other
		// required check in this codebase relies on -- so naming the exact
		// degraded stage on readyz costs nothing the HTTP surface's "never
		// return check error text" invariant already protects.
		for _, stage := range orderedStages {
			stage := stage
			checkErr := config.Registry.RegisterRequired(stageReadinessCheckName(stage), func(context.Context) error {
				if pipeline.stages.isDegraded(stage) {
					return errStageDegraded
				}
				// CHAOS-4239 round 3: a stage that ignores its own stageCtx
				// and never returns produces no failure and no success --
				// isDegraded above would never see it. isOverrunActive is a
				// flag the watchdog timer armed in runStage sets when it
				// fires (see stageTelemetry.reportOverrun); this check only
				// reads it, it never computes the overrun itself, so
				// detection does not depend on anyone ever calling this
				// check at all.
				if pipeline.stages.isOverrunActive(stage) {
					return errStepOverrun
				}
				return nil
			})
			if checkErr != nil {
				return nil, fmt.Errorf("register sync reconciler stage %q readiness: %w", stage, checkErr)
			}
		}
	}
	return pipeline, nil
}

// errStageDegraded is never returned to a caller or exposed on the HTTP
// surface -- health.CheckFunc's contract already keeps check error TEXT off
// /readyz (see health/registry.go's doc comment); only the pre-registered
// check NAME (stageReadinessCheckName) is exposed, which is what names the
// stage.
var errStageDegraded = errors.New("syncreconciler: stage failed its last " +
	fmt.Sprintf("%d consecutive ticks", consecutiveFailureDegradeThreshold))

// errStepOverrun is never returned to a caller or exposed on the HTTP
// surface -- see errStageDegraded's doc comment for the same contract. It
// distinguishes "still running, past its budget plus margin" from
// errStageDegraded's "has already returned and failed repeatedly" in logs
// and in which of the two sentinels a reader searches for, even though both
// fail the identical readyz check by the identical name.
var errStepOverrun = errors.New("syncreconciler: stage has been running past its budget and margin")

// runStage bounds one stage call to its own budget instead of letting it
// share the whole tick's envelope with its seven siblings (CHAOS-4239), and
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

	// markRunning/markIdle bracket the call so a stage that ignores stageCtx
	// and never returns is still visible from a completely different
	// goroutine (CHAOS-4239 round 3) -- everything below this line only runs
	// once fn actually returns, which is exactly the case this pair exists
	// to cover for.
	pipeline.stages.markRunning(stage)
	// The watchdog is an ARMED timer, not something computed lazily by
	// whoever happens to poll readyz or scrape /metrics later -- chris
	// explicitly required this in round-3 follow-up review, since nothing
	// guarantees a deployment does either. Its callback runs on ITS OWN
	// goroutine, independent of whatever goroutine ends up stuck inside fn,
	// so it fires and reports the overrun regardless of whether fn ever
	// returns. Stop()'d the instant the stage actually does return;
	// reportOverrun itself is a no-op if that race is lost by a hair (the
	// stage returned right as the timer was firing).
	watchdog := time.AfterFunc(budget+stepOverrunMargin, func() {
		if pipeline.stages.reportOverrun(stage) {
			slog.Warn(
				"syncreconciler.step_overrun",
				"stage", string(stage),
				"budget_ms", budget.Milliseconds(),
				"threshold_ms", (budget + stepOverrunMargin).Milliseconds(),
			)
		}
	})
	defer func() {
		watchdog.Stop()
		pipeline.stages.markIdle(stage)
	}()

	start := time.Now()
	err := fn(stageCtx)
	elapsed := time.Since(start)
	pipeline.stages.recordDuration(stage, elapsed)

	if err == nil {
		pipeline.stages.recordSuccess(stage)
		return nil
	}
	if ctx.Err() != nil {
		// Shutdown, not a stage failure -- the caller decides whether to
		// propagate ctx.Err() itself.
		return err
	}
	pipeline.stages.recordFailure(stage)
	sqlstate := stageSQLState(err)
	if sqlstate != "" {
		pipeline.stages.recordCancellation(stage, sqlstate)
	}
	slog.Error(
		"syncreconciler.stage_failed",
		"stage", string(stage),
		"budget_ms", budget.Milliseconds(),
		"elapsed_ms", elapsed.Milliseconds(),
		"sqlstate", sqlstate,
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

	var repairResult LeaseRepairResult
	if err := pipeline.runStage(ctx, StageLeaseRepair, func(stageCtx context.Context) error {
		var stepErr error
		repairResult, stepErr = pipeline.repair.Step(stageCtx, now, limit)
		return stepErr
	}); err != nil {
		if ctx.Err() != nil {
			return swept, err
		}
		aborted = true
	}
	// CHAOS-4586: markExpiredLeaseFailed already recomputed sync_runs'
	// rollup via syncrunrollup.Bump for each of these; repairResult.Failed
	// is zero on a failed Step (see LeaseRepairResult's own zero-value
	// return on error), so this is a safe no-op in that case.
	pipeline.rollupBumps.record("failed", "lease_repair", repairResult.Failed)

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
			// CHAOS-4586: terminalize() already recomputed sync_runs' rollup
			// via syncrunrollup.Bump for each of these (always zero in shadow
			// mode, matching Terminalized's own doc comment).
			pipeline.rollupBumps.record("failed", "unreclaimable_sweep", sweepResult.Terminalized)
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

	// CHAOS-4583: closes a 'dispatched' outbox row once its owning domain
	// state has itself gone terminal (see terminal_outbox_close.go's package
	// doc). A read-adjacent safety net over largely disjoint state from lease
	// repair and the kernel's claim-and-deliver path, so -- like sweep and
	// terminal delivery repair above -- its failure is absorbed rather than
	// aborting the rest of this tick's mutation work.
	if !aborted {
		var closed TerminalOutboxCloseResult
		closeErr := pipeline.runStage(ctx, StageTerminalOutboxClose, func(stageCtx context.Context) error {
			var stepErr error
			closed, stepErr = pipeline.outboxClose.Step(stageCtx, now, limit)
			return stepErr
		})
		if closeErr != nil && ctx.Err() != nil {
			return recovered, closeErr
		}
		if closeErr == nil {
			pipeline.stages.recordOutboxClosed(closed.ClosedByOutcome)
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
	// CHAOS-4357 round 2 (codex P2): sourced from the narrow
	// DiscoveryRearmed field, not the general Discovery affected-row count
	// -- Discovery also counts fresh inserts and non-river transitions,
	// neither of which is the "recovered a stranded row" event this metric's
	// HELP text claims. materialized.DiscoveryRearmed is populated whether
	// or not repair aborted the rest of the pass (set unconditionally on
	// !aborted before that guard is checked), so no separate aborted-guard
	// is needed here -- a zero-value MaterializerResult on an aborted pass
	// already reports DiscoveryRearmed=0, the correct "materializer never
	// ran" answer.
	recovered.DiscoveryRearmed = materialized.DiscoveryRearmed

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
	observation.DiscoveryRearmed = recovered.DiscoveryRearmed
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

// consecutiveFailureDegradeThreshold is chris's readiness ruling on
// CHAOS-4239: a single stage blip must not flap the reconciler's overall
// readiness (that's the false-alarm failure mode), but a stage failing on
// EVERY tick -- the shape the ticket's own cold-start symptom took -- must be
// visible on readyz by name, not only to someone reading a Prometheus
// counter. Three consecutive failures is the line; one success clears it
// immediately, so a stage that recovers is never held degraded by a stale
// streak.
const consecutiveFailureDegradeThreshold = 3

// stepOverrunMargin pads a stage's own budget before its in-flight duration
// counts as an overrun (CHAOS-4239 round 3). It exists for the same reason
// stageBudgetOuterEnvelopeMargin does in dependencies.go: a stage that
// returns right at its own deadline should not immediately read as stuck.
const stepOverrunMargin = 250 * time.Millisecond

// stageDurationBuckets bounds the CHAOS-4262 per-stage duration histogram.
// Stage budgets top out at maximumStageBudget (10s), so the buckets stay
// fine-grained in the sub-second range where every budget actually lives
// (400ms-1000ms by default) and only need one bucket past a second.
var stageDurationBuckets = []float64{
	0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 0.75, 1, 2, 5, 10,
}

// stageHistogram is a dependency-free Prometheus histogram, deliberately
// reimplemented here rather than imported from internal/jobruntime: that
// package's histogram is sized for whole-job durations up to an hour
// (durationBuckets tops out at 3600s) and pulling it in would couple the
// reconciler's stage telemetry to the worker job-execution package for a
// handful of shared lines.
type stageHistogram struct {
	buckets []uint64
	count   uint64
	sum     float64
}

func newStageHistogram() *stageHistogram {
	return &stageHistogram{buckets: make([]uint64, len(stageDurationBuckets)+1)}
}

func (histogram *stageHistogram) observe(value float64) {
	index := len(stageDurationBuckets)
	for candidate, upperBound := range stageDurationBuckets {
		if value <= upperBound {
			index = candidate
			break
		}
	}
	histogram.buckets[index]++
	histogram.count++
	histogram.sum += value
}

// stageSQLState recovers the SQLSTATE a stage's own step-identified error
// carries, or "" if none was recovered. It never exposes the driver's own
// message -- only the five-character code -- matching the contract
// materializerStepError and unreclaimableStepError already hold.
func stageSQLState(err error) string {
	var materializerErr materializerStepError
	if errors.As(err, &materializerErr) {
		return materializerErr.sqlState
	}
	var sweepErr unreclaimableStepError
	if errors.As(err, &sweepErr) {
		return sweepErr.sqlState
	}
	var outboxCloseErr terminalOutboxCloseStepError
	if errors.As(err, &outboxCloseErr) {
		return outboxCloseErr.sqlState
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

// stageTelemetry is the CHAOS-4239 metrics fragment: a lifetime failure
// counter, a last-observed-duration gauge, a readiness-affecting
// consecutive-failure streak, and in-flight overrun tracking per stage,
// keyed the same way sync_dispatch_observer's own WritePrometheus keys its
// fixed kinds -- every known stage always exports a line, zero until it
// first runs or fails, so a stage that has never failed is visibly zero
// rather than absent.
type stageTelemetry struct {
	mu          sync.Mutex
	failures    map[StageName]uint64
	durations   map[StageName]time.Duration
	consecutive map[StageName]int
	degraded    map[StageName]bool

	// running, overrunActive and overruns answer a question none of the
	// fields above can: a stage whose call IGNORES its own stageCtx and
	// simply never returns produces no failure, no success, and no duration
	// sample -- runStage is itself blocked inside fn(stageCtx) and nothing
	// about that call graph ever reaches recordFailure or recordSuccess.
	// Every non-fatal-degradation mechanism this file builds
	// (ErrDegradedStage, ErrStepEnvelopeExceeded) depends on the call
	// RETURNING eventually; a stage that never returns is invisible to all
	// of them, which would make CHAOS-4239's "process stays up" fix trade a
	// loud crash-loop for a silent zombie hang (chris's round-3 finding).
	//
	// Detection is an ARMED per-step timer, not something computed lazily
	// when a poller happens to ask (chris explicitly rejected the lazy
	// design in round-3 follow-up review: nothing guarantees a deployment
	// polls readyz or scrapes metrics at all, and lazy detection would then
	// never fire). runStage arms a time.AfterFunc at stage start, for
	// budget+stepOverrunMargin; its callback runs on ITS OWN goroutine --
	// independent of whatever goroutine is stuck inside fn -- and reports
	// the overrun unconditionally the moment it fires, whether or not
	// anyone is watching. The timer is stopped the instant the stage
	// actually returns.
	running       map[StageName]bool
	overrunActive map[StageName]bool
	overruns      map[StageName]uint64

	// histograms is the CHAOS-4262 duration distribution per stage --
	// durations above already exports only the MOST RECENT pass, which
	// cannot answer "how often does this stage run near its budget" the way
	// a histogram can.
	histograms map[StageName]*stageHistogram
	// cancellations counts a stage failure by the SQLSTATE its step error
	// carried, keyed [stage][sqlstate]. This is the CHAOS-4262 fix for the
	// masking pattern CHAOS-4242 already named: every stage failure folds
	// into the same ErrUnavailable/"database unavailable" classification
	// regardless of cause, so a real statement cancellation (57014, a
	// JIT-inflated planner estimate crossing the stage budget) read
	// identically to an actual outage. errors.Is(err, ErrUnavailable) still
	// holds for every caller that branches on it -- this is additional,
	// non-replacing classification surfaced only here, in metrics.
	cancellations map[StageName]map[string]uint64

	// outboxClosed is CHAOS-4583's own counter: lifetime sync_dispatch_outbox
	// rows closed, keyed [kind][outcome] -> count. Unlike every other field on
	// this struct (all keyed by StageName), this one is keyed by the outbox
	// row's own kind -- there is exactly one stage (StageTerminalOutboxClose)
	// but four kinds, and dev_health_sync_dispatch_outbox_closed_total's own
	// HELP text promises a {kind,outcome} breakdown, not a per-stage one.
	outboxClosed map[string]map[string]uint64
}

func newStageTelemetry() stageTelemetry {
	histograms := make(map[StageName]*stageHistogram, len(orderedStages))
	for _, stage := range orderedStages {
		histograms[stage] = newStageHistogram()
	}
	return stageTelemetry{
		failures:      make(map[StageName]uint64, len(orderedStages)),
		durations:     make(map[StageName]time.Duration, len(orderedStages)),
		consecutive:   make(map[StageName]int, len(orderedStages)),
		degraded:      make(map[StageName]bool, len(orderedStages)),
		running:       make(map[StageName]bool, len(orderedStages)),
		overrunActive: make(map[StageName]bool, len(orderedStages)),
		overruns:      make(map[StageName]uint64, len(orderedStages)),
		histograms:    histograms,
		cancellations: make(map[StageName]map[string]uint64, len(orderedStages)),
		outboxClosed:  make(map[string]map[string]uint64, 4),
	}
}

// recordOutboxClosed accumulates one pass's CHAOS-4583 closes into the
// lifetime counter. closedThisPass is [kind][outcome] -> count for the pass
// that just committed; a nil or empty map (nothing closed) is a valid no-op.
func (telemetry *stageTelemetry) recordOutboxClosed(closedThisPass map[string]map[string]int64) {
	if len(closedThisPass) == 0 {
		return
	}
	telemetry.mu.Lock()
	defer telemetry.mu.Unlock()
	for kind, byOutcome := range closedThisPass {
		total := telemetry.outboxClosed[kind]
		if total == nil {
			total = make(map[string]uint64, len(byOutcome))
			telemetry.outboxClosed[kind] = total
		}
		for outcome, count := range byOutcome {
			if count > 0 {
				total[outcome] += uint64(count)
			}
		}
	}
}

func (telemetry *stageTelemetry) outboxClosedSnapshot() map[string]map[string]uint64 {
	telemetry.mu.Lock()
	defer telemetry.mu.Unlock()
	snapshot := make(map[string]map[string]uint64, len(telemetry.outboxClosed))
	for kind, byOutcome := range telemetry.outboxClosed {
		copied := make(map[string]uint64, len(byOutcome))
		for outcome, count := range byOutcome {
			copied[outcome] = count
		}
		snapshot[kind] = copied
	}
	return snapshot
}

// markRunning records that a stage's call has started. Call this BEFORE
// invoking the stage's own function, never after -- the whole point is
// visibility into a call that might never return.
func (telemetry *stageTelemetry) markRunning(stage StageName) {
	telemetry.mu.Lock()
	defer telemetry.mu.Unlock()
	telemetry.running[stage] = true
	telemetry.overrunActive[stage] = false
}

// markIdle clears a stage's in-flight and overrun state once its call
// returns, however it returned -- readiness must clear the instant a
// previously-stuck stage finally does, per chris's ruling. Call via defer
// immediately after markRunning so it always runs, on every exit path.
func (telemetry *stageTelemetry) markIdle(stage StageName) {
	telemetry.mu.Lock()
	defer telemetry.mu.Unlock()
	telemetry.running[stage] = false
	telemetry.overrunActive[stage] = false
}

// reportOverrun is called from the watchdog timer's OWN goroutine (armed by
// runStage, independent of whatever goroutine is potentially still stuck
// inside the stage's call) when a stage has been running past its budget
// plus margin. It returns false, doing nothing, if the stage has already
// returned by the time the timer fires -- a benign race between the
// timer firing and runStage's deferred Stop()/markIdle -- so a fast stage
// that finished right at the wire is never misreported.
func (telemetry *stageTelemetry) reportOverrun(stage StageName) bool {
	telemetry.mu.Lock()
	defer telemetry.mu.Unlock()
	if !telemetry.running[stage] {
		return false
	}
	telemetry.overrunActive[stage] = true
	telemetry.overruns[stage]++
	return true
}

func (telemetry *stageTelemetry) isOverrunActive(stage StageName) bool {
	telemetry.mu.Lock()
	defer telemetry.mu.Unlock()
	return telemetry.overrunActive[stage]
}

func (telemetry *stageTelemetry) overrunSnapshot() map[StageName]uint64 {
	telemetry.mu.Lock()
	defer telemetry.mu.Unlock()
	overruns := make(map[StageName]uint64, len(telemetry.overruns))
	for stage, count := range telemetry.overruns {
		overruns[stage] = count
	}
	return overruns
}

// recordFailure is called ONLY for a failure runStage has already decided is
// a real stage failure (not a parent-context cancellation -- see runStage).
// It both grows the lifetime counter and advances the readiness-affecting
// consecutive streak.
func (telemetry *stageTelemetry) recordFailure(stage StageName) {
	telemetry.mu.Lock()
	defer telemetry.mu.Unlock()
	telemetry.failures[stage]++
	telemetry.consecutive[stage]++
	if telemetry.consecutive[stage] >= consecutiveFailureDegradeThreshold {
		telemetry.degraded[stage] = true
	}
}

// recordSuccess clears the consecutive-failure streak the moment a stage
// succeeds. One success is enough -- there is no separate "recovered N times"
// threshold on the way back down, so a stage does not stay marked degraded
// on readyz after it has visibly started working again.
func (telemetry *stageTelemetry) recordSuccess(stage StageName) {
	telemetry.mu.Lock()
	defer telemetry.mu.Unlock()
	telemetry.consecutive[stage] = 0
	telemetry.degraded[stage] = false
}

func (telemetry *stageTelemetry) isDegraded(stage StageName) bool {
	telemetry.mu.Lock()
	defer telemetry.mu.Unlock()
	return telemetry.degraded[stage]
}

func (telemetry *stageTelemetry) recordDuration(stage StageName, elapsed time.Duration) {
	telemetry.mu.Lock()
	defer telemetry.mu.Unlock()
	telemetry.durations[stage] = elapsed
	if histogram := telemetry.histograms[stage]; histogram != nil {
		histogram.observe(elapsed.Seconds())
	}
}

// recordCancellation is called only when a stage's own step error carried a
// SQLSTATE (see stageSQLState). It is keyed by the exact code -- 57014 for a
// canceled statement, 42501 for a permission fault, and so on -- rather than
// collapsed to a single "had a sqlstate" bucket, because CHAOS-4262 and
// CHAOS-4261 were two different production incidents that both surfaced as
// the same "database unavailable" log line and needed different first
// responses.
func (telemetry *stageTelemetry) recordCancellation(stage StageName, sqlstate string) {
	telemetry.mu.Lock()
	defer telemetry.mu.Unlock()
	byState := telemetry.cancellations[stage]
	if byState == nil {
		byState = make(map[string]uint64, 1)
		telemetry.cancellations[stage] = byState
	}
	byState[sqlstate]++
}

func (telemetry *stageTelemetry) histogramSnapshot() map[StageName]*stageHistogram {
	telemetry.mu.Lock()
	defer telemetry.mu.Unlock()
	snapshot := make(map[StageName]*stageHistogram, len(telemetry.histograms))
	for stage, histogram := range telemetry.histograms {
		if histogram == nil {
			continue
		}
		copied := *histogram
		copied.buckets = append([]uint64(nil), histogram.buckets...)
		snapshot[stage] = &copied
	}
	return snapshot
}

func (telemetry *stageTelemetry) cancellationSnapshot() map[StageName]map[string]uint64 {
	telemetry.mu.Lock()
	defer telemetry.mu.Unlock()
	snapshot := make(map[StageName]map[string]uint64, len(telemetry.cancellations))
	for stage, byState := range telemetry.cancellations {
		copied := make(map[string]uint64, len(byState))
		for sqlstate, count := range byState {
			copied[sqlstate] = count
		}
		snapshot[stage] = copied
	}
	return snapshot
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

func (telemetry *stageTelemetry) degradedSnapshot() map[StageName]bool {
	telemetry.mu.Lock()
	defer telemetry.mu.Unlock()
	degraded := make(map[StageName]bool, len(telemetry.degraded))
	for stage, isDegraded := range telemetry.degraded {
		degraded[stage] = isDegraded
	}
	return degraded
}

// stageReadinessCheckName is the health.Registry required-check name for a
// stage's degraded state, sharing sync_reconciler_stage_*'s prefix so an
// operator can go from a metric label straight to the matching readyz check
// name without translating between two vocabularies.
func stageReadinessCheckName(stage StageName) string {
	return "sync_reconciler_stage_" + string(stage)
}

// WritePrometheus satisfies health.MetricsSource. It is registered only when
// MutationPipelineConfig.Registry is set.
func (pipeline *MutationPipeline) WritePrometheus(output io.Writer) error {
	if pipeline == nil || output == nil {
		return errors.New("Prometheus output is required")
	}
	failures, durations := pipeline.stages.snapshot()
	degraded := pipeline.stages.degradedSnapshot()
	budgets := pipeline.config.StageBudgets
	histograms := pipeline.stages.histogramSnapshot()
	cancellations := pipeline.stages.cancellationSnapshot()

	stages := make([]string, 0, len(orderedStages))
	for _, stage := range orderedStages {
		stages = append(stages, string(stage))
	}
	sort.Strings(stages)
	// Reads only: the watchdog armed in runStage (CHAOS-4239 round 3) is the
	// sole source of truth for overrun detection, so a scrape never needs to
	// (and must not) recompute it here.
	overruns := pipeline.stages.overrunSnapshot()

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
	fmt.Fprintf(&text, "# HELP sync_reconciler_stage_degraded Whether this stage has failed its last %d consecutive ticks. Matches the sync_reconciler_stage_<name> readyz check by name; one success clears both.\n# TYPE sync_reconciler_stage_degraded gauge\n", consecutiveFailureDegradeThreshold)
	for _, name := range stages {
		value := 0
		if degraded[StageName(name)] {
			value = 1
		}
		fmt.Fprintf(&text, "sync_reconciler_stage_degraded{stage=%q} %d\n", name, value)
	}
	text.WriteString("# HELP sync_reconciler_step_overrun_total Stage calls that ignored their own bounded context and were still running past their budget plus margin the last time this was checked (CHAOS-4239 round 3). Unlike sync_reconciler_stage_failures_total, this counts calls that never returned at all -- the only way such a call is visible.\n# TYPE sync_reconciler_step_overrun_total counter\n")
	for _, name := range stages {
		fmt.Fprintf(&text, "sync_reconciler_step_overrun_total{stage=%q} %d\n", name, overruns[StageName(name)])
	}
	// CHAOS-4262: a full distribution, not just the most recent pass --
	// sync_reconciler_stage_duration_seconds above answers "how long was the
	// last pass", this answers "how often does this stage run close to its
	// budget", which is what caught the materializer JIT-compile regression
	// crossing its 600ms budget on a query that takes ~2ms to execute.
	text.WriteString("# HELP dev_health_reconciler_stage_duration_seconds Distribution of wall-clock time spent per pipeline stage pass, whether it succeeded or not (CHAOS-4262).\n# TYPE dev_health_reconciler_stage_duration_seconds histogram\n")
	for _, name := range stages {
		writeStageHistogram(&text, "dev_health_reconciler_stage_duration_seconds", name, histograms[StageName(name)])
	}
	// CHAOS-4262: every stage failure collapses to the same ErrUnavailable
	// classification (see stageSQLState's doc comment) so that lifecycle and
	// readiness code never depends on driver detail -- but that means an
	// operator reading sync_reconciler_stage_failures_total alone cannot tell
	// a real statement cancellation (57014) apart from a permission fault
	// (42501, CHAOS-4261's incident) or an actual outage. This is keyed only
	// by the pairs actually observed, like any other Prometheus counter with
	// a dynamic label -- there is no bounded enumeration of every SQLSTATE to
	// pre-declare zero for.
	text.WriteString("# HELP dev_health_reconciler_stage_cancelled_total Pipeline stage failures broken out by the driver SQLSTATE recovered from the failing statement, so a canceled statement (57014) is never folded into the generic \"database unavailable\" classification.\n# TYPE dev_health_reconciler_stage_cancelled_total counter\n")
	for _, name := range stages {
		byState := cancellations[StageName(name)]
		sqlstates := make([]string, 0, len(byState))
		for sqlstate := range byState {
			sqlstates = append(sqlstates, sqlstate)
		}
		sort.Strings(sqlstates)
		for _, sqlstate := range sqlstates {
			fmt.Fprintf(&text, "dev_health_reconciler_stage_cancelled_total{stage=%q,sqlstate=%q} %d\n",
				name, sqlstate, byState[sqlstate])
		}
	}
	// CHAOS-4583: rows this pipeline has terminal-closed, lifetime, broken out
	// by outbox kind and by the owning domain state's terminal value that
	// licensed the close. Keyed by (kind, outcome) rather than by StageName --
	// there is one stage (terminal_outbox_close) but four outbox kinds, and
	// this counter's whole purpose is telling them apart.
	outboxClosed := pipeline.stages.outboxClosedSnapshot()
	text.WriteString("# HELP dev_health_sync_dispatch_outbox_closed_total sync_dispatch_outbox rows terminal-closed by the CHAOS-4583 reconciler step: a 'dispatched' row whose owning domain state (sync_runs, or for reference_discovery the sync_run_reference_discoveries ledger) had itself already reached a terminal outcome. Labeled by outbox kind and by that owning outcome.\n# TYPE dev_health_sync_dispatch_outbox_closed_total counter\n")
	kinds := make([]string, 0, len(outboxClosed))
	for kind := range outboxClosed {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	for _, kind := range kinds {
		byOutcome := outboxClosed[kind]
		outcomes := make([]string, 0, len(byOutcome))
		for outcome := range byOutcome {
			outcomes = append(outcomes, outcome)
		}
		sort.Strings(outcomes)
		for _, outcome := range outcomes {
			fmt.Fprintf(&text, "dev_health_sync_dispatch_outbox_closed_total{kind=%q,outcome=%q} %d\n",
				kind, outcome, byOutcome[outcome])
		}
	}
	// CHAOS-4586: SAME metric name/HELP as internal/providerfoundation's own
	// RecordSyncRunRollupBumped in the dev-health-worker binary -- one
	// Prometheus family, two independent processes each reporting the path
	// values only they can produce. Both known paths are declared even at
	// zero (matching this function's own stage-loop convention above): a
	// pass with nothing to terminalize is a finding, not an absence, same
	// rationale as swept.UnreclaimableMeasured.
	text.WriteString("# HELP dev_health_sync_run_rollup_bumped_total sync_runs.completed_units/failed_units live recomputes on a per-unit terminal commit, by which outcome triggered it and which code path made the write (CHAOS-4559, CHAOS-4586).\n# TYPE dev_health_sync_run_rollup_bumped_total counter\n")
	rollupBumps := pipeline.rollupBumps.snapshot()
	for _, path := range []string{"unreclaimable_sweep", "lease_repair"} {
		fmt.Fprintf(&text, "dev_health_sync_run_rollup_bumped_total{outcome=%q,path=%q} %d\n",
			"failed", path, rollupBumps[[2]string{"failed", path}])
	}
	_, err := io.WriteString(output, text.String())
	return err
}

// writeStageHistogram emits one stage's Prometheus histogram series
// (_bucket/_sum/_count), mirroring internal/jobruntime's writeHistogram shape
// without importing that package (see stageHistogram's doc comment).
func writeStageHistogram(text *strings.Builder, name, stage string, metric *stageHistogram) {
	if metric == nil {
		metric = newStageHistogram()
	}
	cumulative := uint64(0)
	for index, bound := range stageDurationBuckets {
		cumulative += metric.buckets[index]
		fmt.Fprintf(text, "%s_bucket{stage=%q,le=%q} %d\n", name, stage, formatMetricFloat(bound), cumulative)
	}
	cumulative += metric.buckets[len(stageDurationBuckets)]
	fmt.Fprintf(text, "%s_bucket{stage=%q,le=\"+Inf\"} %d\n", name, stage, cumulative)
	fmt.Fprintf(text, "%s_sum{stage=%q} %s\n", name, stage, formatMetricFloat(metric.sum))
	fmt.Fprintf(text, "%s_count{stage=%q} %d\n", name, stage, metric.count)
}

func formatMetricFloat(value float64) string {
	if value == 0 {
		return "0"
	}
	return strconv.FormatFloat(value, 'g', -1, 64)
}

func formatSeconds(duration time.Duration) string {
	return strconv.FormatFloat(duration.Seconds(), 'g', -1, 64)
}

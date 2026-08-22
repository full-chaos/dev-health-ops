package syncreconciler

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
)

const (
	defaultMutationLeaseDuration    = 5 * time.Minute
	maximumMutationStaleDispatchAge = 24 * time.Hour
)

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
	}
}

func (config MutationPipelineConfig) valid() bool {
	return config.StaleDispatchAge > 0 &&
		config.StaleDispatchAge <= maximumMutationStaleDispatchAge &&
		config.LeaseDuration >= minimumLeaseDuration &&
		config.LeaseDuration <= maximumLeaseDuration
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
	return &MutationPipeline{
		repair:       repair,
		terminal:     terminal,
		materializer: materializer,
		kernel:       kernel,
		observer:     observer,
		publish:      publish,
		postSync:     postSync,
		sweep:        sweep,
		config:       config,
	}, nil
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
	// swept carries the sweep and materializer figures out through EVERY
	// return below, not just the happy one. The existing
	// ExhaustedDeliveriesRecovered comment states the rule and the reason: a
	// later stage failing does not un-happen what an earlier one already
	// committed, and an observation reporting zero after the sweep destroyed
	// work would put a real terminalization under its own alert threshold.
	swept := Observation{}
	if _, err := pipeline.repair.Step(ctx, now, limit); err != nil {
		return swept, err
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
	if pipeline.sweep != nil {
		sweepResult, sweepErr := pipeline.sweep.Step(ctx, now, limit)
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
			// Cancellation belongs to the caller and must propagate; anything
			// else is the safety net failing, which must not fail the pass.
			if errors.Is(sweepErr, context.Canceled) ||
				errors.Is(sweepErr, context.DeadlineExceeded) {
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
	terminal, err := pipeline.terminal.Step(ctx, now, limit)
	if err != nil {
		return swept, err
	}
	// A repair cannot recover more rows than its own bounded window. Treating
	// an out-of-range count as a failed step keeps a miscounting repair from
	// quietly inflating the recovery metric operators alert on.
	if terminal.ExhaustedRecovered < 0 || terminal.ExhaustedRecovered > terminal.Recovered ||
		terminal.RescueOnlyCancelsRecovered < 0 ||
		terminal.RescueOnlyCancelsRecovered > terminal.Recovered ||
		terminal.ExhaustedRecovered+terminal.RescueOnlyCancelsRecovered > terminal.Recovered ||
		terminal.Recovered > limit {
		// The count itself is untrustworthy here, so it is the one case that
		// deliberately reports nothing rather than a number it cannot stand behind.
		return swept, ErrUnavailable
	}
	// The repair commits its own transaction before anything below runs, so its
	// recoveries are already durable no matter how this step ends. Every return
	// from here on carries the count: an observation reporting zero recoveries
	// after rows were in fact reclaimed would let a cycling delivery stay under
	// its own alert threshold, which is the failure the counter exists to catch.
	recovered := swept
	recovered.ExhaustedDeliveriesRecovered = int64(terminal.ExhaustedRecovered)
	// A rescue-only cancel is a registry or queue-routing fault: the job was
	// inserted onto a queue whose client does not execute that kind. Unlike
	// the other two recoveries it will repeat deterministically until someone
	// changes a deployment, so it is logged rather than only counted
	// (CHAOS-4097).
	if terminal.RescueOnlyCancelsRecovered > 0 {
		slog.Warn(
			"syncreconciler.rescue_only_cancel_recovered",
			"recovered", terminal.RescueOnlyCancelsRecovered,
		)
	}
	materialized, err := pipeline.materializer.Step(
		ctx,
		now,
		now.Add(-pipeline.config.StaleDispatchAge),
		limit,
	)
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
	reportDelivered = materialized.RunawayReportStep == "" && err == nil
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
	if err != nil {
		return recovered, err
	}
	if _, err := pipeline.kernel.Step(
		ctx,
		now,
		limit,
		pipeline.config.LeaseDuration,
		pipeline.publish,
		pipeline.postSync,
	); err != nil {
		return recovered, err
	}
	observation, err = pipeline.observer.Step(ctx, now, limit)
	// The read-only Observer leaves every pipeline-authored field zero, so
	// they are copied across rather than merged. One assignment per field,
	// spelled out: a struct-level copy would silently drop the observer's own
	// queue snapshot, and a loop over reflection would hide the next field
	// somebody forgets to carry.
	observation.ExhaustedDeliveriesRecovered = recovered.ExhaustedDeliveriesRecovered
	observation.RunawayDispatchWakeups = recovered.RunawayDispatchWakeups
	observation.UnreclaimableCandidates = recovered.UnreclaimableCandidates
	observation.UnreclaimableTerminalized = recovered.UnreclaimableTerminalized
	observation.UnreclaimableSweepFailures = recovered.UnreclaimableSweepFailures
	observation.RunawayMeasured = recovered.RunawayMeasured
	observation.UnreclaimableMeasured = recovered.UnreclaimableMeasured
	return observation, err
}

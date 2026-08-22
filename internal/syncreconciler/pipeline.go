package syncreconciler

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
)

const (
	// defaultMutationStaleDispatchAge is the same value the Celery
	// sync-provider quiescer uses to decide whether a DISPATCHING row is
	// still live (internal/jobroute's PostgresCelerySyncProviderQuiescer) --
	// both read syncdispatchcontract.DefaultDispatchStaleAge so the two never
	// drift apart (CHAOS-3929).
	defaultMutationStaleDispatchAge = syncdispatchcontract.DefaultDispatchStaleAge
	defaultMutationLeaseDuration    = 5 * time.Minute
	maximumMutationStaleDispatchAge = 24 * time.Hour
)

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
		StaleDispatchAge: defaultMutationStaleDispatchAge,
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
) (Observation, error) {
	if pipeline == nil || ctx == nil || now.IsZero() ||
		limit < minimumStepLimit || limit > maximumStepLimit ||
		!pipeline.config.valid() {
		return Observation{}, ErrInvalidConfiguration
	}
	if err := ctx.Err(); err != nil {
		return Observation{}, err
	}
	now = now.UTC()
	if _, err := pipeline.repair.Step(ctx, now, limit); err != nil {
		return Observation{}, err
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
		if sweepErr != nil {
			// Cancellation belongs to the caller and must propagate; anything
			// else is the safety net failing, which must not fail the pass.
			if errors.Is(sweepErr, context.Canceled) ||
				errors.Is(sweepErr, context.DeadlineExceeded) {
				return Observation{}, sweepErr
			}
			// But it must never fail SILENTLY. Swallowing this without a word
			// reports a healthy pass while the strand the sweep exists to
			// clear is still there -- the same invisibility that let
			// CHAOS-3990 sit unnoticed for sixteen hours (review finding).
			slog.Warn(
				"syncreconciler.unreclaimable_sweep_failed",
				"error", sweepErr.Error(),
			)
		}
	}
	terminal, err := pipeline.terminal.Step(ctx, now, limit)
	if err != nil {
		return Observation{}, err
	}
	// A repair cannot recover more rows than its own bounded window. Treating
	// an out-of-range count as a failed step keeps a miscounting repair from
	// quietly inflating the recovery metric operators alert on.
	if terminal.ExhaustedRecovered < 0 || terminal.ExhaustedRecovered > terminal.Recovered ||
		terminal.Recovered > limit {
		// The count itself is untrustworthy here, so it is the one case that
		// deliberately reports nothing rather than a number it cannot stand behind.
		return Observation{}, ErrUnavailable
	}
	// The repair commits its own transaction before anything below runs, so its
	// recoveries are already durable no matter how this step ends. Every return
	// from here on carries the count: an observation reporting zero recoveries
	// after rows were in fact reclaimed would let a cycling delivery stay under
	// its own alert threshold, which is the failure the counter exists to catch.
	recovered := Observation{
		ExhaustedDeliveriesRecovered: int64(terminal.ExhaustedRecovered),
	}
	if _, err := pipeline.materializer.Step(
		ctx,
		now,
		now.Add(-pipeline.config.StaleDispatchAge),
		limit,
	); err != nil {
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
	observation, err := pipeline.observer.Step(ctx, now, limit)
	observation.ExhaustedDeliveriesRecovered = recovered.ExhaustedDeliveriesRecovered
	return observation, err
}

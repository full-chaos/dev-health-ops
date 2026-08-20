package syncreconciler

import (
	"context"
	"time"
)

const (
	defaultMutationStaleDispatchAge = 15 * time.Minute
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
	config       MutationPipelineConfig
}

func NewMutationPipeline(
	repair LeaseRepairStepper,
	terminal TerminalDeliveryRepairStepper,
	materializer MaterializerStepper,
	kernel KernelStepper,
	observer Stepper,
	publish AtLeastOncePublisher,
	postSync PostSyncHandoff,
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
	terminal, err := pipeline.terminal.Step(ctx, now, limit)
	if err != nil {
		return Observation{}, err
	}
	// A repair cannot recover more rows than its own bounded window. Treating
	// an out-of-range count as a failed step keeps a miscounting repair from
	// quietly inflating the recovery metric operators alert on.
	if terminal.ExhaustedRecovered < 0 || terminal.ExhaustedRecovered > terminal.Recovered ||
		terminal.Recovered > limit {
		return Observation{}, ErrUnavailable
	}
	if _, err := pipeline.materializer.Step(
		ctx,
		now,
		now.Add(-pipeline.config.StaleDispatchAge),
		limit,
	); err != nil {
		return Observation{}, err
	}
	if _, err := pipeline.kernel.Step(
		ctx,
		now,
		limit,
		pipeline.config.LeaseDuration,
		pipeline.publish,
		pipeline.postSync,
	); err != nil {
		return Observation{}, err
	}
	// The repair commits its own transaction, so its recoveries are durable
	// even when a later stage fails. Stamp the count before returning either
	// way: an observation that reports zero recoveries after rows were in fact
	// reclaimed would let a cycling run stay under the alert threshold.
	observation, err := pipeline.observer.Step(ctx, now, limit)
	observation.ExhaustedDeliveriesRecovered = int64(terminal.ExhaustedRecovered)
	return observation, err
}

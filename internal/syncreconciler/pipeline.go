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
// Construction and execution do not change transport routes. With the
// checked-in Celery-only registry, Kernel performs no transport transaction.
// A future River route still fails closed unless a concrete publisher is
// supplied by command composition.
type MutationPipeline struct {
	repair       LeaseRepairStepper
	materializer MaterializerStepper
	kernel       KernelStepper
	observer     Stepper
	publish      AtLeastOncePublisher
	postSync     PostSyncHandoff
	config       MutationPipelineConfig
}

func NewMutationPipeline(
	repair LeaseRepairStepper,
	materializer MaterializerStepper,
	kernel KernelStepper,
	observer Stepper,
	publish AtLeastOncePublisher,
	postSync PostSyncHandoff,
	config MutationPipelineConfig,
) (*MutationPipeline, error) {
	if repair == nil || materializer == nil || kernel == nil || observer == nil || !config.valid() {
		return nil, ErrInvalidConfiguration
	}
	return &MutationPipeline{
		repair:       repair,
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
	return pipeline.observer.Step(ctx, now, limit)
}

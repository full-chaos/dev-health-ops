package syncreconciler

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

type pipelineLeaseRepairFunc func(context.Context, time.Time, int) (LeaseRepairResult, error)

func (function pipelineLeaseRepairFunc) Step(
	ctx context.Context,
	now time.Time,
	limit int,
) (LeaseRepairResult, error) {
	return function(ctx, now, limit)
}

type pipelineMaterializerFunc func(
	context.Context,
	time.Time,
	time.Time,
	int,
) (MaterializerResult, error)

func (function pipelineMaterializerFunc) Step(
	ctx context.Context,
	now time.Time,
	stale time.Time,
	limit int,
) (MaterializerResult, error) {
	return function(ctx, now, stale, limit)
}

type pipelineTerminalDeliveryRepairFunc func(
	context.Context,
	time.Time,
	int,
) (TerminalDeliveryRepairResult, error)

func (function pipelineTerminalDeliveryRepairFunc) Step(
	ctx context.Context,
	now time.Time,
	limit int,
) (TerminalDeliveryRepairResult, error) {
	return function(ctx, now, limit)
}

type pipelineKernelFunc func(
	context.Context,
	time.Time,
	int,
	time.Duration,
	AtLeastOncePublisher,
	PostSyncHandoff,
) (KernelResult, error)

func (function pipelineKernelFunc) Step(
	ctx context.Context,
	now time.Time,
	limit int,
	lease time.Duration,
	publish AtLeastOncePublisher,
	postSync PostSyncHandoff,
) (KernelResult, error) {
	return function(ctx, now, limit, lease, publish, postSync)
}

type pipelineObserverFunc func(context.Context, time.Time, int) (Observation, error)

func (function pipelineObserverFunc) Step(
	ctx context.Context,
	now time.Time,
	limit int,
) (Observation, error) {
	return function(ctx, now, limit)
}

func TestMutationPipelineRunsCommittedStagesBeforeObservation(t *testing.T) {
	now := time.Date(2026, time.July, 23, 18, 0, 0, 0, time.FixedZone("local", -7*60*60))
	config := DefaultMutationPipelineConfig()
	var calls []string
	publish := AtLeastOncePublisher(func(context.Context, pgx.Tx, TransportClaim) (string, error) {
		return "", nil
	})
	postSync := PostSyncHandoff(func(context.Context, TransportClaim) error { return nil })

	pipeline, err := NewMutationPipeline(
		pipelineLeaseRepairFunc(func(_ context.Context, got time.Time, limit int) (LeaseRepairResult, error) {
			calls = append(calls, "repair")
			if !got.Equal(now.UTC()) || limit != 17 {
				t.Fatalf("repair now=%s limit=%d", got, limit)
			}
			return LeaseRepairResult{}, nil
		}),
		pipelineTerminalDeliveryRepairFunc(func(_ context.Context, got time.Time, limit int) (TerminalDeliveryRepairResult, error) {
			calls = append(calls, "recover-terminal-delivery")
			if !got.Equal(now.UTC()) || limit != 17 {
				t.Fatalf("terminal repair now=%s limit=%d", got, limit)
			}
			return TerminalDeliveryRepairResult{Recovered: 1}, nil
		}),
		pipelineMaterializerFunc(func(_ context.Context, got, stale time.Time, limit int) (MaterializerResult, error) {
			calls = append(calls, "materialize")
			if !got.Equal(now.UTC()) || !stale.Equal(now.UTC().Add(-config.StaleDispatchAge)) || limit != 17 {
				t.Fatalf("materializer now=%s stale=%s limit=%d", got, stale, limit)
			}
			return MaterializerResult{}, nil
		}),
		pipelineKernelFunc(func(
			_ context.Context,
			got time.Time,
			limit int,
			lease time.Duration,
			gotPublish AtLeastOncePublisher,
			gotPostSync PostSyncHandoff,
		) (KernelResult, error) {
			calls = append(calls, "kernel")
			if !got.Equal(now.UTC()) || limit != 17 || lease != config.LeaseDuration ||
				gotPublish == nil || gotPostSync == nil {
				t.Fatalf("kernel now=%s limit=%d lease=%s", got, limit, lease)
			}
			return KernelResult{}, nil
		}),
		pipelineObserverFunc(func(_ context.Context, got time.Time, limit int) (Observation, error) {
			calls = append(calls, "observe")
			if !got.Equal(now.UTC()) || limit != 17 {
				t.Fatalf("observer now=%s limit=%d", got, limit)
			}
			return Observation{CandidateDigest: "sha256:result"}, nil
		}),
		publish,
		postSync,
		nil,
		config,
	)
	if err != nil {
		t.Fatal(err)
	}

	observation, err := pipeline.Step(context.Background(), now, 17)
	if err != nil {
		t.Fatal(err)
	}
	if observation.CandidateDigest != "sha256:result" {
		t.Fatalf("observation = %#v", observation)
	}
	if want := []string{"repair", "recover-terminal-delivery", "materialize", "kernel", "observe"}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("calls = %v, want %v", calls, want)
	}
}

// TestMutationPipelineAbortsRemainingMutationStagesWhenRepairFails pins the
// CHAOS-4239 stage classification for lease repair: it is one of the two
// stages (with kernel) whose failure aborts the rest of the tick's MUTATION
// work, matching exactly what the pre-CHAOS-4239 pipeline already did (a
// repair failure always skipped sweep, terminal repair, materializer and
// kernel). What changed is what happens next: the tick no longer ends there.
// The observer still runs -- it is read-only and independent -- and because
// it succeeds here, Step returns nil: a stage failing no longer has to kill
// the whole reconciler process to be reported (it already logged and counted
// itself via runStage before Step ever returns).
func TestMutationPipelineAbortsRemainingMutationStagesWhenRepairFails(t *testing.T) {
	sentinel := errors.New("repair unavailable")
	var terminalCalled, materializerCalled, kernelCalled, observerCalled bool
	pipeline, err := NewMutationPipeline(
		pipelineLeaseRepairFunc(func(context.Context, time.Time, int) (LeaseRepairResult, error) {
			return LeaseRepairResult{}, sentinel
		}),
		pipelineTerminalDeliveryRepairFunc(func(context.Context, time.Time, int) (TerminalDeliveryRepairResult, error) {
			terminalCalled = true
			return TerminalDeliveryRepairResult{}, nil
		}),
		pipelineMaterializerFunc(func(context.Context, time.Time, time.Time, int) (MaterializerResult, error) {
			materializerCalled = true
			return MaterializerResult{}, nil
		}),
		pipelineKernelFunc(func(context.Context, time.Time, int, time.Duration, AtLeastOncePublisher, PostSyncHandoff) (KernelResult, error) {
			kernelCalled = true
			return KernelResult{}, nil
		}),
		pipelineObserverFunc(func(context.Context, time.Time, int) (Observation, error) {
			observerCalled = true
			return Observation{}, nil
		}),
		nil,
		nil,
		nil,
		DefaultMutationPipelineConfig(),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = pipeline.Step(context.Background(), time.Now(), 10)
	if err != nil {
		t.Fatalf("a repair failure must not fail the tick -- it degrades gracefully, not fatally: %v", err)
	}
	if terminalCalled || materializerCalled || kernelCalled {
		t.Fatalf("a mutation stage ran after repair aborted the tick: terminal=%v materializer=%v kernel=%v",
			terminalCalled, materializerCalled, kernelCalled)
	}
	if !observerCalled {
		t.Fatal("the observer must still run after repair aborts the tick -- it is read-only and independent")
	}
}

func TestMutationPipelineRejectsIncompleteComposition(t *testing.T) {
	validRepair := pipelineLeaseRepairFunc(func(context.Context, time.Time, int) (LeaseRepairResult, error) {
		return LeaseRepairResult{}, nil
	})
	validMaterializer := pipelineMaterializerFunc(func(context.Context, time.Time, time.Time, int) (MaterializerResult, error) {
		return MaterializerResult{}, nil
	})
	validTerminalRepair := pipelineTerminalDeliveryRepairFunc(func(context.Context, time.Time, int) (TerminalDeliveryRepairResult, error) {
		return TerminalDeliveryRepairResult{}, nil
	})
	validKernel := pipelineKernelFunc(func(context.Context, time.Time, int, time.Duration, AtLeastOncePublisher, PostSyncHandoff) (KernelResult, error) {
		return KernelResult{}, nil
	})
	validObserver := pipelineObserverFunc(func(context.Context, time.Time, int) (Observation, error) {
		return Observation{}, nil
	})
	if _, err := NewMutationPipeline(
		nil,
		validTerminalRepair,
		validMaterializer,
		validKernel,
		validObserver,
		nil,
		nil,
		nil,
		DefaultMutationPipelineConfig(),
	); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("missing repair error = %v", err)
	}
	invalid := DefaultMutationPipelineConfig()
	invalid.LeaseDuration = 0
	if _, err := NewMutationPipeline(
		validRepair,
		validTerminalRepair,
		validMaterializer,
		validKernel,
		validObserver,
		nil,
		nil,
		nil,
		invalid,
	); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("invalid config error = %v", err)
	}
	if _, err := NewMutationPipeline(
		validRepair,
		nil,
		validMaterializer,
		validKernel,
		validObserver,
		nil,
		nil,
		nil,
		DefaultMutationPipelineConfig(),
	); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("missing terminal repair error = %v", err)
	}
}

// TestMutationPipelineReportsExhaustedDeliveryRecoveries pins the count from
// the terminal-delivery repair onto the observation the metrics loop reads.
// The repair's own return value was previously discarded, which is why
// CHAOS-3951's reclaim would otherwise be invisible: the row it rescues returns
// to 'pending' and looks exactly like one that never wedged.
func TestMutationPipelineReportsExhaustedDeliveryRecoveries(t *testing.T) {
	pipeline, err := newRecoveryCountingPipeline(
		t,
		TerminalDeliveryRepairResult{Recovered: 3, ExhaustedRecovered: 2},
	)
	if err != nil {
		t.Fatal(err)
	}
	observation, err := pipeline.Step(context.Background(), time.Now().UTC(), 17)
	if err != nil {
		t.Fatal(err)
	}
	if observation.ExhaustedDeliveriesRecovered != 2 {
		t.Fatalf(
			"ExhaustedDeliveriesRecovered = %d, want the exhausted subset (2), not the total (3) or zero",
			observation.ExhaustedDeliveriesRecovered,
		)
	}
}

// A repair reporting more exhausted recoveries than recoveries, or more
// recoveries than its own window allows, is miscounting. Exporting that would
// inflate the metric operators page on, so the count itself is never
// trusted -- ExhaustedDeliveriesRecovered stays at its safe zero default for
// this tick. Since CHAOS-4239, terminal-delivery repair is a continue-safe
// stage (see the classification comment on MutationPipeline.Step): an
// untrustworthy count is a data-integrity guard, not the flat-deadline
// timeout this ticket exists to stop treating as fatal, so it no longer
// aborts materializer/kernel/observer or fails the tick.
func TestMutationPipelineRejectsImpossibleRecoveryCounts(t *testing.T) {
	for name, result := range map[string]TerminalDeliveryRepairResult{
		"exhausted exceeds recovered": {Recovered: 1, ExhaustedRecovered: 2},
		"negative exhausted":          {Recovered: 1, ExhaustedRecovered: -1},
		"recovered exceeds limit":     {Recovered: 18, ExhaustedRecovered: 1},
	} {
		t.Run(name, func(t *testing.T) {
			pipeline, err := newRecoveryCountingPipeline(t, result)
			if err != nil {
				t.Fatal(err)
			}
			observation, err := pipeline.Step(context.Background(), time.Now().UTC(), 17)
			if err != nil {
				t.Fatalf("an untrustworthy count must not fail the tick: %v", err)
			}
			if observation.ExhaustedDeliveriesRecovered != 0 {
				t.Fatalf("ExhaustedDeliveriesRecovered = %d, want 0: an untrustworthy count "+
					"must never be published", observation.ExhaustedDeliveriesRecovered)
			}
		})
	}
}

func newRecoveryCountingPipeline(
	t *testing.T,
	terminal TerminalDeliveryRepairResult,
) (*MutationPipeline, error) {
	t.Helper()
	return NewMutationPipeline(
		pipelineLeaseRepairFunc(func(context.Context, time.Time, int) (LeaseRepairResult, error) {
			return LeaseRepairResult{}, nil
		}),
		pipelineTerminalDeliveryRepairFunc(func(context.Context, time.Time, int) (TerminalDeliveryRepairResult, error) {
			return terminal, nil
		}),
		pipelineMaterializerFunc(func(context.Context, time.Time, time.Time, int) (MaterializerResult, error) {
			return MaterializerResult{}, nil
		}),
		pipelineKernelFunc(func(context.Context, time.Time, int, time.Duration, AtLeastOncePublisher, PostSyncHandoff) (KernelResult, error) {
			return KernelResult{}, nil
		}),
		pipelineObserverFunc(func(context.Context, time.Time, int) (Observation, error) {
			return Observation{CandidateDigest: "sha256:result"}, nil
		}),
		AtLeastOncePublisher(func(context.Context, pgx.Tx, TransportClaim) (string, error) { return "", nil }),
		PostSyncHandoff(func(context.Context, TransportClaim) error { return nil }),
		nil,
		DefaultMutationPipelineConfig(),
	)
}

// TestMutationPipelineReportsRecoveriesWhenALaterStageFails closes the gap an
// adversarial review found in the first attempt at this guard. Counting was
// moved ahead of the observer's error path but NOT ahead of the materializer's
// or the kernel's, both of which still returned a zero Observation -- so a
// transient database error in either one silently discarded reclaims that the
// repair had already committed, which is exactly the under-report that would
// hold a cycling delivery below its alert threshold.
func TestMutationPipelineReportsRecoveriesWhenALaterStageFails(t *testing.T) {
	sentinel := errors.New("stage unavailable")
	for name, stages := range map[string]struct{ failMaterializer, failKernel bool }{
		"materializer fails": {failMaterializer: true},
		"kernel fails":       {failKernel: true},
	} {
		t.Run(name, func(t *testing.T) {
			pipeline, err := NewMutationPipeline(
				pipelineLeaseRepairFunc(func(context.Context, time.Time, int) (LeaseRepairResult, error) {
					return LeaseRepairResult{}, nil
				}),
				pipelineTerminalDeliveryRepairFunc(func(context.Context, time.Time, int) (TerminalDeliveryRepairResult, error) {
					return TerminalDeliveryRepairResult{Recovered: 4, ExhaustedRecovered: 4}, nil
				}),
				pipelineMaterializerFunc(func(context.Context, time.Time, time.Time, int) (MaterializerResult, error) {
					if stages.failMaterializer {
						return MaterializerResult{}, sentinel
					}
					return MaterializerResult{}, nil
				}),
				pipelineKernelFunc(func(context.Context, time.Time, int, time.Duration, AtLeastOncePublisher, PostSyncHandoff) (KernelResult, error) {
					if stages.failKernel {
						return KernelResult{}, sentinel
					}
					return KernelResult{}, nil
				}),
				pipelineObserverFunc(func(context.Context, time.Time, int) (Observation, error) {
					return Observation{CandidateDigest: "sha256:result"}, nil
				}),
				AtLeastOncePublisher(func(context.Context, pgx.Tx, TransportClaim) (string, error) { return "", nil }),
				PostSyncHandoff(func(context.Context, TransportClaim) error { return nil }),
				nil,
				DefaultMutationPipelineConfig(),
			)
			if err != nil {
				t.Fatal(err)
			}
			observation, err := pipeline.Step(context.Background(), time.Now().UTC(), 17)
			// CHAOS-4239: materializer and kernel are both continue-safe (or,
			// for kernel, nothing mutation-shaped follows it anyway); the
			// observer still runs and succeeds, so the tick as a whole is not
			// an error. The failing stage already logged and counted itself
			// via runStage before Step returned.
			if err != nil {
				t.Fatalf("err = %v, want nil: a single stage failing must not fail the tick", err)
			}
			if observation.ExhaustedDeliveriesRecovered != 4 {
				t.Fatalf(
					"ExhaustedDeliveriesRecovered = %d, want 4: the repair already committed these reclaims",
					observation.ExhaustedDeliveriesRecovered,
				)
			}
		})
	}
}

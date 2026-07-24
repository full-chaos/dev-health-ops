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
	if want := []string{"repair", "materialize", "kernel", "observe"}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("calls = %v, want %v", calls, want)
	}
}

func TestMutationPipelineStopsAtFirstFailedStage(t *testing.T) {
	sentinel := errors.New("repair unavailable")
	called := false
	pipeline, err := NewMutationPipeline(
		pipelineLeaseRepairFunc(func(context.Context, time.Time, int) (LeaseRepairResult, error) {
			return LeaseRepairResult{}, sentinel
		}),
		pipelineMaterializerFunc(func(context.Context, time.Time, time.Time, int) (MaterializerResult, error) {
			called = true
			return MaterializerResult{}, nil
		}),
		pipelineKernelFunc(func(context.Context, time.Time, int, time.Duration, AtLeastOncePublisher, PostSyncHandoff) (KernelResult, error) {
			called = true
			return KernelResult{}, nil
		}),
		pipelineObserverFunc(func(context.Context, time.Time, int) (Observation, error) {
			called = true
			return Observation{}, nil
		}),
		nil,
		nil,
		DefaultMutationPipelineConfig(),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = pipeline.Step(context.Background(), time.Now(), 10)
	if !errors.Is(err, sentinel) || called {
		t.Fatalf("err=%v called=%v", err, called)
	}
}

func TestMutationPipelineRejectsIncompleteComposition(t *testing.T) {
	validRepair := pipelineLeaseRepairFunc(func(context.Context, time.Time, int) (LeaseRepairResult, error) {
		return LeaseRepairResult{}, nil
	})
	validMaterializer := pipelineMaterializerFunc(func(context.Context, time.Time, time.Time, int) (MaterializerResult, error) {
		return MaterializerResult{}, nil
	})
	validKernel := pipelineKernelFunc(func(context.Context, time.Time, int, time.Duration, AtLeastOncePublisher, PostSyncHandoff) (KernelResult, error) {
		return KernelResult{}, nil
	})
	validObserver := pipelineObserverFunc(func(context.Context, time.Time, int) (Observation, error) {
		return Observation{}, nil
	})
	if _, err := NewMutationPipeline(
		nil,
		validMaterializer,
		validKernel,
		validObserver,
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
		validMaterializer,
		validKernel,
		validObserver,
		nil,
		nil,
		invalid,
	); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("invalid config error = %v", err)
	}
}

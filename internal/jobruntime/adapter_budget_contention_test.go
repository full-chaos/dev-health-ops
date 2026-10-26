package jobruntime

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
)

type contentionObserver struct {
	recordingObserver
	events []string
	starts []time.Time
	ends   []time.Time
}

func (observer *contentionObserver) BudgetContended(_ context.Context, _ JobLabels, started time.Time) {
	observer.events = append(observer.events, "contended")
	observer.starts = append(observer.starts, started)
}

func (observer *contentionObserver) BudgetContentionEnded(_ context.Context, _ JobLabels, started time.Time) {
	observer.events = append(observer.events, "ended")
	observer.ends = append(observer.ends, started)
}

func contentionAdapter(t *testing.T, observer *contentionObserver, budget budgetFunc, handled *bool) *Adapter[RetentionCleanupArgs] {
	t.Helper()
	registry, err := newRegistry(testContractRegistry(), testMigrationState())
	if err != nil {
		t.Fatalf("newRegistry: %v", err)
	}
	spec, _ := registry.Descriptor(jobcontract.KindRetentionCleanup)
	adapter, err := NewAdapter(registry, spec, HandlerFunc[RetentionCleanupArgs](func(context.Context, *Execution[RetentionCleanupArgs]) error {
		*handled = true
		return nil
	}), Dependencies{
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Observer:    observer,
		TenantScope: tenantScopeFunc(func(ctx context.Context, _ ScopeRequest) (context.Context, error) { return ctx, nil }),
		Budget:      budget,
		Idempotency: idempotencyFunc(func(context.Context, ClaimRequest) (IdempotencyClaim, error) {
			return &recordingClaim{state: ClaimProceed}, nil
		}),
	})
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}
	return adapter
}

func assertBalancedContention(t *testing.T, observer *contentionObserver, want []string) {
	t.Helper()
	if len(observer.events) != len(want) {
		t.Fatalf("events = %v, want %v", observer.events, want)
	}
	for index := range want {
		if observer.events[index] != want[index] {
			t.Fatalf("events = %v, want %v", observer.events, want)
		}
	}
	if len(want) == 2 && !observer.starts[0].Equal(observer.ends[0]) {
		t.Fatalf("ended with %v, started with %v: the wait must carry one identity", observer.ends[0], observer.starts[0])
	}
}

func TestBudgetContentionStartsOnlyAfterALiveFullRead(t *testing.T) {
	t.Parallel()
	observer := &contentionObserver{}
	var handled bool
	adapter := contentionAdapter(t, observer, func(_ context.Context, request BudgetRequest) (BudgetLease, error) {
		if len(observer.events) != 0 {
			t.Errorf("credit before any contended read: %v", observer.events)
		}
		request.OnContended()
		request.OnContended()
		if len(observer.events) != 1 {
			t.Errorf("repeated contended reads must start one wait, got %v", observer.events)
		}
		return &recordingLease{}, nil
	}, &handled)
	if err := adapter.Work(context.Background(), retentionJob(t, 1)); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if !handled {
		t.Fatal("handler did not run")
	}
	assertBalancedContention(t, observer, []string{"contended", "ended"})
}

func TestBudgetWithoutContentionReportsNothing(t *testing.T) {
	t.Parallel()
	observer := &contentionObserver{}
	var handled bool
	adapter := contentionAdapter(t, observer, func(context.Context, BudgetRequest) (BudgetLease, error) {
		return &recordingLease{}, nil
	}, &handled)
	if err := adapter.Work(context.Background(), retentionJob(t, 1)); err != nil {
		t.Fatalf("Work: %v", err)
	}
	assertBalancedContention(t, observer, nil)
}

func TestBudgetContentionEndsOnErrorExit(t *testing.T) {
	t.Parallel()
	observer := &contentionObserver{}
	var handled bool
	adapter := contentionAdapter(t, observer, func(_ context.Context, request BudgetRequest) (BudgetLease, error) {
		request.OnContended()
		return nil, errors.New("budget unavailable")
	}, &handled)
	_ = adapter.Work(context.Background(), retentionJob(t, 1))
	if handled {
		t.Fatal("handler ran after a failed acquire")
	}
	assertBalancedContention(t, observer, []string{"contended", "ended"})
}

func TestBudgetContentionEndsOnContextEnd(t *testing.T) {
	t.Parallel()
	observer := &contentionObserver{}
	var handled bool
	ctx, cancel := context.WithCancel(context.Background())
	adapter := contentionAdapter(t, observer, func(ctx context.Context, request BudgetRequest) (BudgetLease, error) {
		request.OnContended()
		cancel()
		<-ctx.Done()
		return nil, ctx.Err()
	}, &handled)
	_ = adapter.Work(ctx, retentionJob(t, 1))
	assertBalancedContention(t, observer, []string{"contended", "ended"})
}

func TestBudgetContentionEndsOnPanicExit(t *testing.T) {
	t.Parallel()
	observer := &contentionObserver{}
	var handled bool
	adapter := contentionAdapter(t, observer, func(_ context.Context, request BudgetRequest) (BudgetLease, error) {
		request.OnContended()
		panic("budget panic")
	}, &handled)
	func() {
		defer func() { _ = recover() }()
		_ = adapter.Work(context.Background(), retentionJob(t, 1))
	}()
	assertBalancedContention(t, observer, []string{"contended", "ended"})
}

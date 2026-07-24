package system

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

func TestRetentionHandlerDeletesExactlyTheRequestedCheckpoint(t *testing.T) {
	t.Parallel()
	store := &retentionStore{}
	handler, err := NewRetentionHandler(allRetentionStores(store))
	if err != nil {
		t.Fatal(err)
	}
	execution := retentionExecution(jobcontract.RetentionCleanupPayload{
		BatchSize: 250, DeleteBefore: "2026-07-14T12:00:00Z", RetentionPolicy: jobcontract.RetentionWorkerTerminal,
	})
	if err := handler.Work(context.Background(), execution); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if store.limit != 250 || !store.before.Equal(time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("delete request = (%v, %d)", store.before, store.limit)
	}
}

func TestRetentionHandlerClassifiesStoreFailureForTheBoundedRetryPolicy(t *testing.T) {
	t.Parallel()
	store := &retentionStore{err: errors.New("database unavailable")}
	handler, err := NewRetentionHandler(allRetentionStores(store))
	if err != nil {
		t.Fatal(err)
	}
	err = handler.Work(context.Background(), retentionExecution(jobcontract.RetentionCleanupPayload{
		BatchSize: 1, DeleteBefore: "2026-07-14T12:00:00Z", RetentionPolicy: jobcontract.RetentionWorkerTerminal,
	}))
	if err == nil {
		t.Fatal("expected retryable error")
	}
	// The public adapter classifies Retryable as a bounded retry; keep this
	// assertion at the stable error boundary instead of duplicating its logic.
	if got := err.Error(); got != "job error category: retryable" {
		t.Fatalf("error = %q", got)
	}
}

func TestRetentionHandlerRejectsUnsupportedPolicyWithoutCallingStorage(t *testing.T) {
	t.Parallel()
	store := &retentionStore{}
	handler, err := NewRetentionHandler(allRetentionStores(store))
	if err != nil {
		t.Fatal(err)
	}
	err = handler.Work(context.Background(), retentionExecution(jobcontract.RetentionCleanupPayload{
		BatchSize: 1, DeleteBefore: "2026-07-14T12:00:00Z", RetentionPolicy: "all_rows",
	}))
	if err == nil || store.called {
		t.Fatalf("err = %v, store called = %v", err, store.called)
	}
}

// allRetentionStores binds every contract policy to one recorder so the
// routing tests observe which policy reached storage without also asserting
// per-table SQL, which the store tests own.
func allRetentionStores(store RetentionStore) map[string]RetentionStore {
	stores := make(map[string]RetentionStore)
	for _, policy := range jobcontract.RetentionPolicies() {
		stores[policy] = store
	}
	return stores
}

type retentionStore struct {
	before time.Time
	limit  int
	called bool
	err    error
}

func (store *retentionStore) DeleteBefore(_ context.Context, before time.Time, limit int) (int64, error) {
	store.before, store.limit, store.called = before, limit, true
	return 0, store.err
}

func retentionExecution(payload jobcontract.RetentionCleanupPayload) *jobruntime.Execution[jobruntime.RetentionCleanupArgs] {
	return &jobruntime.Execution[jobruntime.RetentionCleanupArgs]{
		Args: jobruntime.RetentionCleanupArgs{EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.RetentionCleanupPayload]{Payload: payload}},
	}
}

func TestRetentionHandlerRoutesEachPolicyToItsOwnStore(t *testing.T) {
	t.Parallel()
	stores := make(map[string]RetentionStore, len(jobcontract.RetentionPolicies()))
	recorders := make(map[string]*retentionStore, len(jobcontract.RetentionPolicies()))
	for _, policy := range jobcontract.RetentionPolicies() {
		recorder := &retentionStore{}
		recorders[policy] = recorder
		stores[policy] = recorder
	}
	handler, err := NewRetentionHandler(stores)
	if err != nil {
		t.Fatal(err)
	}
	for _, policy := range jobcontract.RetentionPolicies() {
		if err := handler.Work(context.Background(), retentionExecution(
			jobcontract.RetentionCleanupPayload{
				BatchSize: 500, DeleteBefore: "2026-07-14T12:00:00Z", RetentionPolicy: policy,
			},
		)); err != nil {
			t.Fatalf("%s: %v", policy, err)
		}
		for other, recorder := range recorders {
			if (other == policy) != recorder.called {
				t.Fatalf("policy %s reached the %s store", policy, other)
			}
		}
		recorders[policy].called = false
	}
}

// TestRetentionHandlerRefusesPartialPolicyCoverage keeps a half-wired ops
// worker from looking ready while one retention family silently never runs.
func TestRetentionHandlerRefusesPartialPolicyCoverage(t *testing.T) {
	t.Parallel()
	if _, err := NewRetentionHandler(map[string]RetentionStore{
		jobcontract.RetentionWorkerTerminal: &retentionStore{},
	}); err == nil {
		t.Fatal("partial retention policy coverage unexpectedly accepted")
	}
	if _, err := NewRetentionHandler(map[string]RetentionStore{
		"all_rows": &retentionStore{},
	}); err == nil {
		t.Fatal("unknown retention policy unexpectedly accepted")
	}
}

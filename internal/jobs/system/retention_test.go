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

// TestRetentionHandlerRefusesAFutureCutoff guards the direction of the
// producer's arithmetic. A cutoff newer than now deletes rows newer than now,
// which no retention schedule can intend; it is what "due + horizon" instead of
// "due - horizon" looks like, and its blast radius is every terminal row.
func TestRetentionHandlerRefusesAFutureCutoff(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 24, 5, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name    string
		cutoff  time.Time
		wantErr bool
	}{
		{name: "well inside the retention window", cutoff: now.Add(-14 * 24 * time.Hour)},
		{name: "cutoff at the occurrence due time", cutoff: now},
		{name: "within tolerated scheduler clock skew", cutoff: now.Add(30 * time.Second)},
		{name: "beyond tolerated skew", cutoff: now.Add(2 * time.Minute), wantErr: true},
		{name: "sign error on a 90 day horizon", cutoff: now.Add(90 * 24 * time.Hour), wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := &retentionStore{}
			handler, err := NewRetentionHandler(allRetentionStores(store))
			if err != nil {
				t.Fatal(err)
			}
			handler.now = func() time.Time { return now }
			err = handler.Work(context.Background(), retentionExecution(
				jobcontract.RetentionCleanupPayload{
					BatchSize:       500,
					DeleteBefore:    test.cutoff.Format(time.RFC3339),
					RetentionPolicy: jobcontract.RetentionExternalIngestBatches,
				},
			))
			if test.wantErr {
				if err == nil {
					t.Fatal("future cutoff was accepted")
				}
				if store.called {
					t.Fatal("future cutoff reached storage")
				}
				return
			}
			if err != nil {
				t.Fatalf("valid cutoff rejected: %v", err)
			}
			if !store.called {
				t.Fatal("valid cutoff never reached storage")
			}
		})
	}
}

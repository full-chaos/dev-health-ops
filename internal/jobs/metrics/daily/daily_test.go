package daily

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/jackc/pgx/v5"
)

const (
	testRunID       = "00000000-0000-4000-8000-000000000001"
	testPartitionID = "00000000-0000-4000-8000-000000000002"
	testOrgID       = "00000000-0000-4000-8000-000000000009"
)

func TestPartitionLoadFailureReleasesClaimAndRetries(t *testing.T) {
	store := &fakeStore{
		partitionClaim: &PartitionClaim{Partition: Partition{ID: testPartitionID, RunID: testRunID}, Token: "00000000-0000-4000-8000-000000000003"},
		loadErr:        ErrUnavailable,
	}
	handler, err := NewPartitionHandler(store, fakePublisher{}, fakeCompatibility{})
	if err != nil {
		t.Fatal(err)
	}
	err = handler.Work(context.Background(), partitionExecution())
	if err == nil || !strings.Contains(err.Error(), string(jobruntime.CategoryRetryable)) || store.partitionReleases != 1 {
		t.Fatalf("load failure = %v, releases=%d", err, store.partitionReleases)
	}
}

func TestPartitionScopeMismatchReleasesClaimAndIsPermanent(t *testing.T) {
	store := &fakeStore{
		partitionClaim: &PartitionClaim{Partition: Partition{ID: testPartitionID, RunID: testRunID}, Token: "00000000-0000-4000-8000-000000000003"},
		run:            Run{ID: testRunID, OrganizationID: "00000000-0000-4000-8000-000000000008", Generation: "v1"},
	}
	handler, err := NewPartitionHandler(store, fakePublisher{}, fakeCompatibility{})
	if err != nil {
		t.Fatal(err)
	}
	err = handler.Work(context.Background(), partitionExecution())
	if err == nil || !strings.Contains(err.Error(), string(jobruntime.CategoryPermanent)) || store.partitionReleases != 1 {
		t.Fatalf("scope mismatch = %v, releases=%d", err, store.partitionReleases)
	}
}

func TestPartitionRenewsLeaseUntilCompatibilityCompletes(t *testing.T) {
	store := &fakeStore{
		partitionClaim: &PartitionClaim{
			Partition:     Partition{ID: testPartitionID, RunID: testRunID},
			Token:         "00000000-0000-4000-8000-000000000003",
			LeaseDuration: 30 * time.Millisecond,
		},
		run: Run{ID: testRunID, OrganizationID: testOrgID, Generation: "daily-v1", Status: "running"},
	}
	compatibility := &blockingCompatibility{partitionDelay: 80 * time.Millisecond}
	handler, err := NewPartitionHandler(store, fakePublisher{}, compatibility)
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.Work(context.Background(), partitionExecution()); err != nil {
		t.Fatal(err)
	}
	if store.partitionRenewals < 2 || store.partitionCompletions != 1 {
		t.Fatalf("renewals=%d completions=%d", store.partitionRenewals, store.partitionCompletions)
	}
}

func TestPartitionLeaseLossCancelsCompatibilityAndCannotComplete(t *testing.T) {
	store := &fakeStore{
		partitionClaim: &PartitionClaim{
			Partition:     Partition{ID: testPartitionID, RunID: testRunID},
			Token:         "00000000-0000-4000-8000-000000000003",
			LeaseDuration: 30 * time.Millisecond,
		},
		run:                       Run{ID: testRunID, OrganizationID: testOrgID, Generation: "daily-v1", Status: "running"},
		partitionRenewalFailureAt: 1,
	}
	compatibility := &blockingCompatibility{waitForCancellation: true}
	handler, err := NewPartitionHandler(store, fakePublisher{}, compatibility)
	if err != nil {
		t.Fatal(err)
	}
	err = handler.Work(context.Background(), partitionExecution())
	if err == nil || !strings.Contains(err.Error(), string(jobruntime.CategoryRetryable)) ||
		!compatibility.partitionCanceled || store.partitionCompletions != 0 {
		t.Fatalf(
			"lease loss = %v canceled=%t completions=%d",
			err,
			compatibility.partitionCanceled,
			store.partitionCompletions,
		)
	}
}

// A completion that fails after the work succeeded used to be the ONE post-claim
// exit that returned retryable without standing the row back down, which is the
// most likely way the lease behind CHAOS-3991 was orphaned. Both layers must
// release, or the row keeps a lease nobody is using for the rest of its term.
func TestPartitionCompletionFailureReleasesTheClaim(t *testing.T) {
	store := &fakeStore{
		partitionClaim: &PartitionClaim{
			Partition:     Partition{ID: testPartitionID, RunID: testRunID},
			Token:         "00000000-0000-4000-8000-000000000003",
			LeaseDuration: 30 * time.Millisecond,
		},
		run:           Run{ID: testRunID, OrganizationID: testOrgID, Generation: "daily-v1", Status: "running"},
		completionErr: ErrUnavailable,
	}
	handler, err := NewPartitionHandler(store, fakePublisher{}, fakeCompatibility{})
	if err != nil {
		t.Fatal(err)
	}
	err = handler.Work(context.Background(), partitionExecution())
	if err == nil || !strings.Contains(err.Error(), string(jobruntime.CategoryRetryable)) {
		t.Fatalf("completion failure = %v, want retryable", err)
	}
	if store.partitionCompletions != 1 || store.partitionReleases != 1 {
		t.Fatalf("completions=%d releases=%d, want 1/1", store.partitionCompletions, store.partitionReleases)
	}
}

func TestFinalizeCompletionFailureReleasesTheClaim(t *testing.T) {
	store := &fakeStore{
		finalizeClaim: &FinalizeClaim{
			Run:           Run{ID: testRunID, OrganizationID: testOrgID, Generation: "daily-v1", Status: "running"},
			Token:         "00000000-0000-4000-8000-000000000004",
			LeaseDuration: 30 * time.Millisecond,
		},
		completionErr: ErrUnavailable,
	}
	handler, err := NewFinalizeHandler(store, fakeCompatibility{})
	if err != nil {
		t.Fatal(err)
	}
	err = handler.Work(context.Background(), finalizeExecution())
	if err == nil || !strings.Contains(err.Error(), string(jobruntime.CategoryRetryable)) {
		t.Fatalf("completion failure = %v, want retryable", err)
	}
	if store.finalizeCompletions != 1 || store.finalizeReleases != 1 {
		t.Fatalf("completions=%d releases=%d, want 1/1", store.finalizeCompletions, store.finalizeReleases)
	}
}

func TestFinalizerRenewsLeaseUntilCompatibilityCompletes(t *testing.T) {
	store := &fakeStore{
		finalizeClaim: &FinalizeClaim{
			Run:           Run{ID: testRunID, OrganizationID: testOrgID, Generation: "daily-v1", Status: "running"},
			Token:         "00000000-0000-4000-8000-000000000004",
			LeaseDuration: 30 * time.Millisecond,
		},
	}
	compatibility := &blockingCompatibility{finalizeDelay: 80 * time.Millisecond}
	handler, err := NewFinalizeHandler(store, compatibility)
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.Work(context.Background(), finalizeExecution()); err != nil {
		t.Fatal(err)
	}
	if store.finalizeRenewals < 2 || store.finalizeCompletions != 1 {
		t.Fatalf("renewals=%d completions=%d", store.finalizeRenewals, store.finalizeCompletions)
	}
}

func TestFinalizerLeaseLossCancelsCompatibilityAndCannotComplete(t *testing.T) {
	store := &fakeStore{
		finalizeClaim: &FinalizeClaim{
			Run:           Run{ID: testRunID, OrganizationID: testOrgID, Generation: "daily-v1", Status: "running"},
			Token:         "00000000-0000-4000-8000-000000000004",
			LeaseDuration: 30 * time.Millisecond,
		},
		finalizeRenewalFailureAt: 1,
	}
	compatibility := &blockingCompatibility{waitForCancellation: true}
	handler, err := NewFinalizeHandler(store, compatibility)
	if err != nil {
		t.Fatal(err)
	}
	err = handler.Work(context.Background(), finalizeExecution())
	if err == nil || !strings.Contains(err.Error(), string(jobruntime.CategoryRetryable)) ||
		!compatibility.finalizeCanceled || store.finalizeCompletions != 0 {
		t.Fatalf(
			"lease loss = %v canceled=%t completions=%d",
			err,
			compatibility.finalizeCanceled,
			store.finalizeCompletions,
		)
	}
}

// TestDailyContractsPreserveMetricsQueueAsRiverDefault guards the metrics
// placement of the daily family regardless of migration route: every kind must
// stay on the "metrics" queue so budget and startup wiring never silently split
// the family across queues. All three
// kinds are checked in at go_default/river now, so the route/executable
// assertions confirm the flip actually reached this family rather than
// asserting continued dormancy.
func TestDailyContractsPreserveMetricsQueueAsRiverDefault(t *testing.T) {
	registry, err := jobruntime.Load("../../../../contracts/jobs/v1")
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{jobcontract.KindDailyMetricsDispatch, jobcontract.KindDailyMetricsPartition, jobcontract.KindDailyMetricsFinalize} {
		descriptor, ok := registry.Descriptor(kind)
		if !ok || descriptor.Queue != "metrics" || descriptor.Route != "river" || !descriptor.Executable() {
			t.Fatalf("daily topology for %s = %#v", kind, descriptor)
		}
	}
}

func TestScheduledFanoutRepositoryPartitioningIsNotBoundedByPostSyncInputLimit(t *testing.T) {
	identifiers := make([]RepositoryID, 0, 1001)
	for value := 1; value <= 1001; value++ {
		identifiers = append(identifiers, RepositoryID(fmt.Sprintf("00000000-0000-4000-8000-%012d", value)))
	}
	partitions, err := normalizeRepositoryPartitions(identifiers)
	if err != nil {
		t.Fatal(err)
	}
	// dailyRepositoryPartitionSize is configurable (DEV_HEALTH_DAILY_PARTITION_
	// MAX_REPOS, CHAOS-4263/CHAOS-4264), so the expected shape is derived from
	// it rather than a hardcoded partition count.
	size := dailyRepositoryPartitionSize
	wantCount := (len(identifiers) + size - 1) / size
	wantLast := len(identifiers) % size
	if wantLast == 0 {
		wantLast = size
	}
	if len(partitions) != wantCount || len(partitions[0]) != size || len(partitions[len(partitions)-1]) != wantLast {
		t.Fatalf("partition lengths = %d/%d/%d, want count=%d first=%d last=%d",
			len(partitions), len(partitions[0]), len(partitions[len(partitions)-1]), wantCount, size, wantLast)
	}
}

func TestDispatcherMaterializesScheduledFanoutBeforeListingPartitions(t *testing.T) {
	store := &fakeStore{run: Run{
		ID: testRunID, OrganizationID: testOrgID, Generation: "fixed-schedule:daily_metrics_fanout:2026-08-12T01:00:00Z",
		Status: "running", RepositoryDiscoveryRequired: true,
	}}
	discoverer := &fakeRepositoryDiscoverer{
		identifiers: []RepositoryID{"00000000-0000-4000-8000-000000000010"},
	}
	handler, err := NewDispatcher(store, fakePublisher{}, discoverer)
	if err != nil {
		t.Fatal(err)
	}
	execution := &jobruntime.Execution[jobruntime.DailyMetricsDispatchArgs]{
		OrganizationID: pointer(testOrgID),
		Envelope:       jobcontract.Envelope{OrganizationID: pointer(testOrgID), Domain: jobcontract.DomainLink{Type: "daily_metrics_run", ID: testRunID}},
		Args: jobruntime.DailyMetricsDispatchArgs{EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.DailyMetricsDispatchPayload]{
			OrganizationID: pointer(testOrgID), Domain: jobcontract.DomainLink{Type: "daily_metrics_run", ID: testRunID}, Payload: jobcontract.DailyMetricsDispatchPayload{RunID: testRunID},
		}},
	}
	if err := handler.Work(context.Background(), execution); err != nil {
		t.Fatal(err)
	}
	if discoverer.calls != 1 || store.materialized != 1 || store.dispatchListCalls != 1 ||
		store.materializedAfterDispatchList {
		t.Fatalf(
			"discoveries=%d materializations=%d dispatch_lists=%d materialized_after_dispatch_list=%t",
			discoverer.calls, store.materialized, store.dispatchListCalls, store.materializedAfterDispatchList,
		)
	}
}

func TestDispatcherDoesNotDiscoverRepositoriesForExistingPartitions(t *testing.T) {
	store := &fakeStore{run: Run{
		ID: testRunID, OrganizationID: testOrgID, Generation: "fixed-schedule:daily_metrics_fanout:2026-08-12T01:00:00Z",
		Status: "running",
	}}
	discoverer := &fakeRepositoryDiscoverer{}
	handler, err := NewDispatcher(store, fakePublisher{}, discoverer)
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.Work(context.Background(), dailyDispatchExecution()); err != nil {
		t.Fatal(err)
	}
	if discoverer.calls != 0 || store.materialized != 0 {
		t.Fatalf("replay rediscovered=%d materialized=%d", discoverer.calls, store.materialized)
	}
}

func TestDispatcherRetriesRepositoryDiscoveryFailure(t *testing.T) {
	store := &fakeStore{run: Run{
		ID: testRunID, OrganizationID: testOrgID, Generation: "fixed-schedule:daily_metrics_fanout:2026-08-12T01:00:00Z",
		Status: "running", RepositoryDiscoveryRequired: true,
	}}
	handler, err := NewDispatcher(store, fakePublisher{}, &fakeRepositoryDiscoverer{err: ErrUnavailable})
	if err != nil {
		t.Fatal(err)
	}
	err = handler.Work(context.Background(), dailyDispatchExecution())
	if err == nil || !strings.Contains(err.Error(), string(jobruntime.CategoryRetryable)) || store.materialized != 0 {
		t.Fatalf("discovery failure=%v materialized=%d", err, store.materialized)
	}
}

func dailyDispatchExecution() *jobruntime.Execution[jobruntime.DailyMetricsDispatchArgs] {
	return &jobruntime.Execution[jobruntime.DailyMetricsDispatchArgs]{
		OrganizationID: pointer(testOrgID),
		Envelope:       jobcontract.Envelope{OrganizationID: pointer(testOrgID), Domain: jobcontract.DomainLink{Type: "daily_metrics_run", ID: testRunID}},
		Args: jobruntime.DailyMetricsDispatchArgs{EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.DailyMetricsDispatchPayload]{
			OrganizationID: pointer(testOrgID), Domain: jobcontract.DomainLink{Type: "daily_metrics_run", ID: testRunID}, Payload: jobcontract.DailyMetricsDispatchPayload{RunID: testRunID},
		}},
	}
}

func partitionExecution() *jobruntime.Execution[jobruntime.DailyMetricsPartitionArgs] {
	return &jobruntime.Execution[jobruntime.DailyMetricsPartitionArgs]{
		OrganizationID: pointer(testOrgID),
		Envelope: jobcontract.Envelope{
			OrganizationID: pointer(testOrgID), Domain: jobcontract.DomainLink{Type: "daily_metrics_partition", ID: testPartitionID},
		},
		Args: jobruntime.DailyMetricsPartitionArgs{EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.DailyMetricsPartitionPayload]{
			OrganizationID: pointer(testOrgID), Domain: jobcontract.DomainLink{Type: "daily_metrics_partition", ID: testPartitionID}, Payload: jobcontract.DailyMetricsPartitionPayload{PartitionID: testPartitionID},
		}},
	}
}

func finalizeExecution() *jobruntime.Execution[jobruntime.DailyMetricsFinalizeArgs] {
	return &jobruntime.Execution[jobruntime.DailyMetricsFinalizeArgs]{
		OrganizationID: pointer(testOrgID),
		Envelope: jobcontract.Envelope{
			OrganizationID: pointer(testOrgID), Domain: jobcontract.DomainLink{Type: "daily_metrics_run", ID: testRunID},
		},
		Args: jobruntime.DailyMetricsFinalizeArgs{EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.DailyMetricsFinalizePayload]{
			OrganizationID: pointer(testOrgID), Domain: jobcontract.DomainLink{Type: "daily_metrics_run", ID: testRunID}, Payload: jobcontract.DailyMetricsFinalizePayload{RunID: testRunID},
		}},
	}
}

func pointer(value string) *string { return &value }

type fakeStore struct {
	run                           Run
	loadErr                       error
	partitionClaim                *PartitionClaim
	partitionReleases             int
	partitionRenewals             int
	partitionRenewalFailureAt     int
	partitionCompletions          int
	finalizeClaim                 *FinalizeClaim
	finalizeRenewals              int
	finalizeRenewalFailureAt      int
	finalizeCompletions           int
	finalizeReleases              int
	completionErr                 error
	materialized                  int
	dispatchListCalls             int
	materializedAfterDispatchList bool
}

func (store *fakeStore) LoadRun(context.Context, string) (Run, error) {
	return store.run, store.loadErr
}
func (store *fakeStore) ClaimDispatch(context.Context, string) (*Run, error) {
	if store.loadErr != nil {
		return nil, store.loadErr
	}
	return &store.run, nil
}
func (store *fakeStore) DispatchablePartitions(context.Context, string) ([]Partition, error) {
	store.dispatchListCalls++
	return nil, nil
}
func (store *fakeStore) MaterializeScheduledFanout(_ context.Context, _ Run, _ []RepositoryID) (bool, error) {
	if store.dispatchListCalls > 0 {
		store.materializedAfterDispatchList = true
	}
	store.materialized++
	return true, nil
}
func (store *fakeStore) ClaimPartition(context.Context, string) (*PartitionClaim, error) {
	return store.partitionClaim, nil
}
func (store *fakeStore) RenewPartition(context.Context, PartitionClaim) error {
	store.partitionRenewals++
	if store.partitionRenewalFailureAt == store.partitionRenewals {
		return ErrLeaseLost
	}
	return nil
}
func (store *fakeStore) CompletePartition(
	ctx context.Context,
	claim PartitionClaim,
	publisher Publisher,
) error {
	store.partitionCompletions++
	if store.completionErr != nil {
		return store.completionErr
	}
	return publisher.PublishFinalizeTx(ctx, nil, store.run)
}
func (store *fakeStore) ReleasePartition(context.Context, PartitionClaim) error {
	store.partitionReleases++
	return nil
}
func (store *fakeStore) ClaimFinalize(context.Context, string) (*FinalizeClaim, error) {
	if store.finalizeClaim == nil {
		return nil, errors.New("unused")
	}
	return store.finalizeClaim, nil
}
func (store *fakeStore) RenewFinalize(context.Context, FinalizeClaim) error {
	store.finalizeRenewals++
	if store.finalizeRenewalFailureAt == store.finalizeRenewals {
		return ErrLeaseLost
	}
	return nil
}
func (store *fakeStore) CompleteFinalize(context.Context, FinalizeClaim) error {
	store.finalizeCompletions++
	return store.completionErr
}
func (store *fakeStore) ReleaseFinalize(context.Context, FinalizeClaim) error {
	store.finalizeReleases++
	return nil
}

type fakePublisher struct{}

func (fakePublisher) PublishPartition(context.Context, Run, Partition) error { return nil }
func (fakePublisher) PublishFinalizeTx(context.Context, pgx.Tx, Run) error   { return nil }

type fakeCompatibility struct{}

func (fakeCompatibility) ComputePartition(context.Context, Run, Partition) error { return nil }
func (fakeCompatibility) Finalize(context.Context, Run) error                    { return nil }

type fakeRepositoryDiscoverer struct {
	identifiers []RepositoryID
	err         error
	calls       int
}

func (discoverer *fakeRepositoryDiscoverer) RepositoryIDs(context.Context, string) ([]RepositoryID, error) {
	discoverer.calls++
	return discoverer.identifiers, discoverer.err
}

type blockingCompatibility struct {
	mu                  sync.Mutex
	partitionDelay      time.Duration
	finalizeDelay       time.Duration
	waitForCancellation bool
	partitionCanceled   bool
	finalizeCanceled    bool
}

func (compatibility *blockingCompatibility) ComputePartition(ctx context.Context, _ Run, _ Partition) error {
	if compatibility.waitForCancellation {
		<-ctx.Done()
		compatibility.mu.Lock()
		compatibility.partitionCanceled = true
		compatibility.mu.Unlock()
		return ctx.Err()
	}
	timer := time.NewTimer(compatibility.partitionDelay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (compatibility *blockingCompatibility) Finalize(ctx context.Context, _ Run) error {
	if compatibility.waitForCancellation {
		<-ctx.Done()
		compatibility.mu.Lock()
		compatibility.finalizeCanceled = true
		compatibility.mu.Unlock()
		return ctx.Err()
	}
	timer := time.NewTimer(compatibility.finalizeDelay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

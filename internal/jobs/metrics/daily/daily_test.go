package daily

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
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
	handler, err := NewPartitionHandler(store, fakeCompatibility{})
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
	handler, err := NewPartitionHandler(store, fakeCompatibility{})
	if err != nil {
		t.Fatal(err)
	}
	err = handler.Work(context.Background(), partitionExecution())
	if err == nil || !strings.Contains(err.Error(), string(jobruntime.CategoryPermanent)) || store.partitionReleases != 1 {
		t.Fatalf("scope mismatch = %v, releases=%d", err, store.partitionReleases)
	}
}

func TestDailyContractsPreserveHeavyMetricsTopologyWhileDormant(t *testing.T) {
	registry, err := jobruntime.Load("../../../../contracts/jobs/v1")
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{jobcontract.KindDailyMetricsDispatch, jobcontract.KindDailyMetricsPartition, jobcontract.KindDailyMetricsFinalize} {
		descriptor, ok := registry.Descriptor(kind)
		if !ok || descriptor.Profile != "heavy" || descriptor.Queue != "metrics" || descriptor.Route != "celery" || descriptor.Executable() {
			t.Fatalf("daily topology for %s = %#v", kind, descriptor)
		}
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

func pointer(value string) *string { return &value }

type fakeStore struct {
	run               Run
	loadErr           error
	partitionClaim    *PartitionClaim
	partitionReleases int
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
func (*fakeStore) DispatchablePartitions(context.Context, string) ([]Partition, error) {
	return nil, nil
}
func (store *fakeStore) ClaimPartition(context.Context, string) (*PartitionClaim, error) {
	return store.partitionClaim, nil
}
func (*fakeStore) CompletePartition(context.Context, PartitionClaim) error { return nil }
func (store *fakeStore) ReleasePartition(context.Context, PartitionClaim) error {
	store.partitionReleases++
	return nil
}
func (*fakeStore) ClaimFinalize(context.Context, string) (*FinalizeClaim, error) {
	return nil, errors.New("unused")
}
func (*fakeStore) CompleteFinalize(context.Context, FinalizeClaim) error { return nil }
func (*fakeStore) ReleaseFinalize(context.Context, FinalizeClaim) error  { return nil }

type fakeCompatibility struct{}

func (fakeCompatibility) ComputePartition(context.Context, Run, Partition) error { return nil }
func (fakeCompatibility) Finalize(context.Context, Run) error                    { return nil }

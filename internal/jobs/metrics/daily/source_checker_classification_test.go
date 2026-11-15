package daily

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

func TestPartitionSourceCheckerFailureReleasesClaimAndRetries(t *testing.T) {
	store := &fakeStore{
		partitionClaim: &PartitionClaim{
			Partition:     Partition{ID: testPartitionID, RunID: testRunID},
			Token:         "00000000-0000-4000-8000-000000000003",
			LeaseDuration: 30 * time.Millisecond,
		},
		run: Run{ID: testRunID, OrganizationID: testOrgID, Status: "running"},
	}
	handler, err := NewPartitionHandler(store, fakePublisher{})
	if err != nil {
		t.Fatal(err)
	}
	handler.SetSourceDataChecker(errorSourceDataChecker{err: ErrUnavailable})
	err = handler.Work(context.Background(), partitionExecution())
	if err == nil || !strings.Contains(err.Error(), string(jobruntime.CategoryRetryable)) {
		t.Fatalf("source checker failure = %v, want retryable", err)
	}
	if store.partitionCompletions != 0 || store.partitionReleases != 1 {
		t.Fatalf("completions=%d releases=%d, want 0/1", store.partitionCompletions, store.partitionReleases)
	}
}

func TestPartitionSourceCheckerInvalidStateFailsTheClaimPermanentlyAndIsPermanent(t *testing.T) {
	store := &fakeStore{
		partitionClaim: &PartitionClaim{
			Partition:     Partition{ID: testPartitionID, RunID: testRunID},
			Token:         "00000000-0000-4000-8000-000000000003",
			LeaseDuration: 30 * time.Millisecond,
		},
		run: Run{ID: testRunID, OrganizationID: testOrgID, Status: "running"},
	}
	handler, err := NewPartitionHandler(store, fakePublisher{})
	if err != nil {
		t.Fatal(err)
	}
	handler.SetSourceDataChecker(errorSourceDataChecker{err: ErrInvalidState})
	err = handler.Work(context.Background(), partitionExecution())
	if err == nil || !strings.Contains(err.Error(), string(jobruntime.CategoryPermanent)) {
		t.Fatalf("source checker invalid state = %v, want permanent", err)
	}
	if store.partitionCompletions != 0 || store.partitionReleases != 0 ||
		store.permanentFailures != 1 || store.permanentFailureReason != "invalid_state" {
		t.Fatalf("completions=%d releases=%d permanent=%d reason=%q, want 0/0/1/invalid_state",
			store.partitionCompletions, store.partitionReleases, store.permanentFailures, store.permanentFailureReason)
	}
}

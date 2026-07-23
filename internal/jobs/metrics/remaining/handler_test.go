package remaining

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

const (
	handlerRunID       = "00000000-0000-4000-8000-000000000101"
	handlerPartitionID = "00000000-0000-4000-8000-000000000102"
	handlerOrgID       = "00000000-0000-4000-8000-000000000103"
)

func TestPartitionHandlerRejectsCrossFamilyExecution(t *testing.T) {
	store := &handlerStore{
		run: Run{
			ID: handlerRunID, OrganizationID: handlerOrgID,
			Family: "dora", Status: "running",
		},
		claim: handlerClaim(),
	}
	handler, err := NewPartitionHandler[jobruntime.RemainingCapacityArgs](
		store, &handlerCompatibility{}, "capacity",
	)
	if err != nil {
		t.Fatal(err)
	}
	err = handler.Work(t.Context(), capacityExecution())
	if err == nil || !strings.Contains(err.Error(), string(jobruntime.CategoryPermanent)) ||
		store.releases != 1 || store.completions != 0 {
		t.Fatalf("cross-family error=%v releases=%d completions=%d", err, store.releases, store.completions)
	}
}

func TestPartitionHandlerRenewsAndCompletesWithBoundedEvidence(t *testing.T) {
	store := &handlerStore{
		run: Run{
			ID: handlerRunID, OrganizationID: handlerOrgID,
			Family: "capacity", Status: "running",
		},
		claim: handlerClaim(),
	}
	compatibility := &handlerCompatibility{delay: 80 * time.Millisecond}
	handler, err := NewPartitionHandler[jobruntime.RemainingCapacityArgs](
		store, compatibility, "capacity",
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.Work(t.Context(), capacityExecution()); err != nil {
		t.Fatal(err)
	}
	if store.renewals < 2 || store.completions != 1 ||
		store.evidence != "compatibility_execution:"+handlerPartitionID {
		t.Fatalf(
			"renewals=%d completions=%d evidence=%q",
			store.renewals, store.completions, store.evidence,
		)
	}
}

func TestPartitionHandlerLeaseLossCancelsCompatibility(t *testing.T) {
	store := &handlerStore{
		run: Run{
			ID: handlerRunID, OrganizationID: handlerOrgID,
			Family: "capacity", Status: "running",
		},
		claim:       handlerClaim(),
		failRenewal: true,
	}
	compatibility := &handlerCompatibility{waitForCancellation: true}
	handler, err := NewPartitionHandler[jobruntime.RemainingCapacityArgs](
		store, compatibility, "capacity",
	)
	if err != nil {
		t.Fatal(err)
	}
	err = handler.Work(t.Context(), capacityExecution())
	if err == nil || !strings.Contains(err.Error(), string(jobruntime.CategoryRetryable)) ||
		!compatibility.canceled || store.completions != 0 || store.releases != 1 {
		t.Fatalf(
			"lease loss=%v canceled=%t completions=%d releases=%d",
			err, compatibility.canceled, store.completions, store.releases,
		)
	}
}

func capacityExecution() *jobruntime.Execution[jobruntime.RemainingCapacityArgs] {
	organizationID := handlerOrgID
	domain := jobcontract.DomainLink{
		Type: "remaining_metric_partition",
		ID:   handlerPartitionID,
	}
	args := jobruntime.RemainingCapacityArgs{
		EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.RemainingMetricsPartitionPayload]{
			ContractVersion: jobcontract.ContractVersionV1,
			OrganizationID:  &organizationID,
			CorrelationID:   "remaining:" + handlerRunID,
			IdempotencyKey:  "remaining:partition:" + handlerPartitionID,
			Domain:          domain,
			Payload: jobcontract.RemainingMetricsPartitionPayload{
				PartitionID: handlerPartitionID,
			},
		},
	}
	return &jobruntime.Execution[jobruntime.RemainingCapacityArgs]{
		Args: args, Envelope: args.ContractEnvelope(), OrganizationID: &organizationID,
	}
}

func handlerClaim() *Claim {
	return &Claim{
		Partition:     Partition{ID: handlerPartitionID, RunID: handlerRunID},
		Token:         "00000000-0000-4000-8000-000000000104",
		LeaseDuration: 30 * time.Millisecond,
	}
}

type handlerStore struct {
	run         Run
	claim       *Claim
	renewals    int
	failRenewal bool
	releases    int
	completions int
	evidence    string
}

func (store *handlerStore) LoadRun(context.Context, string) (Run, error) {
	return store.run, nil
}
func (store *handlerStore) ClaimPartition(context.Context, string) (*Claim, error) {
	return store.claim, nil
}
func (store *handlerStore) RenewPartition(context.Context, Claim) error {
	store.renewals++
	if store.failRenewal {
		return ErrLeaseLost
	}
	return nil
}
func (store *handlerStore) CompletePartition(_ context.Context, _ Claim, evidence string) error {
	store.completions++
	store.evidence = evidence
	return nil
}
func (store *handlerStore) ReleasePartition(context.Context, Claim) error {
	store.releases++
	return nil
}

type handlerCompatibility struct {
	delay               time.Duration
	waitForCancellation bool
	canceled            bool
}

func (executor *handlerCompatibility) ComputePartition(
	ctx context.Context,
	_ Run,
	_ Partition,
) error {
	if executor.waitForCancellation {
		<-ctx.Done()
		executor.canceled = true
		return ctx.Err()
	}
	timer := time.NewTimer(executor.delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		executor.canceled = true
		return ctx.Err()
	}
}

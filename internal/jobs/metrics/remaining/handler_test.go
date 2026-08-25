package remaining

import (
	"context"
	"errors"
	"fmt"
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

// TestPartitionHandlerClassifiesAnInvalidStateComputeFailureAsPermanent is
// the CHAOS-4242 classification fix: before this fix, EVERY
// ComputePartition failure -- including one wrapping ErrInvalidState, which
// is by construction deterministic (the same scope produces the same
// json.Unmarshal failure on every attempt) -- was marked Retryable, so a
// native-executor precondition failure burned a job's entire attempt
// budget on three identical failures before discarding. This asserts the
// SAME failure the ClaimPartition-scope regression produces is now
// Permanent, carries jobruntime.ReasonInvalidState, and releases the claim
// exactly as the Retryable path always did.
func TestPartitionHandlerClassifiesAnInvalidStateComputeFailureAsPermanent(t *testing.T) {
	store := &handlerStore{
		run: Run{
			ID: handlerRunID, OrganizationID: handlerOrgID,
			Family: "capacity", Status: "running",
		},
		claim: handlerClaim(),
	}
	wrapped := fmt.Errorf("%w: partition %s scope: unexpected end of JSON input", ErrInvalidState, handlerPartitionID)
	compatibility := &handlerCompatibility{computeErr: jobruntime.WithSafeCause(wrapped)}
	handler, err := NewPartitionHandler[jobruntime.RemainingCapacityArgs](
		store, compatibility, "capacity",
	)
	if err != nil {
		t.Fatal(err)
	}
	workErr := handler.Work(t.Context(), capacityExecution())
	if workErr == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(workErr.Error(), string(jobruntime.CategoryPermanent)) {
		t.Fatalf(
			"an ErrInvalidState compute failure was not classified Permanent: %v",
			workErr,
		)
	}
	// PartitionHandler.Work returns the raw jobruntime-marked error, one
	// layer below Adapter -- its ReasonInvalidState attachment only becomes
	// externally observable (in the safeError text, and via
	// Observer.ObserveDeterministicFailure) once Adapter.Work classifies
	// it, which TestAdapterCarriesReasonInvalidStateFromAPermanentCompute
	// Failure (internal/jobruntime) proves end to end.
	if store.releases != 1 || store.completions != 0 {
		t.Fatalf(
			"releases=%d completions=%d, want the same release-not-complete "+
				"shape the (now unreachable for this cause) Retryable path had",
			store.releases, store.completions,
		)
	}
}

// TestPartitionHandlerKeepsAGenuinelyTransientComputeFailureRetryable proves
// the classification fix in the sibling test above is narrowly scoped to
// ErrInvalidState -- an ordinary transient failure (a dropped ClickHouse
// connection, a Postgres timeout) must still retry, or a real blip would
// discard permanently after one attempt instead of getting the retry budget
// it needs.
func TestPartitionHandlerKeepsAGenuinelyTransientComputeFailureRetryable(t *testing.T) {
	store := &handlerStore{
		run: Run{
			ID: handlerRunID, OrganizationID: handlerOrgID,
			Family: "capacity", Status: "running",
		},
		claim: handlerClaim(),
	}
	compatibility := &handlerCompatibility{computeErr: errors.New("dial tcp: connection refused")}
	handler, err := NewPartitionHandler[jobruntime.RemainingCapacityArgs](
		store, compatibility, "capacity",
	)
	if err != nil {
		t.Fatal(err)
	}
	workErr := handler.Work(t.Context(), capacityExecution())
	if workErr == nil || !strings.Contains(workErr.Error(), string(jobruntime.CategoryRetryable)) {
		t.Fatalf("a non-ErrInvalidState compute failure was not Retryable: %v", workErr)
	}
}

// TestPartitionHandlerRecordsAZeroRowCompletionDistinctly is the CHAOS-4243
// acceptance case at the handler layer: a reported rows_written (including an
// explicit zero) must be embedded in the durable CompletePartition result so
// a zero-row completion is never stored identically to
// "compatibility_execution:<id>", the same string a real write produces.
func TestPartitionHandlerRecordsAZeroRowCompletionDistinctly(t *testing.T) {
	store := &handlerStore{
		run: Run{
			ID: handlerRunID, OrganizationID: handlerOrgID,
			Family: "release_impact", Status: "running",
		},
		claim: handlerClaim(),
	}
	zero := 0
	compatibility := &handlerCompatibility{delay: 5 * time.Millisecond, rowsWritten: &zero}
	handler, err := NewPartitionHandler[jobruntime.RemainingReleaseImpactArgs](
		store, compatibility, "release_impact",
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.Work(t.Context(), releaseImpactExecution()); err != nil {
		t.Fatal(err)
	}
	want := "compatibility_execution:" + handlerPartitionID + ":rows_written=0"
	if store.evidence != want {
		t.Fatalf("evidence = %q, want %q", store.evidence, want)
	}
}

func TestCompatibilityCompletionResult(t *testing.T) {
	if got := compatibilityCompletionResult("p1", CompatibilityOutcome{}); got != "compatibility_execution:p1" {
		t.Fatalf("nil RowsWritten = %q, want unqualified format", got)
	}
	five := 5
	if got := compatibilityCompletionResult("p1", CompatibilityOutcome{RowsWritten: &five}); got != "compatibility_execution:p1:rows_written=5" {
		t.Fatalf("RowsWritten=5 = %q", got)
	}
	zero := 0
	if got := compatibilityCompletionResult("p1", CompatibilityOutcome{RowsWritten: &zero}); got != "compatibility_execution:p1:rows_written=0" {
		t.Fatalf("RowsWritten=0 = %q, must be distinct from the unqualified format", got)
	}
}

// CHAOS-4002: handler.go has a third releaseClaim discard site (LoadRun
// failure) that TestPartitionHandlerRejectsCrossFamilyExecution (validation
// mismatch) and TestPartitionHandlerLeaseLossCancelsCompatibility (lease loss
// during work) do not exercise -- this was the one release site with no
// existing coverage at all before this ticket.
func TestPartitionHandlerReleasesClaimOnLoadRunFailure(t *testing.T) {
	store := &handlerStore{
		claim:      handlerClaim(),
		loadRunErr: ErrUnavailable,
	}
	handler, err := NewPartitionHandler[jobruntime.RemainingCapacityArgs](
		store, &handlerCompatibility{}, "capacity",
	)
	if err != nil {
		t.Fatal(err)
	}
	err = handler.Work(t.Context(), capacityExecution())
	if err == nil || store.releases != 1 || store.completions != 0 {
		t.Fatalf("LoadRun failure error=%v releases=%d completions=%d", err, store.releases, store.completions)
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

func releaseImpactExecution() *jobruntime.Execution[jobruntime.RemainingReleaseImpactArgs] {
	organizationID := handlerOrgID
	domain := jobcontract.DomainLink{
		Type: "remaining_metric_partition",
		ID:   handlerPartitionID,
	}
	args := jobruntime.RemainingReleaseImpactArgs{
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
	return &jobruntime.Execution[jobruntime.RemainingReleaseImpactArgs]{
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
	loadRunErr  error
	releases    int
	completions int
	evidence    string
}

func (store *handlerStore) LoadRun(context.Context, string) (Run, error) {
	if store.loadRunErr != nil {
		return Run{}, store.loadRunErr
	}
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
	// computeErr, when set, is returned immediately instead of the
	// delay/cancellation behavior below -- CHAOS-4242's classification test
	// uses this to simulate a native executor's precondition failure
	// (ErrInvalidState-wrapped) without needing a real ClickHouse/scope.
	computeErr error
	// rowsWritten is returned verbatim as the outcome's RowsWritten. nil
	// (the zero value) keeps existing callers' "not applicable" behavior.
	rowsWritten *int
}

func (executor *handlerCompatibility) ComputePartition(
	ctx context.Context,
	_ Run,
	_ Partition,
) (CompatibilityOutcome, error) {
	if executor.computeErr != nil {
		return CompatibilityOutcome{}, executor.computeErr
	}
	if executor.waitForCancellation {
		<-ctx.Done()
		executor.canceled = true
		return CompatibilityOutcome{}, ctx.Err()
	}
	timer := time.NewTimer(executor.delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return CompatibilityOutcome{RowsWritten: executor.rowsWritten}, nil
	case <-ctx.Done():
		executor.canceled = true
		return CompatibilityOutcome{}, ctx.Err()
	}
}

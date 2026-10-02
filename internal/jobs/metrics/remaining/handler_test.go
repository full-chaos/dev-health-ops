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
		store, &handlerExecutor{}, "capacity",
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
	executor := &handlerExecutor{delay: 80 * time.Millisecond}
	handler, err := NewPartitionHandler[jobruntime.RemainingCapacityArgs](
		store, executor, "capacity",
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
	executor := &handlerExecutor{computeErr: jobruntime.WithSafeCause(wrapped)}
	handler, err := NewPartitionHandler[jobruntime.RemainingCapacityArgs](
		store, executor, "capacity",
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
	//
	// This Permanent/ErrInvalidState branch is the one release site that
	// must reach ReleasePartitionTerminally, not the ordinary
	// ReleasePartition every other release site uses: River discards a
	// Permanent job outright, so only the terminal variant's
	// same-transaction "was this the run's last outstanding partition"
	// check ever runs for this failure shape.
	if store.terminalReleases != 1 || store.releases != 0 || store.completions != 0 {
		t.Fatalf(
			"terminalReleases=%d releases=%d completions=%d, want exactly one terminal release and no ordinary release/completion",
			store.terminalReleases, store.releases, store.completions,
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
	executor := &handlerExecutor{computeErr: errors.New("dial tcp: connection refused")}
	handler, err := NewPartitionHandler[jobruntime.RemainingCapacityArgs](
		store, executor, "capacity",
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
	executor := &handlerExecutor{delay: 5 * time.Millisecond, rowsWritten: &zero}
	handler, err := NewPartitionHandler[jobruntime.RemainingReleaseImpactArgs](
		store, executor, "release_impact",
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
// mismatch) and TestPartitionHandlerLeaseLossCancelsExecutor (lease loss
// during work) do not exercise -- this was the one release site with no
// existing coverage at all before this ticket.
func TestPartitionHandlerReleasesClaimOnLoadRunFailure(t *testing.T) {
	store := &handlerStore{
		claim:      handlerClaim(),
		loadRunErr: ErrUnavailable,
	}
	handler, err := NewPartitionHandler[jobruntime.RemainingCapacityArgs](
		store, &handlerExecutor{}, "capacity",
	)
	if err != nil {
		t.Fatal(err)
	}
	err = handler.Work(t.Context(), capacityExecution())
	if err == nil || store.releases != 1 || store.completions != 0 {
		t.Fatalf("LoadRun failure error=%v releases=%d completions=%d", err, store.releases, store.completions)
	}
}

func TestPartitionHandlerLeaseLossCancelsExecutor(t *testing.T) {
	store := &handlerStore{
		run: Run{
			ID: handlerRunID, OrganizationID: handlerOrgID,
			Family: "capacity", Status: "running",
		},
		claim:       handlerClaim(),
		failRenewal: true,
	}
	executor := &handlerExecutor{waitForCancellation: true}
	handler, err := NewPartitionHandler[jobruntime.RemainingCapacityArgs](
		store, executor, "capacity",
	)
	if err != nil {
		t.Fatal(err)
	}
	err = handler.Work(t.Context(), capacityExecution())
	if err == nil || !strings.Contains(err.Error(), string(jobruntime.CategoryRetryable)) ||
		!executor.canceled || store.completions != 0 || store.releases != 1 {
		t.Fatalf(
			"lease loss=%v canceled=%t completions=%d releases=%d",
			err, executor.canceled, store.completions, store.releases,
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
	claimErr    error
	completeErr error
	releases    int
	// terminalReleases counts ReleasePartitionTerminally calls separately
	// from the ordinary releases above, so a test can pin WHICH release
	// variant a given Work() failure path reaches (the ErrInvalidState/
	// Permanent branch, and a retryable failure on the job's last attempt,
	// call the terminal one).
	terminalReleases int
	completions      int
	evidence         string
}

func (store *handlerStore) LoadRun(context.Context, string) (Run, error) {
	if store.loadRunErr != nil {
		return Run{}, store.loadRunErr
	}
	return store.run, nil
}
func (store *handlerStore) ClaimPartition(context.Context, string) (*Claim, error) {
	if store.claimErr != nil {
		return nil, store.claimErr
	}
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
	if store.completeErr != nil {
		return store.completeErr
	}
	store.completions++
	store.evidence = evidence
	return store.completeErr
}
func (store *handlerStore) ReleasePartition(context.Context, Claim) error {
	store.releases++
	return nil
}
func (store *handlerStore) ReleasePartitionTerminally(context.Context, Claim) error {
	store.terminalReleases++
	return nil
}

type handlerExecutor struct {
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

func (executor *handlerExecutor) ComputePartition(
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

// attemptedCapacityExecution is capacityExecution with the River attempt counter and the descriptor's attempt
// budget set, the two values the handler compares to know whether a failure is the job's LAST attempt.
func attemptedCapacityExecution(attempt, maxAttempts int) *jobruntime.Execution[jobruntime.RemainingCapacityArgs] {
	execution := capacityExecution()
	execution.Attempt = attempt
	execution.Definition.MaxAttempts = maxAttempts
	return execution
}

// TestPartitionHandlerTerminalizesTheRunOnlyOnTheFinalAttemptOfARetryableFailure pins CHAOS-8024 at the handler layer:
// River discards a job that fails its last attempt with a retryable error, and nothing comes back to move the run out
// of 'running'. So the release of a failed attempt must be the terminal variant (which finalizes the run when no other
// partition is outstanding) exactly when this is the last attempt, at every release site that holds a claim; earlier
// attempts keep the ordinary release, because the retry reclaims the partition. A zero attempt budget (an execution
// built without a descriptor) is never "final".
func TestPartitionHandlerTerminalizesTheRunOnlyOnTheFinalAttemptOfARetryableFailure(t *testing.T) {
	transient := errors.New("dial tcp: connection refused")
	sites := []struct {
		name  string
		setup func(*handlerStore, *handlerExecutor)
	}{
		{"compute failure", func(_ *handlerStore, executor *handlerExecutor) { executor.computeErr = transient }},
		{"load run failure", func(store *handlerStore, _ *handlerExecutor) { store.loadRunErr = transient }},
		{"complete failure", func(store *handlerStore, _ *handlerExecutor) { store.completeErr = transient }},
	}
	attempts := []struct {
		name         string
		attempt, max int
		wantTerminal bool
	}{
		{"first of three", 1, 3, false},
		{"second of three", 2, 3, false},
		{"last of three", 3, 3, true},
		{"past the budget", 4, 3, true},
		{"no budget declared", 0, 0, false},
		{"attempt without a budget", 5, 0, false},
	}
	for _, site := range sites {
		for _, attempt := range attempts {
			t.Run(site.name+"/"+attempt.name, func(t *testing.T) {
				store := &handlerStore{
					run:   Run{ID: handlerRunID, OrganizationID: handlerOrgID, Family: "capacity", Status: "running"},
					claim: handlerClaim(),
				}
				executor := &handlerExecutor{}
				site.setup(store, executor)
				handler, err := NewPartitionHandler[jobruntime.RemainingCapacityArgs](store, executor, "capacity")
				if err != nil {
					t.Fatal(err)
				}
				workErr := handler.Work(t.Context(), attemptedCapacityExecution(attempt.attempt, attempt.max))
				if workErr == nil || !strings.Contains(workErr.Error(), string(jobruntime.CategoryRetryable)) {
					t.Fatalf("the failure was not returned Retryable: %v", workErr)
				}
				wantTerminal, wantOrdinary := 0, 1
				if attempt.wantTerminal {
					wantTerminal, wantOrdinary = 1, 0
				}
				// the complete-failure site never released before the fix at any attempt: it keeps the claim
				// (the lease expiring is the retry's reclaim), so earlier attempts still release nothing there
				if site.name == "complete failure" {
					wantOrdinary = 0
				}
				if store.terminalReleases != wantTerminal || store.releases != wantOrdinary {
					t.Fatalf("terminalReleases=%d releases=%d, want %d and %d", store.terminalReleases, store.releases, wantTerminal, wantOrdinary)
				}
			})
		}
	}
}

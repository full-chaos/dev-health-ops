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

// TestPartitionNativeFamilySuccessSkipsCompatibility proves a native family
// that computes successfully is excluded from the compatibility bridge's
// work for this partition (CHAOS-4276): the compatibility call still
// happens (every other family still needs it), but with the successful
// family named in skipFamilies.
func TestPartitionNativeFamilySuccessSkipsCompatibility(t *testing.T) {
	store := &fakeStore{
		partitionClaim: &PartitionClaim{Partition: Partition{ID: testPartitionID, RunID: testRunID}, Token: "00000000-0000-4000-8000-000000000003", LeaseDuration: 30 * time.Millisecond},
		run:            Run{ID: testRunID, OrganizationID: testOrgID, Generation: "daily-v1", Status: "running"},
	}
	compatibility := &recordingCompatibility{}
	handler, err := NewPartitionHandler(store, fakePublisher{}, compatibility)
	if err != nil {
		t.Fatal(err)
	}
	executor := &fakeNativeFamilyExecutor{rowsWritten: 7}
	observer := &recordingNativeFamilyObserver{}
	handler.SetNativeFamilies(map[string]NativeFamilyExecutor{"team_wellbeing": executor})
	handler.SetNativeFamilyObserver(observer)

	if err := handler.Work(context.Background(), partitionExecution()); err != nil {
		t.Fatal(err)
	}
	if executor.calls != 1 {
		t.Fatalf("native executor calls=%d, want 1", executor.calls)
	}
	if got := compatibility.lastSkipFamilies(); len(got) != 1 || got[0] != "team_wellbeing" {
		t.Fatalf("skipFamilies=%v, want [team_wellbeing]", got)
	}
	if len(observer.calls) != 1 || observer.calls[0].family != "team_wellbeing" ||
		observer.calls[0].outcome != jobruntime.DailyMetricsNativeFamilyOutcomeComputed || observer.calls[0].rowsWritten != 7 {
		t.Fatalf("observations=%#v", observer.calls)
	}
}

// TestPartitionNativeFamilyFailureFallsOpenToCompatibility proves a native
// family's RUNTIME failure is fail-open (chris's ruling, relayed via
// team-lead, CHAOS-4276): the partition still succeeds, the failed family is
// NOT in skipFamilies (so the compatibility bridge computes it as before),
// and the refusal is observed rather than silently swallowed.
func TestPartitionNativeFamilyFailureFallsOpenToCompatibility(t *testing.T) {
	store := &fakeStore{
		partitionClaim: &PartitionClaim{Partition: Partition{ID: testPartitionID, RunID: testRunID}, Token: "00000000-0000-4000-8000-000000000003", LeaseDuration: 30 * time.Millisecond},
		run:            Run{ID: testRunID, OrganizationID: testOrgID, Generation: "daily-v1", Status: "running"},
	}
	compatibility := &recordingCompatibility{}
	handler, err := NewPartitionHandler(store, fakePublisher{}, compatibility)
	if err != nil {
		t.Fatal(err)
	}
	executor := &fakeNativeFamilyExecutor{err: errors.New("transient clickhouse failure")}
	observer := &recordingNativeFamilyObserver{}
	handler.SetNativeFamilies(map[string]NativeFamilyExecutor{"team_wellbeing": executor})
	handler.SetNativeFamilyObserver(observer)

	if err := handler.Work(context.Background(), partitionExecution()); err != nil {
		t.Fatalf("a native family failure must not fail the partition: %v", err)
	}
	if executor.calls != 1 {
		t.Fatalf("native executor calls=%d, want 1", executor.calls)
	}
	if got := compatibility.lastSkipFamilies(); len(got) != 0 {
		t.Fatalf("skipFamilies=%v, want empty -- the failed family must stay on the compatibility path", got)
	}
	if store.partitionCompletions != 1 {
		t.Fatalf("partition completions=%d, want 1 (fail-open must still complete the partition)", store.partitionCompletions)
	}
	if len(observer.calls) != 1 || observer.calls[0].family != "team_wellbeing" ||
		observer.calls[0].outcome != jobruntime.DailyMetricsNativeFamilyOutcomeRefused {
		t.Fatalf("observations=%#v", observer.calls)
	}
}

// TestPartitionWithNoNativeFamiliesIsANoop proves the default (no
// SetNativeFamilies call) behaves exactly as before this capability existed:
// compatibility receives a nil/empty skipFamilies.
func TestPartitionWithNoNativeFamiliesIsANoop(t *testing.T) {
	store := &fakeStore{
		partitionClaim: &PartitionClaim{Partition: Partition{ID: testPartitionID, RunID: testRunID}, Token: "00000000-0000-4000-8000-000000000003", LeaseDuration: 30 * time.Millisecond},
		run:            Run{ID: testRunID, OrganizationID: testOrgID, Generation: "daily-v1", Status: "running"},
	}
	compatibility := &recordingCompatibility{}
	handler, err := NewPartitionHandler(store, fakePublisher{}, compatibility)
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.Work(context.Background(), partitionExecution()); err != nil {
		t.Fatal(err)
	}
	if got := compatibility.lastSkipFamilies(); len(got) != 0 {
		t.Fatalf("skipFamilies=%v, want empty with no native families registered", got)
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

// CHAOS-4316: without SetLivenessCeiling, runWithLeaseRenewal has no bound on
// a compatibility call other than the lease renewal loop -- which keeps
// succeeding forever as long as the store's RenewPartition call succeeds,
// completely independent of whether the compatibility call is making any
// progress. blockingCompatibility{waitForCancellation: true} models exactly
// that: a hung bridge call that only ever returns when its ctx is canceled.
// On origin/main (no SetLivenessCeiling call anywhere), nothing ever cancels
// that ctx here -- fakeStore's RenewPartition always succeeds and the lease
// is generous -- so Work would block for the full test timeout. This test
// asserts the FIXED behavior (the ceiling fires and Work returns quickly);
// verified manually before this commit that removing the SetLivenessCeiling
// call below reproduces the hang (Work does not return within the 2s guard,
// confirming the bug this ticket fixes).
func TestPartitionLivenessCeilingReclaimsAHungCompatibilityCall(t *testing.T) {
	store := &fakeStore{
		partitionClaim: &PartitionClaim{
			Partition:     Partition{ID: testPartitionID, RunID: testRunID, RepoIDs: []RepositoryID{"repo-1", "repo-2"}},
			Token:         "00000000-0000-4000-8000-000000000003",
			LeaseDuration: time.Hour, // generous: renewal must never be what unblocks this
		},
		run: Run{ID: testRunID, OrganizationID: testOrgID, Generation: "daily-v1", Status: "running"},
	}
	compatibility := &blockingCompatibility{waitForCancellation: true}
	handler, err := NewPartitionHandler(store, fakePublisher{}, compatibility)
	if err != nil {
		t.Fatal(err)
	}
	// Tiny, work-size-derived ceiling: base 10ms + perRepo 5ms * 2 repos =
	// 20ms -- proportional to len(RepoIDs), never a flat number, matching
	// the production formula exactly (see SetLivenessCeiling's doc comment).
	handler.SetLivenessCeiling(10*time.Millisecond, 5*time.Millisecond)

	done := make(chan error, 1)
	go func() { done <- handler.Work(context.Background(), partitionExecution()) }()

	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), string(jobruntime.CategoryRetryable)) {
			t.Fatalf("Work() = %v, want retryable", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Work did not return within the liveness ceiling -- CHAOS-4316 backstop regression")
	}
	compatibility.mu.Lock()
	canceled := compatibility.partitionCanceled
	compatibility.mu.Unlock()
	if !canceled {
		t.Fatal("compatibility call never observed ctx cancellation -- ceiling did not fire")
	}
	if store.partitionReleases != 1 {
		t.Fatalf("partitionReleases=%d, want 1 (a ceiling-fired attempt must release its claim)", store.partitionReleases)
	}
}

// SetLivenessCeiling with a zero/unset base must leave Work's ctx exactly as
// unbounded as before this capability existed -- existing callers/tests that
// never call it (every other test in this file) must see no behavior change.
// TestLivenessCeilingEnabledByDefault proves the backstop ships ON by
// default (team-lead ruling 2026-08-26): deployed compose/helm manifests do
// not set the new env vars, so an opt-in design would never activate in
// production. NewPartitionHandler alone (no SetLivenessCeiling call) must
// already produce a nonzero, repo_count-derived bound.
func TestLivenessCeilingEnabledByDefault(t *testing.T) {
	handler, err := NewPartitionHandler(&fakeStore{}, fakePublisher{}, fakeCompatibility{})
	if err != nil {
		t.Fatal(err)
	}
	zero := handler.livenessCeiling(0)
	if zero != defaultLivenessCeilingBase {
		t.Fatalf("livenessCeiling(0) = %v, want the default base %v", zero, defaultLivenessCeilingBase)
	}
	three := handler.livenessCeiling(3)
	want := defaultLivenessCeilingBase + 3*defaultLivenessCeilingPerRepo
	if three != want {
		t.Fatalf("livenessCeiling(3) = %v, want %v (base + 3*perRepo)", three, want)
	}
	if three <= zero {
		t.Fatalf("livenessCeiling(3) = %v must exceed livenessCeiling(0) = %v -- derived from repo_count, not flat", three, zero)
	}
}

// TestLivenessCeilingExplicitZeroOptsOut proves the ONE sanctioned way to
// disable the backstop: SetLivenessCeiling(0, _). Never calling
// SetLivenessCeiling at all does NOT disable it (see the default-on test
// above) -- only an explicit zero base does.
func TestLivenessCeilingExplicitZeroOptsOut(t *testing.T) {
	handler, err := NewPartitionHandler(&fakeStore{}, fakePublisher{}, fakeCompatibility{})
	if err != nil {
		t.Fatal(err)
	}
	handler.SetLivenessCeiling(0, 0)
	if got := handler.livenessCeiling(5); got != 0 {
		t.Fatalf("livenessCeiling() = %v, want 0 after an explicit SetLivenessCeiling(0, 0) opt-out", got)
	}
}

// CHAOS-4264: a signaled/resource-exhausted/refused compatibility bridge
// attempt must still be Retryable (no behavior change to River's retry
// decision) but must carry the matching bounded jobruntime.Reason instead of
// the pre-existing bare ErrUnavailable, so an attempt log line explains
// itself without anyone reading Sentry or host dmesg.
func TestRetryCompatibilityErrorAttachesBoundedReasonAndPreservesCause(t *testing.T) {
	cases := []struct {
		name  string
		cause error
	}{
		{name: "signaled", cause: ErrCompatibilityProcessSignaled},
		{name: "resource_exhausted", cause: ErrCompatibilityResourceExhausted},
		{name: "ambiguous_refused", cause: ErrCompatibilityAmbiguousRefused},
		{name: "progress_stalled", cause: ErrCompatibilityProgressStalled},
		{name: "unclassified", cause: ErrUnavailable},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			marked := retryCompatibilityError(testCase.cause)
			if !strings.Contains(marked.Error(), string(jobruntime.CategoryRetryable)) {
				t.Fatalf("Error() = %q, want the retryable category", marked.Error())
			}
			if !errors.Is(marked, testCase.cause) {
				t.Fatalf("retryCompatibilityError(%v) lost its cause: %v", testCase.cause, marked)
			}
		})
	}
}

// TestPartitionCompatibilityFailureIsRetryableWithReason drives the failure
// through the real Handler.Work path (not just retryCompatibilityError in
// isolation) so the wiring at the actual call site is what's under test, not
// just the helper.
func TestPartitionCompatibilityFailureIsRetryableWithReason(t *testing.T) {
	store := &fakeStore{
		partitionClaim: &PartitionClaim{
			Partition:     Partition{ID: testPartitionID, RunID: testRunID},
			Token:         "00000000-0000-4000-8000-000000000003",
			LeaseDuration: 30 * time.Millisecond,
		},
		run: Run{ID: testRunID, OrganizationID: testOrgID, Generation: "daily-v1", Status: "running"},
	}
	handler, err := NewPartitionHandler(store, fakePublisher{}, failingCompatibility{err: ErrCompatibilityProcessSignaled})
	if err != nil {
		t.Fatal(err)
	}
	err = handler.Work(context.Background(), partitionExecution())
	if err == nil || !strings.Contains(err.Error(), string(jobruntime.CategoryRetryable)) {
		t.Fatalf("compatibility failure = %v, want retryable", err)
	}
	if !errors.Is(err, ErrCompatibilityProcessSignaled) {
		t.Fatalf("compatibility failure = %v, want it to unwrap to ErrCompatibilityProcessSignaled", err)
	}
	if store.partitionReleases != 1 {
		t.Fatalf("partitionReleases = %d, want 1", store.partitionReleases)
	}
}

// TestPartitionProgressStalledPersistsFailureReasonAndStaysRetryable proves
// CHAOS-4316's liveness-kill path writes the shared failure_reason column
// (migration 0113, CHAOS-4319) through ReleasePartitionWithReason instead of
// the plain ReleasePartition every other compatibility failure uses --
// status stays 'failed' (silently re-dispatchable, unlike
// ErrCompatibilityAmbiguousStuck's 'failed_permanent': a liveness kill is
// not a claim this row can never satisfy).
func TestPartitionProgressStalledPersistsFailureReasonAndStaysRetryable(t *testing.T) {
	store := &fakeStore{
		partitionClaim: &PartitionClaim{
			Partition:     Partition{ID: testPartitionID, RunID: testRunID},
			Token:         "00000000-0000-4000-8000-000000000003",
			LeaseDuration: 30 * time.Millisecond,
		},
		run: Run{ID: testRunID, OrganizationID: testOrgID, Generation: "daily-v1", Status: "running"},
	}
	handler, err := NewPartitionHandler(store, fakePublisher{}, failingCompatibility{err: ErrCompatibilityProgressStalled})
	if err != nil {
		t.Fatal(err)
	}
	err = handler.Work(context.Background(), partitionExecution())
	if err == nil || !strings.Contains(err.Error(), string(jobruntime.CategoryRetryable)) {
		t.Fatalf("compatibility failure = %v, want retryable", err)
	}
	if !errors.Is(err, ErrCompatibilityProgressStalled) {
		t.Fatalf("compatibility failure = %v, want it to unwrap to ErrCompatibilityProgressStalled", err)
	}
	if store.releasesWithReason != 1 || store.releaseReason != "progress_stalled" {
		t.Fatalf("releasesWithReason=%d releaseReason=%q, want 1/progress_stalled", store.releasesWithReason, store.releaseReason)
	}
	if store.partitionReleases != 0 {
		t.Fatalf("partitionReleases = %d, want 0 -- a liveness kill must release WITH a reason, not through the plain path", store.partitionReleases)
	}
}

// TestPartitionAmbiguousStuckPersistsFailurePermanentlyInsteadOfDiscarding is
// the CHAOS-4319 red-first proof. Before this ticket's fix, an
// ambiguous_refused response whose ledger state is "ambiguous" was
// classified identically to every other compatibility-bridge failure --
// unconditionally Retryable, released back to 'failed' (silently
// re-dispatchable), with no durable record of why. River would keep
// re-claiming a partition that could only ever reproduce the same 409 until
// its attempt budget ran out, then discard the job -- the partition simply
// vanished, with nothing durable left behind explaining the loss. This test
// drives Handler.Work with a fake bridge that always returns
// ErrCompatibilityAmbiguousStuck (the state=="ambiguous" classification) and
// asserts: (1) the error is Permanent, not Retryable -- River stops
// retrying a lost cause instead of burning its whole attempt budget on
// guaranteed 409s; (2) the partition is durably persisted via
// FailPartitionPermanently (not the ordinary, re-dispatchable
// ReleasePartition path); (3) the bounded telemetry decision fires. Calling
// Work a second time proves the SAME durable outcome recurs deterministically
// rather than eventually silently discarding -- there is no attempt count
// after which the failure stops being recorded.
func TestPartitionAmbiguousStuckPersistsFailurePermanentlyInsteadOfDiscarding(t *testing.T) {
	newHandler := func() (*PartitionHandler, *fakeStore, *recordingCompatRetryObserver) {
		store := &fakeStore{
			partitionClaim: &PartitionClaim{
				Partition:     Partition{ID: testPartitionID, RunID: testRunID},
				Token:         "00000000-0000-4000-8000-000000000003",
				LeaseDuration: 30 * time.Millisecond,
			},
			run: Run{ID: testRunID, OrganizationID: testOrgID, Generation: "daily-v1", Status: "running"},
		}
		handler, err := NewPartitionHandler(store, fakePublisher{}, failingCompatibility{err: ErrCompatibilityAmbiguousStuck})
		if err != nil {
			t.Fatal(err)
		}
		observer := &recordingCompatRetryObserver{}
		handler.SetCompatRetryObserver(observer)
		return handler, store, observer
	}

	for attempt := 1; attempt <= 2; attempt++ {
		handler, store, observer := newHandler()
		err := handler.Work(context.Background(), partitionExecution())
		if err == nil || !strings.Contains(err.Error(), string(jobruntime.CategoryPermanent)) {
			t.Fatalf("attempt %d: ambiguous-stuck failure = %v, want permanent", attempt, err)
		}
		if !errors.Is(err, ErrCompatibilityAmbiguousStuck) {
			t.Fatalf("attempt %d: err = %v, want it to unwrap to ErrCompatibilityAmbiguousStuck", attempt, err)
		}
		if store.permanentFailures != 1 {
			t.Fatalf("attempt %d: permanentFailures = %d, want 1 (durable record must be written)", attempt, store.permanentFailures)
		}
		if store.permanentFailureReason != jobruntime.ReasonAmbiguousRefused.String() {
			t.Fatalf("attempt %d: permanentFailureReason = %q, want %q", attempt, store.permanentFailureReason, jobruntime.ReasonAmbiguousRefused.String())
		}
		if store.partitionReleases != 0 {
			t.Fatalf("attempt %d: partitionReleases = %d, want 0 -- a stuck-ambiguous partition must not go back to the silently re-dispatchable 'failed' status", attempt, store.partitionReleases)
		}
		if len(observer.decisions) != 1 || observer.decisions[0] != jobruntime.DailyMetricsCompatRetryDecisionPersistedFailed {
			t.Fatalf("attempt %d: observer decisions = %v, want exactly [persisted_failed]", attempt, observer.decisions)
		}
	}
}

// TestPartitionAmbiguousStuckDurableWriteFailureStaysRetryable is the
// codex-round-1 (P1) red-first proof: the detached, 5s-timeout-boxed
// FailPartitionPermanently write can itself fail (a transient DB error, or
// the lease already expired by the time it runs). Before this fix, Work
// classified the job Permanent unconditionally whenever the bridge
// response was ErrCompatibilityAmbiguousStuck, regardless of whether the
// durable write actually landed -- so a failed write meant River stopped
// retrying with NOTHING durable to show for it, the exact silent-loss shape
// CHAOS-4319 exists to close. A failed write must fall back to ordinary
// Retryable so River keeps trying instead of canceling on an unconfirmed
// write.
func TestPartitionAmbiguousStuckDurableWriteFailureStaysRetryable(t *testing.T) {
	store := &fakeStore{
		partitionClaim: &PartitionClaim{
			Partition:     Partition{ID: testPartitionID, RunID: testRunID},
			Token:         "00000000-0000-4000-8000-000000000003",
			LeaseDuration: 30 * time.Millisecond,
		},
		run:                 Run{ID: testRunID, OrganizationID: testOrgID, Generation: "daily-v1", Status: "running"},
		permanentFailureErr: ErrUnavailable,
	}
	handler, err := NewPartitionHandler(store, fakePublisher{}, failingCompatibility{err: ErrCompatibilityAmbiguousStuck})
	if err != nil {
		t.Fatal(err)
	}
	observer := &recordingCompatRetryObserver{}
	handler.SetCompatRetryObserver(observer)

	err = handler.Work(context.Background(), partitionExecution())
	if err == nil || !strings.Contains(err.Error(), string(jobruntime.CategoryRetryable)) {
		t.Fatalf("failed durable write = %v, want retryable (never Permanent on an unconfirmed write)", err)
	}
	if store.partitionReleases != 1 {
		t.Fatalf("partitionReleases = %d, want 1 -- the fallback path must still release the claim", store.partitionReleases)
	}
	if len(observer.decisions) != 0 {
		t.Fatalf("observer decisions = %v, want none -- a failed write is not a persisted_failed outcome", observer.decisions)
	}
}

// TestPartitionAmbiguousRefusedTransientCollisionStaysRetryable is the
// control case: a live concurrent claim (state=="executing") is a genuine,
// self-resolving overlap, not a stuck ledger row, and must keep the
// pre-CHAOS-4319 Retryable/ReleasePartition behavior exactly.
func TestPartitionAmbiguousRefusedTransientCollisionStaysRetryable(t *testing.T) {
	store := &fakeStore{
		partitionClaim: &PartitionClaim{
			Partition:     Partition{ID: testPartitionID, RunID: testRunID},
			Token:         "00000000-0000-4000-8000-000000000003",
			LeaseDuration: 30 * time.Millisecond,
		},
		run: Run{ID: testRunID, OrganizationID: testOrgID, Generation: "daily-v1", Status: "running"},
	}
	handler, err := NewPartitionHandler(store, fakePublisher{}, failingCompatibility{err: ErrCompatibilityAmbiguousRefused})
	if err != nil {
		t.Fatal(err)
	}
	err = handler.Work(context.Background(), partitionExecution())
	if err == nil || !strings.Contains(err.Error(), string(jobruntime.CategoryRetryable)) {
		t.Fatalf("transient ambiguous_refused = %v, want retryable", err)
	}
	if store.partitionReleases != 1 || store.permanentFailures != 0 {
		t.Fatalf("partitionReleases=%d permanentFailures=%d, want 1/0", store.partitionReleases, store.permanentFailures)
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
	permanentFailures             int
	permanentFailureReason        string
	permanentFailureErr           error
	releasesWithReason            int
	releaseReason                 string
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
func (store *fakeStore) ReleasePartitionWithReason(_ context.Context, _ PartitionClaim, reason string) error {
	store.releasesWithReason++
	store.releaseReason = reason
	return nil
}
func (store *fakeStore) FailPartitionPermanently(_ context.Context, _ PartitionClaim, reason string) error {
	store.permanentFailures++
	if store.permanentFailureErr != nil {
		return store.permanentFailureErr
	}
	store.permanentFailureReason = reason
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

func (fakeCompatibility) ComputePartition(context.Context, Run, Partition, []string) error {
	return nil
}
func (fakeCompatibility) Finalize(context.Context, Run) error { return nil }

// failingCompatibility always fails with a fixed, caller-chosen error --
// used to prove the classified compatibility bridge sentinels (CHAOS-4264)
// reach the caller unchanged through Handler.Work's retry wrapping.
type failingCompatibility struct{ err error }

func (compatibility failingCompatibility) ComputePartition(context.Context, Run, Partition, []string) error {
	return compatibility.err
}

func (compatibility failingCompatibility) Finalize(context.Context, Run) error {
	return compatibility.err
}

// recordingCompatibility records the skipFamilies each ComputePartition call
// received, so a test can assert exactly which families a native executor's
// outcome caused to be skipped.
type recordingCompatibility struct {
	mu           sync.Mutex
	skipFamilies [][]string
}

func (compatibility *recordingCompatibility) ComputePartition(_ context.Context, _ Run, _ Partition, skipFamilies []string) error {
	compatibility.mu.Lock()
	defer compatibility.mu.Unlock()
	compatibility.skipFamilies = append(compatibility.skipFamilies, skipFamilies)
	return nil
}
func (*recordingCompatibility) Finalize(context.Context, Run) error { return nil }

func (compatibility *recordingCompatibility) lastSkipFamilies() []string {
	compatibility.mu.Lock()
	defer compatibility.mu.Unlock()
	if len(compatibility.skipFamilies) == 0 {
		return nil
	}
	return compatibility.skipFamilies[len(compatibility.skipFamilies)-1]
}

// fakeNativeFamilyExecutor is a NativeFamilyExecutor test double: either
// returns a fixed row count, or a fixed error to exercise the fail-open path.
type fakeNativeFamilyExecutor struct {
	rowsWritten int
	err         error
	calls       int
}

func (executor *fakeNativeFamilyExecutor) ComputeFamily(context.Context, Run, Partition) (int, error) {
	executor.calls++
	return executor.rowsWritten, executor.err
}

// recordingNativeFamilyObserver captures every ObserveDailyMetricsNativeFamily
// call so a test can assert both success and fail-open telemetry.
type recordingNativeFamilyObserver struct {
	mu    sync.Mutex
	calls []recordingNativeFamilyObservation
}

type recordingNativeFamilyObservation struct {
	family      string
	outcome     jobruntime.DailyMetricsNativeFamilyOutcome
	rowsWritten int
}

func (observer *recordingNativeFamilyObserver) ObserveDailyMetricsNativeFamily(
	family string, outcome jobruntime.DailyMetricsNativeFamilyOutcome, rowsWritten int, _ time.Duration,
) error {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	observer.calls = append(observer.calls, recordingNativeFamilyObservation{family: family, outcome: outcome, rowsWritten: rowsWritten})
	return nil
}

type recordingCompatRetryObserver struct {
	mu        sync.Mutex
	decisions []jobruntime.DailyMetricsCompatRetryDecision
}

func (observer *recordingCompatRetryObserver) ObserveDailyMetricsCompatRetry(
	decision jobruntime.DailyMetricsCompatRetryDecision,
) error {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	observer.decisions = append(observer.decisions, decision)
	return nil
}

type errorSourceDataChecker struct{ err error }

func (checker errorSourceDataChecker) ZeroRowFamiliesWithSourceData(context.Context, string) ([]string, error) {
	return nil, checker.err
}

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

func (compatibility *blockingCompatibility) ComputePartition(ctx context.Context, _ Run, _ Partition, _ []string) error {
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

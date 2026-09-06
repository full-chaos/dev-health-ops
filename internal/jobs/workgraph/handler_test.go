package workgraph

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

const (
	testRequestID = "00000000-0000-4000-8000-000000000101"
	testToken     = "00000000-0000-4000-8000-000000000102"
	testOrgID     = "00000000-0000-4000-8000-000000000009"
)

func TestBuildRenewsFenceAndCompletes(t *testing.T) {
	store := &fakeStore{claim: testClaim(30 * time.Millisecond)}
	handler, err := NewBuildHandler(store, []NativePreStep{blockingPreStep{delay: 80 * time.Millisecond}}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.Work(context.Background(), buildExecution()); err != nil {
		t.Fatal(err)
	}
	if store.renewals < 2 || store.completions != 1 || store.ambiguous != 0 {
		t.Fatalf("renewals=%d completions=%d ambiguous=%d", store.renewals, store.completions, store.ambiguous)
	}
}

// A live lease must park the job rather than retire it. Retiring it is what
// stranded the fanout in CHAOS-3991: the request's completion is the fence key
// for everything gated on it, so a request nobody comes back to reclaim holds
// its whole chain forever. The snooze is asserted rather than the eventual
// outcome, because only the snooze proves the job survived to reclaim at all.
func TestBuildParksWhileAnotherClaimantHoldsALiveLease(t *testing.T) {
	store := &fakeStore{claimErr: &LeaseActiveError{RetryAfter: 7 * time.Minute}}
	handler, err := NewBuildHandler(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = handler.Work(context.Background(), buildExecution())
	if err == nil {
		t.Fatal("a live lease was reported as a completed request")
	}
	delay, snoozed := jobruntime.SnoozeDelay(err)
	if !snoozed || delay != 7*time.Minute {
		t.Fatalf("snooze = %v/%t, want 7m", delay, snoozed)
	}
	if store.completions != 0 || store.ambiguous != 0 {
		t.Fatalf("completions=%d ambiguous=%d, want the request untouched", store.completions, store.ambiguous)
	}
}

func TestBuildLeaseLossCancelsStepsAndCannotComplete(t *testing.T) {
	store := &fakeStore{claim: testClaim(30 * time.Millisecond), loseAt: 1}
	step := blockingPreStep{waitForCancellation: true}
	handler, err := NewBuildHandler(store, []NativePreStep{step}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = handler.Work(context.Background(), buildExecution())
	if err == nil || !strings.Contains(err.Error(), string(jobruntime.CategoryRetryable)) || store.completions != 0 {
		t.Fatalf("error=%v completions=%d", err, store.completions)
	}
}

// CHAOS-4924 cutover: Ambiguous exists to record a HALF-APPLIED bridge write.
// Build has no bridge at all, so a step failure -- pre-step or post-step --
// NEVER releases Ambiguous; it classifies by error type instead (see
// TestClassifyBuildStepErrorByType below). This is the "transition" case
// team-lead's ruling asked for: before the cutover this exact shape (a
// generic, non-transient step failure) went through the bridge path and WAS
// ambiguous; after it, it is a plain Permanent failure.
func TestBuildStepFailureIsPermanentNeverAmbiguous(t *testing.T) {
	store := &fakeStore{claim: testClaim(time.Second)}
	step := failingPreStep{err: errors.New("upstream unavailable")}
	handler, err := NewBuildHandler(store, []NativePreStep{step}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = handler.Work(context.Background(), buildExecution())
	if err == nil || !strings.Contains(err.Error(), string(jobruntime.CategoryPermanent)) {
		t.Fatalf("error=%v, want category %s", err, jobruntime.CategoryPermanent)
	}
	if store.ambiguous != 0 {
		t.Fatalf("ambiguous releases = %d, want 0 -- Build never has a half-applied bridge write to repair", store.ambiguous)
	}
	if store.completions != 0 {
		t.Fatalf("completions = %d, want 0", store.completions)
	}
}

// The other half of the classifier: a transient/connectivity failure retries
// instead of permanently failing the build. Misclassifying this as Permanent
// would silently stop every future build for the org on one bad ClickHouse
// blip -- the failure mode this classifier exists to avoid.
func TestBuildTransientStepFailureRetries(t *testing.T) {
	store := &fakeStore{claim: testClaim(time.Second)}
	step := failingPreStep{err: fmt.Errorf("dial ClickHouse: %w", context.DeadlineExceeded)}
	handler, err := NewBuildHandler(store, []NativePreStep{step}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = handler.Work(context.Background(), buildExecution())
	if err == nil || !strings.Contains(err.Error(), string(jobruntime.CategoryRetryable)) {
		t.Fatalf("error=%v, want category %s", err, jobruntime.CategoryRetryable)
	}
	if store.ambiguous != 0 {
		t.Fatalf("ambiguous releases = %d, want 0", store.ambiguous)
	}
}

// Table-driven unit test for the classifier itself, independent of the full
// claim/lease machinery above.
func TestClassifyBuildStepErrorByType(t *testing.T) {
	for _, testCase := range []struct {
		name string
		err  error
		want string
	}{
		{"context deadline exceeded", context.DeadlineExceeded, string(jobruntime.CategoryRetryable)},
		{"context canceled", context.Canceled, string(jobruntime.CategoryRetryable)},
		{"wrapped deadline exceeded", fmt.Errorf("dial: %w", context.DeadlineExceeded), string(jobruntime.CategoryRetryable)},
		{"net timeout error", &net.DNSError{IsTimeout: true}, string(jobruntime.CategoryRetryable)},
		{"net dial error (not a timeout)", &net.OpError{Op: "dial", Err: errors.New("connection refused")}, string(jobruntime.CategoryRetryable)},
		{"a plain defect", errors.New("nil pointer somewhere"), string(jobruntime.CategoryPermanent)},
		{"a ClickHouse query syntax error", errors.New("code: 62, message: Syntax error"), string(jobruntime.CategoryPermanent)},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var got error
			if isTransientStepError(testCase.err) {
				got = jobruntime.Retryable(testCase.err)
			} else {
				got = jobruntime.Permanent(testCase.err)
			}
			if !strings.Contains(got.Error(), testCase.want) {
				t.Fatalf("classified %v, want category %s", got, testCase.want)
			}
		})
	}
}

func TestBuildRejectsTenantEnvelopeMismatchBeforeClaim(t *testing.T) {
	store := &fakeStore{claim: testClaim(time.Second)}
	handler, err := NewBuildHandler(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	execution := buildExecution()
	wrong := "00000000-0000-4000-8000-000000000008"
	execution.OrganizationID = &wrong
	err = handler.Work(context.Background(), execution)
	if err == nil || !strings.Contains(err.Error(), string(jobruntime.CategoryPermanent)) || store.claims != 1 || store.ambiguous != 1 {
		t.Fatalf("error=%v claims=%d ambiguous=%d", err, store.claims, store.ambiguous)
	}
}

// TestMaterializeCompatibilityFailureIsAmbiguousNotRetried is the
// Materialize-side counterpart of the old (pre-CHAOS-4924) Build test of the
// same shape: Materialize still bridges, so a generic, unclassified bridge
// failure still has no positive "never sent"/"declined" placement and still
// releases Ambiguous -- unchanged by the Build cutover.
func TestMaterializeCompatibilityFailureIsAmbiguousNotRetried(t *testing.T) {
	store := &fakeStore{claim: testMaterializeClaim(time.Second)}
	handler, err := NewMaterializeHandler(store, failingExecutor{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = handler.Work(context.Background(), materializeExecution())
	if err == nil || !strings.Contains(err.Error(), string(jobruntime.CategoryPermanent)) || store.ambiguous != 1 || store.completions != 0 {
		t.Fatalf("error=%v ambiguous=%d completions=%d", err, store.ambiguous, store.completions)
	}
}

func buildExecution() *jobruntime.Execution[jobruntime.WorkGraphBuildArgs] {
	return &jobruntime.Execution[jobruntime.WorkGraphBuildArgs]{
		OrganizationID: pointer(testOrgID),
		Envelope:       jobcontract.Envelope{OrganizationID: pointer(testOrgID), Domain: jobcontract.DomainLink{Type: "work_graph_request", ID: testRequestID}},
		Args:           jobruntime.WorkGraphBuildArgs{EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.WorkGraphBuildPayload]{OrganizationID: pointer(testOrgID), Domain: jobcontract.DomainLink{Type: "work_graph_request", ID: testRequestID}, Payload: jobcontract.WorkGraphBuildPayload{RequestID: testRequestID}}},
	}
}

// materializeExecution mirrors buildExecution for investment.materialize's
// own domain type ("investment_request", see domainFor) -- used by tests that
// exercise the still-bridged Materialize path directly (compatibility
// classification is now Materialize-only, since Build has no bridge).
func materializeExecution() *jobruntime.Execution[jobruntime.InvestmentMaterializeArgs] {
	return &jobruntime.Execution[jobruntime.InvestmentMaterializeArgs]{
		OrganizationID: pointer(testOrgID),
		Envelope:       jobcontract.Envelope{OrganizationID: pointer(testOrgID), Domain: jobcontract.DomainLink{Type: "investment_request", ID: testRequestID}},
		Args:           jobruntime.InvestmentMaterializeArgs{EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.InvestmentMaterializePayload]{OrganizationID: pointer(testOrgID), Domain: jobcontract.DomainLink{Type: "investment_request", ID: testRequestID}, Payload: jobcontract.InvestmentMaterializePayload{RequestID: testRequestID}}},
	}
}

func testClaim(lease time.Duration) *Claim {
	return &Claim{Request: Request{ID: testRequestID, OrganizationID: testOrgID, Kind: KindBuild, Scope: []byte(`{"from_date":"2026-07-01"}`), LLMConcurrency: 1, SpendLimitMicrounits: 0, CorrelationID: "test", IdempotencyKey: "workgraph:test"}, Token: testToken, LeaseDuration: lease}
}

func testMaterializeClaim(lease time.Duration) *Claim {
	return &Claim{Request: Request{ID: testRequestID, OrganizationID: testOrgID, Kind: KindMaterialize, Scope: []byte(`{}`), LLMConcurrency: 1, SpendLimitMicrounits: 0, CorrelationID: "test", IdempotencyKey: "workgraph:test"}, Token: testToken, LeaseDuration: lease}
}
func pointer(value string) *string { return &value }

type fakeStore struct {
	claim                                            *Claim
	claimErr                                         error
	claims, renewals, completions, ambiguous, loseAt int
	// lastEvidence is what Complete received, so a test can assert what the
	// step fragments merged into rather than only that a completion happened.
	lastEvidence []byte
	// lastAmbiguousDetail is what Ambiguous received. Before CHAOS-4970 every
	// ambiguous release wrote one fixed literal, so the ledger row carried no
	// discriminator at all; a test asserting the classified detail ACTUALLY
	// reaches the store is what keeps that from silently regressing.
	lastAmbiguousDetail string
}

func (s *fakeStore) Claim(context.Context, string, Kind) (*Claim, error) {
	s.claims++
	return s.claim, s.claimErr
}
func (s *fakeStore) Renew(context.Context, Claim) error {
	s.renewals++
	if s.loseAt == s.renewals {
		return ErrLeaseLost
	}
	return nil
}
func (s *fakeStore) Complete(_ context.Context, _ Claim, evidence []byte) error {
	s.completions++
	s.lastEvidence = evidence
	return nil
}
func (*fakeStore) Fail(context.Context, Claim, string) error { return nil }
func (s *fakeStore) Ambiguous(_ context.Context, _ Claim, detail string) error {
	s.ambiguous++
	s.lastAmbiguousDetail = detail
	return nil
}

type blockingExecutor struct {
	delay               time.Duration
	waitForCancellation bool
}

func (e blockingExecutor) Execute(ctx context.Context, _ Claim) ([]byte, error) {
	if e.waitForCancellation {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	select {
	case <-time.After(e.delay):
		return []byte(`{"edges":1}`), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type failingExecutor struct{}

func (failingExecutor) Execute(context.Context, Claim) ([]byte, error) {
	return nil, errors.New("upstream unavailable")
}

// blockingPreStep is blockingExecutor's NativePreStep counterpart, for
// Build-side tests -- Build has no bridge/executor to block on anymore.
type blockingPreStep struct {
	delay               time.Duration
	waitForCancellation bool
}

func (blockingPreStep) Name() string { return "blocking" }

func (s blockingPreStep) Run(ctx context.Context, _ Claim) (map[string]any, error) {
	if s.waitForCancellation {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	select {
	case <-time.After(s.delay):
		return map[string]any{"edges": 1}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// failingPreStep is failingExecutor's NativePreStep counterpart.
type failingPreStep struct{ err error }

func (failingPreStep) Name() string { return "failing" }

func (s failingPreStep) Run(context.Context, Claim) (map[string]any, error) {
	return nil, s.err
}

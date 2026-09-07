package workgraph

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

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

// classifyingExecutor returns a fixed pre-classified error, so the handler's
// own retry/ambiguous branch is exercised without a network or a concrete
// CompatibilityExecutor implementation at all. Moved here from the deleted
// compatibility_classification_test.go (CHAOS-3092: the HTTP bridge executor
// these tests were written alongside is gone, but the classification
// contract they cover -- ErrCompatibilityNotSent/Refused/Unknown, defined in
// handler.go -- is still live for any executor, including the native
// investment one).
type classifyingExecutor struct{ err error }

func (executor classifyingExecutor) Execute(context.Context, Claim) ([]byte, error) {
	return nil, executor.err
}

func TestHandlerRetriesNotSentAndRefusedWithoutReleasingAmbiguous(t *testing.T) {
	for _, testCase := range []struct {
		name string
		err  error
	}{
		{name: "not sent", err: fmt.Errorf("%w: status=0 dial tcp: connect: connection refused", ErrCompatibilityNotSent)},
		{name: "refused", err: fmt.Errorf("%w: status=%d bad token", ErrCompatibilityRefused, http.StatusUnauthorized)},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			// Materialize, not Build: this classification lives entirely in
			// the bridge path, and Build has no bridge since CHAOS-4924.
			store := &fakeStore{claim: testMaterializeClaim(time.Second)}
			handler, err := NewMaterializeHandler(store, classifyingExecutor{err: testCase.err}, nil)
			if err != nil {
				t.Fatal(err)
			}
			workErr := handler.Work(context.Background(), materializeExecution())
			if workErr == nil {
				t.Fatal("Work succeeded, want a retryable failure")
			}
			if !strings.Contains(workErr.Error(), string(jobruntime.CategoryRetryable)) {
				t.Fatalf("Work = %v, want category %s", workErr, jobruntime.CategoryRetryable)
			}
			if strings.Contains(workErr.Error(), string(jobruntime.CategoryPermanent)) {
				t.Fatalf("Work = %v, must not be Permanent", workErr)
			}
			// The whole point: this request must NOT be parked in a state
			// only a human /repair call can leave.
			if store.ambiguous != 0 {
				t.Fatalf("ambiguous releases = %d, want 0", store.ambiguous)
			}
			if store.completions != 0 {
				t.Fatalf("completions = %d, want 0", store.completions)
			}
		})
	}
}

func TestHandlerReleasesUnknownAmbiguousWithTheClassifiedDetail(t *testing.T) {
	executeErr := fmt.Errorf("%w: status=%d bridge exploded", ErrCompatibilityUnknown, http.StatusInternalServerError)
	// Materialize, not Build: see TestHandlerRetriesNotSentAndRefusedWithoutReleasingAmbiguous.
	store := &fakeStore{claim: testMaterializeClaim(time.Second)}
	handler, err := NewMaterializeHandler(store, classifyingExecutor{err: executeErr}, nil)
	if err != nil {
		t.Fatal(err)
	}
	workErr := handler.Work(context.Background(), materializeExecution())
	if workErr == nil || !strings.Contains(workErr.Error(), string(jobruntime.CategoryPermanent)) {
		t.Fatalf("Work = %v, want category %s", workErr, jobruntime.CategoryPermanent)
	}
	if store.ambiguous != 1 {
		t.Fatalf("ambiguous releases = %d, want 1", store.ambiguous)
	}
	// The fixed literal is exactly what made 22 ledger rows indistinguishable
	// from each other. The detail that reaches the store must now name the
	// classification, the status, and the executor's own text.
	detail := store.lastAmbiguousDetail
	if detail == "compatibility execution outcome is unknown" {
		t.Fatalf("ledger detail is still the fixed literal: %q", detail)
	}
	for _, want := range []string{"outcome is unknown", "status=500", "bridge exploded"} {
		if !strings.Contains(detail, want) {
			t.Fatalf("ledger detail %q is missing %q", detail, want)
		}
	}
	if length := utf8.RuneCountInString(detail); length == 0 || length > maxAmbiguousDetailBytes {
		t.Fatalf("ledger detail length = %d, want 1..%d", length, maxAmbiguousDetailBytes)
	}
}

// A pre-step failure is not a classified bridge outcome, so it keeps today's
// ambiguous release -- but it must still produce a NON-EMPTY detail, because
// PostgresStore.transition rejects an empty one outright.
func TestAmbiguousDetailIsNeverEmptyOrOverBound(t *testing.T) {
	if detail := compatibilityAmbiguousDetail(nil); detail == "" {
		t.Fatal("a nil error produced an empty ledger detail")
	}
	if detail := compatibilityAmbiguousDetail(errors.New("\x00\x01\x02")); detail == "" {
		t.Fatal("an all-control-character error produced an empty ledger detail")
	}
	long := compatibilityAmbiguousDetail(errors.New(strings.Repeat("verbose ", 4096)))
	if len(long) > maxAmbiguousDetailBytes {
		t.Fatalf("ledger detail is %d bytes, over the %d bound", len(long), maxAmbiguousDetailBytes)
	}
	if !utf8.ValidString(long) {
		t.Fatal("ledger detail is not valid UTF-8 after truncation")
	}
}

func TestSanitizeDetailStripsControlCharactersAndCutsOnRuneBoundaries(t *testing.T) {
	if got := sanitizeDetail("a\nb\tc", 64); got != "a b c" {
		t.Fatalf("sanitizeDetail = %q, want %q", got, "a b c")
	}
	if got := sanitizeDetail("   ", 64); got != "" {
		t.Fatalf("sanitizeDetail of whitespace = %q, want empty", got)
	}
	// Every rune here is 3 bytes; a naive byte cut would split one and
	// produce invalid UTF-8.
	multibyte := strings.Repeat("字", 40)
	for _, limit := range []int{1, 2, 3, 4, 7, 11, 100} {
		got := sanitizeDetail(multibyte, limit)
		if len(got) > limit {
			t.Fatalf("sanitizeDetail(limit=%d) = %d bytes", limit, len(got))
		}
		if !utf8.ValidString(got) {
			t.Fatalf("sanitizeDetail(limit=%d) = %q, not valid UTF-8", limit, got)
		}
	}
	if got := sanitizeDetail("anything", 0); got != "" {
		t.Fatalf("sanitizeDetail(limit=0) = %q, want empty", got)
	}
}

// The three sentinels must stay distinguishable from each other. A refactor
// that made any two of them the same value, or that dropped the ErrUnavailable
// wrapping, would silently restore the collapse this ticket removes.
func TestClassificationSentinelsAreDistinctAndWrapErrUnavailable(t *testing.T) {
	sentinels := map[string]error{
		"not sent": ErrCompatibilityNotSent,
		"refused":  ErrCompatibilityRefused,
		"unknown":  ErrCompatibilityUnknown,
	}
	for name, sentinel := range sentinels {
		if !errors.Is(sentinel, ErrUnavailable) {
			t.Fatalf("%s does not wrap ErrUnavailable", name)
		}
		for otherName, other := range sentinels {
			if name == otherName {
				continue
			}
			if errors.Is(sentinel, other) {
				t.Fatalf("%s is indistinguishable from %s", name, otherName)
			}
		}
	}
}

package workgraph

import (
	"context"
	"errors"
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
	handler, err := NewBuildHandler(store, blockingExecutor{delay: 80 * time.Millisecond})
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

func TestBuildLeaseLossCancelsCompatibilityAndCannotComplete(t *testing.T) {
	store := &fakeStore{claim: testClaim(30 * time.Millisecond), loseAt: 1}
	executor := blockingExecutor{waitForCancellation: true}
	handler, err := NewBuildHandler(store, executor)
	if err != nil {
		t.Fatal(err)
	}
	err = handler.Work(context.Background(), buildExecution())
	if err == nil || !strings.Contains(err.Error(), string(jobruntime.CategoryRetryable)) || store.completions != 0 {
		t.Fatalf("error=%v completions=%d", err, store.completions)
	}
}

func TestCompatibilityFailureIsAmbiguousNotRetried(t *testing.T) {
	store := &fakeStore{claim: testClaim(time.Second)}
	handler, err := NewBuildHandler(store, failingExecutor{})
	if err != nil {
		t.Fatal(err)
	}
	err = handler.Work(context.Background(), buildExecution())
	if err == nil || !strings.Contains(err.Error(), string(jobruntime.CategoryPermanent)) || store.ambiguous != 1 || store.completions != 0 {
		t.Fatalf("error=%v ambiguous=%d completions=%d", err, store.ambiguous, store.completions)
	}
}

func TestBuildRejectsTenantEnvelopeMismatchBeforeClaim(t *testing.T) {
	store := &fakeStore{claim: testClaim(time.Second)}
	handler, err := NewBuildHandler(store, failingExecutor{})
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

func buildExecution() *jobruntime.Execution[jobruntime.WorkGraphBuildArgs] {
	return &jobruntime.Execution[jobruntime.WorkGraphBuildArgs]{
		OrganizationID: pointer(testOrgID),
		Envelope:       jobcontract.Envelope{OrganizationID: pointer(testOrgID), Domain: jobcontract.DomainLink{Type: "work_graph_request", ID: testRequestID}},
		Args:           jobruntime.WorkGraphBuildArgs{EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.WorkGraphBuildPayload]{OrganizationID: pointer(testOrgID), Domain: jobcontract.DomainLink{Type: "work_graph_request", ID: testRequestID}, Payload: jobcontract.WorkGraphBuildPayload{RequestID: testRequestID}}},
	}
}

func testClaim(lease time.Duration) *Claim {
	return &Claim{Request: Request{ID: testRequestID, OrganizationID: testOrgID, Kind: KindBuild, Scope: []byte(`{"from_date":"2026-07-01"}`), LLMConcurrency: 1, SpendLimitMicrounits: 0, CorrelationID: "test", IdempotencyKey: "workgraph:test"}, Token: testToken, LeaseDuration: lease}
}
func pointer(value string) *string { return &value }

type fakeStore struct {
	claim                                            *Claim
	claims, renewals, completions, ambiguous, loseAt int
}

func (s *fakeStore) Claim(context.Context, string, Kind) (*Claim, error) {
	s.claims++
	return s.claim, nil
}
func (s *fakeStore) Renew(context.Context, Claim) error {
	s.renewals++
	if s.loseAt == s.renewals {
		return ErrLeaseLost
	}
	return nil
}
func (s *fakeStore) Complete(context.Context, Claim, []byte) error  { s.completions++; return nil }
func (*fakeStore) Fail(context.Context, Claim, string) error        { return nil }
func (s *fakeStore) Ambiguous(context.Context, Claim, string) error { s.ambiguous++; return nil }

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

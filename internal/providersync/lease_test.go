package providersync

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
)

func TestExpiredLeaseRecoveryUsesNewOwnerAndStableGeneration(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	repository := newMemoryLeaseRepository(testUnit(), "dispatching")
	first, err := repository.Claim(context.Background(), ClaimRequest{
		UnitID: firstUnitID, Owner: uuid.NewString(), Now: now, LeaseDuration: time.Minute,
		AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if first.Recovered || first.Attempt != 1 {
		t.Fatalf("first claim=%+v", first)
	}
	if _, err := repository.Claim(context.Background(), ClaimRequest{
		UnitID: firstUnitID, Owner: uuid.NewString(), Now: now.Add(30 * time.Second), LeaseDuration: time.Minute,
		AllowExpiredRecovery: true,
	}); !errors.Is(err, ErrUnitNotClaimable) {
		t.Fatalf("live lease recovery error=%v", err)
	}
	second, err := repository.Claim(context.Background(), ClaimRequest{
		UnitID: firstUnitID, Owner: uuid.NewString(), Now: now.Add(61 * time.Second), LeaseDuration: time.Minute,
		AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !second.Recovered || second.Attempt != 2 || second.Owner == first.Owner {
		t.Fatalf("recovered claim=%+v first=%+v", second, first)
	}
	if second.GenerationKey() != first.GenerationKey() {
		t.Fatalf("generation changed across kill recovery: %q != %q", second.GenerationKey(), first.GenerationKey())
	}
}

func TestLeaseSessionCancelsWorkWhenHeartbeatLosesClaim(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	repository := newMemoryLeaseRepository(testUnit(), "dispatching")
	claim, err := repository.Claim(context.Background(), ClaimRequest{
		UnitID: firstUnitID, Owner: uuid.NewString(), Now: now, LeaseDuration: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	repository.loseAfterRenewals = 1
	session := &LeaseSession{
		Repository: repository, Claim: claim, LeaseDuration: time.Second,
		Deadline: now.Add(time.Minute),
	}
	workObservedCancellation := false
	err = session.Run(context.Background(), time.Millisecond, func(ctx context.Context, guard providerfoundation.LeaseGuard) error {
		if assertErr := guard.Assert(ctx); assertErr != nil {
			return assertErr
		}
		<-ctx.Done()
		workObservedCancellation = true
		return ctx.Err()
	})
	if !errors.Is(err, ErrLeaseLost) || !workObservedCancellation {
		t.Fatalf("run error=%v cancellation=%v", err, workObservedCancellation)
	}
}

func TestLeaseSessionGuardRejectsTerminalRunBeforeProviderBoundary(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	repository := newMemoryLeaseRepository(testUnit(), "dispatching")
	claim, err := repository.Claim(context.Background(), ClaimRequest{
		UnitID: firstUnitID, Owner: uuid.NewString(), Now: now, LeaseDuration: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	repository.terminal = true
	session := &LeaseSession{
		Repository: repository, Claim: claim, LeaseDuration: time.Minute,
		Deadline: now.Add(time.Hour), Now: func() time.Time { return now.Add(time.Second) },
	}
	if err := session.Assert(context.Background()); !errors.Is(err, providerfoundation.ErrLeaseLost) {
		t.Fatalf("guard error=%v", err)
	}
}

func TestLeaseSessionRunRejectsUnsafeHeartbeatIntervals(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	repository := newMemoryLeaseRepository(testUnit(), "dispatching")
	claim, err := repository.Claim(context.Background(), ClaimRequest{
		UnitID: firstUnitID, Owner: uuid.NewString(), Now: now, LeaseDuration: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	session := &LeaseSession{
		Repository: repository, Claim: claim, LeaseDuration: time.Minute,
		Deadline: now.Add(time.Hour), Now: func() time.Time { return now },
	}
	work := func(context.Context, providerfoundation.LeaseGuard) error {
		t.Fatal("work ran with an unsafe heartbeat interval")
		return nil
	}
	for _, interval := range []time.Duration{0, 30*time.Second + time.Nanosecond, time.Minute} {
		if err := session.Run(context.Background(), interval, work); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("interval=%s error=%v", interval, err)
		}
	}
}

func TestLeaseSessionRunAcceptsHalfLeaseHeartbeatInterval(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	repository := newMemoryLeaseRepository(testUnit(), "dispatching")
	claim, err := repository.Claim(context.Background(), ClaimRequest{
		UnitID: firstUnitID, Owner: uuid.NewString(), Now: now, LeaseDuration: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	session := &LeaseSession{
		Repository: repository, Claim: claim, LeaseDuration: time.Minute,
		Deadline: now.Add(time.Hour), Now: func() time.Time { return now },
	}
	ran := false
	if err := session.Run(context.Background(), 30*time.Second, func(context.Context, providerfoundation.LeaseGuard) error {
		ran = true
		return nil
	}); err != nil || !ran {
		t.Fatalf("run error=%v ran=%v", err, ran)
	}
}

const (
	firstUnitID        = "11111111-1111-4111-8111-111111111111"
	firstRunID         = "22222222-2222-4222-8222-222222222222"
	firstIntegrationID = "33333333-3333-4333-8333-333333333333"
	firstSourceID      = "44444444-4444-4444-8444-444444444444"
	firstCredentialID  = "55555555-5555-4555-8555-555555555555"
)

func testUnit() Unit {
	return Unit{
		ID: firstUnitID, SyncRunID: firstRunID, OrgID: "org-acme",
		IntegrationID: firstIntegrationID, SourceID: firstSourceID,
		SourceExternalID: "acme/api", SourceName: "acme/api",
		Provider: "github", Dataset: "commits", CostClass: CostMedium,
		Mode: "incremental", CredentialID: firstCredentialID,
		AuthSource: "integration_credential",
	}
}

type memoryLeaseRepository struct {
	mu                sync.Mutex
	unit              Unit
	status            string
	owner             string
	expiresAt         time.Time
	attempts          int
	terminal          bool
	renewals          int
	loseAfterRenewals int
}

func newMemoryLeaseRepository(unit Unit, status string) *memoryLeaseRepository {
	return &memoryLeaseRepository{unit: unit, status: status}
}

func (repository *memoryLeaseRepository) Claim(_ context.Context, request ClaimRequest) (Claim, error) {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if request.validate() != nil || request.UnitID != repository.unit.ID || repository.terminal {
		return Claim{}, ErrUnitNotClaimable
	}
	recovered := repository.status == "running" && !repository.expiresAt.After(request.Now)
	if repository.status != "dispatching" && !(request.AllowExpiredRecovery && recovered) {
		return Claim{}, ErrUnitNotClaimable
	}
	repository.status = "running"
	repository.owner = request.Owner
	repository.expiresAt = request.Now.Add(request.LeaseDuration)
	repository.attempts++
	return Claim{
		Unit: repository.unit, Owner: request.Owner, Attempt: repository.attempts,
		LeaseExpiresAt: repository.expiresAt, Recovered: recovered,
	}, nil
}

func (repository *memoryLeaseRepository) Assert(_ context.Context, claim Claim, now time.Time) error {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if repository.terminal || repository.status != "running" || repository.owner != claim.Owner ||
		!repository.expiresAt.After(now) {
		return ErrLeaseLost
	}
	return nil
}

func (repository *memoryLeaseRepository) Renew(_ context.Context, claim Claim, now, expiresAt time.Time) error {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	repository.renewals++
	if repository.terminal || repository.status != "running" || repository.owner != claim.Owner ||
		!repository.expiresAt.After(now) ||
		(repository.loseAfterRenewals > 0 && repository.renewals > repository.loseAfterRenewals) {
		return ErrLeaseLost
	}
	repository.expiresAt = expiresAt
	return nil
}

var _ LeaseRepository = (*memoryLeaseRepository)(nil)

package providersync

import (
	"context"
	"errors"
	"reflect"
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

// scriptedLeaseRepository answers Assert from the claim owner and delegates
// Renew to a per-test function, so each test decides exactly when and how a
// renewal ends instead of racing a wall-clock ticker.
type scriptedLeaseRepository struct {
	owner string
	renew func(ctx context.Context, call int) error
	mu    sync.Mutex
	calls int
}

func (repository *scriptedLeaseRepository) Claim(context.Context, ClaimRequest) (Claim, error) {
	return Claim{}, ErrUnitNotClaimable
}

func (repository *scriptedLeaseRepository) Assert(_ context.Context, claim Claim, _ time.Time) error {
	if claim.Owner != repository.owner {
		return ErrLeaseLost
	}
	return nil
}

func (repository *scriptedLeaseRepository) Renew(ctx context.Context, claim Claim, now, expiresAt time.Time) error {
	repository.mu.Lock()
	repository.calls++
	call := repository.calls
	repository.mu.Unlock()
	if claim.Owner != repository.owner || !expiresAt.After(now) {
		return ErrLeaseLost
	}
	return repository.renew(ctx, call)
}

func scriptedLeaseSession(t *testing.T, renew func(context.Context, int) error) (*LeaseSession, Claim) {
	t.Helper()
	now := time.Now().UTC()
	claim := Claim{Unit: testUnit(), Owner: uuid.NewString(), Attempt: 1, LeaseExpiresAt: now.Add(time.Second)}
	if err := claim.Validate(); err != nil {
		t.Fatal(err)
	}
	return &LeaseSession{
		Repository:    &scriptedLeaseRepository{owner: claim.Owner, renew: renew},
		Claim:         claim,
		LeaseDuration: time.Second,
		Deadline:      now.Add(time.Minute),
	}, claim
}

func TestLeaseSessionCancelsWorkWhenHeartbeatLosesClaim(t *testing.T) {
	t.Parallel()
	session, _ := scriptedLeaseSession(t, func(_ context.Context, call int) error {
		if call >= 2 {
			return ErrLeaseLost
		}
		return nil
	})
	var cause error
	err := session.Run(context.Background(), time.Millisecond, func(ctx context.Context, _ providerfoundation.LeaseGuard) error {
		<-ctx.Done()
		cause = context.Cause(ctx)
		return ctx.Err()
	})
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("run error=%v", err)
	}
	if !errors.Is(cause, ErrLeaseLost) || !errors.Is(cause, providerfoundation.ErrLeaseLost) {
		t.Fatalf("work cancellation cause=%v", cause)
	}
}

func TestLeaseSessionRunReportsLeaseLossOnHeartbeatStoreError(t *testing.T) {
	t.Parallel()
	storeErr := errors.New("connection reset by peer")
	session, _ := scriptedLeaseSession(t, func(context.Context, int) error { return storeErr })
	workErr := errors.New("work stopped")
	var cause error
	err := session.Run(context.Background(), time.Millisecond, func(ctx context.Context, _ providerfoundation.LeaseGuard) error {
		<-ctx.Done()
		cause = context.Cause(ctx)
		return workErr
	})
	if !errors.Is(err, ErrLeaseLost) || errors.Is(err, workErr) || errors.Is(err, storeErr) {
		t.Fatalf("run error=%v", err)
	}
	if !errors.Is(cause, ErrLeaseLost) || !errors.Is(cause, providerfoundation.ErrLeaseLost) {
		t.Fatalf("work cancellation cause=%v", cause)
	}
}

func TestLeaseSessionRunReportsLeaseLossWhenDeadlinePasses(t *testing.T) {
	t.Parallel()
	session, _ := scriptedLeaseSession(t, func(context.Context, int) error { return nil })
	start := time.Now().UTC()
	var ticks sync.Mutex
	calls := 0
	session.Now = func() time.Time {
		ticks.Lock()
		defer ticks.Unlock()
		calls++
		if calls > 1 {
			return session.Deadline
		}
		return start
	}
	var cause error
	err := session.Run(context.Background(), time.Millisecond, func(ctx context.Context, _ providerfoundation.LeaseGuard) error {
		<-ctx.Done()
		cause = context.Cause(ctx)
		return nil
	})
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("run error=%v", err)
	}
	if !errors.Is(cause, ErrLeaseLost) || !errors.Is(cause, providerfoundation.ErrLeaseLost) {
		t.Fatalf("work cancellation cause=%v", cause)
	}
}

func TestLeaseSessionRunReturnsWorkResultWhenWorkEndsFirst(t *testing.T) {
	t.Parallel()
	workErr := errors.New("provider returned 422")
	for _, want := range []error{nil, workErr} {
		session, _ := scriptedLeaseSession(t, func(context.Context, int) error { return nil })
		var workContext context.Context
		err := session.Run(context.Background(), 500*time.Millisecond, func(ctx context.Context, _ providerfoundation.LeaseGuard) error {
			workContext = ctx
			return want
		})
		if err != want {
			t.Fatalf("want=%v run error=%v", want, err)
		}
		if cause := context.Cause(workContext); cause != context.Canceled {
			t.Fatalf("want=%v work context cause=%v", want, cause)
		}
	}
}

// A renewal in flight when the work returns is awaited, and the store's
// answer to it decides the outcome. The renewal runs on the caller's context,
// so the work returning does not cancel it.
func TestLeaseSessionRunKeepsWorkResultWhenInFlightRenewalIsGrantedAfterWorkReturns(t *testing.T) {
	t.Parallel()
	for _, want := range []error{nil, errors.New("provider returned 422")} {
		entered := make(chan struct{})
		workFinished := make(chan struct{})
		var once sync.Once
		session, _ := scriptedLeaseSession(t, func(ctx context.Context, _ int) error {
			once.Do(func() { close(entered) })
			<-workFinished
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(50 * time.Millisecond):
				return nil
			}
		})
		err := session.Run(context.Background(), time.Millisecond, func(context.Context, providerfoundation.LeaseGuard) error {
			defer close(workFinished)
			<-entered
			return want
		})
		if err != want {
			t.Fatalf("want=%v run error=%v", want, err)
		}
	}
}

func TestLeaseSessionRunReportsLeaseLossWhenInFlightRenewalIsRefusedAfterWorkReturns(t *testing.T) {
	t.Parallel()
	entered := make(chan struct{})
	workFinished := make(chan struct{})
	var once sync.Once
	session, _ := scriptedLeaseSession(t, func(context.Context, int) error {
		once.Do(func() { close(entered) })
		<-workFinished
		return ErrLeaseLost
	})
	err := session.Run(context.Background(), time.Millisecond, func(context.Context, providerfoundation.LeaseGuard) error {
		defer close(workFinished)
		<-entered
		return nil
	})
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("run error=%v", err)
	}
}

// sessionClockAhead makes the session's clock run offset ahead of the wall
// clock, so the claim-time grant has only a short time left on that clock.
func sessionClockAhead(session *LeaseSession, offset time.Duration) {
	session.Now = func() time.Time { return time.Now().UTC().Add(offset) }
}

func TestLeaseSessionRunReportsLeaseLossWhenRenewalOutlivesGrantedExpiry(t *testing.T) {
	t.Parallel()
	entered := make(chan struct{})
	var once sync.Once
	session, _ := scriptedLeaseSession(t, func(ctx context.Context, _ int) error {
		once.Do(func() { close(entered) })
		<-ctx.Done()
		return ctx.Err()
	})
	sessionClockAhead(session, 900*time.Millisecond)
	result := make(chan error, 1)
	go func() {
		result <- session.Run(context.Background(), time.Millisecond, func(context.Context, providerfoundation.LeaseGuard) error {
			<-entered
			return nil
		})
	}()
	select {
	case err := <-result:
		if !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("run error=%v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run did not return after the granted expiry passed")
	}
}

func TestLeaseSessionRunKeepsRenewingPastTheClaimTimeExpiry(t *testing.T) {
	t.Parallel()
	session, _ := scriptedLeaseSession(t, func(ctx context.Context, _ int) error { return ctx.Err() })
	sessionClockAhead(session, 900*time.Millisecond)
	err := session.Run(context.Background(), 20*time.Millisecond, func(ctx context.Context, guard providerfoundation.LeaseGuard) error {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-time.After(300 * time.Millisecond):
		}
		return guard.Assert(ctx)
	})
	if err != nil {
		t.Fatalf("run error=%v", err)
	}
}

func TestLeaseSessionRunReturnsWorkResultWhenCallerContextEnds(t *testing.T) {
	t.Parallel()
	callerCause := errors.New("worker shutting down")
	entered := make(chan struct{})
	var once sync.Once
	session, _ := scriptedLeaseSession(t, func(ctx context.Context, _ int) error {
		once.Do(func() { close(entered) })
		<-ctx.Done()
		return ctx.Err()
	})
	parent, cancelParent := context.WithCancelCause(context.Background())
	var cause error
	err := session.Run(parent, time.Millisecond, func(ctx context.Context, _ providerfoundation.LeaseGuard) error {
		<-entered
		cancelParent(callerCause)
		<-ctx.Done()
		cause = context.Cause(ctx)
		return ctx.Err()
	})
	if !errors.Is(err, context.Canceled) || errors.Is(err, ErrLeaseLost) {
		t.Fatalf("run error=%v", err)
	}
	if cause != callerCause {
		t.Fatalf("work cancellation cause=%v", cause)
	}
}

// Work functions read session.Claim while the heartbeat renews; the claim a
// session was built with is the claim it keeps.
func TestLeaseSessionRunLeavesClaimUnchangedAcrossRenewals(t *testing.T) {
	t.Parallel()
	renewed := make(chan struct{}, 8)
	session, claim := scriptedLeaseSession(t, func(context.Context, int) error {
		select {
		case renewed <- struct{}{}:
		default:
		}
		return nil
	})
	err := session.Run(context.Background(), time.Millisecond, func(ctx context.Context, guard providerfoundation.LeaseGuard) error {
		for range 3 {
			<-renewed
			if observed := session.Claim; !reflect.DeepEqual(observed, claim) {
				t.Errorf("claim during work=%+v want=%+v", observed, claim)
			}
			if err := guard.Assert(ctx); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("run error=%v", err)
	}
	if !reflect.DeepEqual(session.Claim, claim) {
		t.Fatalf("claim after run=%+v want=%+v", session.Claim, claim)
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
	mu        sync.Mutex
	unit      Unit
	status    string
	owner     string
	expiresAt time.Time
	attempts  int
	terminal  bool
	renewals  int
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
		!repository.expiresAt.After(now) {
		return ErrLeaseLost
	}
	repository.expiresAt = expiresAt
	return nil
}

var _ LeaseRepository = (*memoryLeaseRepository)(nil)

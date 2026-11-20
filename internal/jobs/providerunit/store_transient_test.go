package providerunit

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// storeWriteRepository refuses Complete and/or Fail with a fixed error, the
// way PostgresRepository answers once its in-place retry is spent.
type storeWriteRepository struct {
	*memoryUnitRepository
	completeErr   error
	failErr       error
	completeCalls int
	failCalls     int
}

func (repository *storeWriteRepository) Complete(
	ctx context.Context, claim providersync.Claim, result map[string]any,
	watermark *time.Time, startedAt, completedAt time.Time,
) error {
	repository.completeCalls++
	if repository.completeErr != nil {
		return repository.completeErr
	}
	return repository.memoryUnitRepository.Complete(ctx, claim, result, watermark, startedAt, completedAt)
}

func (repository *storeWriteRepository) Fail(
	ctx context.Context, claim providersync.Claim, category string, startedAt, completedAt time.Time,
) error {
	repository.failCalls++
	if repository.failErr != nil {
		return repository.failErr
	}
	return repository.memoryUnitRepository.Fail(ctx, claim, category, startedAt, completedAt)
}

func transientStoreWrite(write, step string) error {
	return &providersync.StoreTransientError{Write: write, Step: step, SQLState: "40P01"}
}

func storeWriteHandler(
	t *testing.T, repository *storeWriteRepository, now time.Time, deterministicFailure bool,
) *Handler {
	t.Helper()
	build := successfulExecutor(t, now)
	return &Handler{
		Repository:    repository,
		LeaseDuration: time.Minute,
		Heartbeat:     10 * time.Second,
		Now:           func() time.Time { return now },
		BuildExecutor: func(session *providersync.LeaseSession) (providersync.CompleteRouteExecutor, error) {
			executor, err := build(session)
			if err != nil || !deterministicFailure {
				return executor, err
			}
			executor.Committer.Ledger = &ambiguousAfterFirstCommitLedger{}
			return executor, nil
		},
	}
}

func safeCauseOf(t *testing.T, err error) string {
	t.Helper()
	cause, ok := jobruntime.SafeCause(err)
	if !ok {
		t.Fatalf("Work()=%v carries no safe cause", err)
	}
	return cause
}

// H1 + C4: a Complete refused transiently -- at the bump or at the unit
// UPDATE itself -- after the repository's in-place retry releases the unit on
// a non-final attempt instead of failing it. Before CHAOS-14371 the refusal was
// ErrInvalidConfiguration ("unclassified", cap 2): attempt 2 FAILED a unit
// whose provider work had succeeded.
func TestCompleteStoreTransientReleasesTheUnitNotFailsIt(t *testing.T) {
	t.Parallel()
	for _, step := range []string{"bump", "unit_update"} {
		t.Run(step, func(t *testing.T) {
			t.Parallel()
			now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
			unit := providerUnit()
			repository := &storeWriteRepository{
				memoryUnitRepository: newMemoryUnitRepository(unit),
				completeErr:          transientStoreWrite("complete", step),
			}
			err := storeWriteHandler(t, repository, now, false).Work(context.Background(), providerExecution(unit, now, 2))
			if err == nil || !errors.Is(err, providersync.ErrStoreTransient) {
				t.Fatalf("Work()=%v, want the transient store error", err)
			}
			if repository.completeCalls != 1 || repository.failCalls != 0 {
				t.Fatalf("complete calls=%d fail calls=%d, want 1 and 0", repository.completeCalls, repository.failCalls)
			}
			if repository.releaseCalls != 1 || repository.status != "dispatching" {
				t.Fatalf("release calls=%d status=%q, want one release to dispatching", repository.releaseCalls, repository.status)
			}
			cause := safeCauseOf(t, err)
			for _, want := range []string{"category=" + retryCauseCompleteWriteTransient, "store_step=" + step, "sqlstate=40P01"} {
				if !strings.Contains(cause, want) {
					t.Fatalf("safe cause %q lacks %q", cause, want)
				}
			}
			if class := causeClass(err, false); class != causeClassStoreTransient {
				t.Fatalf("cause class=%q, want %q", class, causeClassStoreTransient)
			}
			if limit := retryMaxAttempts(err); limit != 0 {
				t.Fatalf("retryMaxAttempts=%d, want 0 (registry ceiling, not the unclassified cap)", limit)
			}
		})
	}
}

// C4, the other mapping: a Complete refused with ErrLeaseLost (a unit UPDATE
// that matched no row, or a non-transient error) keeps today's answer -- the
// ordinary arm, which on attempt 2 records the exhausted failure.
func TestCompleteLeaseLostKeepsTheOrdinaryArm(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	unit := providerUnit()
	repository := &storeWriteRepository{
		memoryUnitRepository: newMemoryUnitRepository(unit),
		completeErr:          providersync.ErrLeaseLost,
	}
	err := storeWriteHandler(t, repository, now, false).Work(context.Background(), providerExecution(unit, now, 2))
	if err == nil || errors.Is(err, providersync.ErrStoreTransient) {
		t.Fatalf("Work()=%v, want a non-transient failure", err)
	}
	if repository.failCalls != 1 || repository.releaseCalls != 0 {
		t.Fatalf("fail calls=%d release calls=%d, want the exhausted Fail and no release", repository.failCalls, repository.releaseCalls)
	}
	if strings.Contains(safeCauseOf(t, err), "store_transient") {
		t.Fatal("a lease-lost Complete was reported as a transient store write")
	}
}

// H2: a durable Fail refused transiently releases the unit, so it does not hold
// its slot until its lease expires (before: Retryable with the unit running).
// A non-transient refusal keeps the unit as it is.
func TestFailStoreTransientReleasesTheUnit(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name        string
		failErr     error
		wantRelease int
		wantCause   string
	}{
		{"transient", transientStoreWrite("fail", "bump"), 1, "category=" + retryCauseFailWriteTransient},
		{"transient_unit_update", transientStoreWrite("fail", "unit_update"), 1, "category=" + retryCauseFailWriteTransient},
		{"lease_lost", providersync.ErrLeaseLost, 0, "category=" + retryCauseFailWriteFailed},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
			unit := providerUnit()
			repository := &storeWriteRepository{
				memoryUnitRepository: newMemoryUnitRepository(unit),
				failErr:              testCase.failErr,
			}
			err := storeWriteHandler(t, repository, now, true).Work(context.Background(), providerExecution(unit, now, 1))
			if err == nil {
				t.Fatal("Work() succeeded; the premise needs a refused Fail")
			}
			if repository.failCalls != 1 {
				t.Fatalf("fail calls=%d, want 1 (premise: the deterministic arm wrote the Fail)", repository.failCalls)
			}
			if repository.releaseCalls != testCase.wantRelease {
				t.Fatalf("release calls=%d, want %d", repository.releaseCalls, testCase.wantRelease)
			}
			if cause := safeCauseOf(t, err); !strings.Contains(cause, testCase.wantCause) {
				t.Fatalf("safe cause %q lacks %q", cause, testCase.wantCause)
			}
		})
	}
}

// H3: on the final attempt River discards the job, so a released unit would be
// stranded dispatching; the unit is left for lease repair.
func TestStoreTransientOnTheFinalAttemptDoesNotRelease(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name          string
		completeErr   error
		failErr       error
		deterministic bool
	}{
		{name: "complete", completeErr: transientStoreWrite("complete", "commit")},
		{name: "fail", failErr: transientStoreWrite("fail", "bump"), deterministic: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
			unit := providerUnit()
			repository := &storeWriteRepository{
				memoryUnitRepository: newMemoryUnitRepository(unit),
				completeErr:          testCase.completeErr,
				failErr:              testCase.failErr,
			}
			err := storeWriteHandler(t, repository, now, testCase.deterministic).
				Work(context.Background(), providerExecution(unit, now, 5))
			if !errors.Is(err, providersync.ErrStoreTransient) {
				t.Fatalf("Work()=%v, want the transient store error", err)
			}
			if repository.releaseCalls != 0 || repository.status != "running" {
				t.Fatalf("release calls=%d status=%q, want no release on the final attempt", repository.releaseCalls, repository.status)
			}
		})
	}
}

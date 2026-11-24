package providerunit

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// THROWAWAY PROBE (CHAOS-14574 r1 opinion; never merged): the progress refund's UPDATE commits and the reply is
// lost (the write returns an error after the fake applied it).
type probeLostReplyRefund struct {
	*exhaustReproRepository
	committed int
}

func (repository *probeLostReplyRefund) RefundChunkProgress(
	ctx context.Context, claim providersync.Claim, start int, availableAt, now time.Time,
) (bool, error) {
	refunded, err := repository.exhaustReproRepository.RefundChunkProgress(ctx, claim, start, availableAt, now)
	if err != nil || !refunded {
		return refunded, err
	}
	repository.committed++
	return false, errors.New("probe: connection lost after COMMIT")
}

func TestProbeLostReplyAfterCommittedRefund(t *testing.T) {
	for _, attempt := range []int{2, 5} {
		t.Run(fmt.Sprintf("attempt_%d", attempt), func(t *testing.T) {
			now := time.Date(2026, 11, 24, 8, 0, 0, 0, time.UTC)
			unit := providerUnit()
			capability, _ := providersync.Capability("github", "cicd")
			descriptor, _ := providersync.Descriptor("github", "cicd")
			unit.SourceExternalID, unit.SourceName = "acme/api", "acme/api"
			unit.Provider, unit.Dataset, unit.CostClass = "github", "cicd", capability.CostClass
			doer := &flakyPageDoer{budget: exhaustReproHTTPTries, left: map[int]int{2: exhaustReproHTTPTries}}
			repository := &probeLostReplyRefund{exhaustReproRepository: &exhaustReproRepository{
				classRepository: &classRepository{memoryUnitRepository: newMemoryUnitRepository(unit)}}}
			var ledger *providersync.InProcessChunkLedger
			var ledgerClaim providersync.Claim
			repository.runCreatedAt = now
			repository.chunkNextOrdinal = func(at time.Time) (int, bool) {
				if ledger == nil {
					return 0, false
				}
				checkpoint, err := ledger.LoadChunkCheckpoint(context.Background(), ledgerClaim, at)
				if err != nil {
					return 0, false
				}
				return checkpoint.NextOrdinal, true
			}
			handler := &Handler{
				Repository: repository, LeaseDuration: time.Minute, Heartbeat: 10 * time.Second,
				Now: func() time.Time { return now },
				BuildExecutor: func(session *providersync.LeaseSession) (providersync.CompleteRouteExecutor, error) {
					if ledger == nil {
						ledger, ledgerClaim = providersync.NewInProcessChunkLedger(session.Claim), session.Claim
					}
					return providersync.CompleteRouteExecutor{
						Credentials: providerfoundation.CredentialResolver{
							Repository: githubCredentialRepository{unit: session.Claim.Unit},
							Decryptor:  githubCredentialDecryptor{},
						},
						Doer:         fakehttp.Client(doer),
						Retry:        providerfoundation.RetryPolicy{MaxAttempts: exhaustReproHTTPTries, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
						Budget:       testBudgetStore{},
						BudgetLimits: map[providersync.CostClass]int{capability.CostClass: 1},
						BudgetTTL:    time.Minute,
						Gate: func(providersync.Claim, *providerfoundation.HTTPClient) providerfoundation.BackoffGate {
							return testBackoffGate{}
						},
						Handler:    pagedWalkHandler{pages: exhaustReproPages, destinations: descriptor.Destinations},
						Comparator: matchingComparator{},
						Committer: providersync.EffectCommitter{
							Ledger: ledger, Sink: &droppingSink{}, Readback: testEffectReadback{}, Now: func() time.Time { return now },
						},
						HeartbeatInterval: 10 * time.Second,
						Now:               func() time.Time { return now },
					}, nil
				},
			}
			execution := providerExecution(unit, now, attempt)
			execution.Definition.MaxAttempts = 5
			execution.Definition.RetryPolicy = "bounded_exponential_jitter"
			err := handler.Work(context.Background(), execution)
			_, snoozed := jobruntime.SnoozeDelay(err)
			river := "retry (River attempt charged)"
			switch {
			case err == nil:
				river = "completed"
			case snoozed:
				river = "snooze (attempt-neutral)"
			case isPermanentReturn(err) || attempt >= execution.Definition.MaxAttempts:
				river = "discard (no River job left)"
			}
			t.Logf("TIP refund, River attempt %d of 5: committed=%d handler_return=%q river=%s "+
				"unit_status=%q unit_attempts_after=%d (claim +1, refund -1) lease_held=%v releases=%d fails=%d",
				attempt, repository.committed, err, river, repository.status, repository.attempt,
				repository.status == "running", repository.releases, repository.failures)
			if repository.committed != 1 || snoozed {
				t.Fatalf("probe premise: committed=%d snoozed=%v", repository.committed, snoozed)
			}
			t.Errorf("GAP: the %s committed (unit_status=%q lease_held=%v) but the handler returned %q: River %s",
				"refund", repository.status, repository.status == "running", err, river)
		})
	}
}

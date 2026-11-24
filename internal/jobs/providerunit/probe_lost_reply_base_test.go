package providerunit

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// THROWAWAY PROBE (CHAOS-14574 r1 opinion; never merged): the plain chunk continuation's UPDATE commits and the
// reply is lost (the write returns an error after the fake applied it).
type probeLostReplyContinuation struct {
	*memoryUnitRepository
	committed int
}

func (repository *probeLostReplyContinuation) DeferChunkContinuation(
	ctx context.Context, claim providersync.Claim, availableAt, now time.Time,
) error {
	if err := repository.memoryUnitRepository.DeferChunkContinuation(ctx, claim, availableAt, now); err != nil {
		return err
	}
	repository.committed++
	return errors.New("probe: connection lost after COMMIT")
}

func TestProbeLostReplyAfterCommittedPlainContinuation(t *testing.T) {
	for _, attempt := range []int{2, 5} {
		t.Run(fmt.Sprintf("attempt_%d", attempt), func(t *testing.T) {
			now := time.Date(2026, 11, 24, 8, 0, 0, 0, time.UTC)
			unit := providerUnit()
			repository := &probeLostReplyContinuation{memoryUnitRepository: newMemoryUnitRepository(unit)}
			handler := &Handler{
				Repository: repository, LeaseDuration: time.Minute, Heartbeat: 10 * time.Second,
				Now: func() time.Time { return now },
				BuildExecutor: func(*providersync.LeaseSession) (providersync.CompleteRouteExecutor, error) {
					return providersync.CompleteRouteExecutor{}, providersync.ChunkContinuationError{
						Next: now.Add(time.Second), Reason: providersync.ChunkStopChunkBound, Chunks: 64,
					}
				},
			}
			execution := providerExecution(unit, now, attempt)
			execution.Definition.MaxAttempts = 5
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
			t.Logf("BASE plain continuation, River attempt %d of 5: committed=%d handler_return=%q river=%s "+
				"unit_status=%q lease_held=%v releases=%d fails=%d",
				attempt, repository.committed, err, river, repository.status,
				repository.status == "running", repository.releaseCalls, repository.failures)
			if repository.committed != 1 || snoozed {
				t.Fatalf("probe premise: committed=%d snoozed=%v", repository.committed, snoozed)
			}
			t.Errorf("GAP: the %s committed (unit_status=%q lease_held=%v) but the handler returned %q: River %s",
				"continuation", repository.status, repository.status == "running", err, river)
		})
	}
}

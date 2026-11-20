package providerunit

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// claimRefusingRepository refuses every claim with err.
type claimRefusingRepository struct {
	*memoryUnitRepository
	err error
}

func (repository claimRefusingRepository) Claim(context.Context, providersync.ClaimRequest) (providersync.Claim, error) {
	return providersync.Claim{}, repository.err
}

func runRefusedClaim(t *testing.T, refusal error) (error, []map[string]any) {
	t.Helper()
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	unit := providerUnit()
	var output bytes.Buffer
	handler := &Handler{
		Repository:    claimRefusingRepository{memoryUnitRepository: newMemoryUnitRepository(unit), err: refusal},
		LeaseDuration: time.Minute,
		Heartbeat:     10 * time.Second,
		Now:           func() time.Time { return now },
		BuildExecutor: func(*providersync.LeaseSession) (providersync.CompleteRouteExecutor, error) {
			t.Error("a refused claim reached the provider executor")
			return providersync.CompleteRouteExecutor{}, ErrUnavailable
		},
	}
	execution := providerExecution(unit, now, 1)
	execution.JobID = 7
	execution.Definition.Kind = jobcontract.KindSyncProviderUnit
	execution.Definition.Queue = "sync_provider"
	execution.Logger = slog.New(slog.NewJSONHandler(&output, nil))
	err := handler.Work(context.Background(), execution)
	if output.Len() == 0 {
		return err, nil
	}
	return err, providerUnitLifecycleRecords(t, output.Bytes())
}

// A unit an operator cancel closed: the job completes without work and one Info line says which
// unit and why.
func TestWorkCompletesAJobWhoseUnitAnOperatorCancelClosed(t *testing.T) {
	t.Parallel()
	closed := &providersync.UnitClosedError{
		Category: providersync.OperatorCancelCategory, SyncRunID: "run-1",
		Provider: "github", Dataset: "commits", Mode: "backfill",
	}
	if !errors.Is(closed, providersync.ErrUnitNotClaimable) {
		t.Fatal("UnitClosedError must still be ErrUnitNotClaimable for callers that check only that")
	}
	err, records := runRefusedClaim(t, closed)
	if err != nil {
		t.Fatalf("Work() error = %v, want nil: the job completes", err)
	}
	if len(records) != 1 {
		t.Fatalf("records=%#v, want one sync_provider_unit_finished line", records)
	}
	for key, want := range map[string]any{
		"level": "INFO", "msg": "sync_provider_unit_finished", "result": "unit_closed", "closed_by": "cancelled",
		"sync_run_id": "run-1", "provider": "github", "dataset": "commits", "mode": "backfill",
		"kind": jobcontract.KindSyncProviderUnit, "queue": "sync_provider", "job_id": float64(7), "attempt": float64(1),
	} {
		if records[0][key] != want {
			t.Errorf("record[%q]=%#v want %#v; record=%#v", key, records[0][key], want, records[0])
		}
	}
	if id, _ := records[0]["sync_unit_id"].(string); id == "" {
		t.Errorf("record has no sync_unit_id: %#v", records[0])
	}
	if _, present := records[0]["error_detail"]; present {
		t.Errorf("record carries error_detail: %#v", records[0])
	}
}

// Any other refusal keeps today's retry and logs no lifecycle line.
func TestWorkRetriesAJobWhoseUnitIsNotClaimableForNow(t *testing.T) {
	t.Parallel()
	err, records := runRefusedClaim(t, providersync.ErrUnitNotClaimable)
	if err == nil || !errors.Is(err, providersync.ErrUnitNotClaimable) {
		t.Fatalf("Work() error = %v, want the retryable ErrUnitNotClaimable", err)
	}
	if len(records) != 0 {
		t.Fatalf("records=%#v, want none", records)
	}
}

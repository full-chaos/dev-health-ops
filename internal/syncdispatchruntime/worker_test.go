package syncdispatchruntime

import (
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/riverqueue/river"
)

type recordingBridge struct {
	calls         []string
	teamReference DomainReference
}

func (bridge *recordingBridge) Dispatch(_ context.Context, _ DispatchSyncRunArgs) error {
	bridge.calls = append(bridge.calls, "dispatch")
	return nil
}

func (bridge *recordingBridge) Finalize(_ context.Context, _ FinalizeSyncRunArgs) error {
	bridge.calls = append(bridge.calls, "finalize")
	return nil
}

func (bridge *recordingBridge) Discover(_ context.Context, _ ReferenceDiscoveryArgs) error {
	bridge.calls = append(bridge.calls, "discover")
	return nil
}

func (bridge *recordingBridge) TeamAutoImport(_ context.Context, reference DomainReference) error {
	bridge.calls = append(bridge.calls, "team_autoimport")
	bridge.teamReference = reference
	return nil
}

func TestCoordinatorWorkersCallTheirDirectBridgeSeams(t *testing.T) {
	t.Parallel()
	bridge := &recordingBridge{}
	// dispatchWorker, finalizeWorker, and referenceDiscoveryWorker no
	// longer hold a bridge (CHAOS-4175: dispatch_sync_run,
	// finalize_sync_run, and run_sync_reference_discovery are all native
	// now) -- their own seams are exercised by
	// native_dispatch_sync_run_service_integration_test.go,
	// native_finalize_sync_run_integration_test.go, and
	// native_reference_discovery_integration_test.go against a real
	// Postgres, not here. teamAutoimportWorker is the last coordinator
	// still bridge-based (sync.team_autoimport is a bounded registry kind,
	// not one of the four sync-dispatch coordinator kinds this ticket
	// ports).
	teamArgs := TeamAutoimportJobArgs{
		Version:       ContractVersionV1,
		OrgID:         testOrg,
		CorrelationID: "post-sync-" + testRun,
		Idempotency:   "post-sync:" + testRun + ":team_autoimport",
		Domain:        jobcontract.DomainLink{Type: "sync_run", ID: testRun},
		Payload:       jobcontract.TeamAutoimportPayload{SyncRunID: testRun},
	}
	if err := (&teamAutoimportWorker{bridge: bridge}).Work(context.Background(), &river.Job[TeamAutoimportJobArgs]{Args: teamArgs}); err != nil {
		t.Fatal(err)
	}
	if got, want := len(bridge.calls), 1; got != want || bridge.calls[0] != "team_autoimport" {
		t.Fatalf("bridge calls=%#v", bridge.calls)
	}
	if bridge.teamReference != (DomainReference{OrganizationID: testOrg, SyncRunID: testRun}) {
		t.Fatalf("team reference=%#v", bridge.teamReference)
	}
}

func TestCoordinatorWorkersFailClosedWithoutBridgeOrJob(t *testing.T) {
	t.Parallel()
	if err := (&dispatchWorker{}).Work(context.Background(), nil); err != ErrWorkerRegistration {
		t.Fatalf("dispatch worker error=%v", err)
	}
	if err := (&postSyncWorker{}).Work(context.Background(), nil); err != ErrWorkerRegistration {
		t.Fatalf("post-sync worker error=%v", err)
	}
	if err := (&finalizeWorker{}).Work(context.Background(), nil); err != ErrWorkerRegistration {
		t.Fatalf("finalize worker error=%v", err)
	}
	if err := (&referenceDiscoveryWorker{}).Work(context.Background(), nil); err != ErrWorkerRegistration {
		t.Fatalf("reference discovery worker error=%v", err)
	}
	if err := (&teamAutoimportWorker{}).Work(context.Background(), nil); err != ErrWorkerRegistration {
		t.Fatalf("team autoimport worker error=%v", err)
	}
	if err := (&teamRepoOwnershipDerivationWorker{}).Work(context.Background(), nil); err != ErrWorkerRegistration {
		t.Fatalf("team repo ownership derivation worker error=%v", err)
	}
}

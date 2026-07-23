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
	base := TransportArgs{Version: ContractVersionV1, OrgID: testOrg, RunID: testRun, DispatchOutbox: testOutbox, RouteGeneration: 1}
	if err := (&dispatchWorker{bridge: bridge}).Work(context.Background(), &river.Job[DispatchSyncRunArgs]{Args: DispatchSyncRunArgs{TransportArgs: base}}); err != nil {
		t.Fatal(err)
	}
	if err := (&finalizeWorker{bridge: bridge}).Work(context.Background(), &river.Job[FinalizeSyncRunArgs]{Args: FinalizeSyncRunArgs{TransportArgs: base}}); err != nil {
		t.Fatal(err)
	}
	if err := (&referenceDiscoveryWorker{bridge: bridge}).Work(context.Background(), &river.Job[ReferenceDiscoveryArgs]{Args: ReferenceDiscoveryArgs{TransportArgs: base}}); err != nil {
		t.Fatal(err)
	}
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
	if got, want := len(bridge.calls), 4; got != want || bridge.calls[0] != "dispatch" || bridge.calls[1] != "finalize" || bridge.calls[2] != "discover" || bridge.calls[3] != "team_autoimport" {
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
	if err := (&teamAutoimportWorker{}).Work(context.Background(), nil); err != ErrWorkerRegistration {
		t.Fatalf("team autoimport worker error=%v", err)
	}
}

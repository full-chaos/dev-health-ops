package syncdispatchruntime

import (
	"context"
	"testing"

	"github.com/riverqueue/river"
)

type recordingBridge struct{ calls []string }

func (bridge *recordingBridge) Dispatch(_ context.Context, _ DispatchSyncRunArgs) error {
	bridge.calls = append(bridge.calls, "dispatch")
	return nil
}

func (bridge *recordingBridge) Finalize(_ context.Context, _ FinalizeSyncRunArgs) error {
	bridge.calls = append(bridge.calls, "finalize")
	return nil
}

func (bridge *recordingBridge) PostSync(_ context.Context, _ PostSyncArgs) error {
	bridge.calls = append(bridge.calls, "post_sync")
	return nil
}

func (bridge *recordingBridge) Discover(_ context.Context, _ ReferenceDiscoveryArgs) error {
	bridge.calls = append(bridge.calls, "discover")
	return nil
}

func (bridge *recordingBridge) TeamAutoImport(context.Context, DomainReference) error { return nil }

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
	if err := (&postSyncWorker{bridge: bridge}).Work(context.Background(), &river.Job[PostSyncArgs]{Args: PostSyncArgs{TransportArgs: base}}); err != nil {
		t.Fatal(err)
	}
	if err := (&referenceDiscoveryWorker{bridge: bridge}).Work(context.Background(), &river.Job[ReferenceDiscoveryArgs]{Args: ReferenceDiscoveryArgs{TransportArgs: base}}); err != nil {
		t.Fatal(err)
	}
	if got, want := len(bridge.calls), 4; got != want || bridge.calls[0] != "dispatch" || bridge.calls[1] != "finalize" || bridge.calls[2] != "post_sync" || bridge.calls[3] != "discover" {
		t.Fatalf("bridge calls=%#v", bridge.calls)
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
}

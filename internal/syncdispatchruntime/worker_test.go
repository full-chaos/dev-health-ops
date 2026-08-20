package syncdispatchruntime

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
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

type stubFanout struct {
	err   error
	calls int
}

func (fanout *stubFanout) Fanout(context.Context, PostSyncArgs) error {
	fanout.calls++
	return fanout.err
}

// TestPostSyncWorkerTerminalizesDeterministicOutboxRejections pins the second
// half of CHAOS-3946.
//
// The fanout's verdict on an outbox policy or contract rejection is decided by
// contracts/jobs/v1, which is loaded once at process start and cannot change
// between attempts of the same job. Returning that error raw made River retry
// it five times -- five identical rollbacks of the entire post-sync generation,
// five identical log lines, and then a discard that names only the last one.
// Deterministic rejections must be terminal on the first attempt, exactly as
// providerunit.deterministicTerminalCategory treats deterministic executor
// faults.
//
// Transport and availability failures must stay retryable: a rollback that
// terminalizes a transient Postgres outage would silently drop the whole
// generation for a cause that a retry would clear.
func TestPostSyncWorkerTerminalizesDeterministicOutboxRejections(t *testing.T) {
	t.Parallel()
	args := PostSyncArgs{TransportArgs: TransportArgs{
		Version: ContractVersionV1, OrgID: testOrg, RunID: testRun,
		DispatchOutbox: testOutbox, RouteGeneration: 1,
	}}
	for _, testCase := range []struct {
		name         string
		err          error
		wantTerminal bool
	}{
		{
			name:         "an outbox policy rejection cannot change between attempts",
			err:          fmt.Errorf("%w: publish_not_permitted_for_route", joboutbox.ErrPolicyRejected),
			wantTerminal: true,
		},
		{
			name: "a policy rejection stays terminal through a domain wrapper",
			err: fmt.Errorf("%w: %w", ErrPostSyncUnavailable,
				fmt.Errorf("%w: publish_not_permitted_for_route", joboutbox.ErrPolicyRejected)),
			wantTerminal: true,
		},
		{
			name:         "a contract rejection cannot change between attempts",
			err:          fmt.Errorf("%w: kind_not_registered", joboutbox.ErrContractRejected),
			wantTerminal: true,
		},
		{
			name:         "an unavailable transport must stay retryable",
			err:          ErrPostSyncUnavailable,
			wantTerminal: false,
		},
		{
			name:         "an outbox availability failure must stay retryable",
			err:          joboutbox.ErrUnavailable,
			wantTerminal: false,
		},
		{
			name:         "a successful fanout returns no error",
			err:          nil,
			wantTerminal: false,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			fanout := &stubFanout{err: testCase.err}
			err := (&postSyncWorker{service: fanout}).Work(
				context.Background(), &river.Job[PostSyncArgs]{Args: args},
			)
			if fanout.calls != 1 {
				t.Fatalf("Fanout calls = %d, want 1", fanout.calls)
			}
			var cancelled *river.JobCancelError
			if got := errors.As(err, &cancelled); got != testCase.wantTerminal {
				t.Fatalf("Work() terminal = %v (err = %v), want terminal = %v",
					got, err, testCase.wantTerminal)
			}
			if testCase.err != nil && !errors.Is(err, testCase.err) {
				t.Fatalf("Work() error = %v, want it to keep the fanout cause %v", err, testCase.err)
			}
			if testCase.err == nil && err != nil {
				t.Fatalf("Work() error = %v, want nil", err)
			}
		})
	}
}

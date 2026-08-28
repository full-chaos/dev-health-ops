package syncdispatchruntime

import (
	"context"
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/riverqueue/river"
)

// team_repo_ownership_derivation_worker_test.go is the audit-required
// coverage for teamRepoOwnershipDerivationWorker.Work (CHAOS-4365 item 1b):
// no existing generic test exercised its outcome mapping (rows_written /
// no_signal / error) or telemetry recording -- TestCoordinatorWorkersCall
// TheirDirectBridgeSeams only covers the bridge-based coordinator workers,
// which this one is not (it holds a TeamRepoOwnershipDerivationRunner, not
// a CoordinatorBridge).

type recordingDerivationRunner struct {
	written int
	err     error
	calls   []string
}

func (runner *recordingDerivationRunner) Derive(_ context.Context, orgID string) (int, error) {
	runner.calls = append(runner.calls, orgID)
	return runner.written, runner.err
}

type recordingDerivationObserver struct {
	outcomes []jobruntime.TeamRepoOwnershipDerivationOutcome
	written  []int
}

func (observer *recordingDerivationObserver) ObserveTeamRepoOwnershipDerivation(
	outcome jobruntime.TeamRepoOwnershipDerivationOutcome, written int,
) error {
	observer.outcomes = append(observer.outcomes, outcome)
	observer.written = append(observer.written, written)
	return nil
}

func validTeamRepoOwnershipDerivationJobArgs() TeamRepoOwnershipDerivationJobArgs {
	return TeamRepoOwnershipDerivationJobArgs{
		Version:       ContractVersionV1,
		OrgID:         testOrg,
		CorrelationID: "post-sync-" + testRun,
		Idempotency:   "post-sync:" + testRun + ":sync.team_repo_ownership_derivation",
		Domain:        jobcontract.DomainLink{Type: "sync_run", ID: testRun},
		Payload:       jobcontract.TeamRepoOwnershipDerivationPayload{SyncRunID: testRun},
	}
}

func TestTeamRepoOwnershipDerivationWorkerRecordsRowsWrittenOutcome(t *testing.T) {
	t.Parallel()
	runner := &recordingDerivationRunner{written: 3}
	observer := &recordingDerivationObserver{}
	worker := &teamRepoOwnershipDerivationWorker{service: runner, observer: observer}
	args := validTeamRepoOwnershipDerivationJobArgs()

	if err := worker.Work(context.Background(), &river.Job[TeamRepoOwnershipDerivationJobArgs]{Args: args}); err != nil {
		t.Fatalf("Work() error = %v, want nil", err)
	}
	if len(runner.calls) != 1 || runner.calls[0] != testOrg {
		t.Fatalf("Derive calls = %v, want [%s]", runner.calls, testOrg)
	}
	if len(observer.outcomes) != 1 || observer.outcomes[0] != jobruntime.TeamRepoOwnershipDerivationOutcomeRowsWritten {
		t.Fatalf("observed outcomes = %v, want [rows_written]", observer.outcomes)
	}
	if len(observer.written) != 1 || observer.written[0] != 3 {
		t.Fatalf("observed written counts = %v, want [3]", observer.written)
	}
}

func TestTeamRepoOwnershipDerivationWorkerRecordsNoSignalOutcome(t *testing.T) {
	t.Parallel()
	runner := &recordingDerivationRunner{written: 0}
	observer := &recordingDerivationObserver{}
	worker := &teamRepoOwnershipDerivationWorker{service: runner, observer: observer}
	args := validTeamRepoOwnershipDerivationJobArgs()

	if err := worker.Work(context.Background(), &river.Job[TeamRepoOwnershipDerivationJobArgs]{Args: args}); err != nil {
		t.Fatalf("Work() error = %v, want nil", err)
	}
	if len(observer.outcomes) != 1 || observer.outcomes[0] != jobruntime.TeamRepoOwnershipDerivationOutcomeNoSignal {
		t.Fatalf("observed outcomes = %v, want [no_signal] -- a zero-row, no-error Derive is the designed-empty case (§0.2), not a failure", observer.outcomes)
	}
}

func TestTeamRepoOwnershipDerivationWorkerRecordsErrorOutcomeAndPropagates(t *testing.T) {
	t.Parallel()
	deriveErr := errors.New("clickhouse unavailable")
	runner := &recordingDerivationRunner{written: 0, err: deriveErr}
	observer := &recordingDerivationObserver{}
	worker := &teamRepoOwnershipDerivationWorker{service: runner, observer: observer}
	args := validTeamRepoOwnershipDerivationJobArgs()

	err := worker.Work(context.Background(), &river.Job[TeamRepoOwnershipDerivationJobArgs]{Args: args})
	if !errors.Is(err, deriveErr) {
		t.Fatalf("Work() error = %v, want %v so River retries the job", err, deriveErr)
	}
	if len(observer.outcomes) != 1 || observer.outcomes[0] != jobruntime.TeamRepoOwnershipDerivationOutcomeError {
		t.Fatalf("observed outcomes = %v, want [error]", observer.outcomes)
	}
}

func TestTeamRepoOwnershipDerivationWorkerRejectsInvalidJobArgsWithoutCallingDerive(t *testing.T) {
	t.Parallel()
	runner := &recordingDerivationRunner{written: 5}
	worker := &teamRepoOwnershipDerivationWorker{service: runner}
	invalid := validTeamRepoOwnershipDerivationJobArgs()
	invalid.Domain.ID = "not-a-uuid"

	if err := worker.Work(context.Background(), &river.Job[TeamRepoOwnershipDerivationJobArgs]{Args: invalid}); err == nil {
		t.Fatal("Work() = nil, want an error for invalid job args")
	}
	if len(runner.calls) != 0 {
		t.Fatalf("Derive was called with invalid job args: %v", runner.calls)
	}
}

func TestTeamRepoOwnershipDerivationWorkerToleratesNilObserver(t *testing.T) {
	t.Parallel()
	runner := &recordingDerivationRunner{written: 1}
	worker := &teamRepoOwnershipDerivationWorker{service: runner, observer: nil}
	args := validTeamRepoOwnershipDerivationJobArgs()

	if err := worker.Work(context.Background(), &river.Job[TeamRepoOwnershipDerivationJobArgs]{Args: args}); err != nil {
		t.Fatalf("Work() with a nil observer error = %v, want nil", err)
	}
}

func TestTeamRepoOwnershipDerivationJobArgsValid(t *testing.T) {
	t.Parallel()
	base := validTeamRepoOwnershipDerivationJobArgs()
	if err := base.valid(); err != nil {
		t.Fatalf("valid base args rejected: %v", err)
	}

	mutate := func(fn func(*TeamRepoOwnershipDerivationJobArgs)) TeamRepoOwnershipDerivationJobArgs {
		args := validTeamRepoOwnershipDerivationJobArgs()
		fn(&args)
		return args
	}

	cases := map[string]TeamRepoOwnershipDerivationJobArgs{
		"wrong contract version": mutate(func(a *TeamRepoOwnershipDerivationJobArgs) { a.Version = 99 }),
		"non-uuid org id":        mutate(func(a *TeamRepoOwnershipDerivationJobArgs) { a.OrgID = "not-a-uuid" }),
		"non-uuid domain id":     mutate(func(a *TeamRepoOwnershipDerivationJobArgs) { a.Domain.ID = "not-a-uuid" }),
		"non-uuid sync run id": mutate(func(a *TeamRepoOwnershipDerivationJobArgs) {
			a.Payload.SyncRunID = "not-a-uuid"
		}),
		"wrong domain type": mutate(func(a *TeamRepoOwnershipDerivationJobArgs) { a.Domain.Type = "work_graph_request" }),
		"domain id does not match sync run id": mutate(func(a *TeamRepoOwnershipDerivationJobArgs) {
			a.Domain.ID = testOrg
		}),
		"empty correlation id": mutate(func(a *TeamRepoOwnershipDerivationJobArgs) { a.CorrelationID = "" }),
		"empty idempotency key": mutate(func(a *TeamRepoOwnershipDerivationJobArgs) { a.Idempotency = "" }),
	}
	for name, args := range cases {
		if err := args.valid(); err == nil {
			t.Errorf("%s: expected valid() to reject, got nil", name)
		}
	}
}

func TestTeamRepoOwnershipDerivationJobArgsKind(t *testing.T) {
	t.Parallel()
	if got, want := (TeamRepoOwnershipDerivationJobArgs{}).Kind(), jobcontract.KindTeamRepoOwnershipDerivation; got != want {
		t.Fatalf("Kind() = %q, want %q", got, want)
	}
}

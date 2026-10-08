package syncdispatchruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/riverqueue/river"
)

// team_repo_ownership_derivation_worker_test.go is the audit-required
// coverage for teamRepoOwnershipDerivationWorker.Work (CHAOS-4365 item 1b):
// no existing generic test exercised its outcome mapping (rows_written /
// no_signal / error) or telemetry recording -- TestCoordinatorWorkersCall
// TheirDirectBridgeSeams only covers the team-autoimport coordinator worker,
// which this one is not (it holds a TeamRepoOwnershipDerivationRunner, not
// a TeamAutoImporter).

type recordingDerivationRunner struct {
	written     int
	retracted   int
	inputsReady bool
	armCounts   map[string]int
	stats       providersync.TeamRepoOwnershipDerivationStats
	err         error
	calls       []string
}

func (runner *recordingDerivationRunner) DeriveWithStats(_ context.Context, orgID string) (int, int, bool, map[string]int, providersync.TeamRepoOwnershipDerivationStats, error) {
	runner.calls = append(runner.calls, orgID)
	return runner.written, runner.retracted, runner.inputsReady, runner.armCounts, runner.stats, runner.err
}

type recordingDerivationObserver struct {
	outcomes []jobruntime.TeamRepoOwnershipDerivationOutcome
	written  []int
	arms     []jobruntime.TeamRepoOwnershipResolutionArm
	armCount []int
}

func (observer *recordingDerivationObserver) ObserveTeamRepoOwnershipDerivation(
	outcome jobruntime.TeamRepoOwnershipDerivationOutcome, written int,
) error {
	observer.outcomes = append(observer.outcomes, outcome)
	observer.written = append(observer.written, written)
	return nil
}

func (observer *recordingDerivationObserver) ObserveTeamRepoOwnershipDerivationResolutionArm(
	arm jobruntime.TeamRepoOwnershipResolutionArm, count int,
) error {
	observer.arms = append(observer.arms, arm)
	observer.armCount = append(observer.armCount, count)
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
	runner := &recordingDerivationRunner{written: 3, inputsReady: true}
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

// TestTeamRepoOwnershipDerivationWorkerRecordsResolutionArmCounts pins
// CHAOS-4458 part (b): the worker reports BOTH registered resolution arms
// every run (even the one that produced 0 rows this run), so the
// project_id vs linear_team_key series never silently goes missing instead
// of reading as a present zero.
func TestTeamRepoOwnershipDerivationWorkerRecordsResolutionArmCounts(t *testing.T) {
	t.Parallel()
	runner := &recordingDerivationRunner{
		written:     2,
		inputsReady: true,
		armCounts:   map[string]int{"linear_team_key": 2},
	}
	observer := &recordingDerivationObserver{}
	worker := &teamRepoOwnershipDerivationWorker{service: runner, observer: observer}
	args := validTeamRepoOwnershipDerivationJobArgs()

	if err := worker.Work(context.Background(), &river.Job[TeamRepoOwnershipDerivationJobArgs]{Args: args}); err != nil {
		t.Fatalf("Work() error = %v, want nil", err)
	}
	if len(observer.arms) != 2 {
		t.Fatalf("observed arms = %v, want 2 (both registered arms every run)", observer.arms)
	}
	counts := map[jobruntime.TeamRepoOwnershipResolutionArm]int{}
	for i, arm := range observer.arms {
		counts[arm] = observer.armCount[i]
	}
	if counts[jobruntime.TeamRepoOwnershipResolutionArmProjectID] != 0 {
		t.Fatalf("project_id count = %d, want 0", counts[jobruntime.TeamRepoOwnershipResolutionArmProjectID])
	}
	if counts[jobruntime.TeamRepoOwnershipResolutionArmLinearTeamKey] != 2 {
		t.Fatalf("linear_team_key count = %d, want 2", counts[jobruntime.TeamRepoOwnershipResolutionArmLinearTeamKey])
	}
}

// TestTeamRepoOwnershipDerivationWorkerSeparatesUnchangedFromNoSignal pins CHAOS-8148: a run that wrote nothing is "unchanged" when it
// DERIVED facts and every one is already carried by an open row, and "no_signal" only when it derived nothing; a run that derived facts
// but wrote none for another reason (some facts are not unchanged) stays no_signal, so the label never claims more than the counts say.
func TestTeamRepoOwnershipDerivationWorkerSeparatesUnchangedFromNoSignal(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		stats providersync.TeamRepoOwnershipDerivationStats
		want  jobruntime.TeamRepoOwnershipDerivationOutcome
	}{
		{"all derived facts unchanged", providersync.TeamRepoOwnershipDerivationStats{Derived: 8, Unchanged: 8}, jobruntime.TeamRepoOwnershipDerivationOutcomeUnchanged},
		{"one derived fact, unchanged", providersync.TeamRepoOwnershipDerivationStats{Derived: 1, Unchanged: 1}, jobruntime.TeamRepoOwnershipDerivationOutcomeUnchanged},
		{"derived nothing", providersync.TeamRepoOwnershipDerivationStats{}, jobruntime.TeamRepoOwnershipDerivationOutcomeNoSignal},
		{"derived some, only some unchanged, none written", providersync.TeamRepoOwnershipDerivationStats{Derived: 8, Unchanged: 5}, jobruntime.TeamRepoOwnershipDerivationOutcomeNoSignal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runner := &recordingDerivationRunner{written: 0, inputsReady: true, stats: tc.stats}
			observer := &recordingDerivationObserver{}
			worker := &teamRepoOwnershipDerivationWorker{service: runner, observer: observer}
			if err := worker.Work(context.Background(), &river.Job[TeamRepoOwnershipDerivationJobArgs]{Args: validTeamRepoOwnershipDerivationJobArgs()}); err != nil {
				t.Fatalf("Work() error = %v, want nil", err)
			}
			if len(observer.outcomes) != 1 || observer.outcomes[0] != tc.want {
				t.Fatalf("observed outcomes = %v, want [%s]", observer.outcomes, tc.want)
			}
		})
	}
}

// TestTeamRepoOwnershipDerivationWorkerLogsTheDerivedAndUnchangedCounts pins the log line CHAOS-8148 adds: it is the only place the counts are
// recorded, so a field dropped or filled with the wrong number would pass every other test. Not parallel: it replaces the default logger.
func TestTeamRepoOwnershipDerivationWorkerLogsTheDerivedAndUnchangedCounts(t *testing.T) {
	var logged bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logged, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	runner := &recordingDerivationRunner{written: 0, retracted: 0, inputsReady: true, stats: providersync.TeamRepoOwnershipDerivationStats{Derived: 7, Unchanged: 5}}
	worker := &teamRepoOwnershipDerivationWorker{service: runner, observer: &recordingDerivationObserver{}}
	if err := worker.Work(context.Background(), &river.Job[TeamRepoOwnershipDerivationJobArgs]{Args: validTeamRepoOwnershipDerivationJobArgs()}); err != nil {
		t.Fatalf("Work() error = %v, want nil", err)
	}
	var found map[string]any
	for _, line := range strings.Split(strings.TrimSpace(logged.String()), "\n") {
		var record map[string]any
		if json.Unmarshal([]byte(line), &record) == nil && record["msg"] == "team_repo_ownership_derivation" {
			found = record
		}
	}
	if found == nil {
		t.Fatalf("no team_repo_ownership_derivation log line in %q", logged.String())
	}
	if found["facts_derived"] != float64(7) || found["facts_unchanged"] != float64(5) {
		t.Fatalf("log fields facts_derived=%v facts_unchanged=%v, want 7 and 5", found["facts_derived"], found["facts_unchanged"])
	}
	if found["outcome"] != "no_signal" {
		t.Fatalf("outcome = %v, want no_signal (7 derived, 5 unchanged: not all unchanged)", found["outcome"])
	}
}

func TestTeamRepoOwnershipDerivationWorkerRecordsNoSignalOutcome(t *testing.T) {
	t.Parallel()
	runner := &recordingDerivationRunner{written: 0, inputsReady: true}
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

// TestTeamRepoOwnershipDerivationWorkerRecordsInputsNotReadyOutcome pins the
// team-lead ruling on codex finding #4 (2026-08-28): a zero-row Derive with
// inputsReady=false (this org's team_project_ownership and/or linkage rows
// have not synced yet -- the first-sync gap) is its own outcome, distinct
// from no_signal, and Work() still returns nil (never a failure; the next
// qualifying sync will re-derive).
func TestTeamRepoOwnershipDerivationWorkerRecordsInputsNotReadyOutcome(t *testing.T) {
	t.Parallel()
	runner := &recordingDerivationRunner{written: 0, inputsReady: false}
	observer := &recordingDerivationObserver{}
	worker := &teamRepoOwnershipDerivationWorker{service: runner, observer: observer}
	args := validTeamRepoOwnershipDerivationJobArgs()

	if err := worker.Work(context.Background(), &river.Job[TeamRepoOwnershipDerivationJobArgs]{Args: args}); err != nil {
		t.Fatalf("Work() error = %v, want nil", err)
	}
	if len(observer.outcomes) != 1 || observer.outcomes[0] != jobruntime.TeamRepoOwnershipDerivationOutcomeInputsNotReady {
		t.Fatalf("observed outcomes = %v, want [inputs_not_ready]", observer.outcomes)
	}
}

// TestTeamRepoOwnershipDerivationWorkerRecordsRowsRetractedOutcome pins the
// team-lead ruling (2026-08-28, codex R3 finding "removed ownership remains
// authorized indefinitely"): retraction is observed as its OWN outcome,
// separately from and in addition to the primary written/no_signal outcome
// -- a single run that both retracts a stale claim and writes a fresh one
// (a repo reassigned from one team to another in the same sync) must record
// BOTH facts, not let one shadow the other.
func TestTeamRepoOwnershipDerivationWorkerRecordsRowsRetractedOutcome(t *testing.T) {
	t.Parallel()
	runner := &recordingDerivationRunner{written: 1, retracted: 1, inputsReady: true}
	observer := &recordingDerivationObserver{}
	worker := &teamRepoOwnershipDerivationWorker{service: runner, observer: observer}
	args := validTeamRepoOwnershipDerivationJobArgs()

	if err := worker.Work(context.Background(), &river.Job[TeamRepoOwnershipDerivationJobArgs]{Args: args}); err != nil {
		t.Fatalf("Work() error = %v, want nil", err)
	}
	if len(observer.outcomes) != 2 {
		t.Fatalf("observed outcomes = %v, want 2 (rows_retracted + rows_written)", observer.outcomes)
	}
	if observer.outcomes[0] != jobruntime.TeamRepoOwnershipDerivationOutcomeRowsRetracted || observer.written[0] != 1 {
		t.Fatalf("first observation = (%v, %d), want (rows_retracted, 1)", observer.outcomes[0], observer.written[0])
	}
	if observer.outcomes[1] != jobruntime.TeamRepoOwnershipDerivationOutcomeRowsWritten || observer.written[1] != 1 {
		t.Fatalf("second observation = (%v, %d), want (rows_written, 1)", observer.outcomes[1], observer.written[1])
	}
}

// TestTeamRepoOwnershipDerivationWorkerRecordsRowsRetractedOutcomeAlongsideNoSignal
// covers the OTHER shape: every prior claim retracted, nothing replaced it
// (e.g. the org's last owned repo lost its donor linkage entirely).
func TestTeamRepoOwnershipDerivationWorkerRecordsRowsRetractedOutcomeAlongsideNoSignal(t *testing.T) {
	t.Parallel()
	runner := &recordingDerivationRunner{written: 0, retracted: 2, inputsReady: true}
	observer := &recordingDerivationObserver{}
	worker := &teamRepoOwnershipDerivationWorker{service: runner, observer: observer}
	args := validTeamRepoOwnershipDerivationJobArgs()

	if err := worker.Work(context.Background(), &river.Job[TeamRepoOwnershipDerivationJobArgs]{Args: args}); err != nil {
		t.Fatalf("Work() error = %v, want nil", err)
	}
	if len(observer.outcomes) != 2 ||
		observer.outcomes[0] != jobruntime.TeamRepoOwnershipDerivationOutcomeRowsRetracted ||
		observer.outcomes[1] != jobruntime.TeamRepoOwnershipDerivationOutcomeNoSignal {
		t.Fatalf("observed outcomes = %v, want [rows_retracted, no_signal]", observer.outcomes)
	}
}

func TestTeamRepoOwnershipDerivationWorkerRecordsErrorOutcomeAndPropagates(t *testing.T) {
	t.Parallel()
	deriveErr := errors.New("clickhouse unavailable")
	runner := &recordingDerivationRunner{written: 0, inputsReady: true, err: deriveErr}
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
	runner := &recordingDerivationRunner{written: 1, inputsReady: true}
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
		"empty correlation id":  mutate(func(a *TeamRepoOwnershipDerivationJobArgs) { a.CorrelationID = "" }),
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

// TestTeamRepoOwnershipDerivationWorkerSignalsAnUnresolvedOwnerTie pins the tie signal: a run that left repos on a full tie (equal link counts
// at every tier, so no owner was named) records the owner_tie_unresolved outcome beside the run's own
// outcome and writes one WARN line with the tie count and the tied repo ids. Not parallel: it replaces the default logger.
func TestTeamRepoOwnershipDerivationWorkerSignalsAnUnresolvedOwnerTie(t *testing.T) {
	var logged bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logged, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	tiedRepo := "0b6f2a4e-7c1d-4f3a-9e2b-5d8c1a7f3e90"
	runner := &recordingDerivationRunner{inputsReady: true, stats: providersync.TeamRepoOwnershipDerivationStats{
		Derived: 1, Unchanged: 1,
		Ties: []providersync.TeamRepoOwnershipTie{{RepoID: tiedRepo, TeamIDs: []string{"gh:ops-team", "jira-team-a"}}},
	}}
	observer := &recordingDerivationObserver{}
	worker := &teamRepoOwnershipDerivationWorker{service: runner, observer: observer}
	if err := worker.Work(context.Background(), &river.Job[TeamRepoOwnershipDerivationJobArgs]{Args: validTeamRepoOwnershipDerivationJobArgs()}); err != nil {
		t.Fatalf("Work() error = %v, want nil", err)
	}
	if len(observer.outcomes) != 2 ||
		observer.outcomes[0] != jobruntime.TeamRepoOwnershipDerivationOutcomeOwnerTieUnresolved || observer.written[0] != 1 ||
		observer.outcomes[1] != jobruntime.TeamRepoOwnershipDerivationOutcomeUnchanged {
		t.Fatalf("observed (outcomes, counts) = (%v, %v), want [owner_tie_unresolved 1, unchanged]", observer.outcomes, observer.written)
	}
	var found map[string]any
	for _, line := range strings.Split(strings.TrimSpace(logged.String()), "\n") {
		var record map[string]any
		if json.Unmarshal([]byte(line), &record) == nil && record["msg"] == "team_repo_ownership_derivation.owner_tie_unresolved" {
			found = record
		}
	}
	if found == nil {
		t.Fatalf("no team_repo_ownership_derivation.owner_tie_unresolved log line in %q", logged.String())
	}
	repoIDs, _ := found["repo_ids"].([]any)
	if found["level"] != "WARN" || found["owner_ties"] != float64(1) || len(repoIDs) != 1 || repoIDs[0] != tiedRepo {
		t.Fatalf("tie log line = %v, want level WARN, owner_ties 1, repo_ids [%s]", found, tiedRepo)
	}
}

// TestTeamRepoOwnershipDerivationWorkerSendsNoTieSignalWithoutATie: the tie outcome and its WARN line appear only on a run with a tie.
func TestTeamRepoOwnershipDerivationWorkerSendsNoTieSignalWithoutATie(t *testing.T) {
	var logged bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logged, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	observer := &recordingDerivationObserver{}
	worker := &teamRepoOwnershipDerivationWorker{service: &recordingDerivationRunner{written: 1, inputsReady: true}, observer: observer}
	if err := worker.Work(context.Background(), &river.Job[TeamRepoOwnershipDerivationJobArgs]{Args: validTeamRepoOwnershipDerivationJobArgs()}); err != nil {
		t.Fatalf("Work() error = %v, want nil", err)
	}
	if len(observer.outcomes) != 1 || observer.outcomes[0] != jobruntime.TeamRepoOwnershipDerivationOutcomeRowsWritten {
		t.Fatalf("observed outcomes = %v, want [rows_written]", observer.outcomes)
	}
	if strings.Contains(logged.String(), "owner_tie_unresolved") {
		t.Fatalf("tie log line written on a run without a tie: %q", logged.String())
	}
}

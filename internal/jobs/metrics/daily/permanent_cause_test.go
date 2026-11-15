package daily

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

const (
	otherOrgID       = "00000000-0000-4000-8000-000000000008"
	otherDomainID    = "00000000-0000-4000-8000-000000000007"
	permanentClaimID = "00000000-0000-4000-8000-000000000003"
	permanentRepoID  = "00000000-0000-4000-8000-000000000010"
)

// Every permanent failure of the three daily kinds must name its cause twice: as a bounded reason in the River
// error row, and as a static cause on one WARN "job failed" line, read here in its production form (the redacting
// JSON handler). Before this, all of them left only "dev-health job failed [permanent]" and no log line.
func TestDailyPermanentFailureNamesItsCauseInTheRiverErrorAndOneRedactedLog(t *testing.T) {
	registry, err := jobruntime.Load("../../../../contracts/jobs/v1")
	if err != nil {
		t.Fatal(err)
	}
	const invalidState = "invalid_state"
	partitionClaim := func() *PartitionClaim {
		return &PartitionClaim{
			Partition:     Partition{ID: testPartitionID, RunID: testRunID, RepoIDs: []RepositoryID{permanentRepoID}},
			Token:         permanentClaimID,
			LeaseDuration: 30 * time.Millisecond,
		}
	}
	runningRun := func() Run {
		return Run{ID: testRunID, OrganizationID: testOrgID, Status: "running", TargetDay: time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC)}
	}
	partition := func(t *testing.T, store Store, checker SourceDataChecker, domainID string) adapterOutcome {
		handler, err := NewPartitionHandler(store, fakePublisher{})
		if err != nil {
			t.Fatal(err)
		}
		if checker != nil {
			handler.SetSourceDataChecker(checker)
		}
		args := jobruntime.DailyMetricsPartitionArgs{EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.DailyMetricsPartitionPayload]{
			ContractVersion: jobcontract.ContractVersionV1, OrganizationID: pointer(testOrgID),
			CorrelationID: "daily:" + testRunID, IdempotencyKey: "metrics.daily_partition:" + testPartitionID,
			Domain:  jobcontract.DomainLink{Type: "daily_metrics_partition", ID: domainID},
			Payload: jobcontract.DailyMetricsPartitionPayload{PartitionID: testPartitionID},
		}}
		return runThroughAdapter(t, registry, jobcontract.KindDailyMetricsPartition, jobruntime.Handler[jobruntime.DailyMetricsPartitionArgs](handler), args)
	}
	dispatch := func(t *testing.T, store Store, domainID string) adapterOutcome {
		handler, err := NewDispatcher(store, fakePublisher{}, &fakeRepositoryDiscoverer{identifiers: []RepositoryID{permanentRepoID}})
		if err != nil {
			t.Fatal(err)
		}
		args := jobruntime.DailyMetricsDispatchArgs{EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.DailyMetricsDispatchPayload]{
			ContractVersion: jobcontract.ContractVersionV1, OrganizationID: pointer(testOrgID),
			CorrelationID: "daily:" + testRunID, IdempotencyKey: "metrics.daily_dispatch:" + testRunID,
			Domain:  jobcontract.DomainLink{Type: "daily_metrics_run", ID: domainID},
			Payload: jobcontract.DailyMetricsDispatchPayload{RunID: testRunID},
		}}
		return runThroughAdapter(t, registry, jobcontract.KindDailyMetricsDispatch, jobruntime.Handler[jobruntime.DailyMetricsDispatchArgs](handler), args)
	}
	finalize := func(t *testing.T, store Store, domainID string) adapterOutcome {
		handler, err := NewFinalizeHandler(store)
		if err != nil {
			t.Fatal(err)
		}
		args := jobruntime.DailyMetricsFinalizeArgs{EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.DailyMetricsFinalizePayload]{
			ContractVersion: jobcontract.ContractVersionV1, OrganizationID: pointer(testOrgID),
			CorrelationID: "daily:" + testRunID, IdempotencyKey: "metrics.daily_finalize:" + testRunID,
			Domain:  jobcontract.DomainLink{Type: "daily_metrics_run", ID: domainID},
			Payload: jobcontract.DailyMetricsFinalizePayload{RunID: testRunID},
		}}
		return runThroughAdapter(t, registry, jobcontract.KindDailyMetricsFinalize, jobruntime.Handler[jobruntime.DailyMetricsFinalizeArgs](handler), args)
	}

	for _, test := range []struct {
		name   string
		run    func(t *testing.T) adapterOutcome
		reason string
		cause  string
	}{
		{
			name: "partition_zero_rows_with_source_data",
			run: func(t *testing.T) adapterOutcome {
				store := &fakeStore{partitionClaim: partitionClaim(), run: runningRun()}
				return partition(t, store, familiesSourceDataChecker{families: []string{"testops_risk", "cicd", "cicd", "not_a_family"}}, testPartitionID)
			},
			reason: "zero_rows_with_source_data",
			cause:  "step=source_check zero_rows_with_source_data families=cicd,other,testops_risk",
		},
		{
			name: "partition_source_check_invalid_state",
			run: func(t *testing.T) adapterOutcome {
				store := &fakeStore{partitionClaim: partitionClaim(), run: runningRun()}
				return partition(t, store, errorSourceDataChecker{err: ErrInvalidState}, testPartitionID)
			},
			reason: invalidState, cause: "step=partition_source_check",
		},
		{
			name: "partition_scope_run_not_running",
			run: func(t *testing.T) adapterOutcome {
				run := runningRun()
				run.Status = "succeeded"
				return partition(t, &fakeStore{partitionClaim: partitionClaim(), run: run}, nil, testPartitionID)
			},
			reason: invalidState, cause: "step=partition_scope condition=run_not_running",
		},
		{
			name: "partition_scope_organization_mismatch",
			run: func(t *testing.T) adapterOutcome {
				run := runningRun()
				run.OrganizationID = otherOrgID
				return partition(t, &fakeStore{partitionClaim: partitionClaim(), run: run}, nil, testPartitionID)
			},
			reason: invalidState, cause: "step=partition_scope condition=organization_mismatch",
		},
		{
			name: "partition_load_run_invalid_state",
			run: func(t *testing.T) adapterOutcome {
				return partition(t, &fakeStore{partitionClaim: partitionClaim(), loadErr: ErrInvalidState}, nil, testPartitionID)
			},
			reason: invalidState, cause: "step=partition_load_run",
		},
		{
			name: "partition_domain_mismatch",
			run: func(t *testing.T) adapterOutcome {
				return partition(t, &fakeStore{partitionClaim: partitionClaim(), run: runningRun()}, nil, otherDomainID)
			},
			reason: invalidState, cause: "step=partition_domain_mismatch",
		},
		{
			name: "dispatch_domain_mismatch",
			run: func(t *testing.T) adapterOutcome {
				return dispatch(t, &fakeStore{run: runningRun()}, otherDomainID)
			},
			reason: invalidState, cause: "step=dispatch_domain_mismatch",
		},
		{
			name: "dispatch_claim_invalid_state",
			run: func(t *testing.T) adapterOutcome {
				return dispatch(t, &fakeStore{loadErr: ErrInvalidState}, testRunID)
			},
			reason: invalidState, cause: "step=dispatch_claim",
		},
		{
			name: "dispatch_scope_run_not_running",
			run: func(t *testing.T) adapterOutcome {
				run := runningRun()
				run.Status = "succeeded"
				return dispatch(t, &fakeStore{run: run}, testRunID)
			},
			reason: invalidState, cause: "step=dispatch_scope condition=run_not_running",
		},
		{
			name: "dispatch_materialize_invalid_state",
			run: func(t *testing.T) adapterOutcome {
				run := runningRun()
				run.RepositoryDiscoveryRequired = true
				return dispatch(t, &fakeStore{run: run, materializeErr: ErrInvalidState}, testRunID)
			},
			reason: invalidState, cause: "step=dispatch_materialize",
		},
		{
			name: "dispatch_partition_scope",
			run: func(t *testing.T) adapterOutcome {
				store := &unscopedPartitionStore{fakeStore: &fakeStore{run: runningRun()}}
				return dispatch(t, store, testRunID)
			},
			reason: invalidState, cause: "step=dispatch_partition_scope",
		},
		{
			name: "finalize_domain_mismatch",
			run: func(t *testing.T) adapterOutcome {
				return finalize(t, &fakeStore{}, otherDomainID)
			},
			reason: invalidState, cause: "step=finalize_domain_mismatch",
		},
		{
			name: "finalize_scope_run_not_running",
			run: func(t *testing.T) adapterOutcome {
				run := runningRun()
				run.Status = "succeeded"
				return finalize(t, &fakeStore{finalizeClaim: &FinalizeClaim{Run: run, Token: permanentClaimID, LeaseDuration: time.Second}}, testRunID)
			},
			reason: invalidState, cause: "step=finalize_scope condition=run_not_running",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := test.run(t)
			workErr, logs, seen := got.err, got.logs, got.seen
			want := "JobCancelError: dev-health job failed [permanent: " + test.reason + "]"
			if workErr == nil || workErr.Error() != want {
				t.Errorf("River error = %v, want %q", workErr, want)
			}
			if len(seen) != 1 || seen[0].String() != test.reason {
				t.Errorf("ObserveDeterministicFailure reasons = %v, want exactly [%s]", seen, test.reason)
			}
			var failed []map[string]any
			var failedLines []string
			for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
				if line == "" {
					continue
				}
				var record map[string]any
				if err := json.Unmarshal([]byte(line), &record); err != nil {
					t.Fatalf("log line is not JSON: %v: %s", err, line)
				}
				if record["msg"] == "job failed" {
					failed = append(failed, record)
					failedLines = append(failedLines, line)
				}
			}
			if len(failed) != 1 {
				t.Fatalf("WARN \"job failed\" records = %d, want 1; logs:\n%s", len(failed), logs.String())
			}
			t.Logf("job failed line: %s", failedLines[0])
			record := failed[0]
			if record["level"] != "WARN" || record["error_category"] != "permanent" || record["cause"] != test.cause {
				t.Errorf("job failed record = %v, want level WARN, error_category permanent, cause %q", record, test.cause)
			}
			for _, identifier := range []string{testOrgID, otherOrgID, testRunID, testPartitionID, otherDomainID, permanentRepoID, "2026-11-14"} {
				if strings.Contains(failedLines[0], identifier) {
					t.Errorf("job failed line carries identifier %s: %s", identifier, failedLines[0])
				}
			}
		})
	}
}

func runThroughAdapter[T jobruntime.ContractArgs](
	t *testing.T, registry *jobruntime.Registry, kind string, handler jobruntime.Handler[T], args T,
) (result adapterOutcome) {
	t.Helper()
	spec, ok := registry.Descriptor(kind)
	if !ok {
		t.Fatalf("kind %s is not in the checked-in registry", kind)
	}
	observer := &reasonObserver{}
	logs := &bytes.Buffer{}
	adapter, err := jobruntime.NewAdapter[T](registry, spec, handler, jobruntime.Dependencies{
		Logger:      logging.NewJSON(logs, slog.LevelDebug),
		Observer:    observer,
		TenantScope: passTenantScope{},
		Budget:      freeBudget{},
		Idempotency: proceedIdempotency{},
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := jobcontract.MarshalCanonical(args.ContractEnvelope())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	job := &river.Job[T]{
		JobRow: &rivertype.JobRow{
			ID: 891756, Attempt: 1, CreatedAt: now, EncodedArgs: raw, Kind: kind,
			MaxAttempts: spec.MaxAttempts, Priority: spec.Priority, Queue: spec.Queue,
			ScheduledAt: now, State: rivertype.JobStateRunning,
		},
		Args: args,
	}
	result.err = adapter.Work(context.Background(), job)
	result.logs = logs
	result.seen = observer.reasons()
	return result
}

type adapterOutcome struct {
	err  error
	logs *bytes.Buffer
	seen []jobruntime.Reason
}

type familiesSourceDataChecker struct{ families []string }

func (checker familiesSourceDataChecker) SourceSnapshot(context.Context, string, time.Time) (SourceSnapshot, error) {
	return SourceSnapshot{}, nil
}

func (checker familiesSourceDataChecker) ZeroRowFamilies(context.Context, SourceSnapshot) ([]string, error) {
	return checker.families, nil
}

// unscopedPartitionStore lists one dispatchable partition with no id, the shape Dispatcher.Work refuses.
type unscopedPartitionStore struct{ *fakeStore }

func (store *unscopedPartitionStore) DispatchablePartitions(context.Context, string) ([]Partition, error) {
	return []Partition{{RunID: testRunID}}, nil
}

type reasonObserver struct {
	mu   sync.Mutex
	seen []jobruntime.Reason
}

func (*reasonObserver) RuntimeRegistered(context.Context, jobruntime.RuntimeInfo) {}
func (*reasonObserver) JobStarted(context.Context, jobruntime.JobLabels)          {}
func (*reasonObserver) JobFinished(context.Context, jobruntime.JobLabels, jobruntime.Result, jobruntime.ErrorCategory, time.Duration) {
}
func (*reasonObserver) JobPanicked(context.Context, jobruntime.JobLabels) {}
func (*reasonObserver) JobCancelled(context.Context, jobruntime.JobLabels, jobruntime.ErrorCategory) {
}
func (*reasonObserver) DomainMismatch(context.Context, string)                                  {}
func (*reasonObserver) BudgetWait(context.Context, jobruntime.JobLabels, time.Duration, string) {}
func (*reasonObserver) JobWait(context.Context, jobruntime.JobLabels, time.Duration)            {}
func (observer *reasonObserver) ObserveDeterministicFailure(_ context.Context, _ jobruntime.JobLabels, reason jobruntime.Reason) {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	observer.seen = append(observer.seen, reason)
}
func (observer *reasonObserver) reasons() []jobruntime.Reason {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	return append([]jobruntime.Reason(nil), observer.seen...)
}

type passTenantScope struct{}

func (passTenantScope) Supports(string) bool { return true }
func (passTenantScope) Resolve(ctx context.Context, _ jobruntime.ScopeRequest) (context.Context, error) {
	return ctx, nil
}

type freeBudget struct{}

func (freeBudget) Supports(string, int) bool { return true }
func (freeBudget) Acquire(context.Context, jobruntime.BudgetRequest) (jobruntime.BudgetLease, error) {
	return freeLease{}, nil
}

type freeLease struct{}

func (freeLease) Release() {}

type proceedIdempotency struct{}

func (proceedIdempotency) Supports(string) bool { return true }
func (proceedIdempotency) Begin(context.Context, jobruntime.ClaimRequest) (jobruntime.IdempotencyClaim, error) {
	return proceedClaim{}, nil
}

type proceedClaim struct{}

func (proceedClaim) State() jobruntime.ClaimState                        { return jobruntime.ClaimProceed }
func (proceedClaim) Finish(context.Context, jobruntime.Completion) error { return nil }

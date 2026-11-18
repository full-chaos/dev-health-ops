//go:build integration

package daily

import (
	"context"
	"errors"
	"net/url"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

func dispatchAttemptExecution(orgID, runID string) *jobruntime.Execution[jobruntime.DailyMetricsDispatchArgs] {
	return &jobruntime.Execution[jobruntime.DailyMetricsDispatchArgs]{
		OrganizationID: pointer(orgID),
		Attempt:        1,
		Envelope: jobcontract.Envelope{
			OrganizationID: pointer(orgID),
			Domain:         jobcontract.DomainLink{Type: "daily_metrics_run", ID: runID},
		},
		Args: jobruntime.DailyMetricsDispatchArgs{
			EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.DailyMetricsDispatchPayload]{
				OrganizationID: pointer(orgID),
				Domain:         jobcontract.DomainLink{Type: "daily_metrics_run", ID: runID},
				Payload:        jobcontract.DailyMetricsDispatchPayload{RunID: runID},
			},
		},
	}
}

// A run whose dispatch cannot materialize it (repository discovery fails on
// every attempt, or the materialize refuses permanently) keeps 0 partitions
// and no finalize handoff. When its last dispatch attempt ends, nothing else
// will ever run it. The run must end failed / failed, so the day reads
// unknown and the next run supersedes it. An empty discovery (handed to its
// finalize) and a run with a partition are never failed.
func TestADispatchThatCannotMaterializeEndsTheRunFailed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	pool, store, publisher := newFinalizeRedriveTestStackWithRiverSchema(t)

	const maxAttempts = 3
	for _, testCase := range []struct {
		name            string
		discovered      []RepositoryID
		discoveryErr    error
		explicitRepos   []RepositoryID
		attempt         int
		wantStatus      string
		wantFinalize    string
		wantPartitions  int
		wantHandoffRows int
	}{
		{name: "discovery fails on the last attempt", discoveryErr: errors.New("clickhouse unavailable"), attempt: maxAttempts,
			wantStatus: "failed", wantFinalize: "failed"},
		{name: "discovery fails before the last attempt", discoveryErr: errors.New("clickhouse unavailable"), attempt: 1,
			wantStatus: "running", wantFinalize: "pending"},
		{name: "materialize refuses permanently", discovered: []RepositoryID{"not-a-uuid"}, attempt: 1,
			wantStatus: "failed", wantFinalize: "failed"},
		{name: "empty discovery is handed to its finalize", discovered: nil, attempt: maxAttempts,
			wantStatus: "running", wantFinalize: "pending", wantHandoffRows: 1},
		{name: "a run with a partition is never failed", explicitRepos: []RepositoryID{RepositoryID(uuid.NewString())}, discoveryErr: errors.New("unused"), attempt: maxAttempts,
			wantStatus: "running", wantFinalize: "pending", wantPartitions: 1},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			orgID := uuid.NewString()
			tx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			run, err := store.StartRunTx(ctx, tx, StartRunRequest{
				OrganizationID: orgID, TargetDay: time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC),
				Generation: "post-sync:" + uuid.NewString(), RepositoryIDs: testCase.explicitRepos,
			}, publisher)
			if err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			dispatcher, err := NewDispatcher(store, publisher, &fakeRepositoryDiscoverer{identifiers: testCase.discovered, err: testCase.discoveryErr})
			if err != nil {
				t.Fatal(err)
			}
			execution := dispatchAttemptExecution(orgID, run.ID)
			execution.Attempt = testCase.attempt
			execution.Definition.MaxAttempts = maxAttempts
			workErr := dispatcher.Work(ctx, execution)

			var status, finalization string
			var partitions, handoffRows int
			if err := pool.QueryRow(ctx, `SELECT status, finalization_status, (SELECT count(*) FROM daily_metrics_partitions WHERE run_id = $1) FROM daily_metrics_runs WHERE id = $1`, run.ID).
				Scan(&status, &finalization, &partitions); err != nil {
				t.Fatal(err)
			}
			if err := pool.QueryRow(ctx, `SELECT count(*) FROM worker_job_outbox WHERE dedupe_key = $1`, FinalizeHandoffKey(run.ID)).Scan(&handoffRows); err != nil {
				t.Fatal(err)
			}
			t.Logf("%s: work=%v status=%s finalization=%s partitions=%d handoff=%d", testCase.name, workErr, status, finalization, partitions, handoffRows)
			if status != testCase.wantStatus || finalization != testCase.wantFinalize ||
				partitions != testCase.wantPartitions || handoffRows != testCase.wantHandoffRows {
				t.Fatalf("status=%s finalization=%s partitions=%d handoff=%d, want %s/%s/%d/%d",
					status, finalization, partitions, handoffRows,
					testCase.wantStatus, testCase.wantFinalize, testCase.wantPartitions, testCase.wantHandoffRows)
			}
		})
	}
}

// A run that its first dispatch materialized empty (the finalize handoff is in
// the outbox) is never failed by a later dispatch attempt that errors: it
// belongs to its finalize.
func TestADispatchErrorAfterAnEmptyHandoffDoesNotFailTheRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	pool, store, publisher := newFinalizeRedriveTestStackWithRiverSchema(t)
	orgID := uuid.NewString()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	run, err := store.StartRunTx(ctx, tx, StartRunRequest{
		OrganizationID: orgID, TargetDay: time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC),
		Generation: "post-sync:" + uuid.NewString(),
	}, publisher)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	empty, err := NewDispatcher(store, publisher, &fakeRepositoryDiscoverer{})
	if err != nil {
		t.Fatal(err)
	}
	if err := empty.Work(ctx, dispatchAttemptExecution(orgID, run.ID)); err != nil {
		t.Fatalf("first dispatch: %v", err)
	}
	failing, err := NewDispatcher(store, publisher, &fakeRepositoryDiscoverer{err: errors.New("clickhouse unavailable")})
	if err != nil {
		t.Fatal(err)
	}
	execution := dispatchAttemptExecution(orgID, run.ID)
	execution.Attempt, execution.Definition.MaxAttempts = 3, 3
	_ = failing.Work(ctx, execution)
	var status, finalization string
	if err := pool.QueryRow(ctx, `SELECT status, finalization_status FROM daily_metrics_runs WHERE id = $1`, run.ID).Scan(&status, &finalization); err != nil {
		t.Fatal(err)
	}
	if status != "running" || finalization != "pending" {
		t.Fatalf("run with the handoff: %s/%s, want running/pending", status, finalization)
	}
}

// The failed run certifies nothing: a day an earlier whole-organization run
// certified reads in flight once a new run of the day is claimed, and stays so
// after that run is failed.
func TestAFailedDispatchLeavesTheDayUncertified(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	stack := newMarkerStack(t)
	orgID := uuid.NewString()
	day := markerDay(3)
	earlier, partition := uuid.NewString(), uuid.NewString()
	stack.seedRun(t, earlier, partition, orgID, day)
	claim, err := stack.store.ClaimFinalize(ctx, earlier)
	if err != nil || claim == nil {
		t.Fatalf("ClaimFinalize: %v %v", claim, err)
	}
	if err := stack.store.CompleteFinalize(ctx, *claim); err != nil {
		t.Fatal(err)
	}
	if state := stack.states(t, orgID, day, day)[day.Format("2006-01-02")]; state != RunMarkerSucceeded {
		t.Fatalf("precondition: the earlier run certified %q, want succeeded", state)
	}

	tx, err := stack.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	run, err := stack.store.StartRunTx(ctx, tx, StartRunRequest{
		OrganizationID: orgID, TargetDay: day, Generation: "post-sync:" + uuid.NewString(),
	}, stack.publisher)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	dispatcher, err := NewDispatcher(stack.store, stack.publisher, &fakeRepositoryDiscoverer{err: errors.New("clickhouse unavailable")})
	if err != nil {
		t.Fatal(err)
	}
	execution := dispatchAttemptExecution(orgID, run.ID)
	execution.Attempt, execution.Definition.MaxAttempts = 3, 3
	_ = dispatcher.Work(ctx, execution)
	var status string
	if err := stack.pool.QueryRow(ctx, `SELECT status FROM daily_metrics_runs WHERE id = $1`, run.ID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	state := stack.states(t, orgID, day, day)[day.Format("2006-01-02")]
	t.Logf("failed run status=%s, day marker=%q", status, state)
	if status != "failed" || state == RunMarkerSucceeded {
		t.Fatalf("status=%s marker=%q, want failed and the day not certified", status, state)
	}
}

// The terminal write runs inside the worker's dispatch, on the domain pool:
// every table it reads or writes is granted to that role by the pinned
// migrations alone (UPDATE on daily_metrics_runs, SELECT on daily_metrics_partitions
// and worker_job_outbox).
func TestFailDispatchPermanentlyRunsUnderTheWorkerDomainRole(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	config, err := pgxpool.ParseConfig(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	config.MaxConns = 8
	admin, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	createDailyTables(t, ctx, admin)
	domainRole, err := containers.RoleName("dispatch_fail_domain", instance)
	if err != nil {
		t.Fatal(err)
	}
	queueRole, err := containers.RoleName("dispatch_fail_queue", instance)
	if err != nil {
		t.Fatal(err)
	}
	defer containers.DropRole(admin, domainRole, t.Logf)
	defer containers.DropRole(admin, queueRole, t.Logf)
	const password = "dispatch_fail_role_password"
	for _, role := range []string{domainRole, queueRole} {
		if _, err := admin.Exec(ctx, "CREATE ROLE "+role+" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '"+password+"'"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := riverstore.ApplyPinnedMigrations(ctx, admin, riverstore.MigrationOptions{
		Schema: "river", DomainRole: domainRole, QueueRole: queueRole,
	}); err != nil {
		t.Fatal(err)
	}
	domainURI, err := url.Parse(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	domainURI.User = url.UserPassword(domainRole, password)
	domain, err := pgxpool.New(ctx, domainURI.String())
	if err != nil {
		t.Fatal(err)
	}
	defer domain.Close()
	store, err := NewPostgresStore(domain)
	if err != nil {
		t.Fatal(err)
	}
	orgID, runID := uuid.NewString(), uuid.NewString()
	if _, err := admin.Exec(ctx, `INSERT INTO daily_metrics_runs (id,org_id,target_day,generation,status,finalization_status,created_at,updated_at,full_org) VALUES ($1::uuid,$2,'2026-11-01','post-sync:' || $1::uuid::text,'running','pending',now(),now(),true)`, runID, orgID); err != nil {
		t.Fatal(err)
	}
	failed, err := store.FailDispatchPermanently(ctx, Run{ID: runID, OrganizationID: orgID, TargetDay: time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)})
	if err != nil || !failed {
		t.Fatalf("FailDispatchPermanently as the domain role: failed=%t err=%v, want true and no error", failed, err)
	}
	var status, finalization string
	if err := admin.QueryRow(ctx, `SELECT status, finalization_status FROM daily_metrics_runs WHERE id = $1`, runID).Scan(&status, &finalization); err != nil {
		t.Fatal(err)
	}
	if status != "failed" || finalization != "failed" {
		t.Fatalf("status=%s finalization=%s, want failed/failed", status, finalization)
	}
}

// The recovery path of a run the dispatch failed: no command moves a failed run
// back; `metrics daily-start --rerun-tag` starts a NEW run of the day
// (StartManualDailyRerun). That run computes and succeeds, the failed run stays
// failed, and the day marker certifies the new run.
func TestARerunOfADayWhoseDispatchFailedRunsTheDayAgain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	stack := newMarkerStack(t)
	orgID := uuid.NewString()
	day := markerDay(4)
	dayText := day.Format("2006-01-02")

	tx, err := stack.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	failedRun, err := stack.store.StartRunTx(ctx, tx, StartRunRequest{
		OrganizationID: orgID, TargetDay: day, Generation: "post-sync:" + uuid.NewString(),
	}, stack.publisher)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	failing, err := NewDispatcher(stack.store, stack.publisher, &fakeRepositoryDiscoverer{err: errors.New("clickhouse unavailable")})
	if err != nil {
		t.Fatal(err)
	}
	execution := dispatchAttemptExecution(orgID, failedRun.ID)
	execution.Attempt, execution.Definition.MaxAttempts = 3, 3
	_ = failing.Work(ctx, execution)

	const tag = "fix-1"
	outcome, err := stack.store.StartManualDailyRerun(ctx, orgID, dayText,
		ManualDailyRerunGeneration(orgID, dayText, nil, tag), nil, stack.publisher, tag)
	if err != nil {
		t.Fatalf("rerun: %v", err)
	}
	if outcome.RunID == failedRun.ID || outcome.AlreadyStarted {
		t.Fatalf("rerun outcome %+v, want a new run", outcome)
	}
	repo := RepositoryID(uuid.NewString())
	dispatcher, err := NewDispatcher(stack.store, stack.publisher, &fakeRepositoryDiscoverer{identifiers: []RepositoryID{repo}})
	if err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.Work(ctx, dispatchAttemptExecution(orgID, outcome.RunID)); err != nil {
		t.Fatalf("rerun dispatch: %v", err)
	}
	partitions, err := stack.store.DispatchablePartitions(ctx, outcome.RunID)
	if err != nil || len(partitions) != 1 {
		t.Fatalf("rerun partitions: %d %v", len(partitions), err)
	}
	claim, err := stack.store.ClaimPartition(ctx, partitions[0].ID)
	if err != nil || claim == nil {
		t.Fatalf("ClaimPartition: %v %v", claim, err)
	}
	if err := stack.store.CompletePartition(ctx, *claim, stack.publisher); err != nil {
		t.Fatal(err)
	}
	finalize, err := stack.store.ClaimFinalize(ctx, outcome.RunID)
	if err != nil || finalize == nil {
		t.Fatalf("ClaimFinalize: %v %v", finalize, err)
	}
	if err := stack.store.CompleteFinalize(ctx, *finalize); err != nil {
		t.Fatal(err)
	}

	statusOf := func(runID string) string {
		t.Helper()
		var status string
		if err := stack.pool.QueryRow(ctx, `SELECT status FROM daily_metrics_runs WHERE id = $1`, runID).Scan(&status); err != nil {
			t.Fatal(err)
		}
		return status
	}
	state := stack.states(t, orgID, day, day)[dayText]
	t.Logf("failed run=%s rerun=%s day marker=%q", statusOf(failedRun.ID), statusOf(outcome.RunID), state)
	if statusOf(failedRun.ID) != "failed" || statusOf(outcome.RunID) != "succeeded" || state != RunMarkerSucceeded {
		t.Fatalf("failed run=%s rerun=%s marker=%q, want failed, succeeded and the day certified",
			statusOf(failedRun.ID), statusOf(outcome.RunID), state)
	}
}

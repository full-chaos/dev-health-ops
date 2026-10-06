//go:build integration

package workgraph

import (
	"context"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/jackc/pgx/v5/pgxpool"
)

// funcExecutor lets a test script one Execute call at a time.
type funcExecutor struct {
	run func(context.Context, Claim) ([]byte, error)
}

func (executor funcExecutor) Execute(ctx context.Context, claim Claim) ([]byte, error) {
	return executor.run(ctx, claim)
}

func insertMaterializeRequest(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO work_graph_execution_requests (
    id, org_id, kind, scope, llm_concurrency, spend_limit_microunits,
    correlation_id, idempotency_key, state
) VALUES ($1,$2,'investment.materialize','{}',1,0,'cancel-fixture','workgraph:cancel-fixture','pending')`,
		testRequestID, testOrgID,
	); err != nil {
		t.Fatal(err)
	}
}

func requestAndLedgerState(t *testing.T, ctx context.Context, pool *pgxpool.Pool) (string, string) {
	t.Helper()
	var request, ledger string
	if err := pool.QueryRow(ctx, `SELECT state FROM work_graph_execution_requests WHERE id = $1`, testRequestID).Scan(&request); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT state FROM work_graph_execution_ledger WHERE request_id = $1`, testRequestID).Scan(&ledger); err != nil {
		t.Fatal(err)
	}
	return request, ledger
}

func succeedingExecutor() funcExecutor {
	return funcExecutor{run: func(context.Context, Claim) ([]byte, error) {
		return []byte(`{"outcome":{"status":"success"}}`), nil
	}}
}

// TestMaterializeCancelledRunIsRetriedNotStranded is the CHAOS-8782
// reproduction through the real handler and the real Postgres state code: a
// worker shutdown cancels the job context while Execute runs. On main the
// request ends 'ambiguous' and Claim refuses it for good; the fixed handler
// leaves it claimable and a second run succeeds and writes the fence.
func TestMaterializeCancelledRunIsRetriedNotStranded(t *testing.T) {
	ctx := context.Background()
	pool, store, _ := startWorkGraphReleaseLostFixture(t, ctx)
	insertMaterializeRequest(t, ctx, pool)

	jobCtx, cancelJob := context.WithCancel(ctx)
	started := make(chan struct{})
	blocking, err := NewMaterializeHandler(store, funcExecutor{run: func(runCtx context.Context, _ Claim) ([]byte, error) {
		close(started)
		<-runCtx.Done()
		return nil, runCtx.Err()
	}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- blocking.Work(jobCtx, materializeExecution()) }()
	<-started
	cancelJob()
	workErr := <-done
	if workErr == nil || !strings.Contains(workErr.Error(), string(jobruntime.CategoryRetryable)) {
		t.Fatalf("first run = %v, want %s", workErr, jobruntime.CategoryRetryable)
	}
	if request, ledger := requestAndLedgerState(t, ctx, pool); request == "ambiguous" || ledger == "ambiguous" {
		t.Fatalf("request=%s ledger=%s: a cancelled run must not strand the request", request, ledger)
	}
	// The exact shape the strand sweep re-arms on the last attempt
	// (joboutbox TestStrandRepairAgainstLivePostgres, "a requeued materialize
	// request whose job was discarded is rearmed"): pending, no token, no lease.
	var pending int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM work_graph_execution_requests
WHERE id = $1 AND state = 'pending' AND claim_token IS NULL AND lease_expires_at IS NULL`, testRequestID).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if pending != 1 {
		t.Fatalf("requeued request is not pending/unclaimed/unleased")
	}

	retry, err := NewMaterializeHandler(store, succeedingExecutor(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := retry.Work(ctx, materializeExecution()); err != nil {
		t.Fatalf("second run = %v, want success", err)
	}
	if request, ledger := requestAndLedgerState(t, ctx, pool); request != "succeeded" || ledger != "succeeded" {
		t.Fatalf("request=%s ledger=%s, want succeeded/succeeded", request, ledger)
	}
	var fences int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM worker_job_completion_fences`).Scan(&fences); err != nil {
		t.Fatal(err)
	}
	if fences != 1 {
		t.Fatalf("completion fences=%d, want 1", fences)
	}
}

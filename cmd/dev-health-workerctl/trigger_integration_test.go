//go:build integration

package main

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/joboperator"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

// codex review, 2026-09-05, CHAOS-5170 r1 P2 (test-coverage finding): the
// unit-level trigger tests all stop at nil pools or exercise dry-run only,
// so the suite would stay green even if authorization were omitted or
// WriteTx/commit/rollback were broken. These integration tests reach the
// REAL write path against a real Postgres instance, proving:
//
//  1. an authorized credential's manual trigger actually inserts a real
//     work_graph_execution_requests row (the positive control every
//     negative-result test below needs to mean anything);
//  2. a workers:read-scoped credential's identical call inserts NOTHING --
//     the direct repro for r1 P1, run against a real database rather than
//     argued from reading the code.

// triggerExecutionTables is a minimal copy of the schema
// internal/jobs/workgraph/postgres_integration_test.go's own
// createExecutionTables creates -- that helper is unexported in a
// different package and cannot be imported here, so this is a deliberate,
// narrower duplicate carrying only what WriteTx itself touches.
func triggerExecutionTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(ctx, `
CREATE TABLE work_graph_execution_requests (
 id uuid PRIMARY KEY, org_id uuid NOT NULL, kind text NOT NULL, scope jsonb NOT NULL,
 model_ref text NULL, prompt_ref text NULL, llm_concurrency integer NOT NULL,
 spend_limit_microunits bigint NOT NULL, correlation_id text NOT NULL, idempotency_key text NOT NULL UNIQUE,
 state text NOT NULL, claim_token uuid NULL, lease_expires_at timestamptz NULL,
 attempt_count integer NOT NULL DEFAULT 0, created_at timestamptz NOT NULL DEFAULT statement_timestamp(), updated_at timestamptz NOT NULL DEFAULT statement_timestamp()
);
CREATE TABLE worker_job_outbox (
 id uuid PRIMARY KEY, dedupe_key varchar(256) NOT NULL UNIQUE, job_kind varchar(96) NOT NULL,
 contract_version integer NOT NULL, args json NOT NULL, payload_hash varchar(71) NOT NULL,
 queue varchar(96) NOT NULL, priority smallint NOT NULL, max_attempts smallint NOT NULL,
 scheduled_at timestamptz NOT NULL, status varchar(16) NOT NULL, attempt_count integer NOT NULL,
 next_attempt_at timestamptz NOT NULL, prerequisite_completion_key text NULL,
 created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL
);
CREATE TABLE worker_job_completion_fences (
 completion_key text PRIMARY KEY,
 completed_at timestamptz NOT NULL DEFAULT statement_timestamp()
)`)
	if err != nil {
		t.Fatal(err)
	}
}

func triggerIntegrationRuntime(t *testing.T, ctx context.Context, authorizer joboperator.Authorizer) *operatorRuntime {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("close PostgreSQL: %v", err)
		}
	})
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	triggerExecutionTables(t, ctx, pool)

	registry, err := jobruntime.Load(filepath.Join("..", "..", "contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	backend := &commandBackend{queues: map[string]joboperator.QueueSummary{}}
	service, err := joboperator.New(joboperator.Dependencies{
		Registry: registry, Backend: backend, Authorizer: authorizer,
		DomainGuard: commandDomainGuard{}, Auditor: commandAuditor{},
		RouteController:    commandRouteController{},
		JobRouteController: commandJobRouteController{},
	})
	if err != nil {
		t.Fatal(err)
	}
	return &operatorRuntime{
		service:  service,
		registry: registry,
		pools:    &postgresstore.RuntimePools{Domain: pool},
		principal: joboperator.Principal{
			Type: "service_credential",
			ID:   "00000000-0000-4000-8000-000000000303",
		},
	}
}

func countRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, table string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// TestManualTriggerWritesARealRequestRowWhenAuthorized is the POSITIVE
// control: without it, a test asserting "zero rows" for the unauthorized
// case would prove nothing -- it could just as easily be a broken harness
// that never writes anything.
func TestManualTriggerWritesARealRequestRowWhenAuthorized(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	runtime := triggerIntegrationRuntime(t, ctx, commandAuthorizer{})

	var stdout, stderr bytes.Buffer
	code := dispatchWorkgraphTrigger(ctx, runtime, []string{
		"--org", validTriggerOrg, "--review-evidence", "testing",
		"--from", "2026-01-01", "--to", "2026-01-31",
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("authorized trigger failed: code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if got := countRows(t, ctx, runtime.pools.Domain, "work_graph_execution_requests"); got != 1 {
		t.Fatalf("requests=%d, want 1", got)
	}
	if got := countRows(t, ctx, runtime.pools.Domain, "worker_job_outbox"); got != 1 {
		t.Fatalf("outbox=%d, want 1", got)
	}
}

// TestManualTriggerRequiresOperateScope is the direct repro for r1 P1: a
// workers:read-scoped credential's IDENTICAL call must write NOTHING.
func TestManualTriggerRequiresOperateScope(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	runtime := triggerIntegrationRuntime(t, ctx, commandAuthorizer{err: joboperator.ErrAuthorization})

	var stdout, stderr bytes.Buffer
	code := dispatchWorkgraphTrigger(ctx, runtime, []string{
		"--org", validTriggerOrg, "--review-evidence", "testing",
		"--from", "2026-01-01", "--to", "2026-01-31",
	}, &stdout, &stderr)
	if code != 1 || !bytes.Contains(stderr.Bytes(), []byte("unauthorized")) {
		t.Fatalf("expected unauthorized, got code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if got := countRows(t, ctx, runtime.pools.Domain, "work_graph_execution_requests"); got != 0 {
		t.Fatalf("requests=%d, want 0 -- an unauthorized credential wrote a real row", got)
	}
	if got := countRows(t, ctx, runtime.pools.Domain, "worker_job_outbox"); got != 0 {
		t.Fatalf("outbox=%d, want 0", got)
	}
}

// TestInvestmentManualTriggerRequiresOperateScope mirrors the workgraph
// case above for `investment trigger`.
func TestInvestmentManualTriggerRequiresOperateScope(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	runtime := triggerIntegrationRuntime(t, ctx, commandAuthorizer{err: joboperator.ErrAuthorization})

	var stdout, stderr bytes.Buffer
	code := dispatchInvestmentTrigger(ctx, runtime, []string{
		"--org", validTriggerOrg, "--review-evidence", "testing",
		"--from", "2026-01-01", "--to", "2026-01-31",
	}, &stdout, &stderr)
	if code != 1 || !bytes.Contains(stderr.Bytes(), []byte("unauthorized")) {
		t.Fatalf("expected unauthorized, got code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if got := countRows(t, ctx, runtime.pools.Domain, "work_graph_execution_requests"); got != 0 {
		t.Fatalf("requests=%d, want 0 -- an unauthorized credential wrote a real row", got)
	}
}

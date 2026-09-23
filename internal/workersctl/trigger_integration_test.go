//go:build integration

package workersctl

import (
	"bytes"
	"context"
	"log/slog"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/joboperator"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/operatorauditschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
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
	return triggerIntegrationRuntimeWithAuditor(t, ctx, authorizer, nil)
}

// triggerIntegrationRuntimeWithAuditor builds the trigger runtime on a fresh
// Postgres. A nil newAuditor keeps the in-memory commandAuditor; otherwise
// newAuditor builds the auditor from the runtime's own pool.
func triggerIntegrationRuntimeWithAuditor(
	t *testing.T, ctx context.Context, authorizer joboperator.Authorizer,
	newAuditor func(t *testing.T, uri string, pool *pgxpool.Pool) joboperator.Auditor,
) *operatorRuntime {
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
	var auditor joboperator.Auditor = commandAuditor{}
	if newAuditor != nil {
		auditor = newAuditor(t, instance.URI, pool)
	}
	backend := &commandBackend{queues: map[string]joboperator.QueueSummary{}}
	service, err := joboperator.New(joboperator.Dependencies{
		Registry: registry, Backend: backend, Authorizer: authorizer,
		DomainGuard: commandDomainGuard{}, Auditor: auditor,
		RouteController:    commandRouteController{},
		JobRouteController: commandJobRouteController{},
	})
	if err != nil {
		t.Fatal(err)
	}
	return &operatorRuntime{
		service:   service,
		registry:  registry,
		pools:     &postgresstore.RuntimePools{Domain: pool},
		principal: joboperator.OperatorPrincipal,
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
		"--reason", "operator_test", "--correlation-id", "corr-1",
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
		"--reason", "operator_test", "--correlation-id", "corr-1",
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

// triggerExecutionRequestsTableOnly creates ONLY work_graph_execution_requests
// -- omitting worker_job_outbox -- so a real Postgres INSERT into the
// requests table succeeds while the outbox producer's own insert fails on a
// genuine missing-table error. This is what forces WriteTx to fail AFTER
// Begin already succeeded, the exact branch r2 P2's finding targets.
func triggerExecutionRequestsTableOnly(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(ctx, `
CREATE TABLE work_graph_execution_requests (
 id uuid PRIMARY KEY, org_id uuid NOT NULL, kind text NOT NULL, scope jsonb NOT NULL,
 model_ref text NULL, prompt_ref text NULL, llm_concurrency integer NOT NULL,
 spend_limit_microunits bigint NOT NULL, correlation_id text NOT NULL, idempotency_key text NOT NULL UNIQUE,
 state text NOT NULL, claim_token uuid NULL, lease_expires_at timestamptz NULL,
 attempt_count integer NOT NULL DEFAULT 0, created_at timestamptz NOT NULL DEFAULT statement_timestamp(), updated_at timestamptz NOT NULL DEFAULT statement_timestamp()
)`)
	if err != nil {
		t.Fatal(err)
	}
}

// captureDefaultSlog swaps slog's default logger for a text-handler writing
// into buf for the duration of the test, restoring the original on cleanup.
func captureDefaultSlog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &buf
}

// TestManualTriggerWriteTxFailureLogsUnderlyingErrorAndIdentifiers is the
// direct repro for r2 P2: WriteTx failing AFTER Begin succeeded (a real
// missing-table error on the outbox insert, not a fabricated one) must be
// logged with the underlying error plus request_id/org/generation -- the
// r1 fix covered Begin/Commit/Rollback but not this branch.
func TestManualTriggerWriteTxFailureLogsUnderlyingErrorAndIdentifiers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
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
	triggerExecutionRequestsTableOnly(t, ctx, pool) // worker_job_outbox deliberately absent

	registry, err := jobruntime.Load(filepath.Join("..", "..", "contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	backend := &commandBackend{queues: map[string]joboperator.QueueSummary{}}
	service, err := joboperator.New(joboperator.Dependencies{
		Registry: registry, Backend: backend, Authorizer: commandAuthorizer{},
		DomainGuard: commandDomainGuard{}, Auditor: commandAuditor{},
		RouteController:    commandRouteController{},
		JobRouteController: commandJobRouteController{},
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime := &operatorRuntime{
		service:   service,
		registry:  registry,
		pools:     &postgresstore.RuntimePools{Domain: pool},
		principal: joboperator.OperatorPrincipal,
	}

	logs := captureDefaultSlog(t)
	var stdout, stderr bytes.Buffer
	code := dispatchWorkgraphTrigger(ctx, runtime, []string{
		"--org", validTriggerOrg, "--review-evidence", "testing",
		"--reason", "operator_test", "--correlation-id", "corr-1",
		"--from", "2026-01-01", "--to", "2026-01-31",
	}, &stdout, &stderr)
	if code != 1 {
		t.Fatalf("expected the missing outbox table to fail the write: code=%d stdout=%q stderr=%q",
			code, stdout.String(), stderr.String())
	}
	logged := logs.String()
	if !bytes.Contains([]byte(logged), []byte("write failed")) {
		t.Fatalf("expected a 'write failed' log record, got: %s", logged)
	}
	for _, want := range []string{"request_id=", "org=" + validTriggerOrg, "generation=", "error="} {
		if !bytes.Contains([]byte(logged), []byte(want)) {
			t.Fatalf("expected the write-failure log to contain %q, got: %s", want, logged)
		}
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
		"--reason", "operator_test", "--correlation-id", "corr-1",
		"--from", "2026-01-01", "--to", "2026-01-31",
	}, &stdout, &stderr)
	if code != 1 || !bytes.Contains(stderr.Bytes(), []byte("unauthorized")) {
		t.Fatalf("expected unauthorized, got code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if got := countRows(t, ctx, runtime.pools.Domain, "work_graph_execution_requests"); got != 0 {
		t.Fatalf("requests=%d, want 0 -- an unauthorized credential wrote a real row", got)
	}
}

// migratedPostgresAuditor applies the real worker_operator_audits
// migrations and returns the production PostgresAuditor on that pool.
func migratedPostgresAuditor(t *testing.T, uri string, pool *pgxpool.Pool) joboperator.Auditor {
	t.Helper()
	ctx := context.Background()
	python := pyoracle.Resolve(t, operatorauditschema.Root())
	command := exec.CommandContext(ctx, python, operatorauditschema.Argv(uri)...)
	command.Env = operatorauditschema.Env()
	output, err := command.CombinedOutput()
	operatorauditschema.CheckApplied(t, python, output, err)
	auditor, err := joboperator.NewPostgresAuditor(pool)
	if err != nil {
		t.Fatal(err)
	}
	return auditor
}

// TestManualTriggersWriteOneAuditRowOrNothing drives the direct-write
// audited path end to end on Postgres, with the table the real migrations
// build and the production PostgresAuditor: a workgraph trigger writes its
// request and exactly one succeeded audit row; with the audit table
// unwritable, an investment trigger answers audit_unavailable and writes no
// request at all.
func TestManualTriggersWriteOneAuditRowOrNothing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	runtime := triggerIntegrationRuntimeWithAuditor(t, ctx, commandAuthorizer{}, migratedPostgresAuditor)
	pool := runtime.pools.Domain

	var stdout, stderr bytes.Buffer
	code := dispatch(ctx, runtime, []string{
		"workgraph", "trigger", "--org", validTriggerOrg, "--review-evidence", "testing",
		"--reason", "operator_test", "--correlation-id", "corr-audit-1",
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("trigger: code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if got := countRows(t, ctx, pool, "work_graph_execution_requests"); got != 1 {
		t.Fatalf("requests=%d, want 1", got)
	}
	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM public.worker_operator_audits
		WHERE action = 'workgraph.manual_trigger' AND principal_type = 'operator' AND principal_id = 'dho-workers'
		  AND credential_id IS NULL AND resource_type = 'organization' AND resource_id = $1
		  AND reason_code = 'operator_test' AND correlation_id = 'corr-audit-1'
		  AND status = 'succeeded' AND completed_at IS NOT NULL`, validTriggerOrg).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 || countRows(t, ctx, pool, "public.worker_operator_audits") != 1 {
		t.Fatalf("matching audit rows=%d, total=%d, want exactly one succeeded row", rows, countRows(t, ctx, pool, "public.worker_operator_audits"))
	}

	if _, err := pool.Exec(ctx, "ALTER TABLE public.worker_operator_audits RENAME TO worker_operator_audits_unwritable"); err != nil {
		t.Fatal(err)
	}
	stdout.Reset()
	stderr.Reset()
	code = dispatch(ctx, runtime, []string{
		"investment", "trigger", "--org", validTriggerOrg, "--review-evidence", "testing",
		"--reason", "operator_test", "--correlation-id", "corr-audit-2",
	}, &stdout, &stderr)
	if code != 1 || stderr.String() != "{\"error\":{\"code\":\"audit_unavailable\"}}\n" || stdout.Len() != 0 {
		t.Fatalf("unauditable trigger: code=%d stdout=%q stderr=%q, want audit_unavailable", code, stdout.String(), stderr.String())
	}
	if got := countRows(t, ctx, pool, "work_graph_execution_requests"); got != 1 {
		t.Fatalf("requests=%d, want still 1 -- a trigger whose audit row could not be written wrote its request", got)
	}
}

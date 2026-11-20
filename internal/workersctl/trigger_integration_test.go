//go:build integration

package workersctl

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/joboperator"
	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/google/uuid"
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

// triggerExecutionTables builds the migrated schema: the execution requests, the worker outbox and
// the completion fences carry their real columns, constraints and foreign keys.
func triggerExecutionTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	pgschema.Apply(ctx, t, pool)
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

// triggerExecutionRequestsTableOnly builds the migrated schema and then DROPS worker_job_outbox, so a
// real Postgres INSERT into the requests table succeeds while the outbox producer's own insert fails
// on a genuine missing-table error. This is what forces WriteTx to fail AFTER Begin already
// succeeded, the exact branch r2 P2's finding targets.
func triggerExecutionRequestsTableOnly(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	pgschema.Apply(ctx, t, pool)
	if _, err := pool.Exec(ctx, `DROP TABLE public.worker_job_outbox CASCADE`); err != nil {
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

// migratedPostgresAuditor returns the production PostgresAuditor on a pool whose database already
// carries the migrated schema (worker_operator_audits included): triggerExecutionTables applied the
// checked-in head, so no migration is replayed on top of it.
func migratedPostgresAuditor(t *testing.T, uri string, pool *pgxpool.Pool) joboperator.Auditor {
	t.Helper()
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

// A second identical `investment trigger` is deduplicated: it must not report
// "started", must exit non-zero, and must not say "enqueued". A --rerun-tag
// makes the same org/window a new request with its own job.
func TestInvestmentManualTriggerReportsDuplicateAndRerunTagEnqueues(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	runtime := triggerIntegrationRuntime(t, ctx, commandAuthorizer{})
	pool := runtime.pools.Domain
	base := []string{
		"--org", validTriggerOrg, "--review-evidence", "testing",
		"--reason", "operator_test", "--correlation-id", "corr-1",
		"--from", "2026-01-01", "--to", "2026-01-31",
	}

	var stdout, stderr bytes.Buffer
	if code := dispatchInvestmentTrigger(ctx, runtime, base, &stdout, &stderr); code != 0 ||
		!bytes.Contains(stdout.Bytes(), []byte(`"status":"started"`)) {
		t.Fatalf("first trigger: code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if got := countRows(t, ctx, pool, "worker_job_outbox"); got != 1 {
		t.Fatalf("outbox after first=%d, want 1", got)
	}

	logs := captureDefaultSlog(t)
	stdout.Reset()
	stderr.Reset()
	code := dispatchInvestmentTrigger(ctx, runtime, base, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("duplicate trigger exited 0: stdout=%q", stdout.String())
	}
	if bytes.Contains(stdout.Bytes(), []byte(`"status":"started"`)) || !bytes.Contains(stdout.Bytes(), []byte(`"status":"duplicate"`)) {
		t.Fatalf("duplicate trigger must report duplicate, got stdout=%q", stdout.String())
	}
	if bytes.Contains(logs.Bytes(), []byte("manual investment trigger: enqueued")) {
		t.Fatalf("duplicate trigger logged enqueued: %s", logs.String())
	}
	if got := countRows(t, ctx, pool, "work_graph_execution_requests"); got != 1 {
		t.Fatalf("requests after duplicate=%d, want 1", got)
	}
	if got := countRows(t, ctx, pool, "worker_job_outbox"); got != 1 {
		t.Fatalf("outbox after duplicate=%d, want 1", got)
	}

	stdout.Reset()
	stderr.Reset()
	tagged := append(append([]string{}, base...), "--rerun-tag", "late-sync")
	if code := dispatchInvestmentTrigger(ctx, runtime, tagged, &stdout, &stderr); code != 0 ||
		!bytes.Contains(stdout.Bytes(), []byte(`"status":"started"`)) {
		t.Fatalf("rerun-tag trigger: code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if got := countRows(t, ctx, pool, "work_graph_execution_requests"); got != 2 {
		t.Fatalf("requests after rerun tag=%d, want 2", got)
	}
	if got := countRows(t, ctx, pool, "worker_job_outbox"); got != 2 {
		t.Fatalf("outbox after rerun tag=%d, want 2", got)
	}
}

func runMembershipTriggerBackstop(t *testing.T, ctx context.Context, runtime *operatorRuntime) (int, map[string]any, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := dispatchMetrics(ctx, runtime, []string{
		"remaining", "trigger-backstop", "--family", "membership_backfill",
		"--org", validTriggerOrg, "--review-evidence", "testing",
		"--reason", "operator_test", "--correlation-id", "corr-1",
	}, &stdout, &stderr)
	result := map[string]any{}
	if stdout.Len() > 0 {
		if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
			t.Fatalf("decode stdout %q: %v", stdout.String(), err)
		}
	}
	return code, result, stderr.String()
}

func countMembershipTriggerRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool) (runs, builds int) {
	t.Helper()
	if err := pool.QueryRow(ctx,
		"SELECT count(*) FROM remaining_metric_runs WHERE family = 'membership_backfill'",
	).Scan(&runs); err != nil {
		t.Fatal(err)
	}
	return runs, countRows(t, ctx, pool, "work_graph_execution_requests")
}

// TestTriggerBackstopMembershipChainsTheGraphBuildLikeTheNightlySchedule:
// the manual membership run is fenced behind a work-graph build written in
// the same transaction, and a repeat of the same request reuses both.
func TestTriggerBackstopMembershipChainsTheGraphBuildLikeTheNightlySchedule(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	runtime := triggerIntegrationRuntime(t, ctx, commandAuthorizer{})
	pool := runtime.pools.Domain

	code, result, stderr := runMembershipTriggerBackstop(t, ctx, runtime)
	if code != 0 || result["status"] != "started" {
		t.Fatalf("first trigger: code=%d result=%v stderr=%q", code, result, stderr)
	}
	yesterday := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -1).Format("2006-01-02")
	if want := "manual-trigger:membership_backfill:" + validTriggerOrg + ":" + yesterday; result["generation"] != want {
		t.Fatalf("generation = %v, want %s", result["generation"], want)
	}
	if runs, builds := countMembershipTriggerRows(t, ctx, pool); runs != 1 || builds != 1 {
		t.Fatalf("runs=%d builds=%d, want 1/1", runs, builds)
	}
	var buildID, fence string
	if err := pool.QueryRow(ctx, "SELECT id::text FROM work_graph_execution_requests").Scan(&buildID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx,
		"SELECT coalesce(prerequisite_completion_key, '') FROM worker_job_outbox WHERE job_kind = $1",
		"metrics.remaining.membership_backfill",
	).Scan(&fence); err != nil {
		t.Fatal(err)
	}
	if fence != "work_graph_execution_request:"+buildID {
		t.Fatalf("membership partition fence = %q, want work_graph_execution_request:%s", fence, buildID)
	}

	code, result, stderr = runMembershipTriggerBackstop(t, ctx, runtime)
	if code != 0 || result["status"] != "already_ran" {
		t.Fatalf("repeat trigger: code=%d result=%v stderr=%q", code, result, stderr)
	}
	if runs, builds := countMembershipTriggerRows(t, ctx, pool); runs != 1 || builds != 1 {
		t.Fatalf("after repeat runs=%d builds=%d, want 1/1", runs, builds)
	}
}

// TestTriggerBackstopMembershipSecondTriggerForACoveredDayIsRefused: once the
// day's membership run succeeded with rows, a second trigger for that day is
// refused and writes neither a run nor a graph build.
func TestTriggerBackstopMembershipSecondTriggerForACoveredDayIsRefused(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	runtime := triggerIntegrationRuntime(t, ctx, commandAuthorizer{})
	pool := runtime.pools.Domain

	code, result, stderr := runMembershipTriggerBackstop(t, ctx, runtime)
	if code != 0 || result["status"] != "started" {
		t.Fatalf("first trigger: code=%d result=%v stderr=%q", code, result, stderr)
	}
	store, err := remaining.NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	partitionID, _ := result["partition_id"].(string)
	claim, err := store.ClaimPartition(ctx, partitionID)
	if err != nil || claim == nil {
		t.Fatalf("claim %q: claim=%v err=%v", partitionID, claim, err)
	}
	if err := store.CompletePartition(ctx, *claim, "compatibility_execution:"+partitionID+":rows_written=5"); err != nil {
		t.Fatal(err)
	}

	code, result, stderr = runMembershipTriggerBackstop(t, ctx, runtime)
	if code != 0 || result["status"] != "already_covered" {
		t.Fatalf("second trigger for a covered day: code=%d result=%v stderr=%q", code, result, stderr)
	}
	if runs, builds := countMembershipTriggerRows(t, ctx, pool); runs != 1 || builds != 1 {
		t.Fatalf("after second trigger runs=%d builds=%d, want 1/1", runs, builds)
	}
}

// TestTriggerBackstopMembershipRefusesWhileAScheduledRunForTheDayIsInFlight:
// a pending nightly-shaped run labelled with the same day, whose graph build
// has completed (its fence exists, so the partition is deliverable), blocks
// the manual trigger as in_progress.
func TestTriggerBackstopMembershipRefusesWhileAScheduledRunForTheDayIsInFlight(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	runtime := triggerIntegrationRuntime(t, ctx, commandAuthorizer{})
	pool := runtime.pools.Domain

	store, err := remaining.NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	yesterday := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -1).Format("2006-01-02")
	publisher, err := remaining.NewPostgresPublisher(pool, runtime.registry)
	if err != nil {
		t.Fatal(err)
	}
	buildKey := "work_graph_execution_request:" + uuid.NewSHA1(uuid.NameSpaceOID, []byte("nightly-build-"+yesterday)).String()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := joboutbox.MarkCompletionTx(ctx, tx, buildKey); err != nil {
		t.Fatal(err)
	}
	scheduled, err := store.StartRunTx(ctx, tx, remaining.StartRunRequest{
		OrganizationID:            validTriggerOrg,
		Family:                    "membership_backfill",
		Generation:                "fixed-schedule:membership_backfill_daily_fanout:" + yesterday + "T10:30:00Z",
		ScopeKey:                  yesterday,
		Scopes:                    []json.RawMessage{json.RawMessage(`{"version":1,"repo_ids":[]}`)},
		PrerequisiteCompletionKey: buildKey,
	}, publisher)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	code, result, stderr := runMembershipTriggerBackstop(t, ctx, runtime)
	if code != 0 || result["status"] != "in_progress" || result["run_id"] != scheduled.ID {
		t.Fatalf("trigger beside an in-flight scheduled run: code=%d result=%v stderr=%q", code, result, stderr)
	}
	if runs, builds := countMembershipTriggerRows(t, ctx, pool); runs != 1 || builds != 0 {
		t.Fatalf("runs=%d builds=%d, want 1/0", runs, builds)
	}
}

func TestTriggerBackstopMembershipRequiresOperateScope(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	runtime := triggerIntegrationRuntime(t, ctx, commandAuthorizer{err: joboperator.ErrAuthorization})

	code, _, stderr := runMembershipTriggerBackstop(t, ctx, runtime)
	if code != 1 || !strings.Contains(stderr, "unauthorized") {
		t.Fatalf("expected unauthorized, got code=%d stderr=%q", code, stderr)
	}
	if runs, builds := countMembershipTriggerRows(t, ctx, runtime.pools.Domain); runs != 0 || builds != 0 {
		t.Fatalf("runs=%d builds=%d, want 0/0", runs, builds)
	}
}

//go:build integration

package postgres

import (
	"context"
	"testing"
	"time"
)

// CHAOS-5459: the Go repair verbs (internal/jobs/repair) write on the operator
// (coordinator) role. This pins the grant delta both ways, executed against the
// grants ApplyPinnedMigrations really emits: the repair statements are permitted
// to the coordinator, and everything beyond the delta -- other columns, DELETE,
// the worker (domain) role -- is refused with 42501.

var repairPermittedStatements = []string{
	`SELECT request.state, request.claim_token::text, request.lease_expires_at::text, ledger.state, ledger.attempt_count
	 FROM public.work_graph_execution_requests AS request
	 JOIN public.work_graph_execution_ledger AS ledger ON ledger.request_id = request.id
	 WHERE request.id = gen_random_uuid() FOR UPDATE OF request, ledger`,
	`INSERT INTO public.work_graph_execution_repairs (id, request_id, expected_attempt_count, resolution, review_evidence)
	 VALUES (gen_random_uuid(), gen_random_uuid(), 1, 'retry_safe', 'e')`,
	`UPDATE public.work_graph_execution_requests SET state = 'pending', updated_at = statement_timestamp()
	 WHERE id = gen_random_uuid() AND state = 'ambiguous'`,
	`UPDATE public.work_graph_execution_ledger SET state = 'repaired', output_evidence = NULL, failure_detail = NULL, completed_at = NULL
	 WHERE request_id = gen_random_uuid() AND state = 'ambiguous'`,
	`SELECT id::text, worker_kind, operation, run_id::text, partition_id::text, claim_token::text, state, attempt_count
	 FROM public.metric_compatibility_executions WHERE id = gen_random_uuid() FOR UPDATE`,
	`UPDATE public.metric_compatibility_executions
	 SET state = 'succeeded', output_evidence = '{}'::jsonb, completed_at = statement_timestamp(), last_attempt_at = statement_timestamp()
	 WHERE id = gen_random_uuid() AND state = 'ambiguous' AND attempt_count = 1 RETURNING id`,
	`INSERT INTO public.metric_compatibility_execution_repairs
	 (id, execution_id, expected_state, expected_attempt_count, resolution, review_evidence)
	 VALUES (gen_random_uuid(), gen_random_uuid(), 'ambiguous', 1, 'retry_safe', 'e')`,
	`SELECT EXISTS (SELECT 1 FROM public.daily_metrics_runs AS r JOIN public.daily_metrics_partitions AS p ON p.run_id = r.id
	 WHERE p.claim_token = gen_random_uuid())`,
	`SELECT EXISTS (SELECT 1 FROM public.remaining_metric_runs AS r JOIN public.remaining_metric_partitions AS p ON p.run_id = r.id
	 WHERE p.claim_token = gen_random_uuid())`,
}

// Statements the repair does NOT need and the operator role must not gain.
var repairRefusedToCoordinator = []string{
	"UPDATE public.work_graph_execution_requests SET claim_token = NULL WHERE id = gen_random_uuid()",
	"UPDATE public.work_graph_execution_requests SET attempt_count = 0 WHERE id = gen_random_uuid()",
	"UPDATE public.work_graph_execution_ledger SET attempt_count = 9 WHERE request_id = gen_random_uuid()",
	"UPDATE public.work_graph_execution_ledger SET claim_token = gen_random_uuid() WHERE request_id = gen_random_uuid()",
	"UPDATE public.metric_compatibility_executions SET attempt_count = 9 WHERE id = gen_random_uuid()",
	"UPDATE public.metric_compatibility_executions SET claim_token = gen_random_uuid() WHERE id = gen_random_uuid()",
	"UPDATE public.work_graph_execution_repairs SET resolution = 'retry_safe'",
	"UPDATE public.metric_compatibility_execution_repairs SET resolution = 'retry_safe'",
	"DELETE FROM public.work_graph_execution_repairs",
	"DELETE FROM public.metric_compatibility_execution_repairs",
	"DELETE FROM public.work_graph_execution_requests WHERE id = gen_random_uuid()",
	"DELETE FROM public.metric_compatibility_executions WHERE id = gen_random_uuid()",
	"INSERT INTO public.metric_compatibility_executions (id, worker_kind, operation, run_id, state) VALUES (gen_random_uuid(), 'daily', 'finalize', gen_random_uuid(), 'ambiguous')",
}

func TestRepairStatementsArePermittedToTheCoordinatorRole(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	_, uri, roles := startGrantHarness(t, ctx)
	coordinator := connectAs(t, ctx, uri, roles.coordinator, grantCoordinatorPass)
	for _, statement := range repairPermittedStatements {
		if err := execInRolledBackTransaction(t, ctx, coordinator, statement); err != nil {
			t.Errorf("denied to the coordinator role, so coordinatorPosture lacks a repair grant\n  statement: %s\n  error: %v", collapse(statement), err)
		}
	}
}

func TestRepairGrantDeltaIsNoWiderThanTheRepairNeeds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	_, uri, roles := startGrantHarness(t, ctx)
	coordinator := connectAs(t, ctx, uri, roles.coordinator, grantCoordinatorPass)
	for _, statement := range repairRefusedToCoordinator {
		err := execInRolledBackTransaction(t, ctx, coordinator, statement)
		if err == nil {
			t.Errorf("the coordinator role was PERMITTED beyond the repair grant delta\n  statement: %s", collapse(statement))
			continue
		}
		if !isInsufficientPrivilege(err) {
			t.Errorf("expected 42501, got %v\n  statement: %s", err, collapse(statement))
		}
	}
}

// The worker (domain) role must not be able to write the repair audit tables.
func TestRepairAuditInsertsAreRefusedToTheWorkerRole(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	_, uri, roles := startGrantHarness(t, ctx)
	domain := connectAs(t, ctx, uri, roles.domain, grantDomainPass)
	for _, statement := range []string{repairPermittedStatements[1], repairPermittedStatements[6]} {
		err := execInRolledBackTransaction(t, ctx, domain, statement)
		if err == nil {
			t.Errorf("the worker role INSERTed a repair audit row\n  statement: %s", collapse(statement))
			continue
		}
		if !isInsufficientPrivilege(err) {
			t.Errorf("expected 42501, got %v\n  statement: %s", err, collapse(statement))
		}
	}
}

// The column-scoped move must lose nothing the coordinator role does today on
// work_graph_execution_requests (before CHAOS-5459: table-wide INSERT + SELECT,
// no UPDATE/DELETE). Its only writers are the RequestWriter producers
// (internal/jobs/workgraph/publisher.go): the INSERT ... ON CONFLICT DO NOTHING
// and the follow-up read of the existing row, both verbatim below. The ledger
// and metric_compatibility_executions tables were not granted to the coordinator
// at all, so nothing there can be lost; the domain and queue roles are
// unchanged (their postures are asserted by the existing domain/queue suites).
func TestCoordinatorKeepsEveryRequestWriterRightAfterTheColumnMove(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, uri, roles := startGrantHarness(t, ctx)
	coordinator := connectAs(t, ctx, uri, roles.coordinator, grantCoordinatorPass)

	for _, statement := range []string{
		`INSERT INTO public.work_graph_execution_requests (
		    id, org_id, kind, scope, model_ref, prompt_ref, llm_concurrency,
		    spend_limit_microunits, correlation_id, idempotency_key, state
		) VALUES (gen_random_uuid(), gen_random_uuid(), 'workgraph.build', '{}'::jsonb, NULLIF('', ''), NULLIF('', ''), 1,
		    0, 'c', 'i', 'pending')
		ON CONFLICT (id) DO NOTHING`,
		`SELECT id::text, org_id::text, kind, scope::text, COALESCE(model_ref, ''),
		        COALESCE(prompt_ref, ''), llm_concurrency, spend_limit_microunits,
		        correlation_id, idempotency_key, state
		 FROM public.work_graph_execution_requests WHERE id = gen_random_uuid()`,
	} {
		if err := execInRolledBackTransaction(t, ctx, coordinator, statement); err != nil {
			t.Errorf("coordinator lost a RequestWriter right: %v\n  statement: %s", err, collapse(statement))
		}
	}

	// Superset check over EVERY real column of the table: the coordinator held
	// table-wide INSERT+SELECT before, so it must still hold both on each column.
	rows, err := admin.Query(ctx, `SELECT column_name FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'work_graph_execution_requests'`)
	if err != nil {
		t.Fatal(err)
	}
	var columns []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		columns = append(columns, name)
	}
	rows.Close()
	if len(columns) == 0 {
		t.Fatal("no columns found; the check would be vacuous")
	}
	for _, column := range columns {
		for _, privilege := range []string{"SELECT", "INSERT"} {
			var held bool
			if err := admin.QueryRow(ctx, `SELECT has_column_privilege($1, 'public.work_graph_execution_requests', $2, $3)`,
				roles.coordinator, column, privilege).Scan(&held); err != nil {
				t.Fatal(err)
			}
			if !held {
				t.Errorf("coordinator lost %s on work_graph_execution_requests.%s", privilege, column)
			}
		}
	}
}

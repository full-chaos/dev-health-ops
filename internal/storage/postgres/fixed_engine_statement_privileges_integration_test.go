//go:build integration

package postgres

import (
	"context"
	"testing"
	"time"
)

// CHAOS-3114, second half: the fixed maintenance engine
// (internal/scheduler/fixed) runs on the coordinator pool, so the coordinator
// role must be able to execute Engine.runOccurrence's ENTIRE statement set in
// one transaction — the coordinator-exclusive occurrence ledger together with
// the domain rows its producers materialize.
//
// This file executes that statement set as the real coordinator role, with
// grants applied by ApplyPinnedMigrations (startGrantHarness), for the same
// reason coordinator_statement_privileges_integration_test.go exists: the
// posture and the grant list share an author and a construction method, so
// they cannot check each other. Only running the real statements as the real
// restricted role can.
//
// Two traps this suite exists to catch, both of which a verb-only reading of
// the code misses:
//
//   - `SELECT ... FOR UPDATE` needs UPDATE on the locked rows even though it
//     writes nothing. The ledger's identity-verification read is exactly that.
//   - `INSERT ... ON CONFLICT (col) DO NOTHING` needs SELECT on the arbiter
//     column as well as INSERT, even though DO NOTHING reads nothing back.
//     That is why the coordinator's worker_job_completion_fences posture is
//     the SELECT+INSERT column pair and not INSERT alone; verified here, and
//     empirically against a live server before it was declared.
//
// The statements are the production shapes with parameters inlined as
// literals. Placeholder values are chosen so each statement reaches the
// permission check: an undefined column or table raises 42703/42P01 during
// parse analysis, BEFORE the ACL check, which would leave the assertion
// measuring nothing.

// fixedEngineStatement is one statement runOccurrence (or the window read
// immediately preceding it) executes, the call site it comes from, and whether
// the coordinator grant set BEFORE CHAOS-3114 could execute it.
type fixedEngineStatement struct {
	name string
	site string
	// privilege names what makes this statement need a grant, in the form the
	// posture declares it.
	privilege string
	sql       string
	// deniedBeforeThisChange marks the statements the pre-CHAOS-3114
	// coordinator posture could not run. They are the whole justification for
	// widening the posture, so the mutation test below demands each one fail
	// with 42501 once those privileges are taken back.
	deniedBeforeThisChange bool
}

func fixedEngineStatements() []fixedEngineStatement {
	const occurrenceKey = "fixed-schedule:heartbeat:2026-07-25T00:00:00Z"
	return []fixedEngineStatement{
		{
			name:      "engine reads the schedule's newest durable occurrence",
			site:      "internal/scheduler/fixed/engine.go lastRecorded -> ledger.go selectLastOccurrenceSQL",
			privilege: "fixed_schedule_occurrences SELECT",
			sql: `SELECT scheduled_for, observed_at
				FROM public.fixed_schedule_occurrences
				WHERE schedule_id = 'heartbeat'
				ORDER BY scheduled_for DESC
				LIMIT 1`,
		},
		{
			name:      "ledger claims the occurrence",
			site:      "internal/scheduler/fixed/ledger.go Claim -> insertOccurrenceSQL",
			privilege: "fixed_schedule_occurrences INSERT (+ SELECT for RETURNING)",
			sql: `INSERT INTO public.fixed_schedule_occurrences (
					occurrence_key, identity_version, schedule_id, target_kind,
					scheduled_for, observed_at, status, handoff_count, created_at, updated_at
				) VALUES ('` + occurrenceKey + `', 'v1', 'heartbeat', 'ops.heartbeat',
					now(), now(), 'claimed', 0, now(), now())
				ON CONFLICT DO NOTHING
				RETURNING occurrence_key`,
		},
		{
			// The trap. Claim's duplicate arm verifies the persisted identity
			// under a row lock; FOR UPDATE requires UPDATE on the locked rows
			// with nothing written, so a reader grepping for UPDATE statements
			// against this table would conclude SELECT was enough.
			name:      "ledger verifies a duplicate claim under a row lock",
			site:      "internal/scheduler/fixed/ledger.go Claim -> selectOccurrenceSQL",
			privilege: "fixed_schedule_occurrences UPDATE (implied by FOR UPDATE)",
			sql: `SELECT identity_version, schedule_id, target_kind, scheduled_for
				FROM public.fixed_schedule_occurrences
				WHERE occurrence_key = '` + occurrenceKey + `'
				FOR UPDATE`,
		},
		{
			name:      "ledger records the producer outcome",
			site:      "internal/scheduler/fixed/ledger.go Complete -> completeOccurrenceSQL",
			privilege: "fixed_schedule_occurrences UPDATE",
			sql: `UPDATE public.fixed_schedule_occurrences
				SET status = 'materialized', handoff_count = 1, skip_reason = NULL,
					completed_at = now(), updated_at = now()
				WHERE occurrence_key = '` + occurrenceKey + `' AND status = 'claimed'`,
		},
		{
			name:      "fan-out producer enumerates active organizations",
			site:      "internal/scheduler/fixed/organizations.go ActiveOrganizationIDs",
			privilege: "organizations SELECT",
			sql: `SELECT id::text FROM public.organizations
				WHERE is_active = TRUE ORDER BY id LIMIT 5001`,
		},
		{
			name:      "Ask Dev retention admission reads explicit entitlement and persisted state",
			site:      "internal/scheduler/fixed/ask_dev_admission.go postgresAskDevRetentionAdmission.State",
			privilege: "feature_flags, organizations, org_licenses, org_feature_overrides, dev_conversations SELECT",
			sql: `WITH ask_dev_feature AS (
					SELECT id FROM public.feature_flags
					WHERE key = 'ask_dev' AND is_enabled = TRUE
					  AND min_tier IN ('community', 'team', 'enterprise')
				), enabled_organization AS (
					SELECT 1
					FROM ask_dev_feature AS feature
					JOIN public.organizations AS organization ON TRUE
					LEFT JOIN public.org_licenses AS license ON license.org_id = organization.id
					LEFT JOIN public.org_feature_overrides AS feature_override
					  ON feature_override.org_id = organization.id
					 AND feature_override.feature_id = feature.id
					WHERE CASE
						WHEN feature_override.id IS NOT NULL AND
							(feature_override.expires_at IS NULL OR feature_override.expires_at > CURRENT_TIMESTAMP)
						THEN feature_override.is_enabled
						WHEN jsonb_typeof((license.features_override::jsonb) -> 'ask_dev') = 'boolean'
						THEN ((license.features_override::jsonb) ->> 'ask_dev')::boolean
						ELSE FALSE
					END LIMIT 1
				)
				SELECT EXISTS (SELECT 1 FROM enabled_organization),
					EXISTS (SELECT 1 FROM public.dev_conversations LIMIT 1)`,
		},
		{
			name:                   "fan-out producer starts a remaining-metrics run",
			site:                   "internal/jobs/metrics/remaining/postgres.go StartRunTx, reached from internal/scheduler/fixed/producers.go Produce",
			privilege:              "remaining_metric_runs INSERT",
			deniedBeforeThisChange: true,
			sql: `INSERT INTO public.remaining_metric_runs
					(id, org_id, family, generation, scope_key, generation_seed, status, created_at, updated_at)
				VALUES (gen_random_uuid(), gen_random_uuid(), 'complexity',
					'fixed-schedule:complexity_daily_fanout:2026-07-25T00:45:00Z', '2026-07-25', NULL,
					'pending', now(), now())
				ON CONFLICT DO NOTHING`,
		},
		{
			// The replay arm: a run whose deterministic identity already exists
			// is re-read and verified rather than duplicated.
			name:                   "remaining-metrics replay re-reads the existing run",
			site:                   "internal/jobs/metrics/remaining/postgres.go loadStartedRun",
			privilege:              "remaining_metric_runs SELECT",
			deniedBeforeThisChange: true,
			sql: `SELECT id::text, org_id::text, family, generation, scope_key, status, generation_seed
				FROM public.remaining_metric_runs WHERE id = gen_random_uuid()`,
		},
		{
			name:                   "fan-out producer writes each run partition",
			site:                   "internal/jobs/metrics/remaining/postgres.go StartRunTx",
			privilege:              "remaining_metric_partitions INSERT",
			deniedBeforeThisChange: true,
			sql: `INSERT INTO public.remaining_metric_partitions
					(id, run_id, ordinal, scope, status, attempt_count, created_at, updated_at)
				VALUES (gen_random_uuid(), gen_random_uuid(), 1, '{}'::jsonb, 'pending', 0, now(), now())`,
		},
		{
			name:                   "remaining-metrics replay verifies the persisted partitions",
			site:                   "internal/jobs/metrics/remaining/postgres.go verifyStartedPartitions",
			privilege:              "remaining_metric_partitions SELECT",
			deniedBeforeThisChange: true,
			sql: `SELECT id::text, ordinal, scope
				FROM public.remaining_metric_partitions
				WHERE run_id = gen_random_uuid() ORDER BY ordinal`,
		},
		{
			name:                   "membership safety net starts its work-graph build",
			site:                   "internal/jobs/workgraph/publisher.go WriteTx, reached from internal/scheduler/fixed/producers.go startGraphBuild",
			privilege:              "work_graph_execution_requests INSERT",
			deniedBeforeThisChange: true,
			sql: `INSERT INTO public.work_graph_execution_requests (
					id, org_id, kind, scope, model_ref, prompt_ref, llm_concurrency,
					spend_limit_microunits, correlation_id, idempotency_key, state
				) VALUES (gen_random_uuid(), gen_random_uuid(), 'build', '{}'::jsonb,
					NULLIF('', ''), NULLIF('', ''), 1, 0, 'correlation', 'idempotency', 'pending')
				ON CONFLICT (id) DO NOTHING`,
		},
		{
			name:                   "work-graph replay re-reads the existing request",
			site:                   "internal/jobs/workgraph/publisher.go WriteTx conflict arm",
			privilege:              "work_graph_execution_requests SELECT",
			deniedBeforeThisChange: true,
			sql: `SELECT id::text, org_id::text, kind, scope::text, COALESCE(model_ref, ''),
					COALESCE(prompt_ref, ''), llm_concurrency, spend_limit_microunits,
					correlation_id, idempotency_key, state
				FROM public.work_graph_execution_requests WHERE id = gen_random_uuid()`,
		},
		{
			name:                   "engine publishes the job handoff",
			site:                   "internal/joboutbox/producer.go publish, reached from internal/scheduler/fixed/engine.go OutboxPublisher.Publish",
			privilege:              "worker_job_outbox INSERT",
			deniedBeforeThisChange: true,
			sql: `INSERT INTO public.worker_job_outbox (
					id, dedupe_key, job_kind, contract_version, args, payload_hash,
					queue, priority, max_attempts, scheduled_at, status, attempt_count,
					next_attempt_at, prerequisite_completion_key, created_at, updated_at
				) VALUES (gen_random_uuid(), 'heartbeat:2026-07-25T00:00:00Z', 'ops.heartbeat', 1,
					'{}'::json, 'sha256:0', 'monitoring', 1, 3, now(),
					'pending', 0, now(), NULLIF('', ''), now(), now())
				ON CONFLICT (dedupe_key) DO NOTHING`,
		},
		{
			name:      "handoff replay verifies the existing outbox row",
			site:      "internal/joboutbox/producer.go publish conflict arm",
			privilege: "worker_job_outbox SELECT",
			sql: `SELECT job_kind, contract_version, payload_hash,
					COALESCE(prerequisite_completion_key, '')
				FROM public.worker_job_outbox WHERE dedupe_key = 'heartbeat:2026-07-25T00:00:00Z'`,
		},
		{
			// The second trap, and the one the pre-3114 analysis got wrong: a
			// grep across internal/scheduler/fixed for MarkCompletionTx finds
			// nothing, because the reach is transitive through StartRunTx and
			// WriteTx's already-succeeded replay arms.
			name:                   "replayed predecessor mints its completion fence",
			site:                   "internal/joboutbox/completion.go MarkCompletionTx, reached from remaining.StartRunTx and workgraph.WriteTx inside the occurrence transaction",
			privilege:              "worker_job_completion_fences completion_key SELECT+INSERT (column-scoped; the arbiter needs SELECT)",
			deniedBeforeThisChange: true,
			sql: `INSERT INTO public.worker_job_completion_fences (completion_key)
				VALUES ('work_graph_execution_request:11111111-1111-5111-8111-111111111111')
				ON CONFLICT (completion_key) DO NOTHING`,
		},
	}
}

// preCHAOS3114CoordinatorRevocations restores the coordinator role to exactly
// the grant set it held before this change: remaining_metric_runs,
// remaining_metric_partitions and work_graph_execution_requests were absent
// from coordinatorPosture entirely, worker_job_outbox was SELECT+UPDATE (the
// UPDATE existing only for the jobroute LOCK), and worker_job_completion_fences
// had no coordinator grant of any kind.
func preCHAOS3114CoordinatorRevocations() []string {
	return []string{
		"REVOKE ALL PRIVILEGES ON TABLE public.remaining_metric_runs FROM " + grantCoordinatorRole,
		"REVOKE ALL PRIVILEGES ON TABLE public.remaining_metric_partitions FROM " + grantCoordinatorRole,
		"REVOKE ALL PRIVILEGES ON TABLE public.work_graph_execution_requests FROM " + grantCoordinatorRole,
		"REVOKE INSERT ON TABLE public.worker_job_outbox FROM " + grantCoordinatorRole,
		"REVOKE SELECT (completion_key), INSERT (completion_key) " +
			"ON TABLE public.worker_job_completion_fences FROM " + grantCoordinatorRole,
	}
}

// TestFixedEngineStatementsArePermittedToTheCoordinatorRole is the fix being
// sufficient: every statement Engine.runOccurrence commits succeeds as the
// coordinator role with the grants the MIGRATION emits, so repointing the
// fixed loop at the coordinator pool needs no further widening of any role —
// and specifically no grant of fixed_schedule_occurrences to the domain role.
func TestFixedEngineStatementsArePermittedToTheCoordinatorRole(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	_, uri := startGrantHarness(t, ctx)

	coordinator := connectAs(t, ctx, uri, grantCoordinatorRole, grantCoordinatorPass)
	for _, statement := range fixedEngineStatements() {
		if err := execInRolledBackTransaction(t, ctx, coordinator, statement.sql); err != nil {
			t.Errorf("%s: denied to the coordinator role, so coordinatorPosture is missing %s\n  site: %s\n  statement: %s\n  error: %v",
				statement.name, statement.privilege, statement.site, collapse(statement.sql), err)
		}
	}
	// Readiness must accept the same role, or the statements succeeding would
	// be worthless: the process would never start.
	if err := CheckCoordinatorAuthorization(ctx, coordinator, grantCoordinatorRole, grantSchema); err != nil {
		t.Fatalf("coordinator readiness rejected the grants the migration emitted for it: %v", err)
	}
}

// TestFixedEngineStatementsAreDeniedByThePreCHAOS3114CoordinatorGrants is the
// mutation check: it takes the coordinator role back to the grant set it held
// before this change and demands that every statement this change was made for
// fails with 42501. Without it, the test above could pass for reasons that
// have nothing to do with the posture edit.
//
// It also pins the negative half of the boundary: the statements that were
// ALREADY permitted (the occurrence ledger and the organization read) must
// still succeed, so the revocation is proven to be targeted rather than a
// blanket one that would make any statement fail.
func TestFixedEngineStatementsAreDeniedByThePreCHAOS3114CoordinatorGrants(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, uri := startGrantHarness(t, ctx)

	coordinator := connectAs(t, ctx, uri, grantCoordinatorRole, grantCoordinatorPass)
	for _, revocation := range preCHAOS3114CoordinatorRevocations() {
		if _, err := admin.Exec(ctx, revocation); err != nil {
			t.Fatalf("%s: %v", revocation, err)
		}
	}

	mutated := 0
	for _, statement := range fixedEngineStatements() {
		err := execInRolledBackTransaction(t, ctx, coordinator, statement.sql)
		if !statement.deniedBeforeThisChange {
			if err != nil {
				t.Errorf("%s: the pre-CHAOS-3114 revocation was not targeted; this statement needed no new privilege\n  site: %s\n  error: %v",
					statement.name, statement.site, err)
			}
			continue
		}
		mutated++
		if err == nil {
			t.Errorf("%s: PERMITTED under the pre-CHAOS-3114 coordinator grants, so %s was not the privilege that unblocked it\n  site: %s\n  statement: %s",
				statement.name, statement.privilege, statement.site, collapse(statement.sql))
			continue
		}
		if !isInsufficientPrivilege(err) {
			// A different SQLSTATE means the statement never reached the
			// permission check, so this assertion would be measuring nothing.
			t.Errorf("%s: expected insufficient_privilege (42501), got a different failure: %v\n  site: %s\n  statement: %s",
				statement.name, err, statement.site, collapse(statement.sql))
		}
	}
	if mutated == 0 {
		t.Fatal("no statement was marked as denied before this change, so this test proves nothing")
	}

	// Readiness must fail too. The posture and the grants are one declaration,
	// so a coordinator login carrying the old grant set is not merely unable to
	// run the engine — it must refuse to report ready at all.
	if err := CheckCoordinatorAuthorization(ctx, coordinator, grantCoordinatorRole, grantSchema); err == nil {
		t.Error("coordinator readiness passed with the pre-CHAOS-3114 grant set, so it is not checking the posture it declares")
	}
}

// TestFixedEngineCompletionFenceGrantStaysColumnScoped pins the reason the
// coordinator's fence grant is a column pair rather than a table row:
// completed_at is server-owned (DEFAULT statement_timestamp()), and a
// table-wide grant would let the coordinator forge a fence that retention
// never reaps. The engine only ever inserts completion_key.
func TestFixedEngineCompletionFenceGrantStaysColumnScoped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	_, uri := startGrantHarness(t, ctx)
	coordinator := connectAs(t, ctx, uri, grantCoordinatorRole, grantCoordinatorPass)

	for _, forbidden := range []struct {
		name string
		sql  string
	}{
		{
			name: "forging the server-owned completion instant",
			sql: `INSERT INTO public.worker_job_completion_fences (completion_key, completed_at)
				VALUES ('remaining_metric_run:11111111-1111-5111-8111-111111111111', now())`,
		},
		{
			name: "reading the server-owned completion instant",
			sql:  "SELECT completed_at FROM public.worker_job_completion_fences",
		},
		{
			name: "reaping a fence",
			sql:  "DELETE FROM public.worker_job_completion_fences WHERE completion_key = 'x'",
		},
	} {
		err := execInRolledBackTransaction(t, ctx, coordinator, forbidden.sql)
		if err == nil {
			t.Errorf("%s: PERMITTED to the coordinator role; the fence grant is not column-scoped\n  statement: %s",
				forbidden.name, collapse(forbidden.sql))
			continue
		}
		if !isInsufficientPrivilege(err) {
			t.Errorf("%s: expected insufficient_privilege (42501), got: %v\n  statement: %s",
				forbidden.name, err, collapse(forbidden.sql))
		}
	}
}

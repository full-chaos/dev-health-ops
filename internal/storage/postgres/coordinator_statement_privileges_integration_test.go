//go:build integration

package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// This suite exists because the claim "the coordinator binaries cannot run as
// the restricted domain role" was, until now, an inference from reading code
// and grant lists. Reading is how the original missing grants survived: the
// grant list and the posture agreed with each other and both disagreed with
// the statements the binaries actually execute. So this file executes the real
// statement shapes, taken verbatim from the production call sites, against a
// real server, as the real roles — the domain role holding exactly the grants
// runtimeGrantStatements emits, and a coordinator role holding exactly the
// grants coordinatorPosture() describes.
//
// It pins both directions of one fact:
//
//   - Every coordinator statement is DENIED to the domain role (42501). This is
//     what CHAOS-3113 was: dev-health-workerctl and dev-health-reconciler ran
//     these call sites on pools.Domain, so each denial was a live runtime
//     failure rather than a hypothetical. Those call sites now run on the
//     coordinator pool, so this half is the regression test for the fix — it
//     fails the moment anything is wired back onto the domain role.
//   - Every one of those same statements is PERMITTED to the coordinator role
//     with the grants the migration emits. This is the fix being sufficient:
//     the repoint needs no further widening of either role.
//
// The second half is what would have caught CHAOS-3113 before it was filed. A
// posture that omits a privilege some real statement needs — the LOCK-implied
// UPDATE on worker_job_outbox being exactly that case — fails here with the
// statement attached, whereas a posture-versus-grant-list comparison cannot
// see it, because neither artefact mentions locking at all.
//
// The fixed-schedule engine's statements used to be deliberately out of scope
// here, on the grounds that an engine committing coordinator-exclusive and
// domain-exclusive writes in ONE transaction had no correct single role at
// all. CHAOS-3114 settled that architecture question — the coordinator role
// was widened to cover the whole runOccurrence transaction, rather than the
// transaction being split or the domain role being widened — so those
// statements now have a privilege shape worth pinning. They live in
// fixed_engine_statement_privileges_integration_test.go, which reuses this
// file's harness, and NOT in coordinatorStatements() below: most of them run
// against tables the domain role legitimately holds too, so they are not
// "denied to domain" statements and belong in their own suite.

const (
	grantCoordinatorRole = "grant_coordinator_runtime"
	grantCoordinatorPass = "grant_coordinator_password"
)

// coordinatorExclusiveDDL creates the relations the Option B split assigns to
// the coordinator role and that the domain-side harness therefore never needed.
// Column sets are faithful to the columns the exercised statements name:
// PostgreSQL raises undefined_column (42703) during parse analysis, BEFORE the
// permission check, so a lazily-typed table would turn "denied" into a
// different error and quietly stop testing privileges at all.
func coordinatorExclusiveDDL() []string {
	return []string{
		`CREATE TABLE public.internal_service_credentials (
			id uuid PRIMARY KEY,
			service_name text NOT NULL,
			token_hash text NOT NULL,
			scopes jsonb NOT NULL,
			revoked_at timestamptz,
			expires_at timestamptz,
			last_used_at timestamptz
		)`,
		// id is an identity column rather than a serial on purpose: the
		// production INSERT ends in RETURNING id, and an identity column needs
		// no separate sequence grant, so this test cannot pass or fail for
		// sequence-privilege reasons that production does not have.
		`CREATE TABLE public.worker_operator_audits (
			id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
			credential_id uuid NOT NULL,
			principal_type text NOT NULL,
			principal_id text NOT NULL,
			action text NOT NULL,
			resource_type text NOT NULL,
			resource_id text NOT NULL,
			reason_code text NOT NULL,
			correlation_id text NOT NULL,
			status text NOT NULL,
			created_at timestamptz NOT NULL,
			completed_at timestamptz
		)`,
		`CREATE TABLE public.worker_job_routes (
			job_kind text PRIMARY KEY,
			transport text NOT NULL,
			paused boolean NOT NULL DEFAULT FALSE,
			generation bigint NOT NULL DEFAULT 1,
			updated_at timestamptz NOT NULL DEFAULT now()
		)`,
		`CREATE TABLE public.sync_run_reference_discoveries (
			id uuid PRIMARY KEY, sync_run_id uuid NOT NULL UNIQUE, org_id text NOT NULL,
			status text NOT NULL, attempts integer NOT NULL, available_at timestamptz NOT NULL,
			created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL
		)`,
		"CREATE TABLE public.sync_run_post_dispatches (id uuid PRIMARY KEY, sync_run_id uuid NOT NULL)",
		"CREATE TABLE public.scheduled_sync_occurrences (id uuid PRIMARY KEY)",
		// Production column shape (internal/scheduler/fixed/ledger.go), because
		// fixed_engine_statement_privileges_integration_test.go executes the
		// real ledger statements against it and a stub would raise 42703 during
		// parse analysis, before the permission check it means to measure.
		`CREATE TABLE public.fixed_schedule_occurrences (
			occurrence_key text PRIMARY KEY,
			identity_version text NOT NULL,
			schedule_id text NOT NULL,
			target_kind text NOT NULL,
			scheduled_for timestamptz NOT NULL,
			observed_at timestamptz NOT NULL,
			status text NOT NULL,
			handoff_count integer NOT NULL,
			skip_reason text,
			completed_at timestamptz,
			created_at timestamptz NOT NULL,
			updated_at timestamptz NOT NULL,
			UNIQUE (schedule_id, scheduled_for)
		)`,
	}
}

// coordinatorStatement is one production statement shape, its call site, and
// the privilege that makes it a coordinator statement. The site is recorded so
// a failure names the code to look at rather than only the SQL.
type coordinatorStatement struct {
	name      string
	site      string
	privilege string
	sql       string
}

func coordinatorStatements() []coordinatorStatement {
	return []coordinatorStatement{
		{
			name: "workerctl authenticator touches its credential",
			site: "internal/joboperator/auth.go Authenticate, wired at cmd/dev-health-workerctl/main.go, on the coordinator pool since the CHAOS-3113 repoint",
			// The UPDATE inside the CTE is why SELECT alone is not enough:
			// authentication is a write. This runs on EVERY workerctl
			// invocation, before any command dispatches, so its denial makes
			// the whole binary unusable rather than degrading one subcommand.
			privilege: "internal_service_credentials SELECT + UPDATE",
			sql: `WITH authenticated AS (
					SELECT id, scopes
					FROM public.internal_service_credentials
					WHERE token_hash = 'not-a-real-token-hash'
						AND service_name = 'worker_operator'
						AND revoked_at IS NULL
						AND (expires_at IS NULL OR expires_at > statement_timestamp())
				), touched AS (
					UPDATE public.internal_service_credentials AS credential
					SET last_used_at = statement_timestamp()
					FROM authenticated
					WHERE credential.id = authenticated.id
					RETURNING authenticated.id::text, authenticated.scopes::text
				)
				SELECT id, scopes FROM touched`,
		},
		{
			name:      "workerctl audit trail opens an entry",
			site:      "internal/joboperator/audit.go PostgresAuditor.Begin, wired at cmd/dev-health-workerctl/main.go, on the coordinator pool since the CHAOS-3113 repoint",
			privilege: "worker_operator_audits INSERT",
			sql: `INSERT INTO public.worker_operator_audits (
					credential_id, principal_type, principal_id, action, resource_type,
					resource_id, reason_code, correlation_id, status, created_at
				) VALUES (gen_random_uuid(), 'service_credential', 'principal', 'apply',
					'job_route', 'resource', 'reason', 'correlation', 'started', now())
				RETURNING id`,
		},
		{
			name:      "workerctl audit trail closes an entry",
			site:      "internal/joboperator/audit.go postgresAuditHandle.Complete",
			privilege: "worker_operator_audits UPDATE",
			sql: `UPDATE public.worker_operator_audits
				SET status = 'succeeded', completed_at = statement_timestamp()
				WHERE id = 1 AND status = 'started'`,
		},
		{
			name: "job-route state read",
			site: "internal/jobroute/control.go Controller.read, reached from workerctl job-routes status AND unconditionally from the reconciler relay (cmd/dev-health-reconciler/dependencies.go buildReconcilerRelay)",
			// The finding that widened CHAOS-3113: worker_job_routes is absent
			// from domainPosture altogether, so even the unlocked read fails.
			// The reconciler relay takes only read paths, so this — not the
			// LOCK below — is what breaks it, on every tick.
			privilege: "worker_job_routes SELECT",
			sql: `SELECT job_kind, transport, paused, generation, updated_at
				FROM public.worker_job_routes WHERE job_kind = 'sync.dispatch'`,
		},
		{
			name:      "job-route state read under a row lock",
			site:      "internal/jobroute/control.go readState with lock=true",
			privilege: "worker_job_routes UPDATE (implied by FOR UPDATE)",
			sql: `SELECT job_kind, transport, paused, generation, updated_at
				FROM public.worker_job_routes WHERE job_kind = 'sync.dispatch' FOR UPDATE`,
		},
		{
			name:      "job-route transport mutation",
			site:      "internal/jobroute/control.go ApplyCheckedIn and Rollback",
			privilege: "worker_job_routes UPDATE",
			sql: `UPDATE public.worker_job_routes
				SET transport = 'river', paused = FALSE, generation = generation + 1, updated_at = now()
				WHERE job_kind = 'sync.dispatch' AND generation = 1
				RETURNING job_kind, transport, paused, generation, updated_at`,
		},
		{
			name: "job-route rollback quiesces the outbox",
			site: "internal/jobroute/control.go Rollback",
			// CHAOS-3113 proper. The domain role holds SELECT and INSERT here,
			// which is why a DML-verb reading of this table looks satisfied:
			// SHARE ROW EXCLUSIVE needs UPDATE with no row ever written.
			// Rollback only — ApplyCheckedIn never touches this table.
			privilege: "worker_job_outbox UPDATE (implied by LOCK TABLE ... IN SHARE ROW EXCLUSIVE MODE)",
			sql:       "LOCK TABLE public.worker_job_outbox IN SHARE ROW EXCLUSIVE MODE",
		},
		{
			name:      "sync-dispatch route pause",
			site:      "internal/syncroute/control.go Pause, wired at cmd/dev-health-workerctl/main.go, on the coordinator pool since the CHAOS-3113 repoint",
			privilege: "sync_dispatch_transport_routes UPDATE (domain side is SELECT-only)",
			sql: `UPDATE public.sync_dispatch_transport_routes
				SET paused = TRUE, paused_at = now(), generation = generation + 1, updated_at = now()
				WHERE kind = 'sync.dispatch' AND generation = 1 AND paused = FALSE
				RETURNING generation, paused_at`,
		},
		{
			name:      "sync-dispatch route read under a row lock",
			site:      "internal/syncroute/control.go readRouteRecord with lock=true",
			privilege: "sync_dispatch_transport_routes UPDATE (implied by FOR UPDATE)",
			sql: `SELECT kind, transport, generation, paused, paused_at, rollback_transport
				FROM public.sync_dispatch_transport_routes
				WHERE kind = 'sync.dispatch' FOR UPDATE`,
		},
		{
			name:      "reconciler materializer reads its reference discoveries",
			site:      "internal/syncreconciler/materializer.go, wired at cmd/dev-health-reconciler/dependencies.go, on the coordinator pool since the CHAOS-3113 repoint",
			privilege: "sync_run_reference_discoveries SELECT",
			sql:       "SELECT id FROM public.sync_run_reference_discoveries WHERE sync_run_id = gen_random_uuid()",
		},
		{
			name:      "reconciler materializer reads its post dispatches",
			site:      "internal/syncreconciler/materializer.go",
			privilege: "sync_run_post_dispatches SELECT",
			sql:       "SELECT id FROM public.sync_run_post_dispatches WHERE sync_run_id = gen_random_uuid()",
		},
	}
}

// TestCoordinatorStatementsAreDeniedToTheDomainRole is the bug, executed, and
// now the regression test for its fix. Every statement here is one a
// coordinator binary runs, and every one is refused by the domain role — which
// is why running them on pools.Domain was CHAOS-3113, and why wiring any of
// them back there fails this test. The grants in force are the real ones —
// startGrantHarness applies ApplyPinnedMigrations, not a copy of its
// statements — so this measures deployment behaviour rather than a model of it.
func TestCoordinatorStatementsAreDeniedToTheDomainRole(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	_, uri := startGrantHarness(t, ctx)
	domain := connectAs(t, ctx, uri, grantDomainRole, grantDomainPass)

	for _, statement := range coordinatorStatements() {
		err := execInRolledBackTransaction(t, ctx, domain, statement.sql)
		if err == nil {
			t.Errorf("%s: the domain role was PERMITTED a coordinator statement (%s)\n  site: %s\n  statement: %s",
				statement.name, statement.privilege, statement.site, collapse(statement.sql))
			continue
		}
		if !isInsufficientPrivilege(err) {
			// Not a pass. A different SQLSTATE means the statement never
			// reached the permission check — an undefined column or table
			// would be raised first — so this assertion would be measuring
			// nothing, which is precisely the failure mode this file exists
			// to avoid.
			t.Errorf("%s: expected insufficient_privilege (42501), got a different failure: %v\n  site: %s\n  statement: %s",
				statement.name, err, statement.site, collapse(statement.sql))
		}
	}
}

// TestCoordinatorStatementsArePermittedToTheCoordinatorRole is the other half:
// coordinatorPosture() is sufficient for every statement above, so repointing
// those call sites at a coordinator pool is a complete fix and needs no
// additional grant.
//
// The grants in force here are now the ones ApplyPinnedMigrations itself
// emits — startGrantHarness passes CoordinatorRole and a CoordinatorGrants set
// derived from CoordinatorPosture(), the same way cmd/dev-health-worker-migrate
// does. That is strictly stronger than granting the posture directly in the
// test: it proves the MIGRATION grants what the statements need, so a bug in
// coordinatorGrantStatements (a dropped flag, a mis-sanitized identifier, a
// to_regclass guard that never matches) fails here instead of in production.
func TestCoordinatorStatementsArePermittedToTheCoordinatorRole(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	_, uri := startGrantHarness(t, ctx)

	coordinator := connectAs(t, ctx, uri, grantCoordinatorRole, grantCoordinatorPass)
	for _, statement := range coordinatorStatements() {
		if err := execInRolledBackTransaction(t, ctx, coordinator, statement.sql); err != nil {
			t.Errorf("%s: denied to the coordinator role, so coordinatorPosture is missing %s\n  site: %s\n  statement: %s\n  error: %v",
				statement.name, statement.privilege, statement.site, collapse(statement.sql), err)
		}
	}
}

// TestCoordinatorMaterializerCanInsertSyncDispatchOutbox is the CHAOS-3146
// regression at the real trust boundary. The reconciler constructs its
// Materializer with the restricted coordinator pool, so only a connection as
// that login can prove the migration grants the INSERT that Step needs. The
// statement is the production post_sync materialization shape, including its
// conflict arbiter; a table owner or a one-column INSERT would miss the defect.
//
// The domain posture is checked in the same harness because sync_dispatch_outbox
// is deliberately dual-role. Fixing the coordinator must not alter the domain
// role's independently declared privileges.
func TestCoordinatorMaterializerCanInsertSyncDispatchOutbox(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	_, uri := startGrantHarness(t, ctx)

	statement := `INSERT INTO public.sync_dispatch_outbox (
			id, org_id, sync_run_id, kind, status, available_at, attempts,
			created_at, updated_at
		) VALUES (
			gen_random_uuid(), gen_random_uuid(), gen_random_uuid(),
			'post_sync', 'pending', now(), 0, now(), now()
		) ON CONFLICT (sync_run_id, kind) DO NOTHING`
	coordinator := connectAs(t, ctx, uri, grantCoordinatorRole, grantCoordinatorPass)
	if err := execInRolledBackTransaction(t, ctx, coordinator, statement); err != nil {
		t.Errorf("restricted coordinator cannot execute syncreconciler materializer INSERT: %v", err)
	}

	domain := connectAs(t, ctx, uri, grantDomainRole, grantDomainPass)
	if err := CheckDomainAuthorization(ctx, domain, grantDomainRole, grantSchema); err != nil {
		t.Errorf("coordinator grant changed the domain role's privilege posture: %v", err)
	}
}

// TestScheduledMaterializerRoleBoundary executes the new materializer's
// decisive statement shapes as the restricted logins. It proves both halves
// of the two-transaction seam and, critically, that the coordinator can read
// only credential metadata rather than encrypted secret material.
func TestScheduledMaterializerRoleBoundary(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	_, uri := startGrantHarness(t, ctx)
	domain := connectAs(t, ctx, uri, grantDomainRole, grantDomainPass)
	coordinator := connectAs(t, ctx, uri, grantCoordinatorRole, grantCoordinatorPass)

	domainStatements := []string{
		"INSERT INTO public.integration_sources (id) VALUES (gen_random_uuid())",
		"INSERT INTO public.integration_datasets (id) VALUES (gen_random_uuid())",
		"INSERT INTO public.sync_runs (id) VALUES (gen_random_uuid())",
		"INSERT INTO public.sync_run_units (id,state) VALUES (gen_random_uuid(),'planned')",
	}
	for _, statement := range domainStatements {
		if err := execInRolledBackTransaction(t, ctx, domain, statement); err != nil {
			t.Errorf("domain materializer statement denied: %v\n  statement: %s", err, collapse(statement))
		}
		if err := execInRolledBackTransaction(t, ctx, coordinator, statement); !isInsufficientPrivilege(err) {
			t.Errorf("coordinator crossed domain write boundary: error=%v\n  statement: %s", err, collapse(statement))
		}
	}

	coordinatorStatements := []string{
		"SELECT id,org_id,provider,is_active,config FROM public.integration_credentials WHERE id=gen_random_uuid()",
		"SELECT id FROM public.organizations WHERE id=gen_random_uuid() FOR KEY SHARE",
		"SELECT id FROM public.feature_flags WHERE key='canonical_incident_ingestion' FOR UPDATE",
		"SELECT id FROM public.org_feature_overrides WHERE org_id=gen_random_uuid() FOR UPDATE",
		"INSERT INTO public.job_runs (id,job_id,status,result,triggered_by,completed_at,error,created_at) VALUES (gen_random_uuid(),gen_random_uuid(),0,'{}','schedule',NULL,NULL,now())",
		"INSERT INTO public.sync_run_reference_discoveries (id,sync_run_id,org_id,status,attempts,available_at,created_at,updated_at) VALUES (gen_random_uuid(),gen_random_uuid(),'org','planned',0,now(),now(),now())",
	}
	for _, statement := range coordinatorStatements {
		if err := execInRolledBackTransaction(t, ctx, coordinator, statement); err != nil {
			t.Errorf("coordinator materializer statement denied: %v\n  statement: %s", err, collapse(statement))
		}
	}
	secretRead := "SELECT credentials_encrypted FROM public.integration_credentials WHERE id=gen_random_uuid()"
	if err := execInRolledBackTransaction(t, ctx, coordinator, secretRead); !isInsufficientPrivilege(err) {
		t.Errorf("coordinator can read encrypted credential material: error=%v", err)
	}
}

// TestCoordinatorReadinessAcceptsTheGrantsThePostureDescribes closes the loop
// on the pair above: statements succeeding is worthless if readiness rejects
// the same role, and readiness passing is worthless if it would also pass
// without the grants. Both are checked, the second by revoking one privilege
// the posture requires and demanding the check notices.
//
// Because the grants now come from ApplyPinnedMigrations rather than from the
// posture directly, this is also the grants-versus-assertion reconciliation
// for the coordinator role — the coordinator counterpart of
// TestDomainAuthorizationAcceptsTheGrantsItIsPairedWith. If
// coordinatorGrantStatements and coordinatorPosture ever disagree, this fails.
func TestCoordinatorReadinessAcceptsTheGrantsThePostureDescribes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, uri := startGrantHarness(t, ctx)

	coordinator := connectAs(t, ctx, uri, grantCoordinatorRole, grantCoordinatorPass)
	if err := CheckCoordinatorAuthorization(ctx, coordinator, grantCoordinatorRole, grantSchema); err != nil {
		t.Fatalf("coordinator readiness rejected the grants the migration emitted for it: %v", err)
	}

	// Non-vacuity: the UPDATE that only the LOCK needs is the one most likely
	// to be "tidied" away by a reader who greps for UPDATE statements and
	// finds none, so that is the privilege revoked here.
	if _, err := admin.Exec(ctx,
		"REVOKE UPDATE ON TABLE public.worker_job_outbox FROM "+grantCoordinatorRole); err != nil {
		t.Fatal(err)
	}
	if err := CheckCoordinatorAuthorization(ctx, coordinator, grantCoordinatorRole, grantSchema); err == nil {
		t.Error("coordinator readiness passed without worker_job_outbox UPDATE, so it is not checking the posture it declares")
	}
	if _, err := admin.Exec(ctx,
		"GRANT UPDATE ON TABLE public.worker_job_outbox TO "+grantCoordinatorRole); err != nil {
		t.Fatal(err)
	}
	if err := CheckCoordinatorAuthorization(ctx, coordinator, grantCoordinatorRole, grantSchema); err != nil {
		t.Fatalf("coordinator readiness did not recover once the privilege was restored: %v", err)
	}
}

// execInRolledBackTransaction runs one statement and discards its effects. A
// transaction is required rather than optional: LOCK TABLE is only legal
// inside one, and it is the statement this suite most needs to exercise.
func execInRolledBackTransaction(
	t *testing.T, ctx context.Context, pool *pgxpool.Pool, statement string,
) error {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	_, execErr := tx.Exec(ctx, statement)
	_ = tx.Rollback(ctx)
	return execErr
}

func isInsufficientPrivilege(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42501"
}

func collapse(statement string) string {
	return strings.Join(strings.Fields(statement), " ")
}

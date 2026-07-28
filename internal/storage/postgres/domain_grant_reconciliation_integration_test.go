//go:build integration

package postgres

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// This suite exists because the grant list and the authorization assertion
// previously agreed with each other and both disagreed with the code. Two
// artefacts that share an author and a construction method cannot check each
// other, so the only useful evidence is executing the real statements as the
// real restricted role against a real server.
//
// Everything here runs AS the domain role. A harness that connects as the
// database owner proves nothing about production, which is exactly how the
// missing grants survived until now.

const (
	grantDomainRole = "grant_domain_runtime"
	grantQueueRole  = "grant_queue_runtime"
	grantDomainPass = "grant_domain_password"
	grantQueuePass  = "grant_queue_password"
	grantSchema     = "river"
)

// domainTables is every relation the domain role is asserted to hold
// privileges on, with the statement shapes the production code actually runs
// against it. The shapes matter more than the flags: PostgreSQL's requirements
// are not what a reader would guess from the verb alone.
type domainTable struct {
	name string
	ddl  string
	// exercise runs the production statement shapes. Each must succeed as the
	// domain role once the checked grants are applied.
	exercise []string
}

func reconciliationTables() []domainTable {
	return []domainTable{
		{
			// Tightened under the Option B two-role split (role-partition
			// manifest, removed in e23ede618; see git history at
			// eda2d6b91): the domain-side call
			// (internal/syncdispatchruntime/native_post_sync.go:263) is a
			// plain SELECT with no lock clause. The FOR UPDATE row-locking
			// requirement this table used to carry belongs to the
			// coordinator-side scheduler code, not the domain worker — see
			// coordinatorPosture's doc comment for the still-open question of
			// whether that makes this table a seventh dual-grant table.
			name: "sync_configurations",
			ddl: `CREATE TABLE public.sync_configurations (
				id uuid PRIMARY KEY, org_id text NOT NULL, is_active boolean NOT NULL,
				sync_options jsonb NOT NULL, last_sync_at timestamptz, created_at timestamptz NOT NULL)`,
			exercise: []string{
				"SELECT id FROM public.sync_configurations WHERE is_active",
			},
		},
		{
			// worker_job_routes, scheduled_jobs, scheduled_sync_occurrences,
			// and fixed_schedule_occurrences moved to coordinatorPosture
			// entirely under the Option B split and no longer appear here —
			// the domain role has no grant on any of them.
			name: "organizations",
			ddl: "CREATE TABLE public.organizations (id uuid PRIMARY KEY, " +
				"is_active boolean NOT NULL, tier text NOT NULL DEFAULT 'community')",
			exercise: []string{
				"SELECT id::text FROM public.organizations WHERE is_active = TRUE ORDER BY id LIMIT 10",
			},
		},
		{
			name: "remaining_metric_runs",
			ddl: `CREATE TABLE public.remaining_metric_runs (
				id uuid PRIMARY KEY, org_id uuid NOT NULL, family text NOT NULL, generation text NOT NULL,
				scope_key text NOT NULL, generation_seed bigint, status text NOT NULL DEFAULT 'pending',
				created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL)`,
			exercise: []string{
				`INSERT INTO public.remaining_metric_runs
					(id, org_id, family, generation, scope_key, generation_seed, status, created_at, updated_at)
				 VALUES (gen_random_uuid(), gen_random_uuid(), 'capacity', 'g', 's', 1, 'pending', now(), now())
				 ON CONFLICT DO NOTHING`,
				"SELECT status FROM public.remaining_metric_runs WHERE family = 'capacity'",
				"UPDATE public.remaining_metric_runs SET status = 'running' WHERE family = 'capacity'",
			},
		},
		{
			name: "remaining_metric_partitions",
			ddl: `CREATE TABLE public.remaining_metric_partitions (
				id uuid PRIMARY KEY, run_id uuid NOT NULL, ordinal integer NOT NULL, scope jsonb NOT NULL,
				status text NOT NULL, attempt_count integer NOT NULL DEFAULT 0,
				created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL)`,
			exercise: []string{
				`INSERT INTO public.remaining_metric_partitions
					(id, run_id, ordinal, scope, status, created_at, updated_at)
				 VALUES (gen_random_uuid(), gen_random_uuid(), 1, '{}'::jsonb, 'pending', now(), now())`,
				"SELECT status FROM public.remaining_metric_partitions WHERE ordinal = 1",
				"UPDATE public.remaining_metric_partitions SET status = 'running' WHERE ordinal = 1",
			},
		},
		{
			name: "work_graph_execution_requests",
			ddl: `CREATE TABLE public.work_graph_execution_requests (
				id uuid PRIMARY KEY, org_id uuid NOT NULL, kind text NOT NULL, scope jsonb NOT NULL,
				model_ref text, prompt_ref text, llm_concurrency integer NOT NULL,
				spend_limit_microunits bigint NOT NULL, correlation_id text NOT NULL,
				idempotency_key text NOT NULL, state text NOT NULL)`,
			exercise: []string{
				`INSERT INTO public.work_graph_execution_requests
					(id, org_id, kind, scope, llm_concurrency, spend_limit_microunits, correlation_id, idempotency_key, state)
				 VALUES (gen_random_uuid(), gen_random_uuid(), 'build', '{}'::jsonb, 1, 0, 'c', 'i', 'pending')
				 ON CONFLICT (id) DO NOTHING`,
				"SELECT state FROM public.work_graph_execution_requests WHERE kind = 'build'",
				"UPDATE public.work_graph_execution_requests SET state = 'running' WHERE kind = 'build'",
			},
		},
		{
			// Ledger half of the same Claim transaction as
			// work_graph_execution_requests (internal/jobs/workgraph/postgres.go
			// Claim/transition). No FK to work_graph_execution_requests here:
			// this suite exercises statement shapes per table, independently of
			// referential setup in other tables.
			name: "work_graph_execution_ledger",
			ddl: `CREATE TABLE public.work_graph_execution_ledger (
				request_id uuid PRIMARY KEY, claim_token uuid NOT NULL, state text NOT NULL,
				attempt_count integer NOT NULL DEFAULT 1, output_evidence jsonb, failure_detail text,
				last_attempt_at timestamptz NOT NULL, completed_at timestamptz)`,
			exercise: []string{
				`INSERT INTO public.work_graph_execution_ledger
					(request_id, claim_token, state, attempt_count, last_attempt_at)
				 VALUES (gen_random_uuid(), gen_random_uuid(), 'executing', 1, now())
				 ON CONFLICT (request_id) DO UPDATE SET
					claim_token = EXCLUDED.claim_token,
					state = 'executing',
					attempt_count = public.work_graph_execution_ledger.attempt_count + 1,
					output_evidence = NULL,
					failure_detail = NULL,
					completed_at = NULL,
					last_attempt_at = EXCLUDED.last_attempt_at
				 WHERE public.work_graph_execution_ledger.state IN ('executing', 'repaired')`,
				"UPDATE public.work_graph_execution_ledger SET state = 'succeeded', completed_at = now() WHERE request_id = gen_random_uuid() AND state = 'executing'",
			},
		},
		{
			// Column-scoped, not table-wide: completed_at is server-owned
			// (DEFAULT statement_timestamp()) and joboutbox.MarkCompletionTx
			// only ever inserts completion_key. See
			// TestDomainRoleCompletionFenceGrantIsColumnScoped below.
			name: "worker_job_completion_fences",
			ddl:  "CREATE TABLE public.worker_job_completion_fences (completion_key text PRIMARY KEY, completed_at timestamptz NOT NULL DEFAULT now())",
			exercise: []string{
				"INSERT INTO public.worker_job_completion_fences (completion_key) VALUES ('d:1') ON CONFLICT (completion_key) DO NOTHING",
			},
		},
		// The three tables below are the only domain-role AllowDelete rows in
		// domainPosture — chaos-3033-role-partition-manifest.md's
		// "schema-level blocker" section, resolved by adding the allow_delete
		// column. Each is a real retention/cleanup deletion, not a synthetic
		// probe: internal/streamhandlers/external_postgres.go for the two
		// external_ingest_* tables, internal/jobs/system/retention_postgres.go
		// (a FOR UPDATE SKIP LOCKED chunked delete-read) for
		// provider_rate_limit_observations.
		{
			name: "external_ingest_batch_payloads",
			ddl: `CREATE TABLE public.external_ingest_batch_payloads (
				id uuid PRIMARY KEY, batch_id uuid NOT NULL, received_at timestamptz NOT NULL)`,
			exercise: []string{
				"SELECT id FROM public.external_ingest_batch_payloads WHERE batch_id = gen_random_uuid()",
				"DELETE FROM public.external_ingest_batch_payloads WHERE id = gen_random_uuid()",
			},
		},
		{
			name: "external_ingest_batches",
			ddl: `CREATE TABLE public.external_ingest_batches (
				id uuid PRIMARY KEY, source_id uuid NOT NULL, status text NOT NULL,
				created_at timestamptz NOT NULL)`,
			exercise: []string{
				"SELECT id FROM public.external_ingest_batches WHERE status = 'pending'",
				"UPDATE public.external_ingest_batches SET status = 'processed' WHERE id = gen_random_uuid()",
				"DELETE FROM public.external_ingest_batches WHERE id = gen_random_uuid()",
			},
		},
		{
			name: "provider_rate_limit_observations",
			ddl: `CREATE TABLE public.provider_rate_limit_observations (
				id uuid PRIMARY KEY, provider text NOT NULL, observed_at timestamptz NOT NULL)`,
			// FOR UPDATE SKIP LOCKED chunked delete-read, the real shape
			// (internal/jobs/system/retention_postgres.go): a locking SELECT
			// followed by a keyed DELETE, not a bare unconditional one.
			exercise: []string{
				"SELECT id FROM public.provider_rate_limit_observations WHERE provider = 'github' FOR UPDATE SKIP LOCKED",
				"UPDATE public.provider_rate_limit_observations SET observed_at = now() WHERE id = gen_random_uuid()",
				"DELETE FROM public.provider_rate_limit_observations WHERE id = gen_random_uuid()",
			},
		},
	}
}

func startGrantHarness(t *testing.T, ctx context.Context) (*pgxpool.Pool, string) {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)

	// The coordinator role is created here, before ApplyPinnedMigrations, for
	// the same reason the coordinator tables are: the migration's own
	// role-eligibility preflight rejects a coordinator role that does not yet
	// exist as a least-privilege login, so this mirrors the real deploy order
	// (provision roles, then migrate) rather than working around it.
	setup := []string{
		"CREATE ROLE " + grantDomainRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + grantDomainPass + "'",
		"CREATE ROLE " + grantQueueRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + grantQueuePass + "'",
		"CREATE ROLE " + grantCoordinatorRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + grantCoordinatorPass + "'",
		"GRANT CONNECT ON DATABASE worker_test TO " + grantDomainRole + ", " + grantQueueRole + ", " + grantCoordinatorRole,
		"REVOKE TEMPORARY ON DATABASE worker_test FROM PUBLIC",
		"REVOKE CREATE ON SCHEMA public FROM PUBLIC",
	}
	for _, table := range reconciliationTables() {
		setup = append(setup, table.ddl)
	}
	// The relations that were already granted with no interesting exercise
	// shape of their own, so the assertion's required set is complete and
	// this suite tests the delta rather than re-testing the baseline.
	//
	// The coordinator-exclusive relations are created here too, BEFORE
	// ApplyPinnedMigrations runs. That ordering is load-bearing in two
	// directions. Every domain GRANT in runtimeGrantStatements is wrapped in
	// a to_regclass guard, so a coordinator table created AFTER the migration
	// would be silently skipped and any test asserting "the domain role holds
	// nothing here" would pass vacuously — it would pass even if migrate.go
	// did grant it. Creating them first also means
	// TestGrantsAndAssertionCoverTheSameRelations now actively polices the
	// other direction: if a future edit hands the domain role a privilege on
	// a coordinator table, that table shows up in its `granted` set, is
	// absent from domainPosture's required set, and the test fails.
	for _, ddl := range []string{
		"CREATE TABLE public.integrations (id uuid PRIMARY KEY)",
		"CREATE TABLE public.integration_sources (id uuid PRIMARY KEY)",
		"CREATE TABLE public.integration_datasets (id uuid PRIMARY KEY)",
		"CREATE TABLE public.integration_credentials (id uuid PRIMARY KEY)",
		"CREATE TABLE public.sync_runs (id uuid PRIMARY KEY)",
		// Carries the columns syncroute's real route-mutation statements name,
		// so a privilege denial cannot be mistaken for an undefined column
		// (42703 is raised during parse analysis, before the ACL check).
		`CREATE TABLE public.sync_dispatch_transport_routes (
			kind text PRIMARY KEY,
			transport text NOT NULL DEFAULT 'celery',
			rollback_transport text NOT NULL DEFAULT 'celery',
			paused boolean NOT NULL DEFAULT FALSE,
			paused_at timestamptz,
			generation bigint NOT NULL DEFAULT 1,
			updated_at timestamptz NOT NULL DEFAULT now()
		)`,
		"CREATE TABLE public.sync_run_units (id uuid PRIMARY KEY, state text NOT NULL)",
		"CREATE TABLE public.sync_watermarks (id uuid PRIMARY KEY, state text NOT NULL)",
		"CREATE TABLE public.sync_dispatch_outbox (id uuid PRIMARY KEY, state text NOT NULL)",
		// Production column shape, not a stub: the fixed-schedule engine's
		// handoff INSERT names every one of these columns, and an undefined
		// column (42703) is raised during parse analysis BEFORE the permission
		// check, which would silently stop the privilege assertions in
		// fixed_engine_statement_privileges_integration_test.go from measuring
		// anything.
		`CREATE TABLE public.worker_job_outbox (
			id uuid PRIMARY KEY,
			dedupe_key text NOT NULL UNIQUE,
			job_kind text NOT NULL,
			contract_version integer NOT NULL,
			args json NOT NULL,
			payload_hash text NOT NULL,
			queue text NOT NULL,
			priority integer NOT NULL,
			max_attempts integer NOT NULL,
			scheduled_at timestamptz NOT NULL,
			status text NOT NULL,
			attempt_count integer NOT NULL,
			next_attempt_at timestamptz NOT NULL,
			prerequisite_completion_key text,
			created_at timestamptz NOT NULL,
			updated_at timestamptz NOT NULL
		)`,
		"CREATE TABLE public.billing_notifications (id bigint PRIMARY KEY)",
		"CREATE TABLE public.daily_metrics_partitions (id bigint PRIMARY KEY)",
		"CREATE TABLE public.daily_metrics_runs (id bigint PRIMARY KEY)",
		"CREATE TABLE public.external_ingest_recompute_jobs (id bigint PRIMARY KEY)",
		"CREATE TABLE public.external_ingest_rejections (id bigint PRIMARY KEY)",
		"CREATE TABLE public.external_ingest_sources (id bigint PRIMARY KEY)",
		`CREATE TABLE public.feature_flags (
			id uuid PRIMARY KEY, key text NOT NULL UNIQUE, min_tier text NOT NULL,
			is_enabled boolean NOT NULL
		)`,
		`CREATE TABLE public.org_feature_overrides (
			id uuid PRIMARY KEY, org_id uuid NOT NULL, feature_id uuid NOT NULL,
			is_enabled boolean NOT NULL, expires_at timestamptz
		)`,
		`CREATE TABLE public.org_licenses (
			id uuid PRIMARY KEY DEFAULT gen_random_uuid(), org_id uuid NOT NULL UNIQUE,
			tier text NOT NULL, features_override json
		)`,
		"CREATE TABLE public.dev_conversations (id uuid PRIMARY KEY)",
		"CREATE TABLE public.report_runs (id bigint PRIMARY KEY)",
		"CREATE TABLE public.saved_reports (id bigint PRIMARY KEY)",
		"CREATE TABLE public.webhook_deliveries (id bigint PRIMARY KEY)",
		"CREATE TABLE public.worker_job_runs (id bigint PRIMARY KEY)",
	} {
		setup = append(setup, ddl)
	}
	setup = append(setup, coordinatorExclusiveDDL()...)
	for _, statement := range setup {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	// Apply the REAL production grants, not a copy of them — for all three
	// roles. CoordinatorGrants is derived from CoordinatorPosture() exactly the
	// way cmd/dev-health-worker-migrate derives it, so the coordinator
	// privileges these tests observe are the ones a real migration produces,
	// not a posture-shaped stand-in for them.
	coordinatorPosture := CoordinatorPosture()
	coordinatorGrants := make([]riverstore.TableGrant, 0, len(coordinatorPosture.RequiredTables))
	for _, table := range coordinatorPosture.RequiredTables {
		coordinatorGrants = append(coordinatorGrants, riverstore.TableGrant{
			TableName:   table.TableName,
			AllowInsert: table.AllowInsert,
			AllowUpdate: table.AllowUpdate,
			AllowDelete: table.AllowDelete,
		})
	}
	// The column-scoped half is derived the same way, for the same reason: the
	// coordinator posture gained worker_job_completion_fences.completion_key
	// with CHAOS-3114, and a harness that granted only the table half would
	// test a privilege set no real migration produces.
	coordinatorColumnGrants := make([]riverstore.ColumnGrant, 0, len(coordinatorPosture.ColumnScoped))
	for _, column := range coordinatorPosture.ColumnScoped {
		coordinatorColumnGrants = append(coordinatorColumnGrants, riverstore.ColumnGrant{
			TableName:  column.TableName,
			ColumnName: column.ColumnName,
			Privilege:  column.Privilege,
		})
	}
	if _, err := riverstore.ApplyPinnedMigrations(ctx, admin, riverstore.MigrationOptions{
		Schema:                  grantSchema,
		DomainRole:              grantDomainRole,
		QueueRole:               grantQueueRole,
		CoordinatorRole:         grantCoordinatorRole,
		CoordinatorGrants:       coordinatorGrants,
		CoordinatorColumnGrants: coordinatorColumnGrants,
	}); err != nil {
		t.Fatal(err)
	}
	return admin, instance.URI
}

func connectAs(t *testing.T, ctx context.Context, rawURI, role, password string) *pgxpool.Pool {
	t.Helper()
	parsed, err := url.Parse(rawURI)
	if err != nil {
		t.Fatal(err)
	}
	parsed.User = url.UserPassword(role, password)
	pool, err := pgxpool.New(ctx, parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// The production statement shapes must all succeed as the restricted role.
// A failure here is the runtime permission error that readiness could not see.
func TestDomainRoleCanRunEveryProductionStatementShape(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	_, uri := startGrantHarness(t, ctx)
	domain := connectAs(t, ctx, uri, grantDomainRole, grantDomainPass)

	for _, table := range reconciliationTables() {
		for index, statement := range table.exercise {
			tx, err := domain.Begin(ctx)
			if err != nil {
				t.Fatalf("%s[%d] begin: %v", table.name, index, err)
			}
			_, execErr := tx.Exec(ctx, statement)
			_ = tx.Rollback(ctx)
			if execErr != nil {
				t.Errorf("%s[%d] denied to the domain role: %v\n  statement: %s",
					table.name, index, execErr, strings.Join(strings.Fields(statement), " "))
			}
		}
	}
}

// Granting is only half of it: the authorization assertion must accept the
// exact privilege set the grants produce. If these two ever disagree the
// deployment breaks in one of two opposite ways — readiness passes while
// queries fail, or readiness fails closed forever.
func TestDomainAuthorizationAcceptsTheGrantsItIsPairedWith(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	_, uri := startGrantHarness(t, ctx)
	domain := connectAs(t, ctx, uri, grantDomainRole, grantDomainPass)

	if err := CheckDomainAuthorization(ctx, domain, grantDomainRole, grantSchema); err != nil {
		t.Fatalf("authorization rejected the privileges its own grants produced: %v", err)
	}
}

// Every relation this suite exercises must appear in the assertion, and every
// relation the assertion requires must be one the grants actually create
// privileges for. This is the mechanical form of "the two artefacts must move
// together", and it is what makes a future table addition fail loudly here
// rather than silently at runtime.
func TestGrantsAndAssertionCoverTheSameRelations(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, _ := startGrantHarness(t, ctx)

	rows, err := admin.Query(ctx, `
SELECT class.relname
FROM pg_catalog.pg_class AS class
JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = class.relnamespace
WHERE namespace.nspname = 'public'
  AND class.relkind IN ('r','p')
  AND (
		has_table_privilege($1, class.oid, 'SELECT')
		OR has_table_privilege($1, class.oid, 'INSERT')
		OR has_table_privilege($1, class.oid, 'UPDATE')
		-- has_table_privilege is blind to column-level grants (e.g. the
		-- deliberately column-scoped worker_job_completion_fences); a table
		-- with ONLY column privileges must still count as granted here.
		OR has_any_column_privilege($1, class.oid, 'SELECT, INSERT, UPDATE')
  )
ORDER BY class.relname`, grantDomainRole)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	granted := map[string]struct{}{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		granted[name] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	for _, table := range reconciliationTables() {
		if _, ok := granted[table.name]; !ok {
			t.Errorf("%s is exercised by production code but the grants give the domain role nothing on it", table.name)
		}
	}
	// required_table_privileges' and column_scoped_privileges' manifests are
	// domainPosture's Go-side data now (rolePostureQuery itself is
	// role-agnostic since the Phase 2 posture parameterization), so the same
	// "granted but not required" check reads that data instead of grep-ing
	// SQL text that no longer contains it. Both fields count: a table
	// granted only at column scope (worker_job_completion_fences) is exactly
	// as "required" as a table-wide one for this check's purpose.
	requiredTableNames := map[string]struct{}{}
	for _, table := range domainPosture().RequiredTables {
		requiredTableNames[table.TableName] = struct{}{}
	}
	for _, column := range domainPosture().ColumnScoped {
		requiredTableNames[column.TableName] = struct{}{}
	}
	missing := make([]string, 0)
	for name := range granted {
		if _, ok := requiredTableNames[name]; !ok {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("granted but absent from required_table_privileges, so authorization will fail closed: %s",
			strings.Join(missing, ", "))
	}
}

// The privilege facts this changeset depends on are surprising enough that they
// are pinned rather than left as commentary. Each was verified against a real
// server; a future PostgreSQL change or a "tidy the flags" pass that breaks one
// of them fails here with the reason attached.
func TestPostgreSQLPrivilegeRequirementsThisChangesetRelinesOn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, uri := startGrantHarness(t, ctx)

	for _, statement := range []string{
		"CREATE TABLE public.privilege_probe (id integer PRIMARY KEY, v text)",
		"INSERT INTO public.privilege_probe VALUES (1, 'a')",
		"GRANT SELECT ON public.privilege_probe TO " + grantDomainRole,
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	domain := connectAs(t, ctx, uri, grantDomainRole, grantDomainPass)

	denied := func(statement string) bool {
		tx, err := domain.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		_, execErr := tx.Exec(ctx, statement)
		_ = tx.Rollback(ctx)
		return execErr != nil
	}

	if denied("SELECT id FROM public.privilege_probe WHERE id = 1") {
		t.Fatal("SELECT privilege does not permit a plain read")
	}
	// This is why sync_configurations carries UPDATE despite never being
	// written: row locking is an UPDATE-class operation in PostgreSQL.
	if !denied("SELECT id FROM public.privilege_probe WHERE id = 1 FOR UPDATE") {
		t.Error("SELECT ... FOR UPDATE no longer requires UPDATE privilege; " +
			"sync_configurations and scheduled_jobs may no longer need allow_update")
	}
	if !denied("SELECT id FROM public.privilege_probe WHERE id = 1 FOR KEY SHARE") {
		t.Error("SELECT ... FOR KEY SHARE no longer requires an UPDATE-class privilege")
	}
}

// The completion-fence grant is deliberately column-scoped to completion_key:
// completed_at is server-owned (DEFAULT statement_timestamp()) and
// joboutbox.MarkCompletionTx never touches it. A future "simplify to a
// table-wide grant" change would let the domain role forge completed_at and
// mint a fence retention never reaps, so this pins the boundary as an
// invariant rather than leaving it as commentary on the grant statement.
func TestDomainRoleCompletionFenceGrantIsColumnScoped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	_, uri := startGrantHarness(t, ctx)
	domain := connectAs(t, ctx, uri, grantDomainRole, grantDomainPass)

	sqlState := func(err error) string {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			return pgErr.Code
		}
		return ""
	}
	denied := func(t *testing.T, statement string) string {
		t.Helper()
		tx, err := domain.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		_, execErr := tx.Exec(ctx, statement)
		_ = tx.Rollback(ctx)
		return sqlState(execErr)
	}

	if _, err := domain.Exec(ctx, "SELECT completion_key FROM public.worker_job_completion_fences"); err != nil {
		t.Fatalf("domain role cannot SELECT the granted completion_key column: %v", err)
	}
	if state := denied(t, "SELECT completed_at FROM public.worker_job_completion_fences"); state != "42501" {
		t.Fatalf("completed_at SELECT sqlstate = %q, want 42501 (server-owned column, no grant)", state)
	}
	if state := denied(
		t,
		"INSERT INTO public.worker_job_completion_fences (completion_key, completed_at) VALUES ('d:2', now())",
	); state != "42501" {
		t.Fatalf("completed_at INSERT sqlstate = %q, want 42501 (domain role must not set it)", state)
	}
	if state := denied(t, "UPDATE public.worker_job_completion_fences SET completion_key = completion_key"); state != "42501" {
		t.Fatalf("completion_key UPDATE sqlstate = %q, want 42501 (fence is insert-only)", state)
	}
	if state := denied(t, "DELETE FROM public.worker_job_completion_fences"); state != "42501" {
		t.Fatalf("DELETE sqlstate = %q, want 42501 (expiry stays queue-side)", state)
	}
}

// A column privilege granted to PUBLIC is held by the domain role — an
// information_schema.column_privileges read filtered to
// "grantee = current_user" would never see it, because PUBLIC is not
// current_user, but has_column_privilege resolves it as effective privilege
// all the same. If the authorization query only checked the former, a
// completed_at column privilege granted to PUBLIC (never to the domain role
// by name) would sail through undetected and reopen exactly the
// forged-completed_at hole the column-scoped grant exists to close.
func TestDomainAuthorizationRejectsColumnPrivilegeGrantedToPublic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, uri := startGrantHarness(t, ctx)
	domain := connectAs(t, ctx, uri, grantDomainRole, grantDomainPass)

	if err := CheckDomainAuthorization(ctx, domain, grantDomainRole, grantSchema); err != nil {
		t.Fatalf("authorization rejected the privileges its own grants produced: %v", err)
	}
	if _, err := admin.Exec(
		ctx,
		"GRANT SELECT (completed_at) ON TABLE public.worker_job_completion_fences TO PUBLIC",
	); err != nil {
		t.Fatal(err)
	}
	if err := CheckDomainAuthorization(ctx, domain, grantDomainRole, grantSchema); err == nil {
		t.Fatal("domain role unexpectedly authorized: a PUBLIC grant on completed_at went undetected")
	}
	if _, err := admin.Exec(
		ctx,
		"REVOKE SELECT (completed_at) ON TABLE public.worker_job_completion_fences FROM PUBLIC",
	); err != nil {
		t.Fatal(err)
	}
	if err := CheckDomainAuthorization(ctx, domain, grantDomainRole, grantSchema); err != nil {
		t.Fatalf("authorization did not recover after revoking the PUBLIC grant: %v", err)
	}
}

// The column-scoped sweep must accept exactly the declared (column,
// privilege) pairs — completion_key SELECT/INSERT, granted to the role by
// startGrantHarness — and reject anything else, including a different
// privilege type on that SAME already-partially-granted column. UPDATE and
// REFERENCES are two of the four column-grantable privilege types declared
// in column_scoped_privilege_types that are deliberately absent from
// completion_key's required set; either one being caught here covers the
// general case a check keyed on (table, column) rather than
// (table, column, privilege) would miss.
func TestDomainAuthorizationColumnScopeAcceptsExactlyTheDeclaredPrivileges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, uri := startGrantHarness(t, ctx)
	domain := connectAs(t, ctx, uri, grantDomainRole, grantDomainPass)

	if err := CheckDomainAuthorization(ctx, domain, grantDomainRole, grantSchema); err != nil {
		t.Fatalf("authorization rejected exactly the declared column privileges: %v", err)
	}

	for _, extra := range []string{"UPDATE", "REFERENCES"} {
		grant := fmt.Sprintf(
			"GRANT %s (completion_key) ON TABLE public.worker_job_completion_fences TO %s",
			extra, grantDomainRole,
		)
		revoke := fmt.Sprintf(
			"REVOKE %s (completion_key) ON TABLE public.worker_job_completion_fences FROM %s",
			extra, grantDomainRole,
		)
		if _, err := admin.Exec(ctx, grant); err != nil {
			t.Fatalf("%s: %v", grant, err)
		}
		if err := CheckDomainAuthorization(ctx, domain, grantDomainRole, grantSchema); err == nil {
			t.Fatalf("domain role unexpectedly authorized with an undeclared %s (completion_key) grant", extra)
		}
		if _, err := admin.Exec(ctx, revoke); err != nil {
			t.Fatalf("%s: %v", revoke, err)
		}
		if err := CheckDomainAuthorization(ctx, domain, grantDomainRole, grantSchema); err != nil {
			t.Fatalf("authorization did not recover after revoking undeclared %s (completion_key): %v", extra, err)
		}
	}
}

// A required privilege held WITH GRANT OPTION lets the domain role
// re-delegate it to another role or to PUBLIC. The base has_table_privilege /
// has_column_privilege calls return true either way (holding the option
// always implies holding the plain privilege), so a check that only compares
// against the declared allow_insert/allow_update flags — or, for the
// column-scoped fence, the declared (column, privilege) set — would miss the
// option silently. Covers both the table-level and column-scoped paths in
// one test, matching that both got the same fix.
func TestDomainAuthorizationRejectsPrivilegeHeldWithGrantOption(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, uri := startGrantHarness(t, ctx)
	domain := connectAs(t, ctx, uri, grantDomainRole, grantDomainPass)

	if err := CheckDomainAuthorization(ctx, domain, grantDomainRole, grantSchema); err != nil {
		t.Fatalf("authorization rejected the privileges its own grants produced: %v", err)
	}

	t.Run("table-level: SELECT on a required table", func(t *testing.T) {
		if _, err := admin.Exec(
			ctx,
			"GRANT SELECT ON TABLE public.integrations TO "+grantDomainRole+" WITH GRANT OPTION",
		); err != nil {
			t.Fatal(err)
		}
		if err := CheckDomainAuthorization(ctx, domain, grantDomainRole, grantSchema); err == nil {
			t.Fatal("domain role unexpectedly authorized: SELECT WITH GRANT OPTION on integrations went undetected")
		}
		// Revoking only the option, not the underlying privilege, proves the
		// check is reacting to the option specifically.
		if _, err := admin.Exec(
			ctx,
			"REVOKE GRANT OPTION FOR SELECT ON TABLE public.integrations FROM "+grantDomainRole,
		); err != nil {
			t.Fatal(err)
		}
		if err := CheckDomainAuthorization(ctx, domain, grantDomainRole, grantSchema); err != nil {
			t.Fatalf("authorization did not recover after revoking the SELECT option, base privilege intact: %v", err)
		}
		if _, err := domain.Exec(ctx, "SELECT id FROM public.integrations"); err != nil {
			t.Fatalf("domain role lost its base SELECT on integrations: %v", err)
		}
	})

	t.Run("column-scoped: SELECT (completion_key) on the fence table", func(t *testing.T) {
		if _, err := admin.Exec(
			ctx,
			"GRANT SELECT (completion_key) ON TABLE public.worker_job_completion_fences TO "+grantDomainRole+" WITH GRANT OPTION",
		); err != nil {
			t.Fatal(err)
		}
		if err := CheckDomainAuthorization(ctx, domain, grantDomainRole, grantSchema); err == nil {
			t.Fatal("domain role unexpectedly authorized: SELECT (completion_key) WITH GRANT OPTION went undetected")
		}
		if _, err := admin.Exec(
			ctx,
			"REVOKE GRANT OPTION FOR SELECT (completion_key) ON TABLE public.worker_job_completion_fences FROM "+grantDomainRole,
		); err != nil {
			t.Fatal(err)
		}
		if err := CheckDomainAuthorization(ctx, domain, grantDomainRole, grantSchema); err != nil {
			t.Fatalf("authorization did not recover after revoking the completion_key SELECT option, base privilege intact: %v", err)
		}
		if _, err := domain.Exec(ctx, "SELECT completion_key FROM public.worker_job_completion_fences"); err != nil {
			t.Fatalf("domain role lost its base SELECT on completion_key: %v", err)
		}
	})
}

// external_ingest_batches is one of the three domain-role AllowDelete rows
// (the schema-level gap the manifest's "Schema-level blocker" section
// flagged, closed by adding the allow_delete column). DELETE is not
// column-grantable in PostgreSQL, so unlike INSERT/UPDATE there is no
// column-level or has_any_column_privilege route for this check to miss —
// but the plain has_table_privilege(..., 'DELETE') <> allow_delete
// comparison alone would still pass a role holding DELETE WITH GRANT OPTION
// unnoticed (holding the option always implies holding the plain privilege),
// letting it re-delegate deletion rights on a retention/cleanup table to
// another role or to PUBLIC. This mirrors
// TestDomainAuthorizationRejectsPrivilegeHeldWithGrantOption's table-level
// case for the one privilege type that check does not cover.
func TestDomainAuthorizationRejectsDeleteHeldWithGrantOption(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, uri := startGrantHarness(t, ctx)
	domain := connectAs(t, ctx, uri, grantDomainRole, grantDomainPass)

	if err := CheckDomainAuthorization(ctx, domain, grantDomainRole, grantSchema); err != nil {
		t.Fatalf("authorization rejected the privileges its own grants produced: %v", err)
	}
	if _, err := admin.Exec(
		ctx,
		"GRANT DELETE ON TABLE public.external_ingest_batches TO "+grantDomainRole+" WITH GRANT OPTION",
	); err != nil {
		t.Fatal(err)
	}
	if err := CheckDomainAuthorization(ctx, domain, grantDomainRole, grantSchema); err == nil {
		t.Fatal("domain role unexpectedly authorized: DELETE WITH GRANT OPTION on external_ingest_batches went undetected")
	}
	// Revoking only the option, not the underlying privilege, proves the
	// check is reacting to the option specifically.
	if _, err := admin.Exec(
		ctx,
		"REVOKE GRANT OPTION FOR DELETE ON TABLE public.external_ingest_batches FROM "+grantDomainRole,
	); err != nil {
		t.Fatal(err)
	}
	if err := CheckDomainAuthorization(ctx, domain, grantDomainRole, grantSchema); err != nil {
		t.Fatalf("authorization did not recover after revoking the DELETE option, base privilege intact: %v", err)
	}
	if _, err := domain.Exec(ctx, "DELETE FROM public.external_ingest_batches WHERE id = gen_random_uuid()"); err != nil {
		t.Fatalf("domain role lost its base DELETE on external_ingest_batches: %v", err)
	}
}

// TestDomainRoleCanRunEveryProductionStatementShape proves each statement
// shape doesn't 42501, but it runs every statement in its own transaction
// that is rolled back immediately after — so for work_graph_execution_ledger
// specifically, the ON CONFLICT DO UPDATE arm of the upsert (and the
// follow-up transition UPDATE, which targets a fresh request_id) never
// actually match a row and never fire. PostgreSQL checks privileges against a
// statement's full text regardless of which branch executes at runtime, so
// that was never a privilege false-pass — but it does mean nothing proved
// the ledger's real upsert-then-transition sequence, the shape
// internal/jobs/workgraph/postgres.go's Claim/transition actually depends
// on, works end to end as the domain role. This test seeds a row, drives the
// real conflict, and checks RowsAffected on every statement, using
// production's full column shape (output_evidence, failure_detail,
// completed_at), not the bare single-column UPDATE in the generic list.
func TestDomainRoleCanClaimAndTransitionTheWorkGraphLedgerThroughARealConflict(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	_, uri := startGrantHarness(t, ctx)
	domain := connectAs(t, ctx, uri, grantDomainRole, grantDomainPass)

	const requestID = "00000000-0000-4000-8000-0000000000f1"
	const firstToken = "00000000-0000-4000-8000-0000000000f2"
	const secondToken = "00000000-0000-4000-8000-0000000000f3"
	const upsert = `
INSERT INTO public.work_graph_execution_ledger (
    request_id, claim_token, state, attempt_count, last_attempt_at
) VALUES ($1::uuid, $2::uuid, 'executing', 1, now())
ON CONFLICT (request_id) DO UPDATE SET
    claim_token = EXCLUDED.claim_token,
    state = 'executing',
    attempt_count = public.work_graph_execution_ledger.attempt_count + 1,
    output_evidence = NULL,
    failure_detail = NULL,
    completed_at = NULL,
    last_attempt_at = EXCLUDED.last_attempt_at
WHERE public.work_graph_execution_ledger.state IN ('executing', 'repaired')`

	// First claim: no existing row, so this exercises the upsert's plain
	// INSERT arm.
	insert, err := domain.Exec(ctx, upsert, requestID, firstToken)
	if err != nil {
		t.Fatalf("domain role cannot insert the first claim: %v", err)
	}
	if got := insert.RowsAffected(); got != 1 {
		t.Fatalf("first claim RowsAffected = %d, want 1", got)
	}

	// Second claim for the SAME request_id: request_id now collides with the
	// row just inserted, driving the actual ON CONFLICT DO UPDATE arm — the
	// branch production's re-lease path depends on and the generic exercise
	// list above never reaches.
	reclaim, err := domain.Exec(ctx, upsert, requestID, secondToken)
	if err != nil {
		t.Fatalf("domain role cannot reclaim through the real conflict path: %v", err)
	}
	if got := reclaim.RowsAffected(); got != 1 {
		t.Fatalf("reclaim (ON CONFLICT DO UPDATE) RowsAffected = %d, want 1 — the conflict arm did not fire", got)
	}
	var attemptCount int
	if err := domain.QueryRow(
		ctx,
		"SELECT attempt_count FROM public.work_graph_execution_ledger WHERE request_id = $1::uuid",
		requestID,
	).Scan(&attemptCount); err != nil {
		t.Fatalf("domain role cannot read back the reclaimed row: %v", err)
	}
	if attemptCount != 2 {
		t.Fatalf("attempt_count after reclaim = %d, want 2 (ON CONFLICT DO UPDATE should have incremented it)", attemptCount)
	}

	// Production's transition(): a genuine UPDATE matching the claim_token
	// just won by the reclaim, with the full production column shape
	// (output_evidence, failure_detail, completed_at) rather than the bare
	// single-column UPDATE in the generic exercise list.
	transition, err := domain.Exec(ctx, `
UPDATE public.work_graph_execution_ledger
SET state = $1, output_evidence = CASE WHEN $1 = 'succeeded' THEN $2::jsonb ELSE NULL END,
    failure_detail = CASE WHEN $1 = 'succeeded' THEN NULL ELSE $3 END,
    completed_at = CASE WHEN $1 = 'succeeded' THEN $4::timestamptz ELSE NULL END
WHERE request_id = $5::uuid AND state = 'executing' AND claim_token = $6::uuid`,
		"succeeded", `{"ok":true}`, "", time.Now(), requestID, secondToken)
	if err != nil {
		t.Fatalf("domain role cannot transition the ledger to succeeded: %v", err)
	}
	if got := transition.RowsAffected(); got != 1 {
		t.Fatalf("transition RowsAffected = %d, want 1 — the UPDATE did not match the reclaimed row", got)
	}
}

// has_table_privilege('... WITH GRANT OPTION') only sees the option on a
// TABLE-level ACL entry: it never inspects column ACLs at all, with or
// without the option. A column-level grant on one of the 20 required_tables
// rows — e.g. GRANT SELECT (id) ON integrations TO domain_runtime WITH GRANT
// OPTION — is therefore invisible to the table-level option check and would
// let the domain role re-delegate SELECT(id) while readiness reports the
// exact expected posture, unless the required_tables check also sweeps
// column-level grant options the same way the column-scoped fence block
// does.
func TestDomainAuthorizationRejectsColumnLevelGrantOptionOnRequiredTable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, uri := startGrantHarness(t, ctx)
	domain := connectAs(t, ctx, uri, grantDomainRole, grantDomainPass)

	if err := CheckDomainAuthorization(ctx, domain, grantDomainRole, grantSchema); err != nil {
		t.Fatalf("authorization rejected the privileges its own grants produced: %v", err)
	}
	if _, err := admin.Exec(
		ctx,
		"GRANT SELECT (id) ON TABLE public.integrations TO "+grantDomainRole+" WITH GRANT OPTION",
	); err != nil {
		t.Fatal(err)
	}
	if err := CheckDomainAuthorization(ctx, domain, grantDomainRole, grantSchema); err == nil {
		t.Fatal("domain role unexpectedly authorized: column-level SELECT (id) WITH GRANT OPTION on integrations went undetected")
	}
	if _, err := admin.Exec(
		ctx,
		"REVOKE GRANT OPTION FOR SELECT (id) ON TABLE public.integrations FROM "+grantDomainRole,
	); err != nil {
		t.Fatal(err)
	}
	if err := CheckDomainAuthorization(ctx, domain, grantDomainRole, grantSchema); err != nil {
		t.Fatalf("authorization did not recover after revoking the column-level option, base privilege intact: %v", err)
	}
	if _, err := domain.Exec(ctx, "SELECT id FROM public.integrations"); err != nil {
		t.Fatalf("domain role lost its base SELECT on integrations: %v", err)
	}
}

// CONNECT on the database and USAGE on the public schema are ambient
// privileges: CONNECT is never asserted true at all (the query already runs
// as this role, so it is implied), and USAGE is asserted true but, before
// this test, only in its plain form. A role holding either WITH GRANT OPTION
// could hand database or schema access onward while every other predicate in
// this query stayed satisfied.
func TestDomainAuthorizationRejectsAmbientPrivilegeGrantOption(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, uri := startGrantHarness(t, ctx)
	domain := connectAs(t, ctx, uri, grantDomainRole, grantDomainPass)

	if err := CheckDomainAuthorization(ctx, domain, grantDomainRole, grantSchema); err != nil {
		t.Fatalf("authorization rejected the privileges its own grants produced: %v", err)
	}

	t.Run("CONNECT on the database", func(t *testing.T) {
		if _, err := admin.Exec(
			ctx,
			"GRANT CONNECT ON DATABASE worker_test TO "+grantDomainRole+" WITH GRANT OPTION",
		); err != nil {
			t.Fatal(err)
		}
		if err := CheckDomainAuthorization(ctx, domain, grantDomainRole, grantSchema); err == nil {
			t.Fatal("domain role unexpectedly authorized: CONNECT WITH GRANT OPTION went undetected")
		}
		if _, err := admin.Exec(
			ctx,
			"REVOKE GRANT OPTION FOR CONNECT ON DATABASE worker_test FROM "+grantDomainRole,
		); err != nil {
			t.Fatal(err)
		}
		if err := CheckDomainAuthorization(ctx, domain, grantDomainRole, grantSchema); err != nil {
			t.Fatalf("authorization did not recover after revoking the CONNECT option: %v", err)
		}
	})

	t.Run("USAGE on the public schema", func(t *testing.T) {
		if _, err := admin.Exec(
			ctx,
			"GRANT USAGE ON SCHEMA public TO "+grantDomainRole+" WITH GRANT OPTION",
		); err != nil {
			t.Fatal(err)
		}
		if err := CheckDomainAuthorization(ctx, domain, grantDomainRole, grantSchema); err == nil {
			t.Fatal("domain role unexpectedly authorized: public-schema USAGE WITH GRANT OPTION went undetected")
		}
		if _, err := admin.Exec(
			ctx,
			"REVOKE GRANT OPTION FOR USAGE ON SCHEMA public FROM "+grantDomainRole,
		); err != nil {
			t.Fatal(err)
		}
		if err := CheckDomainAuthorization(ctx, domain, grantDomainRole, grantSchema); err != nil {
			t.Fatalf("authorization did not recover after revoking the public-schema USAGE option: %v", err)
		}
	})
}

// The public schema's CREATE privilege is asserted absent independently of
// what migrate.go claims to have revoked; the River schema's was not, which
// is an asymmetry rather than a deliberate difference — migrate.go revokes
// ALL privileges there, so this should never be reachable in practice, but
// this assertion exists precisely to not take that on trust.
func TestDomainAuthorizationRejectsRiverSchemaCreate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, uri := startGrantHarness(t, ctx)
	domain := connectAs(t, ctx, uri, grantDomainRole, grantDomainPass)

	if err := CheckDomainAuthorization(ctx, domain, grantDomainRole, grantSchema); err != nil {
		t.Fatalf("authorization rejected the privileges its own grants produced: %v", err)
	}
	if _, err := admin.Exec(ctx, "GRANT CREATE ON SCHEMA "+grantSchema+" TO "+grantDomainRole); err != nil {
		t.Fatal(err)
	}
	if err := CheckDomainAuthorization(ctx, domain, grantDomainRole, grantSchema); err == nil {
		t.Fatal("domain role unexpectedly authorized: River-schema CREATE went undetected")
	}
	if _, err := admin.Exec(ctx, "REVOKE CREATE ON SCHEMA "+grantSchema+" FROM "+grantDomainRole); err != nil {
		t.Fatal(err)
	}
	if err := CheckDomainAuthorization(ctx, domain, grantDomainRole, grantSchema); err != nil {
		t.Fatalf("authorization did not recover after revoking River-schema CREATE: %v", err)
	}
}

// Ownership is the exception to the "holding a privilege WITH GRANT OPTION
// implies holding the plain privilege" argument every required-ABSENT check
// in this query otherwise relies on: an owner can REVOKE ALL on its own
// object while PostgreSQL still treats ownership as carrying every grant
// option, so the base has_*_privilege checks read false while the role can
// still re-grant to itself, another role, or PUBLIC. This pins the concrete
// exploit: the domain role owns a schema, revokes every ordinary privilege on
// it from itself, and readiness must still fail — because it is ownership
// itself, not any surviving privilege, that the role could leverage.
func TestDomainAuthorizationRejectsSelfOwnedSchema(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, uri := startGrantHarness(t, ctx)
	domain := connectAs(t, ctx, uri, grantDomainRole, grantDomainPass)

	if err := CheckDomainAuthorization(ctx, domain, grantDomainRole, grantSchema); err != nil {
		t.Fatalf("authorization rejected the privileges its own grants produced: %v", err)
	}
	if _, err := admin.Exec(ctx, "CREATE SCHEMA owned_probe AUTHORIZATION "+grantDomainRole); err != nil {
		t.Fatal(err)
	}
	// The owner still holds every privilege WITH GRANT OPTION at this point;
	// revoking the ordinary form is exactly what a base has_*_privilege check
	// cannot distinguish from never having had access at all.
	if _, err := admin.Exec(ctx, "REVOKE ALL ON SCHEMA owned_probe FROM "+grantDomainRole); err != nil {
		t.Fatal(err)
	}
	if err := CheckDomainAuthorization(ctx, domain, grantDomainRole, grantSchema); err == nil {
		t.Fatal("domain role unexpectedly authorized: self-owned schema with all ordinary privileges revoked went undetected")
	}
	if _, err := admin.Exec(ctx, "DROP SCHEMA owned_probe"); err != nil {
		t.Fatal(err)
	}
	if err := CheckDomainAuthorization(ctx, domain, grantDomainRole, grantSchema); err != nil {
		t.Fatalf("authorization did not recover after dropping the self-owned schema: %v", err)
	}
}

func init() {
	// Keep the exercised set and the assertion honest about their own size, so
	// a table added to one and not the other is visible in the failure text.
	_ = fmt.Sprintf("%d reconciliation tables", len(reconciliationTables()))
}

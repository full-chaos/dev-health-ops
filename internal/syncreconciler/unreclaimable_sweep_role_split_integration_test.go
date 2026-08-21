//go:build integration

package syncreconciler

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CHAOS-4035. The sweep shipped constructed on the DOMAIN pool while its first
// statement read worker_job_routes, a table the role manifest attributes
// exclusively to the COORDINATOR role. Production returned 42501 once per
// second from the deploy onward and the sweep never completed a single pass.
//
// The sibling suite in unreclaimable_sweep_integration_test.go connects as the
// container superuser. That is precisely why this defect shipped green: a
// superuser holds every privilege, so no single-pool wiring mistake is
// observable there and no amount of additional behavioural coverage on that
// harness could have caught it. This file therefore connects as REAL,
// separately granted, least-privilege logins -- one holding exactly what
// postgres.DomainPosture() declares, one holding exactly what
// postgres.CoordinatorPosture() declares -- and runs the sweep across them.
//
// The grants are DERIVED from those two posture values rather than hand
// listed, so this harness cannot drift from the manifest, and widening either
// posture to make a test pass would be visible in the diff of
// domain_authorization.go rather than hidden in a test fixture here.

const (
	splitDomainRole      = "sweep_domain_runtime"
	splitDomainPass      = "sweep_domain_password"
	splitCoordinatorRole = "sweep_coordinator_runtime"
	splitCoordinatorPass = "sweep_coordinator_password"

	splitOrg    = "00000000-0000-4000-8000-000000005000"
	splitRun    = "00000000-0000-4000-8000-000000005001"
	splitUnit   = "00000000-0000-4000-8000-000000005002"
	splitIntgr  = "00000000-0000-4000-8000-000000005003"
	splitSource = "00000000-0000-4000-8000-000000005004"
)

// splitSchemaDDL is derived from the alembic migrations column for column,
// NOT invented for this test. CHAOS-3997 is the standing lesson: an
// integration suite that hand-writes a convenient schema tests its own
// fixture rather than production, and it hid a nonexistent-column defect
// through a full green gate.
//
//   - sync_runs: 0015, plus 0030's credential stamp and 0105's trace_parent.
//   - sync_run_units: 0015, plus 0019 (lease), 0022 (rate-limit deferrals),
//     0028 (expired-lease retry), 0085 (budget deferrals).
//   - worker_job_routes: 0055, including its transport and generation check
//     constraints.
//   - worker_job_outbox: 0046.
//
// `result` and `processor_flags` are sa.JSON(), which is PostgreSQL `json`
// and NOT `jsonb`. That distinction is load-bearing rather than pedantic:
// terminalizeUnreclaimableSQL binds `result = $4::jsonb`, so only the real
// column type proves the assignment cast production depends on actually
// exists.
func splitSchemaDDL() []string {
	return []string{
		// FK anchors only. The sweep never reads either table; they exist so
		// the real foreign keys on sync_runs/sync_run_units can too, because
		// dropping a constraint to simplify a fixture is how a test stops
		// describing production.
		"CREATE TABLE public.integrations (id uuid PRIMARY KEY)",
		"CREATE TABLE public.integration_sources (id uuid PRIMARY KEY)",
		`CREATE TABLE public.sync_runs (
			id uuid NOT NULL,
			org_id text NOT NULL,
			integration_id uuid NOT NULL,
			triggered_by text NOT NULL,
			mode varchar NOT NULL,
			status varchar NOT NULL,
			total_units integer NOT NULL,
			completed_units integer NOT NULL,
			failed_units integer NOT NULL,
			started_at timestamptz,
			completed_at timestamptz,
			result json,
			error text,
			created_at timestamptz NOT NULL,
			credential_id uuid,
			credential_fingerprint text,
			auth_source text,
			trace_parent text,
			CONSTRAINT sync_runs_pkey PRIMARY KEY (id),
			CONSTRAINT fk_sync_runs_integration_id
				FOREIGN KEY (integration_id) REFERENCES public.integrations (id)
		)`,
		`CREATE TABLE public.sync_run_units (
			id uuid NOT NULL,
			org_id text NOT NULL,
			sync_run_id uuid NOT NULL,
			integration_id uuid NOT NULL,
			source_id uuid NOT NULL,
			provider text NOT NULL,
			dataset_key varchar NOT NULL,
			cost_class varchar NOT NULL,
			mode varchar NOT NULL,
			since_at timestamptz,
			before_at timestamptz,
			status varchar NOT NULL,
			attempts integer NOT NULL,
			duration_seconds integer,
			error text,
			result json,
			processor_flags json,
			created_at timestamptz NOT NULL,
			updated_at timestamptz NOT NULL,
			lease_owner text,
			lease_expires_at timestamptz,
			last_heartbeat_at timestamptz,
			available_at timestamptz,
			rate_limit_deferrals integer NOT NULL DEFAULT 0,
			rate_limit_first_seen_at timestamptz,
			expired_lease_retry_count integer NOT NULL DEFAULT 0,
			last_retry_reason text,
			retry_exhausted_at timestamptz,
			budget_deferrals integer NOT NULL DEFAULT 0,
			budget_first_deferred_at timestamptz,
			first_blocked_at timestamptz,
			CONSTRAINT sync_run_units_pkey PRIMARY KEY (id),
			CONSTRAINT fk_sync_run_units_source_id
				FOREIGN KEY (source_id) REFERENCES public.integration_sources (id),
			CONSTRAINT fk_sync_run_units_sync_run_id
				FOREIGN KEY (sync_run_id) REFERENCES public.sync_runs (id)
		)`,
		`CREATE TABLE public.worker_job_routes (
			job_kind varchar(96) NOT NULL,
			transport varchar(16) NOT NULL,
			paused boolean NOT NULL DEFAULT false,
			generation bigint NOT NULL DEFAULT 1,
			updated_at timestamptz NOT NULL,
			CONSTRAINT ck_worker_job_route_transport
				CHECK (transport IN ('celery', 'shadow', 'river_canary', 'river')),
			CONSTRAINT ck_worker_job_route_generation CHECK (generation >= 1),
			CONSTRAINT worker_job_routes_pkey PRIMARY KEY (job_kind)
		)`,
		`CREATE TABLE public.worker_job_outbox (
			id uuid NOT NULL,
			dedupe_key varchar(256) NOT NULL,
			job_kind varchar(96) NOT NULL,
			contract_version integer NOT NULL,
			args json NOT NULL,
			payload_hash varchar(71) NOT NULL,
			queue varchar(96) NOT NULL,
			priority smallint NOT NULL,
			max_attempts smallint NOT NULL,
			scheduled_at timestamptz NOT NULL,
			status varchar(16) NOT NULL,
			claim_token uuid,
			claimed_at timestamptz,
			claim_expires_at timestamptz,
			attempt_count integer NOT NULL,
			first_attempt_at timestamptz,
			last_attempt_at timestamptz,
			next_attempt_at timestamptz NOT NULL,
			last_error_code varchar(64),
			last_error_detail varchar(256),
			last_error_at timestamptz,
			river_job_id bigint,
			delivered_at timestamptz,
			created_at timestamptz NOT NULL,
			updated_at timestamptz NOT NULL,
			CONSTRAINT worker_job_outbox_pkey PRIMARY KEY (id),
			CONSTRAINT uq_worker_job_outbox_dedupe_key UNIQUE (dedupe_key)
		)`,
	}
}

// splitGrantStatements derives the GRANTs for one role straight from its own
// RolePosture. Two properties follow, and both are the point:
//
//   - the role holds nothing the manifest does not declare, so worker_job_routes
//     is absent from the domain role by construction rather than by omission;
//   - a future edit that widens domainPosture() to paper over this defect makes
//     the CHAOS-4035 regression test below go green again, which is exactly
//     what the reviewer of that edit needs to see.
//
// Only tables this harness actually creates are granted, mirroring the real
// migration's to_regclass guard around every grant.
func splitGrantStatements(role string, posture postgres.RolePosture, present map[string]bool) []string {
	statements := make([]string, 0, len(posture.RequiredTables))
	for _, table := range posture.RequiredTables {
		if !present[table.TableName] {
			continue
		}
		privileges := []string{"SELECT"}
		if table.AllowInsert {
			privileges = append(privileges, "INSERT")
		}
		if table.AllowUpdate {
			privileges = append(privileges, "UPDATE")
		}
		if table.AllowDelete {
			privileges = append(privileges, "DELETE")
		}
		statements = append(statements, fmt.Sprintf(
			"GRANT %s ON TABLE public.%s TO %s",
			strings.Join(privileges, ", "), table.TableName, role,
		))
	}
	for _, column := range posture.ColumnScoped {
		if !present[column.TableName] {
			continue
		}
		statements = append(statements, fmt.Sprintf(
			"GRANT %s (%s) ON TABLE public.%s TO %s",
			column.Privilege, column.ColumnName, column.TableName, role,
		))
	}
	return statements
}

func splitTablesPresent() map[string]bool {
	return map[string]bool{
		"integrations":        true,
		"integration_sources": true,
		"sync_runs":           true,
		"sync_run_units":      true,
		"worker_job_routes":   true,
		"worker_job_outbox":   true,
	}
}

// startRoleSplitHarness provisions the production role shape: two
// least-privilege logins, each granted exactly its own posture, on a schema
// derived from the alembic migrations.
func startRoleSplitHarness(t *testing.T, ctx context.Context) (admin *pgxpool.Pool, uri string) {
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
	admin, err = pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)

	statements := []string{
		"CREATE ROLE " + splitDomainRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + splitDomainPass + "'",
		"CREATE ROLE " + splitCoordinatorRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + splitCoordinatorPass + "'",
		"GRANT CONNECT ON DATABASE worker_test TO " + splitDomainRole + ", " + splitCoordinatorRole,
		"GRANT USAGE ON SCHEMA public TO " + splitDomainRole + ", " + splitCoordinatorRole,
		// PUBLIC holds CREATE on public in older defaults and TEMPORARY on the
		// database; both are privileges neither runtime role is supposed to
		// have, and leaving them would weaken every assertion below.
		"REVOKE CREATE ON SCHEMA public FROM PUBLIC",
		"REVOKE TEMPORARY ON DATABASE worker_test FROM PUBLIC",
	}
	statements = append(statements, splitSchemaDDL()...)
	present := splitTablesPresent()
	statements = append(statements,
		splitGrantStatements(splitDomainRole, postgres.DomainPosture(), present)...)
	statements = append(statements,
		splitGrantStatements(splitCoordinatorRole, postgres.CoordinatorPosture(), present)...)
	for _, statement := range statements {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("harness setup failed: %v\n  statement: %s", err, collapseSQL(statement))
		}
	}
	return admin, instance.URI
}

func connectAsRole(t *testing.T, ctx context.Context, rawURI, role, password string) *pgxpool.Pool {
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

func collapseSQL(statement string) string {
	return strings.Join(strings.Fields(statement), " ")
}

func isDeniedByPrivilege(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42501"
}

// seedSplitStrand writes the production shape: claimed to 'dispatching',
// never published, no lease, no heartbeat, no attempt, long-lived and idle.
func seedSplitStrand(t *testing.T, ctx context.Context, admin *pgxpool.Pool, now time.Time) {
	t.Helper()
	for _, seed := range []struct {
		sql  string
		args []any
	}{
		{"INSERT INTO public.integrations (id) VALUES ($1)", []any{splitIntgr}},
		{"INSERT INTO public.integration_sources (id) VALUES ($1)", []any{splitSource}},
		{`INSERT INTO public.sync_runs (
			id, org_id, integration_id, triggered_by, mode, status,
			total_units, completed_units, failed_units, created_at
		) VALUES ($1, $2, $3, 'schedule', 'incremental', 'dispatching', 1, 0, 0, $4)`,
			[]any{splitRun, splitOrg, splitIntgr, now.Add(-16 * time.Hour)}},
		{`INSERT INTO public.sync_run_units (
			id, org_id, sync_run_id, integration_id, source_id, provider,
			dataset_key, cost_class, mode, status, attempts, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, 'github', 'tests', 'heavy', 'incremental',
			'dispatching', 0, $6, $7)`,
			[]any{splitUnit, splitOrg, splitRun, splitIntgr, splitSource,
				now.Add(-16 * time.Hour), now.Add(-90 * time.Minute)}},
		{`INSERT INTO public.worker_job_routes (job_kind, transport, updated_at)
			VALUES ($1, 'river_canary', $2)`,
			[]any{unreclaimableProviderUnitID, now}},
	} {
		if _, err := admin.Exec(ctx, seed.sql, seed.args...); err != nil {
			t.Fatalf("seed: %v\n  statement: %s", err, collapseSQL(seed.sql))
		}
	}
}

func splitSweepConfig(mode SweepMode) UnreclaimableSweepConfig {
	return UnreclaimableSweepConfig{
		Age:  DefaultUnreclaimableAge,
		Idle: DefaultUnreclaimableIdle,
		Mode: mode,
		// tests / pr-reviews / pr-comments left off: the production wedge.
		Switches: providersync.CompleteRouteSwitches{GithubRepoMetadata: true},
	}
}

// TestUnreclaimableSweepStatementsAgainstTheRealRoleSplit pins the grant
// matrix the CHAOS-4035 production diagnosis established, as executable
// assertions rather than a table in a ticket. It holds before and after the
// fix -- it describes the postures, which do not change -- and it is what
// makes the split MANDATORY rather than stylistic: neither pool alone can run
// the whole sweep.
func TestUnreclaimableSweepStatementsAgainstTheRealRoleSplit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, uri := startRoleSplitHarness(t, ctx)
	now := time.Now().UTC()
	seedSplitStrand(t, ctx, admin, now)
	domain := connectAsRole(t, ctx, uri, splitDomainRole, splitDomainPass)
	coordinator := connectAsRole(t, ctx, uri, splitCoordinatorRole, splitCoordinatorPass)

	// The route read: coordinator-only. This is the 42501 production served
	// once a second.
	_, err := domain.Exec(ctx, selectProviderUnitRouteSQL, unreclaimableProviderUnitID)
	if !isDeniedByPrivilege(err) {
		t.Errorf("the domain role was not denied the durable route read (got %v); "+
			"if this now passes, domainPosture() was widened to include "+
			"worker_job_routes, which CheckRolePosture forbids", err)
	}
	if _, err := coordinator.Exec(ctx, selectProviderUnitRouteSQL, unreclaimableProviderUnitID); err != nil {
		t.Errorf("the coordinator role was denied the durable route read: %v", err)
	}

	// The terminalize write: domain-only. coordinatorPosture declares
	// sync_run_units SELECT-only, so "just move the whole sweep to the
	// coordinator pool" is not available and the split is forced.
	terminalize := func(pool *pgxpool.Pool) error {
		tx, beginErr := pool.Begin(ctx)
		if beginErr != nil {
			t.Fatalf("begin: %v", beginErr)
		}
		_, execErr := tx.Exec(ctx, terminalizeUnreclaimableSQL,
			splitUnit, unreclaimableErrorCategory, "reason", "{}",
			now, now.Add(-90*time.Minute))
		_ = tx.Rollback(ctx)
		return execErr
	}
	if err := terminalize(domain); err != nil {
		t.Errorf("the domain role was denied the terminalize write: %v", err)
	}
	if err := terminalize(coordinator); !isDeniedByPrivilege(err) {
		t.Errorf("the coordinator role was PERMITTED the terminalize write (got %v); "+
			"coordinatorPosture declares sync_run_units SELECT-only", err)
	}

	// The candidate read and the outbox filter: both domain-granted, so
	// selection correctly stays inside the domain transaction.
	for _, statement := range []struct {
		name string
		sql  string
		args []any
	}{
		{"candidate page read", selectUnreclaimableCandidatesSQL,
			[]any{now, now, time.Time{}, "00000000-0000-0000-0000-000000000000", 10}},
		{"published-outbox filter", selectPublishedDedupeKeysSQL,
			[]any{[]string{unreclaimableDedupeKey(splitUnit)}}},
	} {
		if _, err := domain.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Errorf("the domain role was denied the %s: %v", statement.name, err)
		}
	}

	// The ticket's production proof matrix, restated as catalog facts so a
	// failure names the privilege rather than only the statement.
	for _, probe := range []struct {
		role      string
		table     string
		privilege string
		want      bool
	}{
		{splitDomainRole, "worker_job_routes", "SELECT", false},
		{splitCoordinatorRole, "worker_job_routes", "SELECT", true},
		{splitDomainRole, "sync_run_units", "UPDATE", true},
		{splitCoordinatorRole, "sync_run_units", "UPDATE", false},
		{splitDomainRole, "worker_job_outbox", "SELECT", true},
	} {
		var got bool
		if err := admin.QueryRow(ctx,
			"SELECT has_table_privilege($1, $2, $3)",
			probe.role, "public."+probe.table, probe.privilege,
		).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != probe.want {
			t.Errorf("has_table_privilege(%s, %s, %s) = %v, want %v",
				probe.role, probe.table, probe.privilege, got, probe.want)
		}
	}
}

// TestUnreclaimableSweepCompletesAFullPassUnderTheRealRoleSplit is CHAOS-4035
// acceptance criterion 1, and the regression control for the defect: run
// against the shipped single-pool wiring it fails with 42501 on the route
// read, exactly as production did.
func TestUnreclaimableSweepCompletesAFullPassUnderTheRealRoleSplit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, uri := startRoleSplitHarness(t, ctx)
	now := time.Now().UTC()
	seedSplitStrand(t, ctx, admin, now)
	domain := connectAsRole(t, ctx, uri, splitDomainRole, splitDomainPass)
	coordinator := connectAsRole(t, ctx, uri, splitCoordinatorRole, splitCoordinatorPass)

	sweep, err := NewUnreclaimableSweep(coordinator, domain, splitSweepConfig(SweepModeActive))
	if err != nil {
		t.Fatalf("construct sweep: %v", err)
	}
	result, err := sweep.Step(ctx, now, 100)
	if err != nil {
		t.Fatalf("sweep failed under the real role split: %v", err)
	}
	if result.Candidates != 1 || result.Terminalized != 1 {
		t.Fatalf("result = %+v, want 1 candidate and 1 terminalized", result)
	}

	var status string
	var reason *string
	if err := admin.QueryRow(ctx,
		"SELECT status, last_retry_reason FROM public.sync_run_units WHERE id = $1",
		splitUnit,
	).Scan(&status, &reason); err != nil {
		t.Fatal(err)
	}
	if status != "failed" {
		t.Fatalf("status = %q, want failed", status)
	}
	if reason == nil || !strings.Contains(*reason, "github/tests") {
		t.Fatalf("last_retry_reason = %v", reason)
	}
}

// Shadow is the DEFAULT mode, so it is the one production actually runs. It
// must complete its selection under the same split -- a sweep that can only
// work in active mode would leave the default deployment reporting nothing.
func TestUnreclaimableSweepShadowPassUnderTheRealRoleSplit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, uri := startRoleSplitHarness(t, ctx)
	now := time.Now().UTC()
	seedSplitStrand(t, ctx, admin, now)
	domain := connectAsRole(t, ctx, uri, splitDomainRole, splitDomainPass)
	coordinator := connectAsRole(t, ctx, uri, splitCoordinatorRole, splitCoordinatorPass)

	sweep, err := NewUnreclaimableSweep(coordinator, domain, splitSweepConfig(SweepModeShadow))
	if err != nil {
		t.Fatalf("construct sweep: %v", err)
	}
	result, err := sweep.Step(ctx, now, 100)
	if err != nil {
		t.Fatalf("shadow sweep failed under the real role split: %v", err)
	}
	if result.Candidates != 1 || result.Terminalized != 0 {
		t.Fatalf("result = %+v, want 1 candidate selected and nothing written", result)
	}
	var status string
	if err := admin.QueryRow(ctx,
		"SELECT status FROM public.sync_run_units WHERE id = $1", splitUnit,
	).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "dispatching" {
		t.Fatalf("status = %q, want shadow mode to write nothing", status)
	}
}

// flipOnSecondRead is the adversarial ordering, made deterministic. It passes
// every route read through to the real coordinator pool, but immediately
// BEFORE the second read -- the fence re-read, which happens with the
// terminalize writes staged and uncommitted -- it commits an operator rollback
// to Celery through the admin connection.
//
// That is precisely the window an adversarial review raised against the
// two-pool split: the sweep decides River owns provider units, ownership is
// rolled back to Celery, and the sweep writes 'failed' onto units Celery is
// about to claim.
type flipOnSecondRead struct {
	t           *testing.T
	ctx         context.Context
	admin       *pgxpool.Pool
	coordinator *pgxpool.Pool
	reads       int
	flipped     bool
}

func (routes *flipOnSecondRead) Query(
	ctx context.Context, sql string, args ...any,
) (pgx.Rows, error) {
	routes.reads++
	if routes.reads == 2 && !routes.flipped {
		routes.flipped = true
		// The real mutation shape from internal/jobroute/control.go Rollback:
		// the transport moves and the generation is bumped in the same write.
		if _, err := routes.admin.Exec(routes.ctx, `
			UPDATE public.worker_job_routes
			SET transport = 'celery', generation = generation + 1, updated_at = now()
			WHERE job_kind = $1`, unreclaimableProviderUnitID); err != nil {
			routes.t.Fatalf("rollback the route mid-pass: %v", err)
		}
	}
	return routes.coordinator.Query(ctx, sql, args...)
}

// TestUnreclaimableSweepDeclinesWhenTheRouteRollsBackMidPass is the negative
// race control. Without the closing fence this test terminalizes the unit:
// the opening read saw river_canary, the domain transaction never observes
// worker_job_routes at all, and the terminalize CAS on updated_at cannot see
// an ownership flip because no runtime has touched the row.
func TestUnreclaimableSweepDeclinesWhenTheRouteRollsBackMidPass(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, uri := startRoleSplitHarness(t, ctx)
	now := time.Now().UTC()
	seedSplitStrand(t, ctx, admin, now)
	domain := connectAsRole(t, ctx, uri, splitDomainRole, splitDomainPass)
	coordinator := connectAsRole(t, ctx, uri, splitCoordinatorRole, splitCoordinatorPass)

	routes := &flipOnSecondRead{t: t, ctx: ctx, admin: admin, coordinator: coordinator}
	sweep, err := newUnreclaimableSweep(routes, domain.Begin, splitSweepConfig(SweepModeActive))
	if err != nil {
		t.Fatalf("construct sweep: %v", err)
	}
	result, err := sweep.Step(ctx, now, 100)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if !routes.flipped {
		t.Fatal("the route was never rolled back, so this test proved nothing; " +
			"the sweep is no longer re-reading the route before it commits")
	}
	if result.Candidates != 1 {
		t.Fatalf("result = %+v, want the strand still selected", result)
	}
	if result.Terminalized != 0 || !result.DeclinedRouteChange {
		t.Fatalf("result = %+v, want the write abandoned and the decline reported; "+
			"the sweep terminalized work Celery had just been handed back", result)
	}

	var status string
	if err := admin.QueryRow(ctx,
		"SELECT status FROM public.sync_run_units WHERE id = $1", splitUnit,
	).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "dispatching" {
		t.Fatalf("status = %q, want the unit left for Celery", status)
	}
}

// The fence must not fire on a pass where nothing changed, or the sweep never
// commits anything and the whole safety net is dead in a quieter way than
// CHAOS-4035 killed it.
func TestUnreclaimableSweepFenceDoesNotFireOnAStableRoute(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, uri := startRoleSplitHarness(t, ctx)
	now := time.Now().UTC()
	seedSplitStrand(t, ctx, admin, now)
	domain := connectAsRole(t, ctx, uri, splitDomainRole, splitDomainPass)
	coordinator := connectAsRole(t, ctx, uri, splitCoordinatorRole, splitCoordinatorPass)

	sweep, err := NewUnreclaimableSweep(coordinator, domain, splitSweepConfig(SweepModeActive))
	if err != nil {
		t.Fatalf("construct sweep: %v", err)
	}
	result, err := sweep.Step(ctx, now, 100)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if result.DeclinedRouteChange {
		t.Fatalf("result = %+v, want no decline on an unchanged route", result)
	}
	if result.Terminalized != 1 {
		t.Fatalf("result = %+v, want the strand terminalized", result)
	}
}

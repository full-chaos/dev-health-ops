//go:build integration

package syncdispatchruntime

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Every native sync-dispatch service in this package is constructed with
// postgresDatabase.pools.Domain (cmd/dev-health-worker/sync_dispatch.go), so
// under the Option B two-role split every statement any of them issues is
// executed by the restricted DOMAIN login. Nothing in this package's other
// integration suites measures that: they all connect as the container's
// superuser, which holds every privilege by ownership, so a service reaching a
// coordinator-exclusive table passes them and fails in production with a
// 42501. That is exactly what CHAOS-4209 is -- two families merged that way,
// caught before the deploy rather than by it.
//
// This suite closes the hole GENERICALLY. It does not enumerate statements:
// hand-written statement tables rot the moment a service grows a query, and a
// query no list mentions is invisible to a list-based check. Instead it runs
// each service's own Discover/Finalize entry point, unmodified, against a real
// server, over a connection authenticated as a real restricted domain role
// holding exactly the grants ApplyPinnedMigrations emits -- the production
// migration itself, not a copy of its statements -- and watches the pgx driver
// for insufficient_privilege.
//
// Watching the driver rather than the returned error is load-bearing. Both
// services deliberately collapse database failures into their own sentinel
// (ErrReferenceDiscoveryUnavailable / ErrFinalizeSyncRunUnavailable) and some
// paths swallow the failure entirely by design -- the compute-checkpoint
// savepoint arm logs and continues, the heartbeat goroutine logs and keeps
// ticking. A test asserting on the returned error would therefore miss a real
// 42501 on any of those paths. A QueryTracer sees the SQLSTATE the server
// actually raised, on every statement, whatever the caller then does with it.
//
// Extending this to a new native service is one function: create its tables,
// seed its row graph, call its entry point through domainPool, then call
// denials.assertNone. Family 3 (NativeDispatchSyncRunService, CHAOS-4175 in
// flight) folds in that way on rebase.
//
// Each test asserts an end state as well as the absence of denials. Without
// that, a service that bailed out early -- an unseeded transport route, a
// missing outbox row -- would issue almost no statements and "pass" this suite
// while measuring nothing.
//
// What a green run does and does not prove: it proves the ARMS IT RAN issue no
// denied statement, not that every branch of every service is privilege-clean.
// The discovery test therefore drives three arms that touch different tables --
// success, terminal failure, and the feature-disabled gate, the last of which
// reaches a site in feature_disabled_termination.go that neither of the others
// does. Branches deliberately left unrun (the retry arm, the already-dispatched
// 23505 early return, the compute-checkpoint savepoint recovery arm) issue no
// statement against a table or privilege some run arm has not already
// exercised; a branch that grows one is a branch this suite must grow an arm
// for.

const (
	privilegeDomainRole     = "grant_domain_runtime"
	privilegeDomainPassword = "grant_domain_password"
	privilegeQueueRole      = "grant_queue_runtime"
	privilegeQueuePassword  = "grant_queue_password"
	privilegeRiverSchema    = "river"
)

// tracedSQLKey carries the statement text from TraceQueryStart to
// TraceQueryEnd. pgx hands the SQL to the start hook and the error to the end
// hook, so a denial can only name the statement that caused it if the two are
// stitched together through the per-query context pgx threads between them.
type tracedSQLKey struct{}

// privilegeDenial is one statement the domain role was refused, with the
// server's own message, so a failure names the SQL to look at rather than only
// the service that issued it.
type privilegeDenial struct {
	statement string
	message   string
}

// denialRecorder is a pgx.QueryTracer that records every insufficient_privilege
// (42501) the server raises on a connection, including on statements whose
// caller catches and discards the error.
type denialRecorder struct {
	mutex   sync.Mutex
	denials []privilegeDenial
}

func (recorder *denialRecorder) TraceQueryStart(
	ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData,
) context.Context {
	return context.WithValue(ctx, tracedSQLKey{}, data.SQL)
}

func (recorder *denialRecorder) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	var pgErr *pgconn.PgError
	if !errors.As(data.Err, &pgErr) || pgErr.Code != "42501" {
		return
	}
	statement, _ := ctx.Value(tracedSQLKey{}).(string)
	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()
	recorder.denials = append(recorder.denials, privilegeDenial{
		statement: collapseStatement(statement),
		message:   pgErr.Message,
	})
}

// assertNone fails the test with every denial the run produced, rather than
// only the first: one missing grant usually means several, and a fix loop that
// learns them one container start at a time is needlessly slow.
func (recorder *denialRecorder) assertNone(t *testing.T, service string) {
	t.Helper()
	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()
	if len(recorder.denials) == 0 {
		return
	}
	t.Errorf("%s issued %d statement(s) the domain role is not granted (SQLSTATE 42501). "+
		"The service runs on postgresDatabase.pools.Domain, so each of these is a production failure. "+
		"Fix by declaring the table in domainPosture() (internal/storage/postgres/domain_authorization.go) "+
		"with ONLY the flags these statements prove, and emitting the matching GRANT in "+
		"runtimeGrantStatements (internal/storage/river/migrate.go).", service, len(recorder.denials))
	for index, denial := range recorder.denials {
		t.Errorf("  [%d] %s\n       statement: %s", index, denial.message, denial.statement)
	}
}

// reset drops everything recorded so far, so one harness can measure a second,
// independent run. It exists only for the negative control below: the ordinary
// tests must never discard a denial.
func (recorder *denialRecorder) reset() {
	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()
	recorder.denials = nil
}

func collapseStatement(statement string) string {
	return strings.Join(strings.Fields(statement), " ")
}

// startDomainRoleHarness builds the venue: production-shaped relations, a real
// least-privilege domain login, and the REAL migration's grants.
//
// Order is load-bearing. Every domain GRANT in runtimeGrantStatements is
// wrapped in a to_regclass guard, so a relation created after
// ApplyPinnedMigrations is silently skipped -- the domain role would then hold
// nothing on it and this suite would report a denial that no production
// deployment has. Tables first, migration second.
//
// It returns an admin pool as well: seeding and reading back end state must NOT
// go through the domain connection, or the assertions would themselves be
// subject to the privileges under test.
func startDomainRoleHarness(
	t *testing.T,
	ctx context.Context,
	createTables func(*testing.T, context.Context, *pgxpool.Pool),
) (*pgxpool.Pool, *pgxpool.Pool, *denialRecorder) {
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

	createTables(t, ctx, admin)

	for _, statement := range []string{
		"CREATE ROLE " + privilegeDomainRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE " +
			"NOREPLICATION NOBYPASSRLS PASSWORD '" + privilegeDomainPassword + "'",
		"CREATE ROLE " + privilegeQueueRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE " +
			"NOREPLICATION NOBYPASSRLS PASSWORD '" + privilegeQueuePassword + "'",
		"GRANT CONNECT ON DATABASE worker_test TO " + privilegeDomainRole + ", " + privilegeQueueRole,
		"REVOKE TEMPORARY ON DATABASE worker_test FROM PUBLIC",
		"REVOKE CREATE ON SCHEMA public FROM PUBLIC",
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}

	if _, err := riverstore.ApplyPinnedMigrations(ctx, admin, riverstore.MigrationOptions{
		Schema:     privilegeRiverSchema,
		DomainRole: privilegeDomainRole,
		QueueRole:  privilegeQueueRole,
	}); err != nil {
		t.Fatal(err)
	}

	parsed, err := url.Parse(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	parsed.User = url.UserPassword(privilegeDomainRole, privilegeDomainPassword)
	config, err := pgxpool.ParseConfig(parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	recorder := &denialRecorder{}
	config.ConnConfig.Tracer = recorder
	domain, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(domain.Close)
	return admin, domain, recorder
}

// TestNativeReferenceDiscoveryExecutesEntirelyAsTheDomainRole is CHAOS-4209
// family 2, executed. NativeReferenceDiscoveryService reads, inserts and
// updates public.sync_run_reference_discoveries at nine sites, and that table
// was declared only in coordinatorPosture() -- so before the fix every one of
// those statements is refused here, on both the success path and the terminal
// failure path.
//
// Both paths run, in one container, because they touch different tables: the
// success path stamps the ledger and arms the dispatch_sync_run wakeup, while
// the terminal failure path additionally fails every nonterminal unit
// (sync_run_units), stamps the run's error (sync_runs) and arms the
// finalize_sync_run wakeup.
func TestNativeReferenceDiscoveryExecutesEntirelyAsTheDomainRole(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, domain, denials := startDomainRoleHarness(t, ctx, createReferenceDiscoveryTables)
	seedDiscoveryRoute(t, ctx, admin)

	executor := &fakeDiscoveryExecutor{summary: map[string]any{"reference_team_keys": []string{"ENG"}}}
	service, err := NewNativeReferenceDiscoveryService(domain, nil, executor)
	if err != nil {
		t.Fatal(err)
	}
	discoverErr := service.Discover(ctx, newDiscoveryArgs())
	// Denials first, always. Both services collapse a 42501 into their own
	// sentinel and abort, so a t.Fatal on the returned error would end the
	// test before the recorded SQLSTATE and statement text were ever
	// printed -- leaving "unavailable" as the only evidence, which names
	// neither the table nor the privilege.
	denials.assertNone(t, "NativeReferenceDiscoveryService success path (CHAOS-4175 family 2)")
	if discoverErr != nil {
		t.Fatalf("Discover (success path): %v", discoverErr)
	}

	// Non-vacuity for the success path: the ledger really was claimed,
	// stamped, and the sibling wakeup armed. A service that never reached
	// the database would produce no denials either.
	var status string
	if err := admin.QueryRow(ctx,
		`SELECT status FROM sync_run_reference_discoveries WHERE sync_run_id=$1`,
		discoveryTestRun).Scan(&status); err != nil {
		t.Fatalf("discovery ledger readback: %v", err)
	}
	if status != discoveryStatusSuccess {
		t.Fatalf("ledger status=%q want %q -- the success path did not complete, so this run measured little",
			status, discoveryStatusSuccess)
	}
	assertOutboxWakeup(t, ctx, admin, outboxKindDispatchSyncRun)

	// The terminal failure path, on the same venue. The ledger is cleared so
	// ensureReferenceDiscoveryLedger takes its INSERT arm a second time, and
	// a unit is planted so failNonterminalUnits has a row to fail.
	if _, err := admin.Exec(ctx, `DELETE FROM sync_run_reference_discoveries WHERE sync_run_id=$1`,
		discoveryTestRun); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at)
VALUES ('00000000-0000-4000-8000-0000000000da',$1,$2,'github','commits',
        '00000000-0000-4000-8000-0000000000db','running',now())`,
		discoveryTestOrg, discoveryTestRun); err != nil {
		t.Fatal(err)
	}

	// A permanent error: isRetryableDiscoveryError matches on transient
	// markers ("timeout", "rate", "429", ...), none of which appear here, so
	// handleFailure takes its terminal arm rather than its retry arm.
	executor.err = errors.New("provider rejected the reference populate request")
	executor.summary = nil
	failureErr := service.Discover(ctx, newDiscoveryArgs())
	denials.assertNone(t, "NativeReferenceDiscoveryService terminal failure path (CHAOS-4175 family 2)")
	if failureErr != nil {
		t.Fatalf("Discover (terminal failure path): %v", failureErr)
	}

	if err := admin.QueryRow(ctx,
		`SELECT status FROM sync_run_reference_discoveries WHERE sync_run_id=$1`,
		discoveryTestRun).Scan(&status); err != nil {
		t.Fatalf("discovery ledger readback after failure: %v", err)
	}
	if status != discoveryStatusFailed {
		t.Fatalf("ledger status=%q want %q -- the terminal failure path did not complete",
			status, discoveryStatusFailed)
	}
	assertOutboxWakeup(t, ctx, admin, outboxKindFinalizeSyncRun)

	// The feature-disabled arm, on the same venue. claim() reaches
	// terminalizeFeatureDisabledPlan (native_reference_discovery.go:257) before
	// the ledger is even created, and that path
	// (feature_disabled_termination.go:276) issues its own UPDATE against
	// sync_run_reference_discoveries. It is a THIRD site for the table this
	// ticket grants, reachable from the same domain-pool entry point and
	// invisible to both arms above -- neither of which takes the gate branch.
	//
	// A pagerduty/incidents unit maps to a canonical-incident-gated legacy
	// target, so claim() runs the entitlement gate. The flag row and a
	// DISABLING org override are both seeded deliberately: the gate short
	// circuits on ErrNoRows if the flag is absent, which would leave its SECOND
	// locking read (org_feature_overrides) unexecuted and therefore unmeasured.
	// Seeded this way the gate takes BOTH `FOR UPDATE` locks and still denies,
	// so one arm proves both privileges and still reaches the termination path.
	featureID := "00000000-0000-4000-8000-0000000000de"
	for _, statement := range []string{
		`DELETE FROM sync_run_reference_discoveries WHERE sync_run_id = '` + discoveryTestRun + `'`,
		`DELETE FROM sync_run_units WHERE sync_run_id = '` + discoveryTestRun + `'`,
		`INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at)
		 VALUES ('00000000-0000-4000-8000-0000000000dc','` + discoveryTestOrg + `','` + discoveryTestRun + `',
		         'pagerduty','incidents','00000000-0000-4000-8000-0000000000dd','planned',now())`,
		`INSERT INTO feature_flags (id,key,min_tier,is_enabled)
		 VALUES ('` + featureID + `','canonical_incident_ingestion','enterprise',true)`,
		`INSERT INTO org_feature_overrides (id,org_id,feature_id,is_enabled,expires_at)
		 VALUES ('00000000-0000-4000-8000-0000000000df','` + discoveryTestOrg + `','` + featureID + `',false,NULL)`,
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("seed %s: %v", collapseStatement(statement), err)
		}
	}
	executor.calls = 0
	executor.err = nil
	gateErr := service.Discover(ctx, newDiscoveryArgs())
	denials.assertNone(t, "NativeReferenceDiscoveryService feature-disabled path (CHAOS-4175 family 2)")
	if gateErr != nil {
		t.Fatalf("Discover (feature-disabled path): %v", gateErr)
	}
	if executor.calls != 0 {
		t.Fatalf("executor called %d times, want 0 -- a feature-disabled run must never be claimed",
			executor.calls)
	}
	var unitStatus string
	if err := admin.QueryRow(ctx, `SELECT status FROM sync_run_units WHERE id=$1`,
		"00000000-0000-4000-8000-0000000000dc").Scan(&unitStatus); err != nil {
		t.Fatalf("gated unit readback: %v", err)
	}
	if unitStatus != syncRunUnitStatusFailed {
		t.Fatalf("gated unit status=%q want %q -- the feature-disabled path did not run, so its "+
			"privileges were not measured", unitStatus, syncRunUnitStatusFailed)
	}
}

// TestNativeFinalizeSyncRunExecutesEntirelyAsTheDomainRole is CHAOS-4209
// family 1. The seeded row graph is chosen so ONE Finalize reaches every table
// the service can touch: a successful unit (compute checkpoint), a canonical
// sync configuration (the last_sync_* stamp and the coverage invalidation),
// an in-flight job_runs row, a backfill_jobs row, and the once-only
// sync_run_post_dispatches ledger.
func TestNativeFinalizeSyncRunExecutesEntirelyAsTheDomainRole(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, domain, denials := startDomainRoleHarness(t, ctx, createFinalizeTables)
	seedFinalizeRoute(t, ctx, admin)

	jobRunID := "00000000-0000-4000-8000-0000000000f8"
	for _, seed := range []struct {
		statement string
		args      []any
	}{
		{`INSERT INTO sync_runs (id,org_id,integration_id,status,total_units,completed_units,failed_units)
		  VALUES ($1,$2,$3,'dispatching',1,0,0)`, []any{finalizeTestRun, finalizeTestOrg, finalizeTestIntegration}},
		{`INSERT INTO integration_sources (id) VALUES ($1)`, []any{finalizeTestSource}},
		{`INSERT INTO sync_run_units
		    (id,org_id,sync_run_id,provider,dataset_key,source_id,status,since_at,before_at,cost_class,mode)
		  VALUES ($1,$2,$3,'github','commits',$4,'success',$5,$6,'heavy','incremental')`,
			[]any{finalizeTestUnit, finalizeTestOrg, finalizeTestRun, finalizeTestSource,
				time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 7, 2, 0, 0, 0, 0, time.UTC)}},
		{`INSERT INTO scheduled_jobs (id) VALUES ($1)`, []any{jobRunID}},
		{`INSERT INTO job_runs (id, job_id, status, result) VALUES ($1, $1, 0, $2::json)`,
			[]any{jobRunID, `{"sync_run_id":"` + finalizeTestRun + `"}`}},
		// observeTerminalSyncRun's backfill UPDATE matches on a
		// celery_task_id containing "sync_run:<id>"; a row that matches is
		// what makes the UPDATE do work rather than silently affect zero rows.
		{`INSERT INTO backfill_jobs (id,org_id,celery_task_id,status)
		  VALUES ('00000000-0000-4000-8000-0000000000fa',$1,$2,'running')`,
			[]any{finalizeTestOrg, "sync_run:" + finalizeTestRun}},
		// invalidateSyncCoverageForIntegration resolves config ids from
		// sync_configurations (seeded by seedFinalizeRoute) and then updates
		// the matching projection row.
		{`INSERT INTO sync_coverage_projections (org_id,sync_config_id) VALUES ($1,$2)`,
			[]any{finalizeTestOrg, finalizeTestSyncConfig}},
	} {
		if _, err := admin.Exec(ctx, seed.statement, seed.args...); err != nil {
			t.Fatalf("seed %s: %v", collapseStatement(seed.statement), err)
		}
	}

	service, err := NewNativeFinalizeSyncRunService(domain, nil)
	if err != nil {
		t.Fatal(err)
	}
	finalizeErr := service.Finalize(ctx, newFinalizeArgs())
	denials.assertNone(t, "NativeFinalizeSyncRunService (CHAOS-4175 family 1)")
	if finalizeErr != nil {
		t.Fatalf("Finalize: %v", finalizeErr)
	}

	// Non-vacuity, one readback per table the run is supposed to have
	// written. Each of these would still be at its seeded value if Finalize
	// had bailed before reaching that table.
	var runStatus string
	if err := admin.QueryRow(ctx, `SELECT status FROM sync_runs WHERE id=$1`, finalizeTestRun).
		Scan(&runStatus); err != nil {
		t.Fatal(err)
	}
	if runStatus != syncRunStatusSuccess {
		t.Fatalf("sync_runs.status=%q want %q -- Finalize did not complete, so this run measured little",
			runStatus, syncRunStatusSuccess)
	}
	assertSingleRow(t, ctx, admin,
		"sync_compute_checkpoints",
		`SELECT count(*) FROM sync_compute_checkpoints WHERE sync_run_id=$1 AND compute_type='work_graph'`,
		finalizeTestRun)
	assertSingleRow(t, ctx, admin,
		"sync_run_post_dispatches",
		`SELECT count(*) FROM sync_run_post_dispatches WHERE sync_run_id=$1 AND kind='post_sync'`,
		finalizeTestRun)
	assertSingleRow(t, ctx, admin,
		"job_runs terminalization",
		`SELECT count(*) FROM job_runs WHERE id=$1 AND status=$2`,
		jobRunID, jobRunStatusSuccess)
	assertSingleRow(t, ctx, admin,
		"backfill_jobs terminalization",
		`SELECT count(*) FROM backfill_jobs WHERE org_id=$1 AND status='completed'`,
		finalizeTestOrg)
	assertSingleRow(t, ctx, admin,
		"sync_configurations canonical stamp",
		`SELECT count(*) FROM sync_configurations WHERE id=$1 AND last_sync_success`,
		finalizeTestSyncConfig)
	assertSingleRow(t, ctx, admin,
		"sync_coverage_projections invalidation",
		`SELECT count(*) FROM sync_coverage_projections WHERE sync_config_id=$1 AND invalidated_at IS NOT NULL`,
		finalizeTestSyncConfig)
	assertSingleRow(t, ctx, admin,
		"post_sync outbox wakeup",
		`SELECT count(*) FROM sync_dispatch_outbox WHERE sync_run_id=$1 AND kind='post_sync'`,
		finalizeTestRun)
}

// TestDomainRoleDenialRecorderActuallyRecords is the negative control for the
// two tests above, and it is not optional.
//
// Both of them assert `denials.assertNone(...)`. That assertion passes when the
// role is clean AND when the recorder is broken -- a tracer never attached to
// the pool's connections, a tracer that misses statements issued on a pgx.Tx, a
// TraceQueryEnd that drops the error -- and those two outcomes are
// indistinguishable from the test output. Every green run above would stay
// green. So the oracle has to prove it can fail.
//
// It does that against a REVOKED grant rather than a fabricated error: one
// privilege the discovery service genuinely needs is taken away, the service is
// driven exactly as the other tests drive it, and the recorder must come back
// with the 42501 AND the offending SQL text. Both halves matter -- a recorder
// that counted denials but lost the statement would leave a failure naming no
// code to look at, which is the failure mode this whole suite exists to remove.
//
// The revoked privilege is INSERT on sync_run_reference_discoveries, reached
// through ensureReferenceDiscoveryLedger inside a pgx.Tx. The Tx path is the
// deliberate choice: pooled connections and transaction-scoped statements are
// exactly where a tracer is most likely to be silently absent.
func TestDomainRoleDenialRecorderActuallyRecords(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, domain, denials := startDomainRoleHarness(t, ctx, createReferenceDiscoveryTables)
	seedDiscoveryRoute(t, ctx, admin)

	if _, err := admin.Exec(ctx,
		"REVOKE INSERT ON TABLE public.sync_run_reference_discoveries FROM "+privilegeDomainRole); err != nil {
		t.Fatal(err)
	}

	service, err := NewNativeReferenceDiscoveryService(
		domain, nil, &fakeDiscoveryExecutor{summary: map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	// The service collapses the denial into its sentinel, which is the whole
	// reason the recorder exists. The returned error is not the assertion.
	_ = service.Discover(ctx, newDiscoveryArgs())

	denials.mutex.Lock()
	recorded := append([]privilegeDenial(nil), denials.denials...)
	denials.mutex.Unlock()

	if len(recorded) == 0 {
		t.Fatal("the recorder observed NO denial against a role whose INSERT was just revoked. " +
			"The tracer is not seeing the statements these tests claim to measure, so every " +
			"`assertNone` in this file is passing vacuously.")
	}
	var sawTable, sawStatement bool
	for _, denial := range recorded {
		if strings.Contains(denial.message, "sync_run_reference_discoveries") {
			sawTable = true
		}
		if strings.Contains(denial.statement, "sync_run_reference_discoveries") {
			sawStatement = true
		}
	}
	if !sawTable {
		t.Errorf("recorded %d denial(s) but none names sync_run_reference_discoveries: %+v",
			len(recorded), recorded)
	}
	if !sawStatement {
		t.Errorf("a denial was recorded without its statement text, so a real failure would name "+
			"no SQL to look at: %+v", recorded)
	}

	// Restoring the grant must make the same run clean again. Without this the
	// test could pass against a role that is broken for some unrelated reason.
	if _, err := admin.Exec(ctx,
		"GRANT INSERT ON TABLE public.sync_run_reference_discoveries TO "+privilegeDomainRole); err != nil {
		t.Fatal(err)
	}
	denials.reset()
	if err := service.Discover(ctx, newDiscoveryArgs()); err != nil {
		t.Fatalf("Discover after restoring the grant: %v", err)
	}
	denials.assertNone(t, "NativeReferenceDiscoveryService with the grant restored")
}

// createBoundaryTables is the finalize venue plus the discovery ledger, so one
// venue carries all four tables CHAOS-4209 widened.
//
// Both DDL sets are needed and neither is sufficient: createFinalizeTables has
// the post-dispatch ledger, job_runs and backfill_jobs but no
// sync_run_reference_discoveries, and createReferenceDiscoveryTables is the
// mirror image. A missing table would make the boundary assertion below raise
// undefined_table (42P01) during parse analysis, BEFORE the permission check --
// so the row would report "a different failure" rather than silently passing,
// but it would still be measuring nothing.
func createBoundaryTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	createFinalizeTables(t, ctx, pool)
	if _, err := pool.Exec(ctx, `
CREATE TABLE sync_run_reference_discoveries (
 id uuid PRIMARY KEY, sync_run_id uuid NOT NULL UNIQUE, org_id text NOT NULL,
 status text NOT NULL, attempts int NOT NULL DEFAULT 0, available_at timestamptz NOT NULL,
 lease_owner text NULL, lease_expires_at timestamptz NULL, last_heartbeat_at timestamptz NULL,
 completed_at timestamptz NULL, error text NULL, result json NULL,
 created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now()
)`); err != nil {
		t.Fatal(err)
	}
}

// TestDomainRoleStillLacksTheVerbsItWasNotGranted is the boundary half of this
// suite, and it is what keeps the CHAOS-4209 widening from degrading into
// "the domain role may do whatever it likes to these tables".
//
// Six tables became dual-grant or gained a verb. For each, the manifest
// deliberately withheld something, and a grant list is only a boundary if the
// withheld verb is still refused. Asserting the PERMITTED half alone would
// pass just as happily against GRANT ALL -- so every row here names a verb no
// production statement issues, and every one must still raise 42501.
//
// Every table the change touches appears, not just the ones that were new.
// An earlier revision covered only the four tables that gained their FIRST
// domain grant and omitted the two that merely gained a verb
// (sync_configurations, sync_compute_checkpoints); a later widening on either
// would then have left this test green while the boundary moved.
func TestDomainRoleStillLacksTheVerbsItWasNotGranted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	_, domain, _ := startDomainRoleHarness(t, ctx, createBoundaryTables)

	for _, withheld := range []struct {
		table     string
		verb      string
		statement string
		why       string
	}{
		{
			table:     "sync_run_reference_discoveries",
			verb:      "DELETE",
			statement: "DELETE FROM public.sync_run_reference_discoveries WHERE sync_run_id = gen_random_uuid()",
			why:       "the ledger IS the durable evidence that discovery ran; nothing removes it",
		},
		{
			table:     "sync_run_post_dispatches",
			verb:      "DELETE",
			statement: "DELETE FROM public.sync_run_post_dispatches WHERE sync_run_id = gen_random_uuid()",
			why:       "once-only dispatch evidence; a run that could delete it could re-dispatch",
		},
		{
			table:     "job_runs",
			verb:      "DELETE",
			statement: "DELETE FROM public.job_runs WHERE id = gen_random_uuid()",
			why:       "the domain worker closes job runs, it does not open or destroy them",
		},
		{
			table:     "backfill_jobs",
			verb:      "DELETE",
			statement: "DELETE FROM public.backfill_jobs WHERE id = gen_random_uuid()",
			why:       "the domain worker terminalizes backfills, it does not create or remove them",
		},
		// The two tables whose change was a verb rather than a first grant.
		// Omitting them (codex round 2) left the boundary incomplete: a future
		// widening on either would not have failed anything here, so this test
		// did not prove the least-privilege boundary it claims to cover.
		{
			table:     "sync_configurations",
			verb:      "INSERT",
			statement: "INSERT INTO public.sync_configurations (id,org_id,integration_id,sync_options,created_at) " +
				"VALUES (gen_random_uuid(),'org',gen_random_uuid(),'{}'::json,now())",
			why:       "finalize stamps last_sync_* on an EXISTING config; creating one is the scheduler's job",
		},
		{
			table:     "sync_configurations",
			verb:      "DELETE",
			statement: "DELETE FROM public.sync_configurations WHERE id = gen_random_uuid()",
			why:       "a sync configuration outlives every run that stamps it",
		},
		{
			table:     "sync_compute_checkpoints",
			verb:      "UPDATE",
			statement: "UPDATE public.sync_compute_checkpoints SET status = 'ok' WHERE id = gen_random_uuid()",
			why:       "the checkpoint is insert-once via ON CONFLICT DO NOTHING; nothing amends one in place",
		},
		{
			table:     "sync_compute_checkpoints",
			verb:      "DELETE",
			statement: "DELETE FROM public.sync_compute_checkpoints WHERE id = gen_random_uuid()",
			why:       "checkpoints are the replay/audit record of what compute input was proven",
		},
	} {
		tx, err := domain.Begin(ctx)
		if err != nil {
			t.Fatalf("%s %s: begin: %v", withheld.table, withheld.verb, err)
		}
		_, execErr := tx.Exec(ctx, withheld.statement)
		_ = tx.Rollback(ctx)

		if execErr == nil {
			t.Errorf("%s: the domain role was PERMITTED %s, a verb the manifest withholds.\n"+
				"  reason it is withheld: %s\n  statement: %s",
				withheld.table, withheld.verb, withheld.why, withheld.statement)
			continue
		}
		var pgErr *pgconn.PgError
		if !errors.As(execErr, &pgErr) || pgErr.Code != "42501" {
			// Not a pass. A different SQLSTATE means the statement never
			// reached the permission check -- an undefined table or column is
			// raised during parse analysis first -- so this row would be
			// measuring nothing, which is the failure mode this file exists
			// to remove.
			t.Errorf("%s %s: expected insufficient_privilege (42501), got a different failure: %v\n  statement: %s",
				withheld.table, withheld.verb, execErr, withheld.statement)
		}
	}
}

// TestDomainRoleCanTakeTheAdvisoryLockFinalizeNeeds probes an assumption the
// grant work rests on rather than asserting it in prose.
//
// runtimeGrantStatements runs `REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA
// public FROM PUBLIC, <domain>, <queue>`, and
// invalidateSyncCoverageForIntegration calls pg_advisory_xact_lock(
// hashtextextended(...)) once per resolved sync configuration
// (native_finalize_sync_run.go:824). Those two are only compatible because the
// functions live in pg_catalog, not public -- which is a claim about where
// PostgreSQL puts them, and claims about the server belong in a test against
// the server. If a future migration ever installed either into public, the
// REVOKE would silently disarm coverage invalidation.
func TestDomainRoleCanTakeTheAdvisoryLockFinalizeNeeds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	_, domain, denials := startDomainRoleHarness(t, ctx, createBoundaryTables)

	tx, err := domain.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// The production shape, verbatim: an xact-scoped advisory lock keyed by a
	// hashed name, not a bare pg_advisory_xact_lock(1).
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
		"sync-coverage:org:config"); err != nil {
		t.Fatalf("the domain role cannot take finalize's advisory lock, so "+
			"invalidateSyncCoverageForIntegration fails in production: %v", err)
	}
	denials.assertNone(t, "finalize's advisory-lock pair (pg_advisory_xact_lock/hashtextextended)")
}

func assertOutboxWakeup(t *testing.T, ctx context.Context, admin *pgxpool.Pool, kind string) {
	t.Helper()
	assertSingleRow(t, ctx, admin, kind+" outbox wakeup",
		`SELECT count(*) FROM sync_dispatch_outbox WHERE sync_run_id=$1 AND kind=$2`,
		discoveryTestRun, kind)
}

// assertSingleRow reports a missing write as a distinct failure from a
// privilege denial. A denial says "the role cannot"; this says "the service
// did not", which is the difference between a grant bug and a harness that
// stopped exercising the code it claims to.
func assertSingleRow(
	t *testing.T, ctx context.Context, admin *pgxpool.Pool, what, query string, args ...any,
) {
	t.Helper()
	var count int
	if err := admin.QueryRow(ctx, query, args...).Scan(&count); err != nil {
		t.Fatalf("%s readback: %v", what, err)
	}
	if count != 1 {
		t.Errorf("%s: rows=%d want 1 -- the service never reached this table, so its privileges were not measured",
			what, count)
	}
}

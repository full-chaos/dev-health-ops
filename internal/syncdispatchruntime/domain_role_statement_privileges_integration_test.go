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

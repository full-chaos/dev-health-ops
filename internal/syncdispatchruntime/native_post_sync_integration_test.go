//go:build integration

package syncdispatchruntime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type markerWriter struct {
	failKind  string
	panicKind string
}

func (writer markerWriter) write(ctx context.Context, tx pgx.Tx, kind string, plan PostSyncPlan, prerequisite string) error {
	if kind == writer.panicKind {
		panic("simulated writer panic: " + kind)
	}
	if kind == writer.failKind {
		return ErrPostSyncUnavailable
	}
	_, err := tx.Exec(ctx, `
INSERT INTO post_sync_markers (sync_run_id, kind, prerequisite)
VALUES ($1::uuid, $2, NULLIF($3, ''))
ON CONFLICT DO NOTHING`, plan.SyncRunID, kind, prerequisite)
	return err
}

type markerDaily struct{ markerWriter }

func (writer markerDaily) StartRunTx(ctx context.Context, tx pgx.Tx, plan PostSyncPlan, prerequisite string) (string, string, error) {
	err := writer.write(ctx, tx, "daily", plan, prerequisite)
	return "daily-dispatch-id", "daily", err
}

type markerRemaining struct{ markerWriter }

func (writer markerRemaining) StartRunTx(ctx context.Context, tx pgx.Tx, family string, plan PostSyncPlan, prerequisite string) (string, error) {
	err := writer.write(ctx, tx, family, plan, prerequisite)
	return family, err
}

type markerWorkGraph struct{ markerWriter }

func (writer markerWorkGraph) StartRequestTx(ctx context.Context, tx pgx.Tx, kind string, plan PostSyncPlan, prerequisite string) (string, error) {
	err := writer.write(ctx, tx, kind, plan, prerequisite)
	return kind, err
}

type markerTeam struct{ markerWriter }

func (writer markerTeam) PublishTx(ctx context.Context, tx pgx.Tx, plan PostSyncPlan) error {
	return writer.write(ctx, tx, "team_autoimport", plan, "")
}

func TestNativePostSyncFanoutIsDuplicateStableAndRollsBackWholeGeneration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createPostSyncTables(t, ctx, pool)

	const (
		orgID         = "00000000-0000-4000-8000-000000000001"
		runID         = "00000000-0000-4000-8000-000000000002"
		outboxID      = "00000000-0000-4000-8000-000000000003"
		integrationID = "00000000-0000-4000-8000-000000000004"
		repositoryID  = "00000000-0000-4000-8000-000000000005"
	)
	seedPostSync(t, ctx, pool, orgID, runID, outboxID, integrationID, repositoryID)
	service, err := NewNativePostSyncService(
		pool,
		markerDaily{},
		markerRemaining{},
		markerWorkGraph{},
		markerTeam{},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	service.now = func() time.Time { return time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC) }
	args := PostSyncArgs{TransportArgs: TransportArgs{
		Version: ContractVersionV1, OrgID: orgID, RunID: runID,
		DispatchOutbox: outboxID, RouteGeneration: 1,
	}}
	for attempt := 0; attempt < 2; attempt++ {
		if err := service.Fanout(ctx, args); err != nil {
			t.Fatalf("attempt %d: %v", attempt, err)
		}
	}
	var markers int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM post_sync_markers WHERE sync_run_id=$1`, runID).Scan(&markers); err != nil {
		t.Fatal(err)
	}
	if markers != 7 {
		t.Fatalf("markers=%d want=7", markers)
	}
	var workGraphMarkers int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM post_sync_markers
WHERE sync_run_id=$1 AND kind='workgraph.build'`, runID).Scan(&workGraphMarkers); err != nil {
		t.Fatal(err)
	}
	if workGraphMarkers != 1 {
		t.Fatal("investment post-sync did not stage its durable workgraph checkpoint")
	}
	rows, err := pool.Query(ctx, `
SELECT kind, COALESCE(prerequisite, '')
FROM post_sync_markers
WHERE sync_run_id=$1`, runID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	prerequisites := map[string]string{}
	for rows.Next() {
		var kind, prerequisite string
		if err := rows.Scan(&kind, &prerequisite); err != nil {
			t.Fatal(err)
		}
		prerequisites[kind] = prerequisite
	}
	wantPrerequisites := map[string]string{
		"complexity":             "",
		"daily":                  "complexity",
		"workgraph.build":        "daily",
		"investment.materialize": "workgraph.build",
		"membership_backfill":    "investment.materialize",
		"dora":                   "",
		"team_autoimport":        "",
	}
	for kind, want := range wantPrerequisites {
		if got := prerequisites[kind]; got != want {
			t.Fatalf("%s prerequisite=%q want=%q", kind, got, want)
		}
	}

	if _, err := pool.Exec(ctx, `DELETE FROM post_sync_markers WHERE sync_run_id=$1`, runID); err != nil {
		t.Fatal(err)
	}
	failing, err := NewNativePostSyncService(
		pool,
		markerDaily{},
		markerRemaining{},
		markerWorkGraph{markerWriter{failKind: "investment.materialize"}},
		markerTeam{},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	failing.now = service.now
	if err := failing.Fanout(ctx, args); !errors.Is(err, ErrPostSyncUnavailable) {
		t.Fatalf("failure err=%v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM post_sync_markers WHERE sync_run_id=$1`, runID).Scan(&markers); err != nil {
		t.Fatal(err)
	}
	if markers != 0 {
		t.Fatalf("failed generation leaked %d markers", markers)
	}

	args.TransportArgs.RouteGeneration = 2
	if err := service.Fanout(ctx, args); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM post_sync_markers WHERE sync_run_id=$1`, runID).Scan(&markers); err != nil {
		t.Fatal(err)
	}
	if markers != 0 {
		t.Fatalf("stale route emitted %d markers", markers)
	}
}

// recordingFanoutObserver captures every jobruntime.PostSyncFanoutOutcome Fanout reports
// (CHAOS-4263, codex adversarial-review round 2) so tests can assert exactly
// what a real gate reading dev_health_post_sync_fanout_total would see.
type recordingFanoutObserver struct {
	outcomes []jobruntime.PostSyncFanoutOutcome
}

func (observer *recordingFanoutObserver) ObservePostSyncFanout(outcome jobruntime.PostSyncFanoutOutcome) error {
	observer.outcomes = append(observer.outcomes, outcome)
	return nil
}

// TestNativePostSyncFanoutObservesPublishedOutcomeWithDispatchID pins the
// telemetry team-lead ordered after diagnosing PR #1916's proof gate staying
// red with zero visibility into whether Fanout ever published a daily
// dispatch: a daily-relevant sync must report "published" with the real
// dispatch id from daily.StartRunTx, not a synthesized placeholder.
func TestNativePostSyncFanoutObservesPublishedOutcomeWithDispatchID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createPostSyncTables(t, ctx, pool)

	const (
		orgID         = "00000000-0000-4000-8000-000000000011"
		runID         = "00000000-0000-4000-8000-000000000012"
		outboxID      = "00000000-0000-4000-8000-000000000013"
		integrationID = "00000000-0000-4000-8000-000000000014"
		repositoryID  = "00000000-0000-4000-8000-000000000015"
	)
	seedPostSync(t, ctx, pool, orgID, runID, outboxID, integrationID, repositoryID)
	service, err := NewNativePostSyncService(pool, markerDaily{}, markerRemaining{}, markerWorkGraph{}, markerTeam{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	service.now = func() time.Time { return time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC) }
	observer := &recordingFanoutObserver{}
	service.SetFanoutObserver(observer)
	args := PostSyncArgs{TransportArgs: TransportArgs{
		Version: ContractVersionV1, OrgID: orgID, RunID: runID,
		DispatchOutbox: outboxID, RouteGeneration: 1,
	}}
	if err := service.Fanout(ctx, args); err != nil {
		t.Fatal(err)
	}
	if len(observer.outcomes) != 1 || observer.outcomes[0] != jobruntime.PostSyncFanoutOutcomePublished {
		t.Fatalf("observed outcomes = %v, want [published]", observer.outcomes)
	}
}

// TestNativePostSyncFanoutObservesNoRepositoriesOutcomeWhenNotDailyRelevant
// pins the healthy skip case: a sync that only touched a capability outside
// dailyMetricsTrigger's set (git/work-items/cicd/deployments/incidents) must
// not publish a daily dispatch, and Fanout must report that decision, not
// stay silent about it.
func TestNativePostSyncFanoutObservesNoRepositoriesOutcomeWhenNotDailyRelevant(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createPostSyncTables(t, ctx, pool)

	const (
		orgID         = "00000000-0000-4000-8000-000000000021"
		runID         = "00000000-0000-4000-8000-000000000022"
		outboxID      = "00000000-0000-4000-8000-000000000023"
		integrationID = "00000000-0000-4000-8000-000000000024"
		repositoryID  = "00000000-0000-4000-8000-000000000025"
	)
	for _, statement := range []struct {
		query string
		args  []any
	}{
		{`INSERT INTO sync_dispatch_transport_routes
		    (kind,transport,generation,paused,rollback_transport)
		  VALUES ('post_sync','river',1,false,'celery')`, nil},
		{`INSERT INTO sync_dispatch_outbox
    (id,sync_run_id,org_id,kind,status,dispatched_transport,dispatched_route_generation)
		  VALUES ($1,$2,$3,'post_sync','dispatched','river',1)`, []any{outboxID, runID, orgID}},
		{`INSERT INTO sync_runs (id,org_id,integration_id) VALUES ($1,$2,$3)`,
			[]any{runID, orgID, integrationID}},
		// "blame" LegacyTargets to ["blame"] alone -- not git, prs, work-items,
		// cicd, deployments, incidents, or operational -- so successfulUnit is
		// true (plan != nil) but dailyMetricsTrigger's every input is false.
		{`INSERT INTO sync_run_units
    (id,sync_run_id,provider,dataset_key,source_id,since_at,before_at,status)
VALUES ('00000000-0000-4000-8000-000000000026',$1,'github','blame',$2,
        '2026-07-23T00:00:00Z','2026-07-23T00:00:00Z','success')`,
			[]any{runID, repositoryID}},
	} {
		if _, err := pool.Exec(ctx, statement.query, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
	service, err := NewNativePostSyncService(pool, markerDaily{}, markerRemaining{}, markerWorkGraph{}, markerTeam{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	service.now = func() time.Time { return time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC) }
	observer := &recordingFanoutObserver{}
	service.SetFanoutObserver(observer)
	args := PostSyncArgs{TransportArgs: TransportArgs{
		Version: ContractVersionV1, OrgID: orgID, RunID: runID,
		DispatchOutbox: outboxID, RouteGeneration: 1,
	}}
	if err := service.Fanout(ctx, args); err != nil {
		t.Fatal(err)
	}
	if len(observer.outcomes) != 1 || observer.outcomes[0] != jobruntime.PostSyncFanoutOutcomeNoRepositories {
		t.Fatalf("observed outcomes = %v, want [no_repositories]", observer.outcomes)
	}
	var markers int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM post_sync_markers WHERE sync_run_id=$1 AND kind='daily'`, runID).Scan(&markers); err != nil {
		t.Fatal(err)
	}
	if markers != 0 {
		t.Fatal("a non-daily-relevant sync published a daily dispatch marker")
	}
}

// TestNativePostSyncFanoutObservesErrorOutcomeWhenDailyStartRunTxFails pins
// that a genuine publish failure is distinguishable from both a healthy skip
// and a healthy publish -- the third bucket the CHAOS-4266 gate needs to tell
// apart "we tried and failed" from "we never tried".
func TestNativePostSyncFanoutObservesErrorOutcomeWhenDailyStartRunTxFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createPostSyncTables(t, ctx, pool)

	const (
		orgID         = "00000000-0000-4000-8000-000000000031"
		runID         = "00000000-0000-4000-8000-000000000032"
		outboxID      = "00000000-0000-4000-8000-000000000033"
		integrationID = "00000000-0000-4000-8000-000000000034"
		repositoryID  = "00000000-0000-4000-8000-000000000035"
	)
	seedPostSync(t, ctx, pool, orgID, runID, outboxID, integrationID, repositoryID)
	service, err := NewNativePostSyncService(
		pool,
		markerDaily{markerWriter{failKind: "daily"}},
		markerRemaining{}, markerWorkGraph{}, markerTeam{}, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	service.now = func() time.Time { return time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC) }
	observer := &recordingFanoutObserver{}
	service.SetFanoutObserver(observer)
	args := PostSyncArgs{TransportArgs: TransportArgs{
		Version: ContractVersionV1, OrgID: orgID, RunID: runID,
		DispatchOutbox: outboxID, RouteGeneration: 1,
	}}
	if err := service.Fanout(ctx, args); !errors.Is(err, ErrPostSyncUnavailable) {
		t.Fatalf("Fanout error = %v, want ErrPostSyncUnavailable", err)
	}
	if len(observer.outcomes) != 1 || observer.outcomes[0] != jobruntime.PostSyncFanoutOutcomeError {
		t.Fatalf("observed outcomes = %v, want [error]", observer.outcomes)
	}
}

// TestNativePostSyncFanoutDoesNotReportPublishedWhenALaterStageRollsBackTheWholeTransaction
// pins the codex adversarial-review finding (round 3, CHAOS-4263): the
// fanout-outcome telemetry must not be transactionally dishonest. daily.
// StartRunTx staging its write inside the still-open transaction is not the
// same as it landing -- a later stage (here: investment.materialize) failing
// rolls back the ENTIRE transaction, daily dispatch included, so this must
// report "error", never "published".
func TestNativePostSyncFanoutDoesNotReportPublishedWhenALaterStageRollsBackTheWholeTransaction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createPostSyncTables(t, ctx, pool)

	const (
		orgID         = "00000000-0000-4000-8000-000000000041"
		runID         = "00000000-0000-4000-8000-000000000042"
		outboxID      = "00000000-0000-4000-8000-000000000043"
		integrationID = "00000000-0000-4000-8000-000000000044"
		repositoryID  = "00000000-0000-4000-8000-000000000045"
	)
	// seedPostSync's "commits" dataset makes plan.Daily AND plan.Investment
	// both true, so daily.StartRunTx (a real writer here, not the failing
	// one) succeeds and stages its work before investment.materialize fails.
	seedPostSync(t, ctx, pool, orgID, runID, outboxID, integrationID, repositoryID)
	service, err := NewNativePostSyncService(
		pool,
		markerDaily{},
		markerRemaining{},
		markerWorkGraph{markerWriter{failKind: "investment.materialize"}},
		markerTeam{},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	service.now = func() time.Time { return time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC) }
	observer := &recordingFanoutObserver{}
	service.SetFanoutObserver(observer)
	args := PostSyncArgs{TransportArgs: TransportArgs{
		Version: ContractVersionV1, OrgID: orgID, RunID: runID,
		DispatchOutbox: outboxID, RouteGeneration: 1,
	}}
	if err := service.Fanout(ctx, args); !errors.Is(err, ErrPostSyncUnavailable) {
		t.Fatalf("Fanout error = %v, want ErrPostSyncUnavailable", err)
	}
	if len(observer.outcomes) != 1 || observer.outcomes[0] != jobruntime.PostSyncFanoutOutcomeError {
		t.Fatalf("observed outcomes = %v, want [error] -- daily's staged write was rolled back, so it must never report published", observer.outcomes)
	}
	var markers int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM post_sync_markers WHERE sync_run_id=$1`, runID).Scan(&markers); err != nil {
		t.Fatal(err)
	}
	if markers != 0 {
		t.Fatalf("rolled-back generation left %d markers behind", markers)
	}
}

// TestNativePostSyncFanoutDoesNotReportPublishedWhenALaterStageWriterPanics
// pins the codex adversarial-review finding (round 4, CHAOS-4263): a panic
// unwinding out of a later-stage writer (here: workGraph.build) leaves the
// named `err` return nil -- Fanout never took a normal error-return path --
// but the deferred fanout-outcome observation still runs during the unwind.
// Checking `err == nil` alone would report "published" for a transaction
// that a panic is actively rolling back. The observation must depend on an
// explicit committed flag, not on the absence of a returned error.
func TestNativePostSyncFanoutDoesNotReportPublishedWhenALaterStageWriterPanics(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createPostSyncTables(t, ctx, pool)

	const (
		orgID         = "00000000-0000-4000-8000-000000000051"
		runID         = "00000000-0000-4000-8000-000000000052"
		outboxID      = "00000000-0000-4000-8000-000000000053"
		integrationID = "00000000-0000-4000-8000-000000000054"
		repositoryID  = "00000000-0000-4000-8000-000000000055"
	)
	// seedPostSync's "commits" dataset makes plan.Daily AND plan.WorkGraph
	// both true, so daily.StartRunTx (a real writer here) succeeds and
	// stages its work before workgraph.build panics.
	seedPostSync(t, ctx, pool, orgID, runID, outboxID, integrationID, repositoryID)
	service, err := NewNativePostSyncService(
		pool,
		markerDaily{},
		markerRemaining{},
		markerWorkGraph{markerWriter{panicKind: "workgraph.build"}},
		markerTeam{},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	service.now = func() time.Time { return time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC) }
	observer := &recordingFanoutObserver{}
	service.SetFanoutObserver(observer)
	args := PostSyncArgs{TransportArgs: TransportArgs{
		Version: ContractVersionV1, OrgID: orgID, RunID: runID,
		DispatchOutbox: outboxID, RouteGeneration: 1,
	}}
	panicked := func() (recovered any) {
		defer func() { recovered = recover() }()
		_ = service.Fanout(ctx, args)
		return nil
	}()
	if panicked == nil {
		t.Fatal("Fanout did not panic -- markerWorkGraph.panicKind was not exercised")
	}
	if len(observer.outcomes) != 1 || observer.outcomes[0] != jobruntime.PostSyncFanoutOutcomeError {
		t.Fatalf("observed outcomes = %v, want [error] -- a panicking writer must never report published", observer.outcomes)
	}
	var markers int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM post_sync_markers WHERE sync_run_id=$1`, runID).Scan(&markers); err != nil {
		t.Fatal(err)
	}
	if markers != 0 {
		t.Fatalf("panicking generation left %d markers behind -- the panic's rollback should have discarded daily's staged write too", markers)
	}
}

func createPostSyncTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(ctx, `
CREATE TABLE sync_dispatch_transport_routes (
 kind text PRIMARY KEY, transport text NOT NULL, generation bigint NOT NULL,
 paused boolean NOT NULL, rollback_transport text NOT NULL
);
CREATE TABLE sync_dispatch_outbox (
 id uuid PRIMARY KEY, sync_run_id uuid NOT NULL, org_id uuid NOT NULL, kind text NOT NULL,
 status text NOT NULL, dispatched_transport text NULL, dispatched_route_generation bigint NULL
);
CREATE TABLE sync_runs (
 id uuid PRIMARY KEY, org_id uuid NOT NULL, integration_id uuid NOT NULL
);
CREATE TABLE sync_run_units (
 id uuid PRIMARY KEY, sync_run_id uuid NOT NULL, provider text NOT NULL,
 dataset_key text NOT NULL, source_id uuid NOT NULL, since_at timestamptz NULL,
 before_at timestamptz NULL, status text NOT NULL
);
CREATE TABLE sync_configurations (
 id uuid PRIMARY KEY, org_id uuid NOT NULL, integration_id uuid NOT NULL,
 parent_id uuid NULL, sync_options json NOT NULL, created_at timestamptz NOT NULL
);
CREATE TABLE post_sync_markers (
 sync_run_id uuid NOT NULL, kind text NOT NULL, prerequisite text NULL,
 PRIMARY KEY(sync_run_id, kind)
)`)
	if err != nil {
		t.Fatal(err)
	}
}

func seedPostSync(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	orgID, runID, outboxID, integrationID, repositoryID string,
) {
	t.Helper()
	statements := []struct {
		query string
		args  []any
	}{
		{`INSERT INTO sync_dispatch_transport_routes
		    (kind,transport,generation,paused,rollback_transport)
		  VALUES ('post_sync','river',1,false,'celery')`, nil},
		{`INSERT INTO sync_dispatch_outbox
    (id,sync_run_id,org_id,kind,status,dispatched_transport,dispatched_route_generation)
		  VALUES ($1,$2,$3,'post_sync','dispatched','river',1)`, []any{outboxID, runID, orgID}},
		{`INSERT INTO sync_runs (id,org_id,integration_id) VALUES ($1,$2,$3)`,
			[]any{runID, orgID, integrationID}},
		{`INSERT INTO sync_run_units
    (id,sync_run_id,provider,dataset_key,source_id,since_at,before_at,status)
VALUES ('00000000-0000-4000-8000-000000000006',$1,'github','commits',$2,
        '2026-07-23T00:00:00Z','2026-07-23T00:00:00Z','success')`,
			[]any{runID, repositoryID}},
		{`INSERT INTO sync_configurations
    (id,org_id,integration_id,parent_id,sync_options,created_at)
VALUES ('00000000-0000-4000-8000-000000000007',$1,$2,NULL,
        '{"auto_import_teams":true}'::json,'2026-07-23T00:00:00Z')`,
			[]any{orgID, integrationID}},
	}
	for _, statement := range statements {
		if _, err := pool.Exec(ctx, statement.query, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
}

// rejectingTeamWriter refuses deterministically, the way the outbox refuses an
// envelope whose kind is not permitted on the route it was published through.
// poison makes it issue failing SQL FIRST, so the enclosing transaction is
// already aborted when the rejection surfaces: swallowing without a savepoint
// would then lose the metric fanout at Commit anyway, silently.
type rejectingTeamWriter struct{ poison bool }

func (writer rejectingTeamWriter) PublishTx(ctx context.Context, tx pgx.Tx, plan PostSyncPlan) error {
	if writer.poison {
		if _, err := tx.Exec(ctx, `
INSERT INTO post_sync_markers (sync_run_id, kind)
VALUES ('this-is-not-a-uuid', 'team_autoimport')`); err == nil {
			return errors.New("the poisoning statement was expected to fail")
		}
	}
	return fmt.Errorf("%w: publish_not_permitted_for_route", joboutbox.ErrPolicyRejected)
}

// unavailableTeamWriter fails the way a transient Postgres or transport blip
// does: a verdict a later attempt can clear.
type unavailableTeamWriter struct{}

func (unavailableTeamWriter) PublishTx(context.Context, pgx.Tx, PostSyncPlan) error {
	return joboutbox.ErrUnavailable
}

// TestNativePostSyncFanoutTeamAutoimportFailurePolicy pins the guarantee the Go
// port dropped, and its boundary.
//
// src/dev_health_ops/workers/post_sync_dispatch.py:285-302 states it outright:
// "Best-effort: a dispatch failure must never break post-sync metric fan-out."
// Python dispatches team autoimport as a separate credential-resolving task
// wrapped in try/except, deliberately outside the metric fanout, because a
// work-RELATIONSHIP refresh and work-UNIT metric work have different
// lifecycles. Go published it inside the metric transaction as the last
// statement before commit, so one rejected publish discarded the complexity
// run, the daily dispatch, the workgraph build, the investment materialize, the
// membership backfill and the DORA run with it.
//
// That guarantee lived only in a try/except and a prose comment, in both
// codebases; nothing asserted it, so nothing caught its removal. This is the
// assertion -- and it pins the BOUNDARY too, which the prose does not give us:
//
//   - a deterministic rejection is swallowed, because re-running the fanout
//     would only reach the same verdict again;
//   - the same rejection AFTER the writer has aborted the transaction is also
//     swallowed, which is the half a bare swallow gets wrong;
//   - a TRANSIENT failure is propagated, because the fanout is duplicate-stable
//     and a retry re-stages all six handoffs and gives this one another chance.
//     Swallowing it would trade a recoverable blip for a permanently missing
//     refresh whose only trace is a log line.
func TestNativePostSyncFanoutTeamAutoimportFailurePolicy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createPostSyncTables(t, ctx, pool)

	const (
		orgID         = "00000000-0000-4000-8000-000000000001"
		runID         = "00000000-0000-4000-8000-000000000002"
		outboxID      = "00000000-0000-4000-8000-000000000003"
		integrationID = "00000000-0000-4000-8000-000000000004"
		repositoryID  = "00000000-0000-4000-8000-000000000005"
	)
	seedPostSync(t, ctx, pool, orgID, runID, outboxID, integrationID, repositoryID)
	args := PostSyncArgs{TransportArgs: TransportArgs{
		Version: ContractVersionV1, OrgID: orgID, RunID: runID,
		DispatchOutbox: outboxID, RouteGeneration: 1,
	}}
	now := func() time.Time { return time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC) }
	// Every handoff the metric fanout owns. Team autoimport is deliberately
	// absent: it is the one that is allowed to be lost.
	metricHandoffs := []string{
		"complexity", "daily", "dora", "investment.materialize",
		"membership_backfill", "workgraph.build",
	}

	for _, testCase := range []struct {
		name      string
		team      TeamAutoimportPostSyncWriter
		wantErr   bool
		wantKinds []string
		wantLog   bool
	}{
		{
			name:      "a deterministic rejection never breaks the metric fanout",
			team:      rejectingTeamWriter{},
			wantKinds: metricHandoffs,
			wantLog:   true,
		},
		{
			name:      "a deterministic rejection that already aborted the transaction is recovered",
			team:      rejectingTeamWriter{poison: true},
			wantKinds: metricHandoffs,
			wantLog:   true,
		},
		{
			name:      "a transient failure is propagated so the whole fanout retries",
			team:      unavailableTeamWriter{},
			wantErr:   true,
			wantKinds: []string{},
		},
		{
			name:      "every handoff commits when the writer accepts",
			team:      markerTeam{},
			wantKinds: append(append([]string{}, metricHandoffs...), "team_autoimport"),
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := pool.Exec(ctx,
				`DELETE FROM post_sync_markers WHERE sync_run_id=$1`, runID,
			); err != nil {
				t.Fatal(err)
			}
			var logged strings.Builder
			logger := slog.New(slog.NewJSONHandler(&logged, &slog.HandlerOptions{Level: slog.LevelError}))
			service, err := NewNativePostSyncService(
				pool, markerDaily{}, markerRemaining{}, markerWorkGraph{}, testCase.team, logger,
			)
			if err != nil {
				t.Fatal(err)
			}
			service.now = now

			err = service.Fanout(ctx, args)
			if testCase.wantErr && err == nil {
				t.Fatal("Fanout() = nil, want an error so River retries the whole fanout: " +
					"a transient failure must not silently drop the handoff")
			}
			if !testCase.wantErr && err != nil {
				t.Fatalf("Fanout() = %v, want nil: a deterministic team-autoimport "+
					"rejection must never break the post-sync metric fanout", err)
			}

			rows, err := pool.Query(ctx,
				`SELECT kind FROM post_sync_markers WHERE sync_run_id=$1 ORDER BY kind`, runID)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			committed := []string{}
			for rows.Next() {
				var kind string
				if err := rows.Scan(&kind); err != nil {
					t.Fatal(err)
				}
				committed = append(committed, kind)
			}
			if rows.Err() != nil {
				t.Fatal(rows.Err())
			}
			slices.Sort(committed)
			want := append([]string{}, testCase.wantKinds...)
			slices.Sort(want)
			if !slices.Equal(committed, want) {
				t.Fatalf("committed handoffs = %v, want %v", committed, want)
			}
			if testCase.wantLog && !strings.Contains(logged.String(), PostSyncTeamAutoimportFailedMessage) {
				t.Fatalf("the dropped handoff was not recorded; logs = %q", logged.String())
			}
			if testCase.wantLog && !strings.Contains(logged.String(), runID) {
				t.Fatalf("the drop record does not name the sync run; logs = %q", logged.String())
			}
		})
	}
}

// seedPostSyncWithSyncOptions mirrors seedPostSync but takes an explicit
// sync_options JSON literal instead of the hardcoded '{"auto_import_teams":true}',
// so a caller can pin the CHAOS-4323 three-flag gate for each flag combination.
func seedPostSyncWithSyncOptions(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	orgID, runID, outboxID, integrationID, repositoryID, unitID, configID, syncOptionsJSON string,
) {
	t.Helper()
	statements := []struct {
		query string
		args  []any
	}{
		// ON CONFLICT DO NOTHING: this helper is called once per sub-test
		// against the SAME pool/schema, and the route row is process-wide
		// config, not per-run data -- only the first call needs to insert it.
		{`INSERT INTO sync_dispatch_transport_routes
		    (kind,transport,generation,paused,rollback_transport)
		  VALUES ('post_sync','river',1,false,'celery')
		  ON CONFLICT (kind) DO NOTHING`, nil},
		{`INSERT INTO sync_dispatch_outbox
    (id,sync_run_id,org_id,kind,status,dispatched_transport,dispatched_route_generation)
		  VALUES ($1,$2,$3,'post_sync','dispatched','river',1)`, []any{outboxID, runID, orgID}},
		{`INSERT INTO sync_runs (id,org_id,integration_id) VALUES ($1,$2,$3)`,
			[]any{runID, orgID, integrationID}},
		{`INSERT INTO sync_run_units
    (id,sync_run_id,provider,dataset_key,source_id,since_at,before_at,status)
VALUES ($1,$2,'github','commits',$3,
        '2026-07-23T00:00:00Z','2026-07-23T00:00:00Z','success')`,
			[]any{unitID, runID, repositoryID}},
		{`INSERT INTO sync_configurations
    (id,org_id,integration_id,parent_id,sync_options,created_at)
VALUES ($1,$2,$3,NULL,
        $4::json,'2026-07-23T00:00:00Z')`,
			[]any{configID, orgID, integrationID, syncOptionsJSON}},
	}
	for _, statement := range statements {
		if _, err := pool.Exec(ctx, statement.query, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
}

// TestNativePostSyncFanoutTeamAutoimportGateIsAnyOfThreeCategories pins the
// CHAOS-4323 split: auto_import_teams became three independent flags
// (auto_import_teams/auto_import_projects/auto_import_members), and the
// native_post_sync.go dispatch gate (loadPostSyncPlan's SQL predicate) must
// fire on ANY of the three, not just the literal auto_import_teams key --
// this is the Go side of the Python/Go parity pair (the Python predicate is
// pinned by test_sync_units.py::
// test_build_post_sync_dispatch_payload_auto_import_teams_is_any_of_three).
// Selecting ONLY projects or ONLY members, with auto_import_teams itself
// false, must still stage the team_autoimport post-sync marker.
func TestNativePostSyncFanoutTeamAutoimportGateIsAnyOfThreeCategories(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createPostSyncTables(t, ctx, pool)

	testCases := []struct {
		name            string
		syncOptionsJSON string
		wantMarker      bool
	}{
		{"all_false", `{"auto_import_teams":false,"auto_import_projects":false,"auto_import_members":false}`, false},
		{"absent", `{}`, false},
		{"teams_only", `{"auto_import_teams":true}`, true},
		{"projects_only_teams_explicitly_false", `{"auto_import_teams":false,"auto_import_projects":true}`, true},
		{"members_only_teams_explicitly_false", `{"auto_import_teams":false,"auto_import_members":true}`, true},
	}

	for index, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			orgID := fmt.Sprintf("00000000-0000-4000-9000-%012d", index*10+1)
			runID := fmt.Sprintf("00000000-0000-4000-9000-%012d", index*10+2)
			outboxID := fmt.Sprintf("00000000-0000-4000-9000-%012d", index*10+3)
			integrationID := fmt.Sprintf("00000000-0000-4000-9000-%012d", index*10+4)
			repositoryID := fmt.Sprintf("00000000-0000-4000-9000-%012d", index*10+5)
			unitID := fmt.Sprintf("00000000-0000-4000-9000-%012d", index*10+6)
			configID := fmt.Sprintf("00000000-0000-4000-9000-%012d", index*10+7)
			seedPostSyncWithSyncOptions(
				t, ctx, pool, orgID, runID, outboxID, integrationID, repositoryID,
				unitID, configID, testCase.syncOptionsJSON,
			)
			service, err := NewNativePostSyncService(
				pool,
				markerDaily{},
				markerRemaining{},
				markerWorkGraph{},
				markerTeam{},
				nil,
			)
			if err != nil {
				t.Fatal(err)
			}
			service.now = func() time.Time { return time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC) }
			args := PostSyncArgs{TransportArgs: TransportArgs{
				Version: ContractVersionV1, OrgID: orgID, RunID: runID,
				DispatchOutbox: outboxID, RouteGeneration: 1,
			}}
			if err := service.Fanout(ctx, args); err != nil {
				t.Fatal(err)
			}
			var markers int
			if err := pool.QueryRow(ctx, `
SELECT count(*) FROM post_sync_markers
WHERE sync_run_id=$1 AND kind='team_autoimport'`, runID).Scan(&markers); err != nil {
				t.Fatal(err)
			}
			gotMarker := markers > 0
			if gotMarker != testCase.wantMarker {
				t.Fatalf("sync_options=%s: team_autoimport marker written=%v want=%v",
					testCase.syncOptionsJSON, gotMarker, testCase.wantMarker)
			}
		})
	}
}

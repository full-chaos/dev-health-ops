//go:build integration

package syncdispatchruntime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type markerWriter struct {
	failKind string
}

func (writer markerWriter) write(ctx context.Context, tx pgx.Tx, kind string, plan PostSyncPlan) error {
	if kind == writer.failKind {
		return ErrPostSyncUnavailable
	}
	_, err := tx.Exec(ctx, `
INSERT INTO post_sync_markers (sync_run_id, kind)
VALUES ($1::uuid, $2)
ON CONFLICT DO NOTHING`, plan.SyncRunID, kind)
	return err
}

type markerDaily struct{ markerWriter }

func (writer markerDaily) StartRunTx(ctx context.Context, tx pgx.Tx, plan PostSyncPlan) error {
	return writer.write(ctx, tx, "daily", plan)
}

type markerRemaining struct{ markerWriter }

func (writer markerRemaining) StartRunTx(ctx context.Context, tx pgx.Tx, family string, plan PostSyncPlan) error {
	return writer.write(ctx, tx, family, plan)
}

type markerWorkGraph struct{ markerWriter }

func (writer markerWorkGraph) StartRequestTx(ctx context.Context, tx pgx.Tx, kind string, plan PostSyncPlan) error {
	return writer.write(ctx, tx, kind, plan)
}

type markerTeam struct{ markerWriter }

func (writer markerTeam) PublishTx(ctx context.Context, tx pgx.Tx, plan PostSyncPlan) error {
	return writer.write(ctx, tx, "team_autoimport", plan)
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
	if markers != 5 {
		t.Fatalf("markers=%d want=5", markers)
	}
	var workGraphMarkers int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM post_sync_markers
WHERE sync_run_id=$1 AND kind='workgraph.build'`, runID).Scan(&workGraphMarkers); err != nil {
		t.Fatal(err)
	}
	if workGraphMarkers != 0 {
		t.Fatal("investment post-sync emitted a racing standalone workgraph request")
	}

	if _, err := pool.Exec(ctx, `DELETE FROM post_sync_markers WHERE sync_run_id=$1`, runID); err != nil {
		t.Fatal(err)
	}
	failing, err := NewNativePostSyncService(
		pool,
		markerDaily{},
		markerRemaining{},
		markerWorkGraph{markerWriter{failKind: "investment.dispatch"}},
		markerTeam{},
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
 parent_id uuid NULL, sync_options jsonb NOT NULL, created_at timestamptz NOT NULL
);
CREATE TABLE post_sync_markers (
 sync_run_id uuid NOT NULL, kind text NOT NULL, PRIMARY KEY(sync_run_id, kind)
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
        '{"auto_import_teams":true}'::jsonb,'2026-07-23T00:00:00Z')`,
			[]any{orgID, integrationID}},
	}
	for _, statement := range statements {
		if _, err := pool.Exec(ctx, statement.query, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
}

//go:build integration

package sync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CHAOS-13827 (Codex r1 P1-2), on the real schema and the real Materialize: the domain graph of an occurrence commits,
// the attempt then fails before the coordinator records readiness, and the budget history of the source changes before
// the replay. The replay keeps the committed cicd window end and succeeds; it is not quarantined as ErrInvalidPlan.
func TestNativeMaterializerTestOpsReplayKeepsTheCommittedWindowAfterTheHistoryMoves(t *testing.T) {
	const (
		orgID         = "00000000-0000-4000-8000-0000000000aa"
		configID      = "00000000-0000-4000-8000-000000002001"
		integrationID = "00000000-0000-4000-8000-000000002002"
		sourceID      = "00000000-0000-4000-8000-000000002003"
		historyRunID  = "00000000-0000-4000-8000-000000002009"
	)
	scheduledFor := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	watermark := scheduledFor.Add(-72 * time.Hour)
	for _, test := range []struct {
		name          string
		before, after int
		wantSpan      time.Duration
	}{
		{"a first block appears after the commit", 0, 1, 72 * time.Hour},
		{"a second block appears after the commit", 1, 2, 36 * time.Hour},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			pool := startTestOpsReplayPostgres(t)
			seedPlannerGraph(ctx, t, pool, plannerGraph{
				orgID: orgID, integrationID: integrationID, provider: "github", configID: configID,
				jobID: "00000000-0000-4000-8000-000000002005", featureID: "00000000-0000-4000-8000-000000002007",
				datasetID: "00000000-0000-4000-8000-000000002004", datasetKey: "cicd",
				targets: `["cicd","tests"]`,
			})
			mustExec(t, pool, `INSERT INTO integration_datasets (id, org_id, integration_id, dataset_key, is_enabled, options)
VALUES ('00000000-0000-4000-8000-000000002006'::uuid, $1, $2::uuid, 'tests', TRUE, '{}'::json)`, orgID, integrationID)
			mustExec(t, pool, `INSERT INTO integration_sources (id,org_id,integration_id,provider,source_type,external_id,name,full_name,is_enabled,metadata,discovered_at,last_seen_at)
VALUES ($1::uuid,$2,$3::uuid,'github','repository','acme/api','api','acme/api',TRUE,'{}'::json,now(),now())`, sourceID, orgID, integrationID)
			for _, dataset := range []string{"cicd", "tests"} {
				mustExec(t, pool, `INSERT INTO sync_watermarks (id,org_id,source_id,dataset_key,repo_id,target,last_synced_at)
VALUES (gen_random_uuid(),$1,'acme/api',$2,'acme/api',$2,$3)`, orgID, dataset, watermark)
			}
			mustExec(t, pool, `INSERT INTO sync_runs (id,org_id,integration_id,triggered_by,mode,status,total_units,completed_units,failed_units,created_at)
VALUES ($1::uuid,$2,$3::uuid,'schedule','incremental','completed',0,0,0,now())`, historyRunID, orgID, integrationID)
			addBlocked := func(n int) {
				for i := 0; i < n; i++ {
					mustExec(t, pool, `INSERT INTO sync_run_units (id,org_id,sync_run_id,integration_id,source_id,provider,dataset_key,cost_class,mode,status,attempts,result,created_at,updated_at)
VALUES (gen_random_uuid(),$1,$2::uuid,$3::uuid,$4::uuid,'github','cicd','medium','incremental','success',1,
'{"incomplete":[{"component":"run_inventory","cause":"page_budget_exhausted","count":1}]}'::json,now(),now() - make_interval(mins => $5))`,
						orgID, historyRunID, integrationID, sourceID, 10+i)
				}
			}
			addBlocked(test.before)
			occurrence := PendingOccurrence{
				ID: "occurrence:v1:testops-replay", IdentityVersion: OccurrenceIdentityVersion,
				OrgID: orgID, ConfigID: configID, JobID: "00000000-0000-4000-8000-000000002005",
				ScheduledFor: scheduledFor, ConfigActive: true, ConfigPlannerManaged: true, JobType: "sync",
			}
			materializer, err := NewNativeMaterializer(pool)
			if err != nil {
				t.Fatal(err)
			}
			crash := errors.New("injected crash after domain commit")
			materializer.afterDomainCommit = func() error { return crash }
			fixture := materializerFixture{pool: pool, occurrence: occurrence}
			if _, err := materializeAndCommit(t, fixture, materializer, occurrence); !errors.Is(err, crash) {
				t.Fatalf("first attempt err=%v, want the injected crash", err)
			}
			committed := committedCicdSpan(t, pool)
			if committed != test.wantSpan {
				t.Fatalf("committed cicd span = %s, want %s", committed, test.wantSpan)
			}
			addBlocked(test.after - test.before)
			materializer.afterDomainCommit = nil
			if _, err := materializeAndCommit(t, fixture, materializer, occurrence); err != nil {
				t.Fatalf("replay after the history moved: %v (ErrInvalidPlan = quarantined)", err)
			}
			if got := committedCicdSpan(t, pool); got != committed {
				t.Fatalf("cicd span after the replay = %s, want the committed %s", got, committed)
			}
		})
	}
}

func startTestOpsReplayPostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	pgschema.Apply(ctx, t, pool)
	return pool
}

func mustExec(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), sql, args...); err != nil {
		t.Fatal(err)
	}
}

func committedCicdSpan(t *testing.T, pool *pgxpool.Pool) time.Duration {
	t.Helper()
	var since, before time.Time
	if err := pool.QueryRow(context.Background(), `SELECT since_at, before_at FROM sync_run_units
WHERE dataset_key='cicd' AND status='planned'`).Scan(&since, &before); err != nil {
		t.Fatal(err)
	}
	return before.Sub(since)
}

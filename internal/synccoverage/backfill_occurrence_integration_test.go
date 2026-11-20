//go:build integration

package synccoverage

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	schedulersync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
	"github.com/full-chaos/dev-health-ops/internal/synchandoff"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

func startBackfillCoveragePostgres(t *testing.T) (context.Context, *pgxpool.Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeContext)
	})
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	pgschema.Apply(ctx, t, pool)
	return ctx, pool
}

// TestCoverageResolvesTheRunTheSchedulerPlansForAnAdminBackfill drives the production creator path: the admin
// route's hand-off (synchandoff.Mint, as submitBackfill calls it) writes the occurrence and its manual trigger, the
// occurrence reconciler plans it with the native materializer, and the coverage resolver must name THAT run for both
// marker shapes the admin route writes, and the build must read it as planned.
func TestCoverageResolvesTheRunTheSchedulerPlansForAnAdminBackfill(t *testing.T) {
	ctx, pool := startBackfillCoveragePostgres(t)
	const (
		orgID         = "00000000-0000-4000-8000-0000000014aa"
		configID      = "00000000-0000-4000-8000-000000001401"
		integrationID = "00000000-0000-4000-8000-000000001402"
		sourceID      = "00000000-0000-4000-8000-000000001403"
	)
	pgseed.Org(ctx, t, pool, orgID, "community")
	pgseed.Integration(ctx, t, pool, integrationID, orgID, "jira")
	for _, statement := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO sync_configurations (id,org_id,name,sync_targets,sync_options,integration_id,is_active,source_id,planner_managed,provider,created_at,updated_at) VALUES ($1::uuid,$2,'jira-config','[]'::json,'{}'::json,$3::uuid,TRUE,NULL,TRUE,'jira',now(),now())`, []any{configID, orgID, integrationID}},
		{`INSERT INTO integration_sources (id,org_id,integration_id,provider,source_type,external_id,name,full_name,is_enabled,metadata,discovered_at,last_seen_at) VALUES ($1::uuid,$2,$3::uuid,'jira','project','PROJ','PROJ','PROJ',TRUE,json_build_object('planner_managed_sync_config_id',$4::text),now(),now())`, []any{sourceID, orgID, integrationID, configID}},
		{`INSERT INTO integration_datasets (id,org_id,integration_id,dataset_key,is_enabled,options) VALUES (gen_random_uuid(),$1,$2::uuid,'work-items',TRUE,'{}'::json)`, []any{orgID, integrationID}},
	} {
		if _, err := pool.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
	since := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	before := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	config, err := synchandoff.LoadConfig(ctx, tx, configID)
	if err != nil || config == nil {
		t.Fatalf("load the configuration: %v (config %v)", err, config)
	}
	trigger, err := synchandoff.Mint(ctx, tx, config, synchandoff.MintInput{
		Mode: "backfill", Since: &since, Before: &before, TriggeredBy: "backfill", Caller: "coverage_test",
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	materializer, err := schedulersync.NewNativeMaterializer(pool)
	if err != nil {
		t.Fatal(err)
	}
	reconciler, err := schedulersync.NewOccurrenceReconciler(pool, materializer)
	if err != nil {
		t.Fatal(err)
	}
	result, err := reconciler.Reconcile(ctx, now, 10)
	if err != nil {
		t.Fatal(err)
	}
	if result.Completed != 1 {
		t.Fatalf("reconcile = %+v, want the backfill occurrence planned", result)
	}
	var planned uuid.UUID
	var units int
	if err := pool.QueryRow(ctx, `
SELECT occurrence.sync_run_id, (SELECT count(*) FROM sync_run_units AS unit WHERE unit.sync_run_id = occurrence.sync_run_id)
FROM scheduled_sync_occurrences AS occurrence
WHERE occurrence.occurrence_id = $1 AND occurrence.org_id = $2`, trigger.OccurrenceID, orgID).Scan(&planned, &units); err != nil {
		t.Fatal(err)
	}
	if units == 0 {
		t.Fatal("the planned backfill run has no units; the cell needs a run with units")
	}

	markers := []string{
		"occurrence:" + trigger.OccurrenceID,
		"idem:" + strings.Repeat("0", 32) + ";occurrence:" + trigger.OccurrenceID,
	}
	for _, task := range markers {
		runID, marker := backfillRunID(&task)
		if marker != backfillMarkerOccurrence || runID != planned {
			t.Fatalf("backfillRunID(%q) = (%s, %q), want the run the reconciler planned (%s)", task, runID, marker, planned)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO backfill_jobs (id,org_id,sync_config_id,celery_task_id,status,since_date,before_date) VALUES ($1,$2,$3::uuid,$4,'pending',$5::date,$6::date)`,
			uuid.New(), orgID, configID, task, since, before.Add(-time.Microsecond)); err != nil {
			t.Fatal(err)
		}
	}

	occurrenceBefore := backfillResolutionMetrics.outcomeCount(backfillOutcomeOccurrence)
	unplannedBefore := backfillResolutionMetrics.outcomeCount(backfillOutcomeOccurrenceUnplanned)
	projector, err := NewProjector(pool, WithClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := projector.Rebuild(ctx, orgID, uuid.MustParse(configID)); err != nil {
		t.Fatal(err)
	}
	if got := backfillResolutionMetrics.outcomeCount(backfillOutcomeOccurrence) - occurrenceBefore; got != uint64(len(markers)) {
		t.Fatalf("occurrence outcomes counted = %d, want %d (both markers read as the planned run)", got, len(markers))
	}
	if got := backfillResolutionMetrics.outcomeCount(backfillOutcomeOccurrenceUnplanned) - unplannedBefore; got != 0 {
		t.Fatalf("occurrence_unplanned outcomes counted = %d, want 0", got)
	}
}

// adminBackfillWindows are the payload's backfill_windows inside the admin job's dates, as "source|dataset" keys.
func adminBackfillWindows(t *testing.T, raw json.RawMessage, end time.Time) []string {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	keys := []string{}
	for _, rawWindow := range payload["backfill_windows"].([]any) {
		window := rawWindow.(map[string]any)
		since, err := time.Parse(time.RFC3339Nano, window["since"].(string))
		if err != nil {
			t.Fatal(err)
		}
		if !since.Before(end) {
			continue
		}
		keys = append(keys, window["source_ids"].([]any)[0].(string)+"|"+window["dataset_keys"].([]any)[0].(string))
	}
	return keys
}

// TestProjectorReadsAnAdminBackfillThroughItsRun pins the served change: an admin backfill job (occurrence marker)
// requests only what its run's units request, never its whole dates over every scope pair, and a job whose
// occurrence has no planned run requests nothing.
func TestProjectorReadsAnAdminBackfillThroughItsRun(t *testing.T) {
	ctx, pool := startBackfillCoveragePostgres(t)
	now := time.Date(2026, time.August, 12, 8, 0, 0, 0, time.UTC)
	projector, err := NewProjector(pool, WithClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	jobSince := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	jobLastDate := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	windowEnd := time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC)
	const occurrence = "sha256:feedfacefeedfacefeedfacefeedfacefeedfacefeedfacefeedfacefeedface"
	derived, err := schedulersync.DeterministicSyncRunID(occurrence)
	if err != nil {
		t.Fatal(err)
	}
	runID := uuid.MustParse(derived)

	type seeded struct {
		fixture      projectorFixture
		secondSource uuid.UUID
	}
	seed := func(t *testing.T, orgID string, withRun bool, unitStatus string) seeded {
		t.Helper()
		resetProjectorTables(t, ctx, pool)
		fixture := seedProjectorFixture(t, ctx, pool, orgID, now)
		second := uuid.New()
		if _, err := pool.Exec(ctx, `INSERT INTO integration_sources (id,org_id,integration_id,provider,source_type,external_id,name,full_name,metadata,is_enabled,discovered_at,last_seen_at) VALUES ($1,$2,$3,'github','repository','acme/web','web','acme/web','{}'::json,TRUE,now(),now())`,
			second, orgID, fixture.IntegrationID); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO backfill_jobs (id,org_id,sync_config_id,celery_task_id,status,since_date,before_date) VALUES ($1,$2,$3,$4,'pending',$5::date,$6::date)`,
			uuid.New(), orgID, fixture.ConfigID, "idem:"+strings.Repeat("b", 32)+";occurrence:"+occurrence, jobSince, jobLastDate); err != nil {
			t.Fatal(err)
		}
		if withRun {
			if _, err := pool.Exec(ctx, `INSERT INTO sync_runs (id,org_id,integration_id,triggered_by,mode,status,total_units,completed_units,failed_units,created_at) VALUES ($1,$2,$3,'backfill','backfill','dispatching',1,0,0,$4)`,
				runID, orgID, fixture.IntegrationID, now.Add(-48*time.Hour)); err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(ctx, `INSERT INTO sync_run_units (id,org_id,sync_run_id,integration_id,source_id,provider,dataset_key,processor_flags,since_at,before_at,status,created_at,updated_at,cost_class,mode,attempts) VALUES ($1,$2,$3,$4,$5,'github','commits','{}',$6,$7,$8,$9,$9,'rest_core','backfill',0)`,
				uuid.New(), orgID, runID, fixture.IntegrationID, fixture.SourceID, jobSince, windowEnd, unitStatus, now.Add(-time.Hour)); err != nil {
				t.Fatal(err)
			}
		}
		return seeded{fixture: fixture, secondSource: second}
	}

	t.Run("a planned run with a success unit requests nothing else", func(t *testing.T) {
		state := seed(t, "org-admin-success", true, "success")
		raw, err := projector.Rebuild(ctx, state.fixture.OrgID, state.fixture.ConfigID)
		if err != nil {
			t.Fatal(err)
		}
		if windows := adminBackfillWindows(t, raw, windowEnd); len(windows) != 0 {
			t.Fatalf("gap windows inside the admin job's dates = %v, want none (the run's only pair is covered)", windows)
		}
	})

	t.Run("an open unit is requested on its own pair and not covered", func(t *testing.T) {
		state := seed(t, "org-admin-open", true, "planned")
		raw, err := projector.Rebuild(ctx, state.fixture.OrgID, state.fixture.ConfigID)
		if err != nil {
			t.Fatal(err)
		}
		want := state.fixture.SourceID.String() + "|commits"
		if windows := adminBackfillWindows(t, raw, windowEnd); len(windows) != 1 || windows[0] != want {
			t.Fatalf("gap windows inside the admin job's dates = %v, want only %q", windows, want)
		}
	})

	t.Run("an occurrence with no planned run requests nothing and is counted", func(t *testing.T) {
		state := seed(t, "org-admin-unplanned", false, "")
		before := backfillResolutionMetrics.outcomeCount(backfillOutcomeOccurrenceUnplanned)
		raw, err := projector.Rebuild(ctx, state.fixture.OrgID, state.fixture.ConfigID)
		if err != nil {
			t.Fatal(err)
		}
		if windows := adminBackfillWindows(t, raw, windowEnd); len(windows) != 0 {
			t.Fatalf("gap windows inside the admin job's dates = %v, want none", windows)
		}
		if got := backfillResolutionMetrics.outcomeCount(backfillOutcomeOccurrenceUnplanned) - before; got != 1 {
			t.Fatalf("occurrence_unplanned counted = %d, want 1", got)
		}
	})

	t.Run("another org's unit under the derived run id is not read", func(t *testing.T) {
		state := seed(t, "org-admin-decoy", true, "success")
		if _, err := pool.Exec(ctx, `INSERT INTO sync_run_units (id,org_id,sync_run_id,integration_id,source_id,provider,dataset_key,processor_flags,since_at,before_at,status,created_at,updated_at,cost_class,mode,attempts) VALUES ($1,'org-other',$2,$3,$4,'github','commits','{}',$5,$6,'planned',$7,$7,'rest_core','backfill',0)`,
			uuid.New(), runID, state.fixture.IntegrationID, state.secondSource, jobSince, windowEnd, now.Add(-time.Hour)); err != nil {
			t.Fatal(err)
		}
		raw, err := projector.Rebuild(ctx, state.fixture.OrgID, state.fixture.ConfigID)
		if err != nil {
			t.Fatal(err)
		}
		if windows := adminBackfillWindows(t, raw, windowEnd); len(windows) != 0 {
			t.Fatalf("gap windows inside the admin job's dates = %v, want none (the other org's unit must not be read)", windows)
		}
	})
}

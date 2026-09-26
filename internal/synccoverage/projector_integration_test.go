//go:build integration

package synccoverage

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type projectorFixture struct {
	OrgID         string
	ConfigID      uuid.UUID
	IntegrationID uuid.UUID
	SourceID      uuid.UUID
	RunID         uuid.UUID
	UnitID        uuid.UUID
}

func TestProjectorPostgresLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeContext); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	}()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	pgschema.Apply(ctx, t, pool)
	now := time.Date(2026, time.August, 12, 8, 0, 0, 0, time.UTC)
	projector, err := NewProjector(pool, WithClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}

	t.Run("cold build and idempotent rerun", func(t *testing.T) {
		resetProjectorTables(t, ctx, pool)
		fixture := seedProjectorFixture(t, ctx, pool, "org-cold", now)
		first, err := projector.Rebuild(ctx, fixture.OrgID, fixture.ConfigID)
		if err != nil {
			t.Fatal(err)
		}
		assertHealthyProjection(t, first, fixture, now)
		second, err := projector.Rebuild(ctx, fixture.OrgID, fixture.ConfigID)
		if err != nil {
			t.Fatal(err)
		}
		assertHealthyProjection(t, second, fixture, now)
		var count int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM sync_coverage_projections WHERE org_id=$1 AND sync_config_id=$2`, fixture.OrgID, fixture.ConfigID).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("projection rows = %d, want one", count)
		}
	})

	// A family unit whose processor_flags is a truthy non-object makes
	// Python's _effective_dataset_keys raise (processor_flags.get), so the
	// rebuild fails for that config instead of treating the unit as its
	// raw composite key.
	t.Run("list-shaped family flags fail the rebuild", func(t *testing.T) {
		resetProjectorTables(t, ctx, pool)
		fixture := seedProjectorFixture(t, ctx, pool, "org-list-flags", now)
		if _, err := pool.Exec(ctx, `UPDATE sync_configurations SET sync_targets='["git","work-items"]' WHERE id=$1`, fixture.ConfigID); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO sync_run_units (id,org_id,sync_run_id,integration_id,source_id,provider,dataset_key,processor_flags,since_at,before_at,status,created_at,updated_at,cost_class,mode,attempts)
VALUES ($1,$2,$3,$4,$5,'github','work-items','["x"]',$6,$7,'success',$8,$8,'rest_core','incremental',0)`,
			uuid.New(), fixture.OrgID, fixture.RunID, fixture.IntegrationID, fixture.SourceID, now.Add(-24*time.Hour), now.Add(-time.Hour), now.Add(-time.Hour)); err != nil {
			t.Fatal(err)
		}
		if _, err := projector.Rebuild(ctx, fixture.OrgID, fixture.ConfigID); err == nil || !strings.Contains(err.Error(), "not a dict") {
			t.Fatalf("rebuild over list-shaped family flags: err = %v, want the processor_flags error", err)
		}
		if _, err := pool.Exec(ctx, `UPDATE sync_run_units SET processor_flags='[]' WHERE dataset_key='work-items'`); err != nil {
			t.Fatal(err)
		}
		if _, err := projector.Rebuild(ctx, fixture.OrgID, fixture.ConfigID); err != nil {
			t.Fatalf("rebuild over empty-list (falsy) family flags: %v", err)
		}
	})

	// Each of the projector's three expansions fails the rebuild alone: a
	// planned unit with no window reaches only the active-pairs read, and a
	// backfill-linked run on another integration reaches only the linked
	// backfill units read.
	t.Run("list-shaped flags fail the rebuild from each read", func(t *testing.T) {
		for _, seed := range []struct {
			name string
			sql  func(fixture projectorFixture) (string, []any)
		}{
			{"active pair", func(fixture projectorFixture) (string, []any) {
				return `WITH run AS (
  INSERT INTO sync_runs (id,org_id,integration_id,triggered_by,mode,status,total_units,completed_units,failed_units,created_at) VALUES ($3,$2,$4,'test','incremental','running',0,0,0,$6) RETURNING id
)
INSERT INTO sync_run_units (id,org_id,sync_run_id,integration_id,source_id,provider,dataset_key,processor_flags,status,created_at,updated_at,cost_class,mode,attempts)
SELECT $1,$2,run.id,$4,$5,'github','work-items','["x"]','planned',$6,$6,'rest_core','incremental',0 FROM run`,
					[]any{uuid.New(), fixture.OrgID, uuid.New(), fixture.IntegrationID, fixture.SourceID, now}
			}},
			{"linked backfill unit", func(fixture projectorFixture) (string, []any) {
				otherRun := uuid.New()
				otherIntegration := uuid.New()
				pgseed.Integration(ctx, t, pool, otherIntegration.String(), fixture.OrgID, "github")
				return `WITH run AS (
  INSERT INTO sync_runs (id,org_id,integration_id,triggered_by,mode,status,total_units,completed_units,failed_units,created_at) VALUES ($1,$2,$3,'test','incremental','success',0,0,0,$6) RETURNING id
), job AS (
  INSERT INTO backfill_jobs (id,org_id,sync_config_id,celery_task_id,since_date,before_date) VALUES ($7,$2,$8,'sync_run:' || $1::text,$9::date,$10::date) RETURNING id
)
INSERT INTO sync_run_units (id,org_id,sync_run_id,integration_id,source_id,provider,dataset_key,processor_flags,since_at,before_at,status,created_at,updated_at,cost_class,mode,attempts)
VALUES ($4,$2,$1,$3,$5,'github','work-items','["x"]',$9,$10,'success',$6,$6,'rest_core','incremental',0)`,
					[]any{otherRun, fixture.OrgID, otherIntegration, uuid.New(), fixture.SourceID, now, uuid.New(), fixture.ConfigID,
						now.Add(-48 * time.Hour), now.Add(-24 * time.Hour)}
			}},
		} {
			resetProjectorTables(t, ctx, pool)
			fixture := seedProjectorFixture(t, ctx, pool, "org-list-flags-"+strings.ReplaceAll(seed.name, " ", "-"), now)
			if _, err := pool.Exec(ctx, `UPDATE sync_configurations SET sync_targets='["git","work-items"]' WHERE id=$1`, fixture.ConfigID); err != nil {
				t.Fatal(err)
			}
			statement, args := seed.sql(fixture)
			if _, err := pool.Exec(ctx, statement, args...); err != nil {
				t.Fatalf("%s: %v", seed.name, err)
			}
			if _, err := projector.Rebuild(ctx, fixture.OrgID, fixture.ConfigID); err == nil || !strings.Contains(err.Error(), "not a dict") {
				t.Fatalf("%s: rebuild err = %v, want the processor_flags error", seed.name, err)
			}
		}
	})

	t.Run("invalidated refresh clears marker", func(t *testing.T) {
		resetProjectorTables(t, ctx, pool)
		fixture := seedProjectorFixture(t, ctx, pool, "org-invalidated", now)
		if _, err := projector.Rebuild(ctx, fixture.OrgID, fixture.ConfigID); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `UPDATE sync_coverage_projections SET invalidated_at=$1 WHERE org_id=$2`, now.Add(time.Minute), fixture.OrgID); err != nil {
			t.Fatal(err)
		}
		result, err := projector.RefreshDue(ctx, 1)
		if err != nil {
			t.Fatal(err)
		}
		if result.Refreshed != 1 || result.Failed != 0 {
			t.Fatalf("refresh result = %+v", result)
		}
		var invalidatedAt *time.Time
		if err := pool.QueryRow(ctx, `SELECT invalidated_at FROM sync_coverage_projections WHERE org_id=$1`, fixture.OrgID).Scan(&invalidatedAt); err != nil {
			t.Fatal(err)
		}
		if invalidatedAt != nil {
			t.Fatalf("invalidated_at = %v, want NULL", invalidatedAt)
		}
	})

	t.Run("user-disabled datasets are not advertised as backfills", func(t *testing.T) {
		resetProjectorTables(t, ctx, pool)
		fixture := seedDisabledDatasetFixture(t, ctx, pool, "org-intent", now)
		raw, err := projector.Rebuild(ctx, fixture.OrgID, fixture.ConfigID)
		if err != nil {
			t.Fatal(err)
		}
		var payload map[string]any
		if err := json.Unmarshal(raw, &payload); err != nil {
			t.Fatal(err)
		}

		// The whole point: no advertised window may name a disabled dataset.
		for _, rawWindow := range payload["backfill_windows"].([]any) {
			window := rawWindow.(map[string]any)
			for _, rawKey := range window["dataset_keys"].([]any) {
				if strings.HasPrefix(rawKey.(string), "work-item") {
					t.Fatalf("backfill window advertises user-disabled dataset %q: %#v", rawKey, window)
				}
			}
		}

		// And the disabled datasets must still be visible as not_enabled --
		// silently vanishing would be its own lie -- while the enabled,
		// selected dataset keeps its real coverage.
		statuses := make(map[string]string)
		gapCounts := make(map[string]int)
		for _, rawDataset := range payload["datasets"].([]any) {
			dataset := rawDataset.(map[string]any)
			key := dataset["dataset_key"].(string)
			statuses[key] = dataset["status"].(string)
			gapCounts[key] = len(dataset["gaps"].([]any)) + len(dataset["failed_ranges"].([]any))
		}
		for _, key := range []string{"work-items", "work-item-labels", "work-item-projects", "work-item-history", "work-item-comments"} {
			if statuses[key] != "not_enabled" {
				t.Fatalf("dataset %q status = %q, want not_enabled", key, statuses[key])
			}
			if gapCounts[key] != 0 {
				t.Fatalf("dataset %q still carries %d advertisable ranges", key, gapCounts[key])
			}
		}
		if statuses["commits"] != "healthy" {
			t.Fatalf("commits status = %q, want healthy: narrowing must not drop an enabled, selected dataset", statuses["commits"])
		}
	})

	t.Run("rows present but all disabled yields an empty scope", func(t *testing.T) {
		resetProjectorTables(t, ctx, pool)
		fixture := seedDisabledDatasetFixture(t, ctx, pool, "org-all-off", now)
		// Switch the remaining git-family rows off too, so the integration has
		// dataset rows but NOT ONE enabled. A query filtered to enabled rows
		// cannot tell this from "never seeded"; treating it as unseeded would
		// advertise every selected dataset the operator just switched off.
		if _, err := pool.Exec(ctx, `UPDATE integration_datasets SET is_enabled=FALSE WHERE org_id=$1`, fixture.OrgID); err != nil {
			t.Fatal(err)
		}
		raw, err := projector.Rebuild(ctx, fixture.OrgID, fixture.ConfigID)
		if err != nil {
			t.Fatal(err)
		}
		var payload map[string]any
		if err := json.Unmarshal(raw, &payload); err != nil {
			t.Fatal(err)
		}
		if windows := payload["backfill_windows"].([]any); len(windows) != 0 {
			t.Fatalf("all datasets disabled, yet %d backfill window(s) advertised: %#v", len(windows), windows)
		}
		for _, rawDataset := range payload["datasets"].([]any) {
			dataset := rawDataset.(map[string]any)
			if status := dataset["status"].(string); status != "not_enabled" {
				t.Fatalf("dataset %q status = %q, want not_enabled when every row is disabled",
					dataset["dataset_key"], status)
			}
		}
	})

	t.Run("tenant isolation", func(t *testing.T) {
		resetProjectorTables(t, ctx, pool)
		first := seedProjectorFixture(t, ctx, pool, "org-first", now)
		second := seedProjectorFixture(t, ctx, pool, "org-second", now)
		if _, err := projector.Rebuild(ctx, first.OrgID, first.ConfigID); err != nil {
			t.Fatal(err)
		}
		var firstCount, secondCount int
		if err := pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE org_id=$1), count(*) FILTER (WHERE org_id=$2) FROM sync_coverage_projections`, first.OrgID, second.OrgID).Scan(&firstCount, &secondCount); err != nil {
			t.Fatal(err)
		}
		if firstCount != 1 || secondCount != 0 {
			t.Fatalf("tenant projection counts = %d, %d", firstCount, secondCount)
		}
		if _, err := projector.Rebuild(ctx, first.OrgID, second.ConfigID); !errors.Is(err, ErrConfigNotFound) {
			t.Fatalf("cross-tenant rebuild error = %v", err)
		}
	})

	t.Run("refresh priority is invalidated then missing then oldest", func(t *testing.T) {
		resetProjectorTables(t, ctx, pool)
		oldest := seedProjectorFixture(t, ctx, pool, "org-oldest", now)
		invalidated := seedProjectorFixture(t, ctx, pool, "org-priority", now)
		missing := seedProjectorFixture(t, ctx, pool, "org-missing", now)
		if _, err := projector.Rebuild(ctx, oldest.OrgID, oldest.ConfigID); err != nil {
			t.Fatal(err)
		}
		if _, err := projector.Rebuild(ctx, invalidated.OrgID, invalidated.ConfigID); err != nil {
			t.Fatal(err)
		}
		oldTimestamp := now.Add(-30 * 24 * time.Hour)
		if _, err := pool.Exec(ctx, `UPDATE sync_coverage_projections SET updated_at=$1 WHERE org_id=$2`, oldTimestamp, oldest.OrgID); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `UPDATE sync_coverage_projections SET invalidated_at=$1 WHERE org_id=$2`, now, invalidated.OrgID); err != nil {
			t.Fatal(err)
		}
		result, err := projector.RefreshDue(ctx, 2)
		if err != nil {
			t.Fatal(err)
		}
		if result.Refreshed != 2 || result.Failed != 0 {
			t.Fatalf("refresh result = %+v", result)
		}
		var missingCount int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM sync_coverage_projections WHERE org_id=$1`, missing.OrgID).Scan(&missingCount); err != nil {
			t.Fatal(err)
		}
		var stillInvalidated bool
		if err := pool.QueryRow(ctx, `SELECT invalidated_at IS NOT NULL FROM sync_coverage_projections WHERE org_id=$1`, invalidated.OrgID).Scan(&stillInvalidated); err != nil {
			t.Fatal(err)
		}
		var oldestUpdated time.Time
		if err := pool.QueryRow(ctx, `SELECT updated_at FROM sync_coverage_projections WHERE org_id=$1`, oldest.OrgID).Scan(&oldestUpdated); err != nil {
			t.Fatal(err)
		}
		if missingCount != 1 || stillInvalidated || !oldestUpdated.Equal(oldTimestamp) {
			t.Fatalf("priority state missing=%d invalidated=%t oldest_updated=%s", missingCount, stillInvalidated, oldestUpdated)
		}
	})

	t.Run("failure preserves prior row", func(t *testing.T) {
		resetProjectorTables(t, ctx, pool)
		fixture := seedProjectorFixture(t, ctx, pool, "org-failure", now)
		original, err := projector.Rebuild(ctx, fixture.OrgID, fixture.ConfigID)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `UPDATE sync_coverage_projections SET invalidated_at=$1 WHERE org_id=$2`, now.Add(time.Minute), fixture.OrgID); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `UPDATE sync_configurations SET sync_targets='{}'::json WHERE id=$1`, fixture.ConfigID); err != nil {
			t.Fatal(err)
		}
		if _, err := projector.Rebuild(ctx, fixture.OrgID, fixture.ConfigID); err == nil {
			t.Fatal("malformed sync target shape did not fail")
		}
		var persisted json.RawMessage
		var invalidatedAt *time.Time
		if err := pool.QueryRow(ctx, `SELECT payload, invalidated_at FROM sync_coverage_projections WHERE org_id=$1`, fixture.OrgID).Scan(&persisted, &invalidatedAt); err != nil {
			t.Fatal(err)
		}
		if string(persisted) != string(original) {
			t.Fatalf("projection changed on failed rebuild\n got: %s\nwant: %s", persisted, original)
		}
		if invalidatedAt == nil {
			t.Fatal("failed rebuild cleared invalidated_at")
		}
	})
}

// seedDisabledDatasetFixture reproduces CHAOS-4106: a target-scoped config whose
// sync_targets still select the work-item family, an intent plane where that
// family is switched OFF, and real work-item history carrying a failed window.
// Before the fix, coverage scoped work-items in purely from sync_targets and
// advertised that failed window as an actionable backfill the planner would
// have refused (planner.py::_load_enabled_datasets filters is_enabled).
func seedDisabledDatasetFixture(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID string, now time.Time) projectorFixture {
	t.Helper()
	fixture := projectorFixture{
		OrgID: orgID, ConfigID: uuid.New(), IntegrationID: uuid.New(), SourceID: uuid.New(),
		RunID: uuid.New(), UnitID: uuid.New(),
	}
	pgseed.Integration(ctx, t, pool, fixture.IntegrationID.String(), orgID, "github")
	statements := []struct {
		SQL  string
		Args []any
	}{
		{`INSERT INTO sync_configurations (id,org_id,name,provider,sync_targets,is_active,planner_managed,integration_id,created_at,updated_at) VALUES ($1,$2,'sync','github','["git", "work-items"]',TRUE,FALSE,$3,now(),now())`, []any{fixture.ConfigID, orgID, fixture.IntegrationID}},
		{`INSERT INTO integration_sources (id,org_id,integration_id,provider,source_type,external_id,name,full_name,metadata,is_enabled,discovered_at,last_seen_at) VALUES ($1,$2,$3,'github','repository','acme/api','api','acme/api','{}'::json,TRUE,now(),now())`, []any{fixture.SourceID, orgID, fixture.IntegrationID}},
		{`INSERT INTO sync_runs (id,org_id,integration_id,triggered_by,mode,status,total_units,completed_units,failed_units,started_at,completed_at,created_at) VALUES ($1,$2,$3,'test','incremental','success',0,0,0,$4,$5,$4)`, []any{fixture.RunID, orgID, fixture.IntegrationID, now.Add(-2 * time.Hour), now.Add(-time.Hour)}},
		// commits is selected AND enabled: it must survive the narrowing.
		{`INSERT INTO sync_run_units (id,org_id,sync_run_id,integration_id,source_id,provider,dataset_key,processor_flags,since_at,before_at,status,created_at,updated_at,cost_class,mode,attempts) VALUES ($1,$2,$3,$4,$5,'github','commits','{}',$6,$7,'success',$8,$8,'rest_core','incremental',0)`, []any{fixture.UnitID, orgID, fixture.RunID, fixture.IntegrationID, fixture.SourceID, now.Add(-24 * time.Hour), now.Add(-time.Hour), now.Add(-time.Hour)}},
		// work-items is selected but DISABLED, and has a failed window wide
		// enough to clear the adjacency tolerance -- i.e. real advertisable history.
		{`INSERT INTO sync_run_units (id,org_id,sync_run_id,integration_id,source_id,provider,dataset_key,processor_flags,since_at,before_at,status,created_at,updated_at,cost_class,mode,attempts) VALUES ($1,$2,$3,$4,$5,'github','work-items','{}',$6,$7,'failed',$8,$8,'rest_core','incremental',0)`, []any{uuid.New(), orgID, fixture.RunID, fixture.IntegrationID, fixture.SourceID, now.Add(-240 * time.Hour), now.Add(-120 * time.Hour), now.Add(-time.Hour)}},
		{`INSERT INTO scheduled_jobs (id,org_id,name,job_type,provider,schedule_cron,status,sync_config_id,next_run_at,created_at,updated_at) VALUES ($1,$2,'sync','sync','github','0 * * * *',0,$3,$4,$5,$5)`, []any{uuid.New(), orgID, fixture.ConfigID, now.Add(time.Hour), now.Add(-24 * time.Hour)}},
	}
	// The intent plane: the git family stays on, the whole work-item family is off.
	for _, key := range []string{"repo-metadata", "commits", "commit-stats", "files"} {
		statements = append(statements, struct {
			SQL  string
			Args []any
		}{`INSERT INTO integration_datasets (id,org_id,integration_id,dataset_key,is_enabled,options) VALUES ($1,$2,$3,$4,TRUE,'{}'::json)`, []any{uuid.New(), orgID, fixture.IntegrationID, key}})
	}
	for _, key := range []string{"work-items", "work-item-labels", "work-item-projects", "work-item-history", "work-item-comments"} {
		statements = append(statements, struct {
			SQL  string
			Args []any
		}{`INSERT INTO integration_datasets (id,org_id,integration_id,dataset_key,is_enabled,options) VALUES ($1,$2,$3,$4,FALSE,'{}'::json)`, []any{uuid.New(), orgID, fixture.IntegrationID, key}})
	}
	for _, statement := range statements {
		if _, err := pool.Exec(ctx, statement.SQL, statement.Args...); err != nil {
			t.Fatal(err)
		}
	}
	return fixture
}

func seedProjectorFixture(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID string, now time.Time) projectorFixture {
	t.Helper()
	fixture := projectorFixture{
		OrgID: orgID, ConfigID: uuid.New(), IntegrationID: uuid.New(), SourceID: uuid.New(),
		RunID: uuid.New(), UnitID: uuid.New(),
	}
	pgseed.Integration(ctx, t, pool, fixture.IntegrationID.String(), orgID, "github")
	statements := []struct {
		SQL  string
		Args []any
	}{
		{`INSERT INTO sync_configurations (id,org_id,name,provider,sync_targets,is_active,planner_managed,integration_id,created_at,updated_at) VALUES ($1,$2,'sync','github','["git"]',TRUE,FALSE,$3,now(),now())`, []any{fixture.ConfigID, orgID, fixture.IntegrationID}},
		{`INSERT INTO integration_sources (id,org_id,integration_id,provider,source_type,external_id,name,full_name,metadata,is_enabled,discovered_at,last_seen_at) VALUES ($1,$2,$3,'github','repository','acme/api','api','acme/api','{}'::json,TRUE,now(),now())`, []any{fixture.SourceID, orgID, fixture.IntegrationID}},
		{`INSERT INTO sync_runs (id,org_id,integration_id,triggered_by,mode,status,total_units,completed_units,failed_units,started_at,completed_at,created_at) VALUES ($1,$2,$3,'test','incremental','success',0,0,0,$4,$5,$4)`, []any{fixture.RunID, orgID, fixture.IntegrationID, now.Add(-2 * time.Hour), now.Add(-time.Hour)}},
		{`INSERT INTO sync_run_units (id,org_id,sync_run_id,integration_id,source_id,provider,dataset_key,processor_flags,since_at,before_at,status,created_at,updated_at,cost_class,mode,attempts) VALUES ($1,$2,$3,$4,$5,'github','commits','{}',$6,$7,'success',$8,$8,'rest_core','incremental',0)`, []any{fixture.UnitID, orgID, fixture.RunID, fixture.IntegrationID, fixture.SourceID, now.Add(-24 * time.Hour), now.Add(-time.Hour), now.Add(-time.Hour)}},
		{`INSERT INTO scheduled_jobs (id,org_id,name,job_type,provider,schedule_cron,status,sync_config_id,next_run_at,created_at,updated_at) VALUES ($1,$2,'sync','sync','github','0 * * * *',0,$3,$4,$5,$5)`, []any{uuid.New(), orgID, fixture.ConfigID, now.Add(time.Hour), now.Add(-24 * time.Hour)}},
	}
	for _, statement := range statements {
		if _, err := pool.Exec(ctx, statement.SQL, statement.Args...); err != nil {
			t.Fatal(err)
		}
	}
	return fixture
}

func assertHealthyProjection(t *testing.T, raw json.RawMessage, fixture projectorFixture, now time.Time) {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["config_id"] != fixture.ConfigID.String() || payload["history_lookback_days"] != float64(HistoryLookbackDays) {
		t.Fatalf("projection identity = %#v", payload)
	}
	overall := payload["overall"].(map[string]any)
	if overall["health"] != "healthy" || overall["gap_count"] != float64(0) {
		t.Fatalf("overall = %#v", overall)
	}
	datasets := payload["datasets"].([]any)
	commitsFound := false
	for _, rawDataset := range datasets {
		dataset := rawDataset.(map[string]any)
		if dataset["dataset_key"] == "commits" {
			commitsFound = true
			if dataset["status"] != "healthy" || dataset["covered_through"] != isoTime(now.Add(-time.Hour)) {
				t.Fatalf("commits coverage = %#v", dataset)
			}
		}
	}
	if !commitsFound {
		t.Fatal("commits coverage is missing")
	}
}

func resetProjectorTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `TRUNCATE sync_coverage_projections, scheduled_jobs, backfill_jobs, sync_run_units, sync_runs, integration_datasets, integration_sources, sync_configurations, integrations CASCADE`); err != nil {
		t.Fatal(err)
	}
}

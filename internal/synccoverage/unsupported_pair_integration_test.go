//go:build integration

package synccoverage

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// seedFailedWindowFixture seeds a config of the given provider that selects git and incidents, with both datasets
// enabled in integration_datasets, a successful commits window and a wide failed incidents window. Planner-managed, the
// coverage scope comes from the enabled rows directly (the path that leaked); target-scoped, it comes from sync_targets
// (already filtered). Whether coverage may advertise the incidents window depends only on whether the provider supports
// the incidents dataset.
func seedFailedWindowFixture(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID, provider string, plannerManaged bool, now time.Time) projectorFixture {
	t.Helper()
	fixture := projectorFixture{
		OrgID: orgID, ConfigID: uuid.New(), IntegrationID: uuid.New(), SourceID: uuid.New(), RunID: uuid.New(), UnitID: uuid.New(),
	}
	pgseed.Integration(ctx, t, pool, fixture.IntegrationID.String(), orgID, provider)
	type statement struct {
		SQL  string
		Args []any
	}
	statements := []statement{
		{`INSERT INTO sync_configurations (id,org_id,name,provider,sync_targets,is_active,planner_managed,integration_id,created_at,updated_at) VALUES ($1,$2,'sync',$3,'["git","incidents"]',TRUE,$5,$4,now(),now())`, []any{fixture.ConfigID, orgID, provider, fixture.IntegrationID, plannerManaged}},
		{`INSERT INTO integration_sources (id,org_id,integration_id,provider,source_type,external_id,name,full_name,metadata,is_enabled,discovered_at,last_seen_at) VALUES ($1,$2,$3,$4,'repository','acme/api','api','acme/api',CASE WHEN $6 THEN json_build_object('planner_managed_sync_config_id', $5::text) ELSE '{}'::json END,TRUE,now(),now())`, []any{fixture.SourceID, orgID, fixture.IntegrationID, provider, fixture.ConfigID.String(), plannerManaged}},
		{`INSERT INTO sync_runs (id,org_id,integration_id,triggered_by,mode,status,total_units,completed_units,failed_units,started_at,completed_at,created_at) VALUES ($1,$2,$3,'test','incremental','success',0,0,0,$4,$5,$4)`, []any{fixture.RunID, orgID, fixture.IntegrationID, now.Add(-2 * time.Hour), now.Add(-time.Hour)}},
		{`INSERT INTO scheduled_jobs (id,org_id,name,job_type,provider,schedule_cron,status,sync_config_id,next_run_at,created_at,updated_at) VALUES ($1,$2,'sync','sync',$3,'0 * * * *',0,$4,$5,$6,$6)`, []any{uuid.New(), orgID, provider, fixture.ConfigID, now.Add(time.Hour), now.Add(-24 * time.Hour)}},
	}
	for _, dataset := range []struct {
		key, status   string
		since, before time.Duration
	}{
		{"commits", "success", -24 * time.Hour, -time.Hour},
		{"incidents", "failed", -240 * time.Hour, -120 * time.Hour},
	} {
		statements = append(statements,
			statement{`INSERT INTO integration_datasets (id,org_id,integration_id,dataset_key,is_enabled,options) VALUES ($1,$2,$3,$4,TRUE,'{}'::json)`, []any{uuid.New(), orgID, fixture.IntegrationID, dataset.key}},
			statement{`INSERT INTO sync_run_units (id,org_id,sync_run_id,integration_id,source_id,provider,dataset_key,processor_flags,since_at,before_at,status,created_at,updated_at,cost_class,mode,attempts) VALUES ($1,$2,$3,$4,$5,$6,$7,'{}',$8,$9,$10,$11,$11,'rest_core','incremental',0)`, []any{uuid.New(), orgID, fixture.RunID, fixture.IntegrationID, fixture.SourceID, provider, dataset.key, now.Add(dataset.since), now.Add(dataset.before), dataset.status, now.Add(-time.Hour)}},
		)
	}
	for _, s := range statements {
		if _, err := pool.Exec(ctx, s.SQL, s.Args...); err != nil {
			t.Fatal(err)
		}
	}
	return fixture
}

// assertProjection checks the rebuilt payload: whether incidents is advertised as a dataset carrying a gap or failed
// range and as a backfill window, and that commits (supported, successfully synced) is healthy and unchanged.
func assertProjection(t *testing.T, raw json.RawMessage, now time.Time, wantIncidents bool) {
	t.Helper()
	var payload struct {
		Overall struct {
			Health   string `json:"health"`
			GapCount int    `json:"gap_count"`
		} `json:"overall"`
		Datasets []struct {
			DatasetKey     string `json:"dataset_key"`
			Status         string `json:"status"`
			CoveredThrough string `json:"covered_through"`
			Gaps           []any  `json:"gaps"`
			FailedRanges   []any  `json:"failed_ranges"`
		} `json:"datasets"`
		BackfillWindows []struct {
			DatasetKeys []string `json:"dataset_keys"`
		} `json:"backfill_windows"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	incidentsRanges, commitsFound := false, false
	for _, dataset := range payload.Datasets {
		switch dataset.DatasetKey {
		case "incidents":
			incidentsRanges = len(dataset.Gaps)+len(dataset.FailedRanges) > 0
			if !wantIncidents {
				t.Errorf("projection carries an incidents dataset: %+v", dataset)
			}
		case "commits":
			commitsFound = true
			if dataset.Status != "healthy" || dataset.CoveredThrough != isoTime(now.Add(-time.Hour)) {
				t.Errorf("commits coverage = %+v, want healthy through %s", dataset, isoTime(now.Add(-time.Hour)))
			}
		}
	}
	if !commitsFound {
		t.Error("commits coverage is missing")
	}
	incidentsWindow := false
	for _, window := range payload.BackfillWindows {
		for _, key := range window.DatasetKeys {
			incidentsWindow = incidentsWindow || key == "incidents"
		}
	}
	if wantIncidents && (!incidentsRanges || !incidentsWindow) {
		t.Errorf("incidents gap/failed range=%t backfill window=%t, want both\npayload: %s", incidentsRanges, incidentsWindow, raw)
	}
	if !wantIncidents {
		if incidentsWindow {
			t.Errorf("backfill window advertises incidents\npayload: %s", raw)
		}
		if payload.Overall.Health != "healthy" || payload.Overall.GapCount != 0 {
			t.Errorf("overall = %+v, want healthy with no gap (the incidents history must not count)", payload.Overall)
		}
	}
}

// CHAOS-11888 (D6467): the rebuilt coverage payload and its backfill offer carry no gap or window for a (provider,
// dataset) pair the provider does not support (GitHub has no incidents dataset), while a supported pair is untouched.
// Test cells by fcdev (the cell with the overall-health and commits asserts) and pm-11888.
func TestProjectorDropsProviderDatasetPairsTheProviderDoesNotSupport(t *testing.T) {
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

	for _, tc := range []struct {
		name, provider string
		plannerManaged bool
		wantIncidents  bool
	}{
		{"github planner-managed drops incidents", "github", true, false},
		{"github target-scoped drops incidents", "github", false, false},
		{"gitlab planner-managed keeps incidents", "gitlab", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resetProjectorTables(t, ctx, pool)
			fixture := seedFailedWindowFixture(t, ctx, pool, "org-"+uuid.NewString()[:8], tc.provider, tc.plannerManaged, now)
			raw, err := projector.Rebuild(ctx, fixture.OrgID, fixture.ConfigID)
			if err != nil {
				t.Fatal(err)
			}
			assertProjection(t, raw, now, tc.wantIncidents)
		})
	}
}

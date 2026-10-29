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

// seedFailedWindowFixture seeds a planner-managed config of the given provider, whose coverage scope comes from the
// enabled integration_datasets rows (an enabled incidents row exists for every provider), with a wide failed window on
// commits and on incidents. Whether coverage may advertise the incidents window depends only on whether the provider
// supports the incidents dataset.
func seedFailedWindowFixture(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID, provider string, now time.Time) projectorFixture {
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
		{`INSERT INTO sync_configurations (id,org_id,name,provider,sync_targets,is_active,planner_managed,integration_id,created_at,updated_at) VALUES ($1,$2,'sync',$3,'[]',TRUE,TRUE,$4,now(),now())`, []any{fixture.ConfigID, orgID, provider, fixture.IntegrationID}},
		{`INSERT INTO integration_sources (id,org_id,integration_id,provider,source_type,external_id,name,full_name,metadata,is_enabled,discovered_at,last_seen_at) VALUES ($1,$2,$3,$4,'repository','acme/api','api','acme/api',json_build_object('planner_managed_sync_config_id', $5::text),TRUE,now(),now())`, []any{fixture.SourceID, orgID, fixture.IntegrationID, provider, fixture.ConfigID.String()}},
		{`INSERT INTO sync_runs (id,org_id,integration_id,triggered_by,mode,status,total_units,completed_units,failed_units,started_at,completed_at,created_at) VALUES ($1,$2,$3,'test','incremental','success',0,0,0,$4,$5,$4)`, []any{fixture.RunID, orgID, fixture.IntegrationID, now.Add(-2 * time.Hour), now.Add(-time.Hour)}},
		{`INSERT INTO scheduled_jobs (id,org_id,name,job_type,provider,schedule_cron,status,sync_config_id,next_run_at,created_at,updated_at) VALUES ($1,$2,'sync','sync',$3,'0 * * * *',0,$4,$5,$6,$6)`, []any{uuid.New(), orgID, provider, fixture.ConfigID, now.Add(time.Hour), now.Add(-24 * time.Hour)}},
	}
	for _, dataset := range []string{"commits", "incidents"} {
		statements = append(statements,
			statement{`INSERT INTO integration_datasets (id,org_id,integration_id,dataset_key,is_enabled,options) VALUES ($1,$2,$3,$4,TRUE,'{}'::json)`, []any{uuid.New(), orgID, fixture.IntegrationID, dataset}},
			statement{`INSERT INTO sync_run_units (id,org_id,sync_run_id,integration_id,source_id,provider,dataset_key,processor_flags,since_at,before_at,status,created_at,updated_at,cost_class,mode,attempts) VALUES ($1,$2,$3,$4,$5,$6,$7,'{}',$8,$9,'failed',$10,$10,'rest_core','incremental',0)`, []any{uuid.New(), orgID, fixture.RunID, fixture.IntegrationID, fixture.SourceID, provider, dataset, now.Add(-240 * time.Hour), now.Add(-120 * time.Hour), now.Add(-time.Hour)}},
		)
	}
	for _, s := range statements {
		if _, err := pool.Exec(ctx, s.SQL, s.Args...); err != nil {
			t.Fatal(err)
		}
	}
	return fixture
}

// datasetKeysAdvertised returns the dataset keys that carry a gap or failed range, and the dataset keys named by a
// backfill window, in the rebuilt payload.
func datasetKeysAdvertised(t *testing.T, raw json.RawMessage) (rangeKeys, windowKeys map[string]bool) {
	t.Helper()
	var payload struct {
		Datasets []struct {
			DatasetKey   string `json:"dataset_key"`
			Gaps         []any  `json:"gaps"`
			FailedRanges []any  `json:"failed_ranges"`
		} `json:"datasets"`
		BackfillWindows []struct {
			DatasetKeys []string `json:"dataset_keys"`
		} `json:"backfill_windows"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	rangeKeys, windowKeys = map[string]bool{}, map[string]bool{}
	for _, dataset := range payload.Datasets {
		if len(dataset.Gaps)+len(dataset.FailedRanges) > 0 {
			rangeKeys[dataset.DatasetKey] = true
		}
	}
	for _, window := range payload.BackfillWindows {
		for _, key := range window.DatasetKeys {
			windowKeys[key] = true
		}
	}
	return rangeKeys, windowKeys
}

// CHAOS-11888 (D6467): the rebuilt coverage payload and its backfill offer carry no gap or window for a (provider,
// dataset) pair the provider does not support (GitHub has no incidents dataset), while a supported pair is untouched.
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

	t.Run("github has no incidents dataset", func(t *testing.T) {
		resetProjectorTables(t, ctx, pool)
		fixture := seedFailedWindowFixture(t, ctx, pool, "org-github-incidents", "github", now)
		raw, err := projector.Rebuild(ctx, fixture.OrgID, fixture.ConfigID)
		if err != nil {
			t.Fatal(err)
		}
		rangeKeys, windowKeys := datasetKeysAdvertised(t, raw)
		if rangeKeys["incidents"] || windowKeys["incidents"] {
			t.Errorf("github incidents advertised: gap/failed range=%t backfill window=%t, want neither\npayload: %s", rangeKeys["incidents"], windowKeys["incidents"], raw)
		}
		if !rangeKeys["commits"] || !windowKeys["commits"] {
			t.Errorf("github commits (supported control) gap/failed range=%t backfill window=%t, want both\npayload: %s", rangeKeys["commits"], windowKeys["commits"], raw)
		}
	})

	t.Run("gitlab supports incidents", func(t *testing.T) {
		resetProjectorTables(t, ctx, pool)
		fixture := seedFailedWindowFixture(t, ctx, pool, "org-gitlab-incidents", "gitlab", now)
		raw, err := projector.Rebuild(ctx, fixture.OrgID, fixture.ConfigID)
		if err != nil {
			t.Fatal(err)
		}
		rangeKeys, windowKeys := datasetKeysAdvertised(t, raw)
		if !rangeKeys["incidents"] || !windowKeys["incidents"] {
			t.Errorf("gitlab incidents (supported control) gap/failed range=%t backfill window=%t, want both\npayload: %s", rangeKeys["incidents"], windowKeys["incidents"], raw)
		}
	})
}

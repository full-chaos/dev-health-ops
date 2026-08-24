//go:build integration

package sync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/syncreconciler"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

const materializerFixtureDDL = `
CREATE TABLE public.sync_configurations (
 id uuid PRIMARY KEY, org_id text NOT NULL, sync_targets json NOT NULL,
 sync_options json NOT NULL, integration_id uuid, is_active boolean NOT NULL,
 source_id uuid, planner_managed boolean NOT NULL,provider text NOT NULL,
 last_sync_at timestamptz,last_sync_success boolean,last_sync_error text,last_sync_stats json,
 updated_at timestamptz NOT NULL
);
CREATE TABLE public.integrations (
 id uuid PRIMARY KEY, org_id text NOT NULL, provider text NOT NULL,
 credential_id uuid, is_active boolean NOT NULL, config json NOT NULL
);
CREATE TABLE public.integration_credentials (
 id uuid PRIMARY KEY, org_id text NOT NULL, provider text NOT NULL,
 is_active boolean NOT NULL, config json,credentials_encrypted text
);
CREATE TABLE public.integration_sources (
 id uuid PRIMARY KEY, org_id text NOT NULL, integration_id uuid NOT NULL,
 provider text NOT NULL,source_type text NOT NULL,external_id text NOT NULL,name text NOT NULL,
 full_name text NOT NULL,is_enabled boolean NOT NULL,metadata json NOT NULL,
 discovered_at timestamptz NOT NULL,last_seen_at timestamptz NOT NULL,
	FOREIGN KEY(integration_id) REFERENCES integrations(id),
 UNIQUE(org_id,integration_id,provider,external_id)
);
CREATE TABLE public.integration_datasets (
 id uuid PRIMARY KEY, org_id text NOT NULL, integration_id uuid NOT NULL,
 dataset_key text NOT NULL, is_enabled boolean NOT NULL, options json NOT NULL,
	FOREIGN KEY(integration_id) REFERENCES integrations(id),
 UNIQUE (org_id,integration_id,dataset_key)
);
CREATE TABLE public.sync_watermarks (
 org_id text NOT NULL, source_id text NOT NULL, dataset_key text NOT NULL,
 repo_id text NOT NULL, target text NOT NULL, last_synced_at timestamptz
);
CREATE TABLE public.organizations (id uuid PRIMARY KEY, tier text);
CREATE TABLE public.org_licenses (
 org_id uuid PRIMARY KEY, tier text NOT NULL, limits_override json NOT NULL,
 features_override json NOT NULL
);
CREATE TABLE public.feature_flags (
 id uuid PRIMARY KEY,key text UNIQUE,min_tier text NOT NULL,is_enabled boolean NOT NULL
);
CREATE TABLE public.org_feature_overrides (
 id uuid PRIMARY KEY,org_id uuid NOT NULL,feature_id uuid NOT NULL,
 is_enabled boolean NOT NULL,expires_at timestamptz
);
CREATE TABLE public.tier_limits (
 tier text NOT NULL, limit_key text NOT NULL, limit_value text,
 UNIQUE(tier,limit_key)
);
CREATE TABLE public.sync_runs (
 id uuid PRIMARY KEY, org_id text NOT NULL, integration_id uuid NOT NULL,
 triggered_by text NOT NULL, mode text NOT NULL, status text NOT NULL,
 total_units integer NOT NULL, completed_units integer NOT NULL, failed_units integer NOT NULL,
 credential_id uuid, credential_fingerprint text, auth_source text, started_at timestamptz, completed_at timestamptz,
	result json, error text, created_at timestamptz NOT NULL,
	FOREIGN KEY(integration_id) REFERENCES integrations(id)
);
CREATE TABLE public.sync_run_units (
 id uuid PRIMARY KEY, org_id text NOT NULL, sync_run_id uuid NOT NULL,
 integration_id uuid NOT NULL, source_id uuid NOT NULL, provider text NOT NULL,
 dataset_key text NOT NULL, cost_class text NOT NULL, mode text NOT NULL,
 since_at timestamptz, before_at timestamptz, status text NOT NULL, attempts integer NOT NULL,
 available_at timestamptz, rate_limit_deferrals integer NOT NULL DEFAULT 0,
 rate_limit_first_seen_at timestamptz, expired_lease_retry_count integer NOT NULL DEFAULT 0,
 last_retry_reason text, retry_exhausted_at timestamptz, duration_seconds integer,
 error text, result json, processor_flags json, lease_owner text, lease_expires_at timestamptz,
 last_heartbeat_at timestamptz, created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL,
	FOREIGN KEY(sync_run_id) REFERENCES sync_runs(id),
	FOREIGN KEY(source_id) REFERENCES integration_sources(id)
);
-- CHAOS-4114: the maintained executed-proof projection. persistDomainGraph
-- stamps every planned pair ATTEMPTED here inside the same transaction that
-- inserts the units, so a venue without it fails materialization outright.
CREATE TABLE public.sync_executed_proof_ledger (
 provider text NOT NULL, dataset_key text NOT NULL,
 attempted_at timestamptz NOT NULL, proven_at timestamptz,
 PRIMARY KEY (provider, dataset_key),
 CONSTRAINT ck_sync_executed_proof_ledger_provider_normalized
  CHECK (provider = lower(provider) AND btrim(provider) <> ''),
 CONSTRAINT ck_sync_executed_proof_ledger_dataset_normalized
  CHECK (dataset_key = lower(dataset_key) AND btrim(dataset_key) <> '')
);
CREATE TABLE public.scheduled_jobs (id uuid PRIMARY KEY);
CREATE TABLE public.job_runs (
 id uuid PRIMARY KEY, job_id uuid NOT NULL, status integer NOT NULL,
 started_at timestamptz, completed_at timestamptz, duration_seconds integer,
 result json, error text, error_traceback text, triggered_by text NOT NULL,
 created_at timestamptz NOT NULL
);
CREATE TABLE public.sync_run_reference_discoveries (
 id uuid PRIMARY KEY, sync_run_id uuid NOT NULL UNIQUE, org_id text NOT NULL,
 status text NOT NULL, attempts integer NOT NULL, available_at timestamptz NOT NULL,
 lease_owner text, lease_expires_at timestamptz, last_heartbeat_at timestamptz,
 completed_at timestamptz, error text, result json,
 created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL
);
CREATE TABLE public.sync_run_post_dispatches (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), org_id text NOT NULL,
 sync_run_id uuid NOT NULL, kind text NOT NULL, dispatched_at timestamptz NOT NULL,
 UNIQUE(sync_run_id,kind)
);
CREATE TABLE public.sync_dispatch_outbox (
 id uuid PRIMARY KEY, org_id text NOT NULL, sync_run_id uuid NOT NULL,
 kind text NOT NULL, status text NOT NULL, available_at timestamptz NOT NULL,
 attempts integer NOT NULL, last_error text, dispatched_at timestamptz,
 claim_token text, claim_expires_at timestamptz, claim_transport text,
 claim_route_generation bigint, dispatched_transport text,
 dispatched_route_generation bigint, transport_job_id text,
 created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL,
 UNIQUE(sync_run_id,kind)
);
CREATE TABLE public.scheduled_sync_occurrences (
 occurrence_id text PRIMARY KEY, identity_version text NOT NULL, org_id text NOT NULL,
 sync_config_id uuid NOT NULL, scheduled_job_id uuid NOT NULL, scheduled_for timestamptz NOT NULL,
 job_run_id uuid, sync_run_id uuid, reconcile_status text NOT NULL,
 FOREIGN KEY(sync_config_id) REFERENCES sync_configurations(id),
 FOREIGN KEY(scheduled_job_id) REFERENCES scheduled_jobs(id),
 FOREIGN KEY(job_run_id) REFERENCES job_runs(id),
 FOREIGN KEY(sync_run_id) REFERENCES sync_runs(id)
);`

type materializerFixture struct {
	pool       *pgxpool.Pool
	occurrence PendingOccurrence
}

func startMaterializerPostgres(t *testing.T) materializerFixture {
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
	if _, err := pool.Exec(ctx, materializerFixtureDDL); err != nil {
		t.Fatal(err)
	}
	const (
		orgID         = "00000000-0000-4000-8000-0000000000aa"
		configID      = "00000000-0000-4000-8000-000000001001"
		integrationID = "00000000-0000-4000-8000-000000001002"
		sourceID      = "00000000-0000-4000-8000-000000001003"
		datasetID     = "00000000-0000-4000-8000-000000001004"
		jobID         = "00000000-0000-4000-8000-000000001005"
	)
	statements := []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO integrations VALUES ($1::uuid,$2,'github',NULL,TRUE,'{}'::jsonb)`, []any{integrationID, orgID}},
		{`INSERT INTO organizations VALUES ($1::uuid,'community')`, []any{orgID}},
		{`INSERT INTO feature_flags VALUES ('00000000-0000-4000-8000-000000001007','canonical_incident_ingestion','community',TRUE)`, nil},
		{`INSERT INTO sync_configurations (id,org_id,sync_targets,sync_options,integration_id,is_active,source_id,planner_managed,provider,updated_at) VALUES ($1::uuid,$2,'["git"]'::jsonb,'{"schedule_cron":"0 * * * *"}'::jsonb,$3::uuid,TRUE,NULL,FALSE,'github',now())`, []any{configID, orgID, integrationID}},
		{`INSERT INTO integration_sources (id,org_id,integration_id,provider,source_type,external_id,name,full_name,is_enabled,metadata,discovered_at,last_seen_at) VALUES ($1::uuid,$2,$3::uuid,'github','repository','full-chaos/dev-health','dev-health','full-chaos/dev-health',TRUE,'{}'::jsonb,now(),now())`, []any{sourceID, orgID, integrationID}},
		{`INSERT INTO integration_datasets VALUES ($1::uuid,$2,$3::uuid,'commits',TRUE,'{}'::jsonb)`, []any{datasetID, orgID, integrationID}},
		{`INSERT INTO sync_watermarks VALUES ($1,'full-chaos/dev-health','commits','full-chaos/dev-health','commits',$2)`, []any{orgID, time.Date(2026, 8, 1, 6, 0, 0, 0, time.UTC)}},
		{`INSERT INTO scheduled_jobs VALUES ($1::uuid)`, []any{jobID}},
	}
	for _, statement := range statements {
		if _, err := pool.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
	return materializerFixture{pool: pool, occurrence: PendingOccurrence{
		ID: "occurrence:v1:scheduled", IdentityVersion: OccurrenceIdentityVersion,
		OrgID: orgID, ConfigID: configID, JobID: jobID,
		ScheduledFor: time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC),
		ConfigActive: true, ConfigPlannerManaged: true, JobStatus: 0, JobType: "sync",
	}}
}

func materializeAndCommit(t *testing.T, fixture materializerFixture, materializer *NativeMaterializer, occurrence PendingOccurrence) (PlanResult, error) {
	t.Helper()
	tx, err := fixture.pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	plan, err := materializer.Materialize(context.Background(), tx, occurrence)
	if err != nil {
		_ = tx.Rollback(context.Background())
		return PlanResult{}, err
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	return plan, nil
}

func TestNativeMaterializerReplaysWithoutDuplicateGraphRows(t *testing.T) {
	fixture := startMaterializerPostgres(t)
	materializer, err := NewNativeMaterializer(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	first, err := materializeAndCommit(t, fixture, materializer, fixture.occurrence)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := materializeAndCommit(t, fixture, materializer, fixture.occurrence)
	if err != nil {
		t.Fatal(err)
	}
	if first != replay {
		t.Fatalf("replay changed graph identities: first=%+v replay=%+v", first, replay)
	}
	for table, want := range map[string]int{"sync_runs": 1, "sync_run_units": 2, "job_runs": 1, "sync_run_reference_discoveries": 1, "sync_dispatch_outbox": 1} {
		var got int
		if err := fixture.pool.QueryRow(context.Background(), "SELECT count(*) FROM public."+table).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("%s row count=%d, want %d", table, got, want)
		}
	}
	var fingerprint *string
	var authSource string
	if err := fixture.pool.QueryRow(context.Background(), `SELECT credential_fingerprint,auth_source FROM sync_runs WHERE id=$1::uuid`, first.SyncRunID).Scan(&fingerprint, &authSource); err != nil {
		t.Fatal(err)
	}
	if fingerprint != nil || authSource != "environment" {
		t.Fatalf("unsafe scheduler credential stamp: fingerprint=%v auth_source=%q", fingerprint, authSource)
	}
	var jobResultJSON []byte
	if err := fixture.pool.QueryRow(context.Background(), `SELECT result::jsonb FROM job_runs WHERE id=$1::uuid`, first.JobRunID).Scan(&jobResultJSON); err != nil {
		t.Fatal(err)
	}
	var jobResult map[string]any
	if err := json.Unmarshal(jobResultJSON, &jobResult); err != nil {
		t.Fatal(err)
	}
	if len(jobResult) != 1 || jobResult["sync_run_id"] != first.SyncRunID {
		t.Fatalf("normal scheduled job result diverged from Python: %v", jobResult)
	}
	var since time.Time
	if err := fixture.pool.QueryRow(context.Background(), `SELECT since_at FROM sync_run_units WHERE sync_run_id=$1::uuid AND dataset_key='commits'`, first.SyncRunID).Scan(&since); err != nil {
		t.Fatal(err)
	}
	if want := time.Date(2026, 8, 1, 6, 0, 0, 0, time.UTC); !since.Equal(want) {
		t.Fatalf("commits watermark start=%s, want %s", since, want)
	}
}

func TestNativeMaterializerReusesDomainGraphAfterCrashBeforeCoordinatorCommit(t *testing.T) {
	fixture := startMaterializerPostgres(t)
	fixture.occurrence.ID = "occurrence:v1:crash-window"
	materializer, err := NewNativeMaterializer(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	crash := errors.New("injected crash after domain commit")
	if _, err := fixture.pool.Exec(context.Background(), `
INSERT INTO scheduled_sync_occurrences
 (occurrence_id,identity_version,org_id,sync_config_id,scheduled_job_id,scheduled_for,reconcile_status)
VALUES ($1,$2,$3,$4::uuid,$5::uuid,$6,'pending')`, fixture.occurrence.ID, fixture.occurrence.IdentityVersion,
		fixture.occurrence.OrgID, fixture.occurrence.ConfigID, fixture.occurrence.JobID, fixture.occurrence.ScheduledFor); err != nil {
		t.Fatal(err)
	}
	materializer.afterDomainCommit = func() error { return crash }
	if _, err := materializeAndCommit(t, fixture, materializer, fixture.occurrence); !errors.Is(err, crash) {
		t.Fatalf("materialize error=%v, want injected crash", err)
	}
	for table, want := range map[string]int{"sync_runs": 1, "sync_run_units": 2, "job_runs": 0, "sync_run_reference_discoveries": 0, "sync_dispatch_outbox": 0} {
		var got int
		if err := fixture.pool.QueryRow(context.Background(), "SELECT count(*) FROM public."+table).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("after crash %s row count=%d, want %d", table, got, want)
		}
	}
	dispatchMaterializer, err := syncreconciler.NewMaterializer(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	materialized, err := dispatchMaterializer.Step(
		context.Background(), fixture.occurrence.ScheduledFor.Add(time.Minute), fixture.occurrence.ScheduledFor.Add(-15*time.Minute), 10,
	)
	if err != nil {
		t.Fatal(err)
	}
	if materialized.Dispatch != 0 || materialized.Finalize != 0 {
		t.Fatalf("partial scheduled graph became dispatchable before coordinator readiness: %#v", materialized)
	}
	materializer.afterDomainCommit = nil
	tx, err := fixture.pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	plan, err := materializer.Materialize(context.Background(), tx, fixture.occurrence)
	if err != nil {
		_ = tx.Rollback(context.Background())
		t.Fatal(err)
	}
	if _, err := tx.Exec(context.Background(), `
UPDATE scheduled_sync_occurrences
SET job_run_id=$2::uuid,sync_run_id=$3::uuid,reconcile_status='completed'
WHERE occurrence_id=$1`, fixture.occurrence.ID, plan.JobRunID, plan.SyncRunID); err != nil {
		_ = tx.Rollback(context.Background())
		t.Fatal(err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if plan.JobRunID == "" || plan.SyncRunID == "" {
		t.Fatalf("replay returned incomplete plan: %+v", plan)
	}
	for table, want := range map[string]int{"sync_runs": 1, "sync_run_units": 2, "job_runs": 1, "sync_run_reference_discoveries": 1, "sync_dispatch_outbox": 1} {
		var got int
		if err := fixture.pool.QueryRow(context.Background(), "SELECT count(*) FROM public."+table).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("after replay %s row count=%d, want %d", table, got, want)
		}
	}
	materialized, err = dispatchMaterializer.Step(
		context.Background(), fixture.occurrence.ScheduledFor.Add(time.Minute), fixture.occurrence.ScheduledFor.Add(-15*time.Minute), 10,
	)
	if err != nil {
		t.Fatal(err)
	}
	if materialized.Dispatch != 1 {
		t.Fatalf("ready scheduled graph dispatch count=%d, want 1", materialized.Dispatch)
	}
}

func TestNativeMaterializerRejectsReplayWhenWatermarkChangesAfterDomainCommit(t *testing.T) {
	fixture := startMaterializerPostgres(t)
	fixture.occurrence.ID = "occurrence:v1:semantic-replay"
	materializer, err := NewNativeMaterializer(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	crash := errors.New("injected crash")
	materializer.afterDomainCommit = func() error { return crash }
	if _, err := materializeAndCommit(t, fixture, materializer, fixture.occurrence); !errors.Is(err, crash) {
		t.Fatalf("first materialize=%v", err)
	}
	if _, err := fixture.pool.Exec(context.Background(), `UPDATE sync_watermarks SET last_synced_at=last_synced_at+interval '1 hour'`); err != nil {
		t.Fatal(err)
	}
	materializer.afterDomainCommit = nil
	if _, err := materializeAndCommit(t, fixture, materializer, fixture.occurrence); !errors.Is(err, ErrInvalidPlan) {
		t.Fatalf("semantic replay error=%v, want ErrInvalidPlan", err)
	}
}

func TestNativeMaterializerRejectsReplayAcrossInitializedFieldFamilies(t *testing.T) {
	fixture := startMaterializerPostgres(t)
	fixture.occurrence.ID = "occurrence:v1:coordinator-semantic-replay"
	materializer, err := NewNativeMaterializer(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := materializeAndCommit(t, fixture, materializer, fixture.occurrence); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name    string
		corrupt string
		restore string
	}{
		{"sync run lifecycle", `UPDATE sync_runs SET started_at=created_at`, `UPDATE sync_runs SET started_at=NULL`},
		{"unit retry lifecycle", `UPDATE sync_run_units SET rate_limit_deferrals=1`, `UPDATE sync_run_units SET rate_limit_deferrals=0`},
		{"job run lifecycle", `UPDATE job_runs SET duration_seconds=1`, `UPDATE job_runs SET duration_seconds=NULL`},
		{"discovery lifecycle", `UPDATE sync_run_reference_discoveries SET error='corrupt'`, `UPDATE sync_run_reference_discoveries SET error=NULL`},
		{"outbox lifecycle", `UPDATE sync_dispatch_outbox SET last_error='corrupt'`, `UPDATE sync_dispatch_outbox SET last_error=NULL`},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := fixture.pool.Exec(context.Background(), test.corrupt); err != nil {
				t.Fatal(err)
			}
			if _, err := materializeAndCommit(t, fixture, materializer, fixture.occurrence); !errors.Is(err, ErrInvalidPlan) {
				t.Fatalf("semantic replay error=%v, want ErrInvalidPlan", err)
			}
			if _, err := fixture.pool.Exec(context.Background(), test.restore); err != nil {
				t.Fatal(err)
			}
			if _, err := materializeAndCommit(t, fixture, materializer, fixture.occurrence); err != nil {
				t.Fatalf("replay did not recover after restore: %v", err)
			}
		})
	}
}

func configurePagerDutyFixture(t *testing.T, fixture materializerFixture, targets string) {
	t.Helper()
	const credentialID = "00000000-0000-4000-8000-000000001006"
	ctx := context.Background()
	statements := []struct {
		sql  string
		args []any
	}{
		{`DELETE FROM sync_watermarks`, nil}, {`DELETE FROM integration_sources`, nil}, {`DELETE FROM integration_datasets`, nil},
		{`INSERT INTO integration_credentials (id,org_id,provider,is_active,config,credentials_encrypted) VALUES ($1::uuid,$2,'pagerduty',TRUE,'{"account_id":"acct-1","subdomain":"full-chaos"}'::jsonb,'ciphertext-must-not-be-read')`, []any{credentialID, fixture.occurrence.OrgID}},
		{`UPDATE integrations SET provider='pagerduty',credential_id=$1::uuid WHERE id=(SELECT integration_id FROM sync_configurations LIMIT 1)`, []any{credentialID}},
		{`UPDATE sync_configurations SET provider='pagerduty',sync_targets=$1::jsonb WHERE id=$2::uuid`, []any{targets, fixture.occurrence.ConfigID}},
	}
	for _, statement := range statements {
		if _, err := fixture.pool.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
}

func TestNativeMaterializerRepairsPagerDutyInventoryWithoutReadingCiphertext(t *testing.T) {
	fixture := startMaterializerPostgres(t)
	configurePagerDutyFixture(t, fixture, `["operational"]`)
	const repositoryMappings = `{"svc-1":{"provider":"github","repository":"full-chaos/dev-health"}}`
	for _, statement := range []struct {
		sql  string
		args []any
	}{
		{`UPDATE sync_configurations
SET planner_managed=TRUE,
    sync_options=jsonb_build_object(
      'schedule_cron','0 * * * *',
      'service_repository_mappings',$1::jsonb
    )
WHERE id=$2::uuid`, []any{repositoryMappings, fixture.occurrence.ConfigID}},
		{`INSERT INTO integration_datasets
 (id,org_id,integration_id,dataset_key,is_enabled,options)
VALUES (
 '00000000-0000-4000-8000-000000001012',$1,
 (SELECT integration_id FROM sync_configurations WHERE id=$2::uuid),
 'services',TRUE,
 jsonb_build_object(
   'legacy_targets',jsonb_build_array('operational'),
   'initial_sync_depth',14,
   'service_repository_mappings',$3::jsonb
 ))`, []any{fixture.occurrence.OrgID, fixture.occurrence.ConfigID, repositoryMappings}},
	} {
		if _, err := fixture.pool.Exec(context.Background(), statement.sql, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
	materializer, err := NewNativeMaterializer(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := materializeAndCommit(t, fixture, materializer, fixture.occurrence)
	if err != nil {
		t.Fatal(err)
	}
	var units int
	var credentialID, authSource string
	var fingerprint *string
	if err := fixture.pool.QueryRow(context.Background(), `SELECT total_units,credential_id::text,credential_fingerprint,auth_source FROM sync_runs WHERE id=$1::uuid`, plan.SyncRunID).Scan(&units, &credentialID, &fingerprint, &authSource); err != nil {
		t.Fatal(err)
	}
	if units != len(pagerDutyOperationalDatasets) || credentialID == "" || fingerprint != nil || authSource != "integration_credential" {
		t.Fatalf("PagerDuty run stamp/units unexpected: units=%d credential=%q fingerprint=%v auth=%q", units, credentialID, fingerprint, authSource)
	}
	var sources, datasets int
	if err := fixture.pool.QueryRow(context.Background(), `SELECT count(*) FROM integration_sources WHERE is_enabled AND external_id='acct-1'`).Scan(&sources); err != nil {
		t.Fatal(err)
	}
	if err := fixture.pool.QueryRow(context.Background(), `SELECT count(*) FROM integration_datasets WHERE is_enabled`).Scan(&datasets); err != nil {
		t.Fatal(err)
	}
	if sources != 1 || datasets != len(pagerDutyOperationalDatasets) {
		t.Fatalf("PagerDuty repaired inventory: sources=%d datasets=%d", sources, datasets)
	}
	var mappingsPreserved, legacyTargetsRepaired bool
	var initialDepth int
	if err := fixture.pool.QueryRow(context.Background(), `
SELECT (options::jsonb)->'service_repository_mappings'=$1::jsonb,
       ((options::jsonb)->>'initial_sync_depth')::integer,
       (options::jsonb)->'legacy_targets'='["operational"]'::jsonb
FROM integration_datasets
WHERE org_id=$2 AND integration_id=(SELECT integration_id FROM sync_configurations WHERE id=$3::uuid)
  AND dataset_key='services'`, repositoryMappings, fixture.occurrence.OrgID, fixture.occurrence.ConfigID).Scan(
		&mappingsPreserved, &initialDepth, &legacyTargetsRepaired,
	); err != nil {
		t.Fatal(err)
	}
	if !mappingsPreserved || initialDepth != 14 || !legacyTargetsRepaired {
		t.Fatalf("PagerDuty planner options were not preserved through repair: mappings=%v depth=%d targets=%v", mappingsPreserved, initialDepth, legacyTargetsRepaired)
	}
}

func TestNativeMaterializerPagerDutyRepairAndUnitsShareDomainTransaction(t *testing.T) {
	fixture := startMaterializerPostgres(t)
	fixture.occurrence.ID = "occurrence:v1:pagerduty-crash"
	configurePagerDutyFixture(t, fixture, `["operational"]`)
	materializer, err := NewNativeMaterializer(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	crash := errors.New("injected crash after PagerDuty domain commit")
	materializer.afterDomainCommit = func() error { return crash }
	if _, err := materializeAndCommit(t, fixture, materializer, fixture.occurrence); !errors.Is(err, crash) {
		t.Fatalf("materialize=%v, want crash", err)
	}
	var sources, units int
	if err := fixture.pool.QueryRow(context.Background(), `SELECT count(*) FROM integration_sources WHERE is_enabled`).Scan(&sources); err != nil {
		t.Fatal(err)
	}
	if err := fixture.pool.QueryRow(context.Background(), `SELECT count(*) FROM sync_run_units`).Scan(&units); err != nil {
		t.Fatal(err)
	}
	if sources != 1 || units != len(pagerDutyOperationalDatasets) {
		t.Fatalf("domain transaction incomplete: sources=%d units=%d", sources, units)
	}
	materializer.afterDomainCommit = nil
	if _, err := materializeAndCommit(t, fixture, materializer, fixture.occurrence); err != nil {
		t.Fatal(err)
	}
}

func TestNativeMaterializerTerminalizesMalformedPagerDutyConfiguration(t *testing.T) {
	fixture := startMaterializerPostgres(t)
	configurePagerDutyFixture(t, fixture, `["incidents"]`)
	materializer, err := NewNativeMaterializer(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := materializeAndCommit(t, fixture, materializer, fixture.occurrence)
	if err != nil {
		t.Fatal(err)
	}
	var runStatus string
	var units, jobStatus int
	var active bool
	if err := fixture.pool.QueryRow(context.Background(), `SELECT status,total_units FROM sync_runs WHERE id=$1::uuid`, plan.SyncRunID).Scan(&runStatus, &units); err != nil {
		t.Fatal(err)
	}
	if err := fixture.pool.QueryRow(context.Background(), `SELECT status FROM job_runs WHERE id=$1::uuid`, plan.JobRunID).Scan(&jobStatus); err != nil {
		t.Fatal(err)
	}
	if err := fixture.pool.QueryRow(context.Background(), `SELECT is_active FROM sync_configurations WHERE id=$1::uuid`, fixture.occurrence.ConfigID).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if runStatus != "failed" || units != 0 || jobStatus != 3 || active {
		t.Fatalf("terminal PagerDuty state: run=%q units=%d job=%d active=%v", runStatus, units, jobStatus, active)
	}
	for _, table := range []string{"sync_run_reference_discoveries", "sync_dispatch_outbox"} {
		var count int
		if err := fixture.pool.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Errorf("terminal plan wrote %s", table)
		}
	}
}

func TestNativeMaterializerLeavesInactivePagerDutyCredentialAndConfigUntouched(t *testing.T) {
	fixture := startMaterializerPostgres(t)
	configurePagerDutyFixture(t, fixture, `["operational"]`)
	for _, statement := range []string{
		`UPDATE integration_credentials SET is_active=FALSE WHERE org_id=$1`,
		`INSERT INTO integration_sources
 (id,org_id,integration_id,provider,source_type,external_id,name,full_name,is_enabled,metadata,discovered_at,last_seen_at)
VALUES ('00000000-0000-4000-8000-000000001010',$1,(SELECT integration_id FROM sync_configurations LIMIT 1),'pagerduty','account','stale-account','stale-account','stale-account',TRUE,'{}',now(),now())`,
		`INSERT INTO integration_datasets
 (id,org_id,integration_id,dataset_key,is_enabled,options)
		 VALUES ('00000000-0000-4000-8000-000000001011',$1,(SELECT integration_id FROM sync_configurations LIMIT 1),'incidents',TRUE,'{"sentinel":"preserve"}')`,
	} {
		if _, err := fixture.pool.Exec(context.Background(), statement, fixture.occurrence.OrgID); err != nil {
			t.Fatal(err)
		}
	}
	materializer, err := NewNativeMaterializer(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := materializeAndCommit(t, fixture, materializer, fixture.occurrence)
	if err != nil {
		t.Fatal(err)
	}
	var active bool
	var units, enabledSources, enabledDatasets int
	var credential, auth *string
	if err := fixture.pool.QueryRow(context.Background(), `SELECT is_active FROM sync_configurations WHERE id=$1::uuid`, fixture.occurrence.ConfigID).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if err := fixture.pool.QueryRow(context.Background(), `SELECT total_units,credential_id::text,auth_source FROM sync_runs WHERE id=$1::uuid`, plan.SyncRunID).Scan(&units, &credential, &auth); err != nil {
		t.Fatal(err)
	}
	if err := fixture.pool.QueryRow(context.Background(), `SELECT count(*) FROM integration_sources WHERE is_enabled`).Scan(&enabledSources); err != nil {
		t.Fatal(err)
	}
	if err := fixture.pool.QueryRow(context.Background(), `SELECT count(*) FROM integration_datasets WHERE is_enabled AND options->>'sentinel'='preserve'`).Scan(&enabledDatasets); err != nil {
		t.Fatal(err)
	}
	if !active || units != 0 || credential != nil || auth != nil || enabledSources != 1 || enabledDatasets != 1 {
		t.Fatalf("inactive credential mutated config/inventory/plan: active=%v units=%d credential=%v auth=%v sources=%d datasets=%d", active, units, credential, auth, enabledSources, enabledDatasets)
	}
}

func TestNativeMaterializerGatesIncidentTargetBeforePagerDutyRepair(t *testing.T) {
	fixture := startMaterializerPostgres(t)
	configurePagerDutyFixture(t, fixture, `["incidents"]`)
	if _, err := fixture.pool.Exec(context.Background(), `UPDATE feature_flags SET is_enabled=FALSE`); err != nil {
		t.Fatal(err)
	}
	materializer, err := NewNativeMaterializer(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := materializeAndCommit(t, fixture, materializer, fixture.occurrence); !errors.Is(err, ErrOccurrenceIneligible) {
		t.Fatalf("feature-disabled materialize error=%v, want ineligible", err)
	}
	var active bool
	if err := fixture.pool.QueryRow(context.Background(), `SELECT is_active FROM sync_configurations WHERE id=$1::uuid`, fixture.occurrence.ConfigID).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if !active {
		t.Fatal("PagerDuty repair ran before feature gate and disabled config")
	}
}

func TestNativeMaterializerRevalidatesScheduleAndOrganizationEligibility(t *testing.T) {
	fixture := startMaterializerPostgres(t)
	materializer, err := NewNativeMaterializer(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(context.Background(), `UPDATE sync_configurations SET sync_options='{}'`); err != nil {
		t.Fatal(err)
	}
	if _, err := materializeAndCommit(t, fixture, materializer, fixture.occurrence); !errors.Is(err, ErrOccurrenceIneligible) {
		t.Fatalf("manual-only configuration error=%v, want ineligible", err)
	}
	for _, statement := range []string{
		`UPDATE sync_configurations SET sync_options='{"schedule_cron":"0 * * * *"}'`,
		`DELETE FROM organizations`,
	} {
		if _, err := fixture.pool.Exec(context.Background(), statement); err != nil {
			t.Fatal(err)
		}
	}
	fixture.occurrence.ID = "occurrence:v1:deleted-organization"
	if _, err := materializeAndCommit(t, fixture, materializer, fixture.occurrence); !errors.Is(err, ErrOccurrenceIneligible) {
		t.Fatalf("deleted organization error=%v, want ineligible", err)
	}
	for _, table := range []string{"sync_runs", "sync_run_units", "job_runs", "sync_run_reference_discoveries", "sync_dispatch_outbox"} {
		var count int
		if err := fixture.pool.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Errorf("ineligible plan wrote %s", table)
		}
	}
}

// TestNativeMaterializerRejectsAPendingOccurrenceForANotPlannerManagedConfig
// is the CHAOS-4174 Phase-B companion to the Phase-A red-first proof in
// transaction_integration_test.go. It closes a codex-review finding: Phase
// A's refusal in evaluateContext cannot protect an occurrence that was
// already minted before this gate existed (or minted by an old binary still
// running mid-rollout) -- without a Phase B re-check, that pending occurrence
// would still materialize into a real sync run for a fixture/legacy config.
//
// This simulates exactly that: a pending occurrence for a config whose
// planner_managed column is FALSE, exactly as lockPendingOccurrence would
// read it under lock. Materialize must refuse it and write nothing.
func TestNativeMaterializerRejectsAPendingOccurrenceForANotPlannerManagedConfig(t *testing.T) {
	fixture := startMaterializerPostgres(t)
	materializer, err := NewNativeMaterializer(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(context.Background(),
		`UPDATE sync_configurations SET planner_managed = FALSE`,
	); err != nil {
		t.Fatal(err)
	}
	fixture.occurrence.ID = "occurrence:v1:not-planner-managed"
	fixture.occurrence.ConfigPlannerManaged = false
	if _, err := materializeAndCommit(t, fixture, materializer, fixture.occurrence); !errors.Is(err, ErrOccurrenceIneligible) {
		t.Fatalf("planner_managed=false config error=%v, want ineligible", err)
	}
	for _, table := range []string{"sync_runs", "sync_run_units", "job_runs", "sync_run_reference_discoveries", "sync_dispatch_outbox"} {
		var count int
		if err := fixture.pool.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Errorf("planner_managed=false occurrence wrote %s", table)
		}
	}
}

func TestNativeMaterializerRejectsDeterministicIdentityCollision(t *testing.T) {
	fixture := startMaterializerPostgres(t)
	ids, err := deterministicMaterializationIDs(fixture.occurrence.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(context.Background(), `
INSERT INTO sync_runs (id,org_id,integration_id,triggered_by,mode,status,total_units,completed_units,failed_units,created_at)
VALUES ($1::uuid,'wrong-org',(SELECT integration_id FROM sync_configurations LIMIT 1),'schedule','incremental','planned',0,0,0,now())`, ids.SyncRunID); err != nil {
		t.Fatal(err)
	}
	materializer, err := NewNativeMaterializer(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := materializeAndCommit(t, fixture, materializer, fixture.occurrence); !errors.Is(err, ErrInvalidPlan) {
		t.Fatalf("identity collision error=%v, want ErrInvalidPlan", err)
	}
}

func TestNativeMaterializerConcurrentReplayConverges(t *testing.T) {
	fixture := startMaterializerPostgres(t)
	materializer, err := NewNativeMaterializer(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	results := make(chan PlanResult, 2)
	errs := make(chan error, 2)
	var group sync.WaitGroup
	for range 2 {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			tx, err := fixture.pool.Begin(context.Background())
			if err != nil {
				errs <- err
				results <- PlanResult{}
				return
			}
			plan, err := materializer.Materialize(context.Background(), tx, fixture.occurrence)
			if err == nil {
				err = tx.Commit(context.Background())
			} else {
				_ = tx.Rollback(context.Background())
			}
			results <- plan
			errs <- err
		}()
	}
	close(start)
	group.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var first *PlanResult
	for plan := range results {
		if first == nil {
			copy := plan
			first = &copy
		} else if *first != plan {
			t.Fatalf("concurrent replay diverged: first=%+v other=%+v", *first, plan)
		}
	}
	for table, want := range map[string]int{"sync_runs": 1, "sync_run_units": 2, "job_runs": 1, "sync_run_reference_discoveries": 1, "sync_dispatch_outbox": 1} {
		var got int
		if err := fixture.pool.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("%s count=%d want=%d", table, got, want)
		}
	}
}

// TestApplyDomainPlanMutationsEnsureSecurityDatasetConcurrentInsertConverges
// reproduces CHAOS-4203: the "ensure scheduled security dataset" INSERT at
// materializer.go:1293-1296 computes a fully deterministic id
// (uuid.NewSHA1 over IntegrationID plus a fixed "security" suffix), so two
// concurrent replays of the same occurrence always attempt to insert the
// SAME primary key -- but its ON CONFLICT clause names only the
// (org_id,integration_id,dataset_key) unique constraint as arbiter.
// integration_datasets ALSO has a separate PRIMARY KEY on id (alembic
// 0015_add_integration_data_model.py). Postgres only protects the
// SPECIFIED arbiter: when both inserts reach the server's physical
// index-insertion phase before either commits, the second one raises a
// raw duplicate-key error on the pkey instead of waiting to recheck the
// (unaffected) arbiter, even though the rows are byte-identical. Confirmed
// empirically (not by source inference), including that a SERIALIZED
// handoff (one insert blocks server-side, confirmed via pg_stat_activity,
// before the other commits) does NOT reproduce it -- Postgres's
// arbiter-wait path resolves that case silently every time. Only true
// concurrent racing -- both inserts in flight, unresolved, at once --
// reproduces the bug, which is why TestNativeMaterializerConcurrentReplayConverges
// (using the same free-running two-goroutine shape as production replay)
// only flaked once under CI's heavier scheduler load despite 4/4 clean
// local runs including -race (the isolation evidence in CHAOS-4203).
//
// This test forces that same close-simultaneity attempt every iteration:
// both goroutines open their transaction and signal ready BEFORE either is
// released past the start barrier, so the INSERTs are issued as close to
// simultaneously as the Go scheduler allows -- it guarantees both
// transactions are already open before either INSERT fires, not that the
// two INSERTs' server-side execution actually overlaps. The attempt is
// repeated because a single racing pair only hits the window some of the
// time (measured ~29%, 87/300, in a local probe using this exact shape),
// so pre-fix this test fails
// within the first several attempts and post-fix all 150 must converge
// silently, which is a much stronger guarantee than CI's incidental load
// ever exercised. TestNativeMaterializerConcurrentReplayConverges (still
// passing, unmodified) is what proves the surrounding persistDomainGraph
// writes and the outer domain-transaction commit are unaffected by the
// savepoint recovery -- this test isolates only the vulnerable statement.
func TestApplyDomainPlanMutationsEnsureSecurityDatasetConcurrentInsertConverges(t *testing.T) {
	fixture := startMaterializerPostgres(t)
	ctx := context.Background()

	var integrationID string
	if err := fixture.pool.QueryRow(ctx, `SELECT integration_id::text FROM sync_configurations WHERE id=$1::uuid`, fixture.occurrence.ConfigID).Scan(&integrationID); err != nil {
		t.Fatal(err)
	}
	loaded := loadedMaterializationPlan{
		ensureSecurityDataset: true,
		input:                 PlannerInput{OrgID: fixture.occurrence.OrgID, IntegrationID: integrationID},
	}

	const attempts = 150
	for attempt := range attempts {
		if _, err := fixture.pool.Exec(ctx, `DELETE FROM integration_datasets WHERE org_id=$1 AND integration_id=$2::uuid AND dataset_key='security'`, loaded.input.OrgID, integrationID); err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		errs := make(chan error, 2)
		var ready, group sync.WaitGroup
		ready.Add(2)
		group.Add(2)
		for range 2 {
			go func() {
				defer group.Done()
				tx, err := fixture.pool.Begin(context.Background())
				if err != nil {
					ready.Done()
					errs <- err
					return
				}
				ready.Done()
				<-start
				err = applyDomainPlanMutations(context.Background(), tx, loaded, time.Now())
				if err == nil {
					err = tx.Commit(context.Background())
				} else {
					_ = tx.Rollback(context.Background())
				}
				errs <- err
			}()
		}
		ready.Wait()
		close(start)
		group.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatalf("attempt %d: concurrent replay of the ensure-INSERT for the same (org,integration,dataset) did not converge: %v", attempt, err)
			}
		}
	}

	var rowCount int
	if err := fixture.pool.QueryRow(ctx, `SELECT count(*) FROM integration_datasets WHERE org_id=$1 AND integration_id=$2::uuid AND dataset_key='security'`, loaded.input.OrgID, integrationID).Scan(&rowCount); err != nil {
		t.Fatal(err)
	}
	if rowCount != 1 {
		t.Fatalf("concurrent replay left %d security dataset rows, want 1", rowCount)
	}
}

func TestNativeMaterializerDoesNotHydrateCredentialMetadataForZeroUnitPlan(t *testing.T) {
	fixture := startMaterializerPostgres(t)
	const missingCredential = "00000000-0000-4000-8000-000000009999"
	for _, statement := range []struct {
		sql  string
		args []any
	}{
		{`UPDATE integrations SET provider='linear',credential_id=$1::uuid`, []any{missingCredential}},
		{`UPDATE integration_sources SET provider='linear'`, nil},
		{`UPDATE sync_configurations SET provider='linear',sync_targets='[]'::jsonb`, nil},
		{`DELETE FROM integration_datasets`, nil},
	} {
		if _, err := fixture.pool.Exec(context.Background(), statement.sql, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
	materializer, err := NewNativeMaterializer(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := materializeAndCommit(t, fixture, materializer, fixture.occurrence)
	if err != nil {
		t.Fatal(err)
	}
	var units int
	var credential, auth *string
	if err := fixture.pool.QueryRow(context.Background(), `SELECT total_units,credential_id::text,auth_source FROM sync_runs WHERE id=$1::uuid`, plan.SyncRunID).Scan(&units, &credential, &auth); err != nil {
		t.Fatal(err)
	}
	if units != 0 || credential != nil || auth != nil {
		t.Fatalf("zero-unit plan hydrated auth: units=%d credential=%v auth=%v", units, credential, auth)
	}
}

// suppressAutoSecurityDataset stops loadPlanDatasets from silently adding a
// SECOND candidate (github/security auto-enables unconditionally for every
// github/gitlab integration, CHAOS-3xxx pre-CHAOS-4060 behavior) to any test
// that only means to reason about ONE dataset's executed-proof state. An
// explicit, disabled integration_datasets row for "security" makes
// securityExists true, which is all loadPlanDatasets checks before deciding
// whether to append it -- is_enabled=false keeps it out of the plan the same
// way it always would for an operator-disabled dataset.
func suppressAutoSecurityDataset(t *testing.T, fixture materializerFixture) {
	t.Helper()
	if _, err := fixture.pool.Exec(context.Background(), `
INSERT INTO integration_datasets (id, org_id, integration_id, dataset_key, is_enabled, options)
VALUES (gen_random_uuid(), $1, (SELECT integration_id FROM sync_configurations LIMIT 1), 'security', FALSE, '{}'::jsonb)`,
		fixture.occurrence.OrgID,
	); err != nil {
		t.Fatal(err)
	}
}

// seedPriorSyncRunUnit inserts one terminal sync_run_units row for the
// fixture's sole integration/source, so a later RefreshExecutedProof call has
// real evidence to find -- exercising the query this materializer actually
// runs, not a hand-built evidence map.
func seedPriorSyncRunUnit(t *testing.T, fixture materializerFixture, dataset, status, result string) {
	t.Helper()
	ctx := context.Background()
	runID := "00000000-0000-4000-8000-000000002001"
	if _, err := fixture.pool.Exec(ctx, `
INSERT INTO sync_runs (
	id, org_id, integration_id, triggered_by, mode, status,
	total_units, completed_units, failed_units, created_at
) VALUES (
	$1::uuid, $2, (SELECT integration_id FROM sync_configurations LIMIT 1),
	'scheduled', 'incremental', $3, 1, 1, 0, now()
) ON CONFLICT (id) DO NOTHING`,
		runID, fixture.occurrence.OrgID, status,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `
INSERT INTO sync_run_units (
	id, org_id, sync_run_id, integration_id, source_id, provider, dataset_key,
	cost_class, mode, status, attempts, result, created_at, updated_at
) VALUES (
	gen_random_uuid(), $1, $2::uuid,
	(SELECT integration_id FROM sync_configurations LIMIT 1),
	(SELECT id FROM integration_sources LIMIT 1),
	'github', $3, 'medium', 'incremental', $4, 1, $5, now(), now()
)`,
		fixture.occurrence.OrgID, runID, dataset, status, result,
	); err != nil {
		t.Fatal(err)
	}
	// CHAOS-4114: in production the ledger is maintained BY the write paths --
	// planning stamps attempted, terminalization stamps proven. This helper
	// fabricates an already-terminal unit directly, so it projects it into the
	// ledger with the same statement alembic 0109 uses. Seeding the unit
	// without the ledger would make every gate assertion below read a history
	// the gate cannot see, which is a fixture bug that would look exactly like
	// a gate bug.
	if _, err := fixture.pool.Exec(ctx, providersync.ExecutedProofLedgerBackfillSQL); err != nil {
		t.Fatal(err)
	}
}

// TestNativeMaterializerExecutedProofBootstrapConvergence is the CHAOS-4060
// end-to-end control chris ruled on directly: a never-attempted pair must
// bootstrap through its first plan (not deadlock forever waiting for proof
// it can only earn by being planned), and from there the SAME durable state
// the gate already reads must converge correctly on its own --
// attempted-but-unproven blocks the next cycle, and a later genuine success
// unblocks the cycle after that, permanently. This drives the real wiring
// (RefreshExecutedProof -> materializer.executedProof -> Materialize ->
// PlannerInput.ExecutedProof -> BuildScheduledPlan) through all three
// states, not just the unit-level gate.
func TestNativeMaterializerExecutedProofBootstrapConvergence(t *testing.T) {
	fixture := startMaterializerPostgres(t)
	suppressAutoSecurityDataset(t, fixture)
	materializer, err := NewNativeMaterializer(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	scanTotalUnits := func(runID string) int {
		t.Helper()
		var units int
		if err := fixture.pool.QueryRow(context.Background(),
			`SELECT total_units FROM sync_runs WHERE id=$1::uuid`, runID,
		).Scan(&units); err != nil {
			t.Fatal(err)
		}
		return units
	}
	// Materialize's replay contract deliberately rejects replanning a
	// committed occurrence with a different result
	// (TestNativeMaterializerRejectsReplayAcrossInitializedFieldFamilies),
	// so each converging state below needs its own occurrence identity.
	occurrenceN := func(n int) PendingOccurrence {
		occurrence := fixture.occurrence
		occurrence.ID = fmt.Sprintf("occurrence:v1:scheduled:convergence-%d", n)
		occurrence.ScheduledFor = fixture.occurrence.ScheduledFor.Add(time.Duration(n) * time.Hour)
		return occurrence
	}

	// Step 1: zero sync_run_units history for github/commits anywhere.
	// Never-attempted must bootstrap through.
	if err := materializer.RefreshExecutedProof(context.Background()); err != nil {
		t.Fatal(err)
	}
	plan, err := materializeAndCommit(t, fixture, materializer, occurrenceN(1))
	if err != nil {
		t.Fatal(err)
	}
	if got := scanTotalUnits(plan.SyncRunID); got != 1 {
		t.Fatalf("total_units=%d, want 1: never-attempted must bootstrap through", got)
	}

	// Step 2: that bootstrap attempt resolved WITHOUT ever persisting a
	// row (a failure, or an empty-but-successful window -- either way, no
	// proof). The pair is now attempted-but-unproven and must block.
	seedPriorSyncRunUnit(t, fixture, "commits", "failed", `{"error":"boom"}`)
	if err := materializer.RefreshExecutedProof(context.Background()); err != nil {
		t.Fatal(err)
	}
	plan, err = materializeAndCommit(t, fixture, materializer, occurrenceN(2))
	if err != nil {
		t.Fatal(err)
	}
	if got := scanTotalUnits(plan.SyncRunID); got != 0 {
		t.Fatalf("total_units=%d, want 0: attempted-and-unproven must block", got)
	}

	// Step 3: a later attempt genuinely persists rows. Proven, permanently.
	seedPriorSyncRunUnit(t, fixture, "commits", "success",
		`{"go_provider_route":{"records":4}}`)
	if err := materializer.RefreshExecutedProof(context.Background()); err != nil {
		t.Fatal(err)
	}
	plan, err = materializeAndCommit(t, fixture, materializer, occurrenceN(3))
	if err != nil {
		t.Fatal(err)
	}
	if got := scanTotalUnits(plan.SyncRunID); got != 1 {
		t.Fatalf("total_units=%d, want 1: github/commits now has live executed proof", got)
	}
}

// TestNativeMaterializerRefreshExecutedProofFailsClosedOnQueryError is the
// codex adversarial-review finding this pins: a failed initial evidence load
// must NOT silently restore full pre-CHAOS-4060 permissive behavior for the
// rest of the process's lifetime. Before this test existed, a startup query
// error left executedProof nil, and nil is the documented "gate not wired"
// pass-through -- meaning a transient Postgres hiccup at boot would have
// permitted every unproven route for as long as the process ran.
// TestNativeMaterializerPlanningStampsExecutedProofAttempted is the
// CHAOS-4114 convergence proof, and the red control for the one direction
// this ledger must never fail in.
//
// Before the ledger, "attempted" was whatever a whole-table scan found, so a
// planned unit made its pair attempted the instant the row committed --
// nobody had to remember to record it. The ledger has to be WRITTEN, and if
// the planning path forgets, a pair stays "never attempted" forever and
// bootstraps through every cycle no matter how many times it fails. That is
// fail-OPEN: exactly the CHAOS-4048/4049 shape the gate exists to catch,
// reintroduced by the gate's own evidence source.
//
// So this seeds NOTHING. It plans (bootstrap), refreshes, and requires the
// second occurrence to be blocked purely because the first one recorded its
// own attempt. Delete the stamp in persistDomainGraph and this test plans a
// unit both times.
func TestNativeMaterializerPlanningStampsExecutedProofAttempted(t *testing.T) {
	fixture := startMaterializerPostgres(t)
	suppressAutoSecurityDataset(t, fixture)
	materializer, err := NewNativeMaterializer(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	scanTotalUnits := func(runID string) int {
		t.Helper()
		var units int
		if err := fixture.pool.QueryRow(context.Background(),
			`SELECT total_units FROM sync_runs WHERE id=$1::uuid`, runID,
		).Scan(&units); err != nil {
			t.Fatal(err)
		}
		return units
	}
	occurrenceN := func(n int) PendingOccurrence {
		occurrence := fixture.occurrence
		occurrence.ID = fmt.Sprintf("occurrence:v1:scheduled:attempt-stamp-%d", n)
		occurrence.ScheduledFor = fixture.occurrence.ScheduledFor.Add(time.Duration(n) * time.Hour)
		return occurrence
	}

	if err := materializer.RefreshExecutedProof(context.Background()); err != nil {
		t.Fatal(err)
	}
	plan, err := materializeAndCommit(t, fixture, materializer, occurrenceN(1))
	if err != nil {
		t.Fatal(err)
	}
	if got := scanTotalUnits(plan.SyncRunID); got != 1 {
		t.Fatalf("total_units=%d, want 1: never-attempted must bootstrap through", got)
	}

	// The stamp itself, before any inference from planner behavior: the pair
	// is attempted and, having only been PLANNED, is not proven.
	var attemptedAt time.Time
	var provenAt *time.Time
	if err := fixture.pool.QueryRow(context.Background(), `
SELECT attempted_at, proven_at FROM public.sync_executed_proof_ledger
WHERE provider = 'github' AND dataset_key = 'commits'`,
	).Scan(&attemptedAt, &provenAt); err != nil {
		t.Fatalf("planning left no executed-proof ledger row for github/commits: %v", err)
	}
	if provenAt != nil {
		t.Fatalf("a merely PLANNED unit proved its pair: proven_at=%s", provenAt)
	}

	// And the behavior that fact buys: the next cycle blocks, with nothing
	// seeded and no terminalization ever having happened.
	if err := materializer.RefreshExecutedProof(context.Background()); err != nil {
		t.Fatal(err)
	}
	plan, err = materializeAndCommit(t, fixture, materializer, occurrenceN(2))
	if err != nil {
		t.Fatal(err)
	}
	if got := scanTotalUnits(plan.SyncRunID); got != 0 {
		t.Fatalf("total_units=%d, want 0: the first plan recorded its own attempt, so "+
			"this pair is attempted-and-unproven and must block", got)
	}

	// The planning-volume series, observed against REAL materializations
	// rather than against fields a test poked. TestPlannedUnitSeriesMoveOnReal
	// Materializations proves WritePrometheus reports them; this proves
	// Materialize is what moves them, which is the half a unit test cannot
	// see. The run above is exactly the CHAOS-4124 shape: a pass that
	// completed successfully and planned nothing.
	var metrics bytes.Buffer
	if err := materializer.WritePrometheus(&metrics); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"devhealth_scheduler_planned_units 0",
		"devhealth_scheduler_zero_planned_occurrences_total 1",
		"devhealth_scheduler_materialized_occurrences_total 2",
	} {
		if !strings.Contains(metrics.String(), want+"\n") {
			t.Errorf("materialization metrics missing %q:\n%s", want, metrics.String())
		}
	}
}

func TestNativeMaterializerRefreshExecutedProofFailsClosedOnQueryError(t *testing.T) {
	fixture := startMaterializerPostgres(t)
	materializer, err := NewNativeMaterializer(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}

	// Force the first load to fail: an already-canceled context makes the
	// underlying query return context.Canceled immediately, standing in for
	// any real query failure (permission error, schema mismatch, connection
	// refused -- QueryExecutedProofEvidence returns the same shape of error
	// for all of them).
	failCtx, cancel := context.WithCancel(context.Background())
	cancel()
	refreshErr := materializer.RefreshExecutedProof(failCtx)
	if refreshErr == nil {
		t.Fatal("RefreshExecutedProof against a canceled context unexpectedly succeeded")
	}

	snapshot := materializer.executedProof.Load()
	if snapshot == nil {
		t.Fatal("a failed FIRST load must still install a non-nil snapshot -- fail closed, not open")
	}
	if !snapshot.Degraded {
		t.Fatalf(
			"failed first load snapshot.Degraded=false, want true -- otherwise an "+
				"empty Proven/Attempted shape reads as a healthy, never-attempted "+
				"database and bootstraps every pair through: snapshot=%+v", snapshot,
		)
	}

	// The gate is now enforced: github/commits carries no waiver and has no
	// evidence, so it must not plan -- exactly the outcome that must survive
	// a query failure, not the permissive nil-evidence pass-through.
	plan, err := materializeAndCommit(t, fixture, materializer, fixture.occurrence)
	if err != nil {
		t.Fatal(err)
	}
	var units int
	if err := fixture.pool.QueryRow(context.Background(),
		`SELECT total_units FROM sync_runs WHERE id=$1::uuid`, plan.SyncRunID,
	).Scan(&units); err != nil {
		t.Fatal(err)
	}
	if units != 0 {
		t.Fatalf(
			"total_units=%d, want 0: a failed initial evidence load must fail the gate closed, not open",
			units,
		)
	}
}

// TestNativeMaterializerRefreshExecutedProofDegradesAfterALaterFailureToo is
// the codex round-4 finding this pins: my earlier fix only forced a
// Degraded snapshot on the very FIRST failed load ("if executedProof.Load()
// == nil"). A LATER refresh failure -- after a real, successful, but
// legitimately EMPTY load -- left that stale empty snapshot in place
// unchanged, and empty now means "never attempted, bootstrap through": an
// outage starting AFTER a clean boot would have kept authorizing every
// never-attempted pair on the strength of a reading that could no longer be
// trusted. This drives a real success (empty, non-degraded) followed by a
// real failure and asserts the pair that would have bootstrapped through is
// blocked once the snapshot is degraded -- and that a genuinely PROVEN pair
// keeps passing regardless, since that fact does not stop being true.
func TestNativeMaterializerRefreshExecutedProofDegradesAfterALaterFailureToo(t *testing.T) {
	fixture := startMaterializerPostgres(t)
	suppressAutoSecurityDataset(t, fixture)
	materializer, err := NewNativeMaterializer(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	scanTotalUnits := func(runID string) int {
		t.Helper()
		var units int
		if err := fixture.pool.QueryRow(context.Background(),
			`SELECT total_units FROM sync_runs WHERE id=$1::uuid`, runID,
		).Scan(&units); err != nil {
			t.Fatal(err)
		}
		return units
	}

	// A real, healthy, but genuinely empty first load: github/commits has
	// zero history, so it bootstraps through.
	if err := materializer.RefreshExecutedProof(context.Background()); err != nil {
		t.Fatal(err)
	}
	if snapshot := materializer.executedProof.Load(); snapshot == nil || snapshot.Degraded {
		t.Fatalf("first load snapshot = %+v, want a healthy (non-degraded) snapshot", snapshot)
	}
	plan, err := materializeAndCommit(t, fixture, materializer, fixture.occurrence)
	if err != nil {
		t.Fatal(err)
	}
	if got := scanTotalUnits(plan.SyncRunID); got != 1 {
		t.Fatalf("total_units=%d, want 1: never-attempted must bootstrap through on a healthy snapshot", got)
	}

	// Now the query itself starts failing. The stale "empty" reading can no
	// longer be trusted as a true "never attempted".
	failCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := materializer.RefreshExecutedProof(failCtx); err == nil {
		t.Fatal("RefreshExecutedProof against a canceled context unexpectedly succeeded")
	}
	snapshot := materializer.executedProof.Load()
	if snapshot == nil || !snapshot.Degraded {
		t.Fatalf(
			"a LATER refresh failure (after a healthy load) must also degrade the snapshot: snapshot=%+v",
			snapshot,
		)
	}
	secondOccurrence := fixture.occurrence
	secondOccurrence.ID = "occurrence:v1:scheduled:degraded-after-healthy"
	secondOccurrence.ScheduledFor = fixture.occurrence.ScheduledFor.Add(time.Hour)
	plan, err = materializeAndCommit(t, fixture, materializer, secondOccurrence)
	if err != nil {
		t.Fatal(err)
	}
	if got := scanTotalUnits(plan.SyncRunID); got != 0 {
		t.Fatalf(
			"total_units=%d, want 0: a degraded snapshot must not keep bootstrapping a "+
				"never-attempted pair through on a reading it can no longer trust",
			got,
		)
	}
}

// TestNativeMaterializerMaybeRefreshExecutedProofUnblocksWithoutRestart is the
// second codex adversarial-review finding this pins: a load-once-at-startup
// snapshot alone can never observe evidence that appears after the process
// booted, so a route earning its first live proof would stay gated for the
// rest of the process's lifetime -- an operator would have to bounce the
// scheduler to unstick it. maybeRefreshExecutedProof's bounded periodic
// reload closes that gap; this drives it through Materialize with a fake
// clock, asserting BOTH that it stays quiet inside the refresh window (no
// thundering-herd query on every occurrence) and that it actually unblocks
// once the window elapses, with no manual RefreshExecutedProof call at all.
func TestNativeMaterializerMaybeRefreshExecutedProofUnblocksWithoutRestart(t *testing.T) {
	fixture := startMaterializerPostgres(t)
	suppressAutoSecurityDataset(t, fixture)
	materializer, err := NewNativeMaterializer(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	materializer.executedProofRefreshInterval = time.Minute
	clock := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	materializer.now = func() time.Time { return clock }

	// Seed an ATTEMPTED-but-unproven row up front, so this pair starts
	// genuinely blocked rather than bootstrapping through on its first
	// occurrence -- that keeps this test about the refresh throttle, not
	// about the separate bootstrap-convergence property
	// (TestNativeMaterializerExecutedProofBootstrapConvergence covers that).
	seedPriorSyncRunUnit(t, fixture, "commits", "failed", `{"error":"boom"}`)

	// Opt into the gate the same way production does: one explicit initial
	// load. At this instant github/commits is attempted but unproven.
	if err := materializer.RefreshExecutedProof(context.Background()); err != nil {
		t.Fatal(err)
	}
	plan, err := materializeAndCommit(t, fixture, materializer, fixture.occurrence)
	if err != nil {
		t.Fatal(err)
	}
	var units int
	scanTotalUnits := func(runID string) int {
		t.Helper()
		if err := fixture.pool.QueryRow(context.Background(),
			`SELECT total_units FROM sync_runs WHERE id=$1::uuid`, runID,
		).Scan(&units); err != nil {
			t.Fatal(err)
		}
		return units
	}
	if got := scanTotalUnits(plan.SyncRunID); got != 0 {
		t.Fatalf("total_units=%d, want 0: attempted and not yet proven", got)
	}

	// Real evidence now exists, but the clock has not moved: a second
	// occurrence inside the same refresh window must NOT see it yet --
	// maybeRefreshExecutedProof is deliberately throttled, not called fresh
	// on every occurrence.
	seedPriorSyncRunUnit(t, fixture, "commits", "success",
		`{"go_provider_route":{"records":7}}`)
	stillWithinWindow := fixture.occurrence
	stillWithinWindow.ID = "occurrence:v1:scheduled:within-window"
	stillWithinWindow.ScheduledFor = fixture.occurrence.ScheduledFor.Add(time.Hour)
	plan, err = materializeAndCommit(t, fixture, materializer, stillWithinWindow)
	if err != nil {
		t.Fatal(err)
	}
	if got := scanTotalUnits(plan.SyncRunID); got != 0 {
		t.Fatalf(
			"total_units=%d, want 0: still inside the refresh window, evidence must not be visible yet",
			got,
		)
	}

	// Advance the clock past the refresh interval and materialize a third
	// occurrence with NO manual RefreshExecutedProof call -- this is the
	// exact "unblocks without a restart" behavior the finding asked for.
	clock = clock.Add(2 * time.Minute)
	afterWindow := fixture.occurrence
	afterWindow.ID = "occurrence:v1:scheduled:after-window"
	afterWindow.ScheduledFor = fixture.occurrence.ScheduledFor.Add(2 * time.Hour)
	plan, err = materializeAndCommit(t, fixture, materializer, afterWindow)
	if err != nil {
		t.Fatal(err)
	}
	if got := scanTotalUnits(plan.SyncRunID); got != 1 {
		t.Fatalf(
			"total_units=%d, want 1: the refresh window elapsed, so Materialize should have "+
				"picked up the new evidence on its own",
			got,
		)
	}
}

//go:build integration

package sync

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// A Jira work-items dataset whose own query is not bounded by the window (fetch_all, or a JQL override) runs
// that full query in every unit, so planning it as bounded windows would repeat the whole query once per
// window. It is planned as ONE window, as before. A Jira dataset without those options is planned in bounded
// windows like every other work-items provider.
func scheduledJiraUnitSpans(t *testing.T, datasetOptions string) []time.Duration {
	t.Helper()
	ctx := context.Background()
	fixture := startMaterializerPostgres(t)
	const (
		integrationID = "00000000-0000-4000-8000-000000009002"
		configID      = "00000000-0000-4000-8000-000000009001"
		sourceID      = "00000000-0000-4000-8000-000000009003"
		datasetID     = "00000000-0000-4000-8000-000000009004"
		jobID         = "00000000-0000-4000-8000-000000009005"
	)
	orgID := fixture.occurrence.OrgID
	scheduledFor := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	occurrenceID := "occurrence:v1:scheduled-jira-full-query"
	statements := []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO integrations (id,org_id,provider,name,config,is_active,created_at,updated_at) VALUES ($1::uuid,$2,'jira','integration-'||$1::text,'{}'::json,TRUE,now(),now())`, []any{integrationID, orgID}},
		{`INSERT INTO sync_configurations (id,org_id,name,sync_targets,sync_options,integration_id,is_active,source_id,planner_managed,provider,created_at,updated_at) VALUES ($1::uuid,$2,'config-'||$1::text,'[]'::json,'{"schedule_cron":"0 * * * *"}'::json,$3::uuid,TRUE,NULL,TRUE,'jira',now(),now())`, []any{configID, orgID, integrationID}},
		{fmt.Sprintf(`INSERT INTO integration_sources (id,org_id,integration_id,provider,source_type,external_id,name,full_name,is_enabled,metadata,discovered_at,last_seen_at) VALUES ($1::uuid,$2,$3::uuid,'jira','project','PROJ','PROJ','PROJ',TRUE,'{"planner_managed_sync_config_id":"%s"}'::jsonb,now(),now())`, configID), []any{sourceID, orgID, integrationID}},
		{`INSERT INTO integration_datasets (id,org_id,integration_id,dataset_key,is_enabled,options) VALUES ($1::uuid,$2,$3::uuid,'work-items',TRUE,$4::json)`, []any{datasetID, orgID, integrationID, datasetOptions}},
		{`INSERT INTO scheduled_jobs (id,org_id,name,sync_config_id,job_type,schedule_cron,timezone,status,is_running,created_at,updated_at) VALUES ($1::uuid,$2,'job-'||$1::text,$3::uuid,'sync','0 * * * *','UTC',0,FALSE,now(),now())`, []any{jobID, orgID, configID}},
		{`INSERT INTO scheduled_sync_occurrences (occurrence_id,identity_version,org_id,sync_config_id,scheduled_job_id,scheduled_for,reconcile_status) VALUES ($1,$2,$3,$4::uuid,$5::uuid,$6,'pending')`,
			[]any{occurrenceID, OccurrenceIdentityVersion, orgID, configID, jobID, scheduledFor}},
	}
	for _, statement := range statements {
		if _, err := fixture.pool.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
	materializer, err := NewNativeMaterializer(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := materializeAndCommit(t, fixture, materializer, PendingOccurrence{
		ID: occurrenceID, IdentityVersion: OccurrenceIdentityVersion,
		OrgID: orgID, ConfigID: configID, JobID: jobID, ScheduledFor: scheduledFor,
		ConfigActive: true, ConfigPlannerManaged: true, JobStatus: 0, JobType: "sync",
	})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := fixture.pool.Query(ctx, `SELECT since_at, before_at FROM sync_run_units WHERE sync_run_id = $1::uuid AND dataset_key = 'work-items'`, plan.SyncRunID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	spans := []time.Duration{}
	for rows.Next() {
		var since, before time.Time
		if err := rows.Scan(&since, &before); err != nil {
			t.Fatal(err)
		}
		spans = append(spans, before.Sub(since))
	}
	return spans
}

func TestNativeMaterializerPlansAJiraFullQueryDatasetAsOneWindow(t *testing.T) {
	for name, options := range map[string]string{
		"fetch_all":    `{"fetch_all": true}`,
		"jql_override": `{"jql_override": "project = PROJ ORDER BY updated DESC"}`,
	} {
		t.Run(name, func(t *testing.T) {
			spans := scheduledJiraUnitSpans(t, options)
			if len(spans) != 1 || spans[0] < 30*24*time.Hour {
				t.Fatalf("spans = %v, want one unit holding the whole initial depth (a full-query dataset repeats its query in every window)", spans)
			}
		})
	}
}

func TestNativeMaterializerPlansAJiraDatasetWithoutFullQueryInABoundedWindow(t *testing.T) {
	spans := scheduledJiraUnitSpans(t, `{}`)
	if len(spans) != 1 || spans[0] > 12*24*time.Hour {
		t.Fatalf("spans = %v, want one capped window", spans)
	}
}

//go:build integration

package sync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// incidentGateConfig is one planner-managed whole-integration configuration of
// a provider, with one enabled source and the named enabled dataset rows,
// seeded beside the shared materializer fixture's organization.
type incidentGateConfig struct {
	fixture       materializerFixture
	provider      string
	integrationID string
	occurrence    PendingOccurrence
}

const (
	incidentGateIntegrationID = "00000000-0000-4000-8000-000000008802"
	incidentGateConfigID      = "00000000-0000-4000-8000-000000008801"
	incidentGateSourceID      = "00000000-0000-4000-8000-000000008803"
	incidentGateJobID         = "00000000-0000-4000-8000-000000008805"
)

func startIncidentGateConfig(t *testing.T, provider string, enabledDatasets, disabledDatasets []string) incidentGateConfig {
	t.Helper()
	fixture := startMaterializerPostgres(t)
	ctx := context.Background()
	orgID := fixture.occurrence.OrgID
	sourceType, externalID := "repository", "full-chaos/incident-gate"
	if provider == "jira" || provider == "linear" {
		sourceType, externalID = "project", "GATE"
	}
	statements := []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO integrations (id,org_id,provider,name,config,is_active,created_at,updated_at) VALUES ($1::uuid,$2,$3,'integration-'||$1::text,'{}'::json,TRUE,now(),now())`, []any{incidentGateIntegrationID, orgID, provider}},
		{`INSERT INTO sync_configurations (id,org_id,name,sync_targets,sync_options,integration_id,is_active,source_id,planner_managed,provider,created_at,updated_at) VALUES ($1::uuid,$2,'config-'||$1::text,'[]'::json,'{"schedule_cron":"0 * * * *"}'::json,$3::uuid,TRUE,NULL,TRUE,$4,now(),now())`, []any{incidentGateConfigID, orgID, incidentGateIntegrationID, provider}},
		{fmt.Sprintf(`INSERT INTO integration_sources (id,org_id,integration_id,provider,source_type,external_id,name,full_name,is_enabled,metadata,discovered_at,last_seen_at) VALUES ($1::uuid,$2,$3::uuid,$4,$5,$6,$6,$6,TRUE,'{"planner_managed_sync_config_id":"%s"}'::jsonb,now(),now())`, incidentGateConfigID), []any{incidentGateSourceID, orgID, incidentGateIntegrationID, provider, sourceType, externalID}},
		{`INSERT INTO scheduled_jobs (id,org_id,name,sync_config_id,job_type,schedule_cron,timezone,status,is_running,created_at,updated_at) VALUES ($1::uuid,$2,'job-'||$1::text,$3::uuid,'sync','0 * * * *','UTC',0,FALSE,now(),now())`, []any{incidentGateJobID, orgID, incidentGateConfigID}},
	}
	for _, key := range enabledDatasets {
		statements = append(statements, struct {
			sql  string
			args []any
		}{`INSERT INTO integration_datasets (id,org_id,integration_id,dataset_key,is_enabled,options) VALUES (gen_random_uuid(),$1,$2::uuid,$3,TRUE,'{}'::json)`, []any{orgID, incidentGateIntegrationID, key}})
	}
	for _, key := range disabledDatasets {
		statements = append(statements, struct {
			sql  string
			args []any
		}{`INSERT INTO integration_datasets (id,org_id,integration_id,dataset_key,is_enabled,options) VALUES (gen_random_uuid(),$1,$2::uuid,$3,FALSE,'{}'::json)`, []any{orgID, incidentGateIntegrationID, key}})
	}
	for _, statement := range statements {
		if _, err := fixture.pool.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
	return incidentGateConfig{
		fixture: fixture, provider: provider, integrationID: incidentGateIntegrationID,
		occurrence: PendingOccurrence{
			ID: "occurrence:v1:incident-gate", IdentityVersion: OccurrenceIdentityVersion,
			OrgID: orgID, ConfigID: incidentGateConfigID, JobID: incidentGateJobID,
			ScheduledFor: time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC),
			ConfigActive: true, ConfigPlannerManaged: true, JobStatus: 0, JobType: "sync",
		},
	}
}

func (config incidentGateConfig) setIncidentFeature(t *testing.T, enabled bool) {
	t.Helper()
	tag, err := config.fixture.pool.Exec(context.Background(),
		`UPDATE feature_flags SET is_enabled=$1 WHERE key='canonical_incident_ingestion'`, enabled)
	if err != nil {
		t.Fatal(err)
	}
	if tag.RowsAffected() != 1 {
		t.Fatalf("canonical_incident_ingestion feature rows changed = %d, want 1", tag.RowsAffected())
	}
}

// addManualTrigger makes the occurrence a manual one: the occurrence row and
// its sync_manual_triggers row, in the shape "Sync now" (incremental) and
// `dho backfill run` (backfill, with dataset keys) write.
func (config incidentGateConfig) addManualTrigger(t *testing.T, mode, triggeredBy string, datasetKeys []string) {
	t.Helper()
	ctx := context.Background()
	if _, err := config.fixture.pool.Exec(ctx, `
INSERT INTO scheduled_sync_occurrences (occurrence_id,identity_version,org_id,sync_config_id,scheduled_job_id,scheduled_for,reconcile_status)
VALUES ($1,$2,$3,$4::uuid,$5::uuid,$6,'pending')`,
		config.occurrence.ID, OccurrenceIdentityVersion, config.occurrence.OrgID,
		config.occurrence.ConfigID, config.occurrence.JobID, config.occurrence.ScheduledFor); err != nil {
		t.Fatal(err)
	}
	var since, before *time.Time
	if mode == SyncModeBackfill {
		start := config.occurrence.ScheduledFor.Add(-7 * 24 * time.Hour)
		since, before = &start, &config.occurrence.ScheduledFor
	}
	if _, err := config.fixture.pool.Exec(ctx, `
INSERT INTO sync_manual_triggers (occurrence_id,mode,since,before,dataset_keys,triggered_by)
VALUES ($1,$2,$3,$4,$5,$6)`,
		config.occurrence.ID, mode, since, before, datasetKeys, triggeredBy); err != nil {
		t.Fatal(err)
	}
}

// mintedUnits returns the count of sync_run_units rows of the integration, by
// dataset key: the units that exist in Postgres after the plan committed.
func (config incidentGateConfig) mintedUnits(t *testing.T) map[string]int {
	t.Helper()
	rows, err := config.fixture.pool.Query(context.Background(), `
SELECT dataset_key, count(*) FROM sync_run_units WHERE integration_id=$1::uuid GROUP BY dataset_key`, config.integrationID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	units := map[string]int{}
	for rows.Next() {
		var key string
		var count int
		if err := rows.Scan(&key, &count); err != nil {
			t.Fatal(err)
		}
		units[key] = count
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return units
}

func (config incidentGateConfig) syncRuns(t *testing.T) int {
	t.Helper()
	var runs int
	if err := config.fixture.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM sync_runs WHERE integration_id=$1::uuid`, config.integrationID).Scan(&runs); err != nil {
		t.Fatal(err)
	}
	return runs
}

// captureIncidentGateSignals routes the default logger to a buffer and clears
// the plan-gate counters, so one plan's WARN and counts can be read exactly.
func captureIncidentGateSignals(t *testing.T) *bytes.Buffer {
	t.Helper()
	var logs bytes.Buffer
	original := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(original) })
	globalPlanGateTelemetry.resetForTest()
	return &logs
}

func skipWarnLines(logs *bytes.Buffer) []string {
	var lines []string
	for _, line := range strings.Split(logs.String(), "\n") {
		if strings.Contains(line, `"msg":"`+planIncidentDatasetsSkippedEvent+`"`) {
			lines = append(lines, line)
		}
	}
	return lines
}

func otherUnits(units map[string]int) map[string]int {
	others := map[string]int{}
	for key, count := range units {
		if key != "incidents" {
			others[key] = count
		}
	}
	return others
}

func equalUnitCounts(left, right map[string]int) bool {
	if len(left) != len(right) {
		return false
	}
	for key, count := range left {
		if right[key] != count {
			return false
		}
	}
	return true
}

// incidentGateProviders are the providers whose catalogue holds a dataset that
// needs the canonical-incident feature beside datasets that do not (pagerduty:
// every dataset needs it, so nothing is left to plan; github and linear: no
// such dataset; see the tests below).
var incidentGateProviders = []struct {
	provider string
	others   []string
}{
	{provider: "jira", others: []string{"work-items"}},
	{provider: "gitlab", others: []string{"commits", "prs"}},
}

// The state the ticket is about: the incident dataset row is ON and the
// incident feature is OFF. The other datasets of the configuration are planned
// and their units are minted; no unit exists for the incident dataset; the
// skip is counted and written once. With the feature ON the incident dataset
// is planned as before.
func TestNativeMaterializerSkipsOnlyTheIncidentDatasetWhenTheIncidentFeatureIsOff(t *testing.T) {
	for _, tc := range incidentGateProviders {
		t.Run(tc.provider, func(t *testing.T) {
			enabled := append([]string{"incidents"}, tc.others...)

			// Feature ON: the reference plan.
			on := startIncidentGateConfig(t, tc.provider, enabled, nil)
			onLogs := captureIncidentGateSignals(t)
			materializer, err := NewNativeMaterializer(on.fixture.pool)
			if err != nil {
				t.Fatal(err)
			}
			onPlan, err := materializeAndCommit(t, on.fixture, materializer, on.occurrence)
			if err != nil {
				t.Fatalf("feature ON: %v", err)
			}
			onUnits := on.mintedUnits(t)
			if onUnits["incidents"] < 1 {
				t.Fatalf("feature ON: incidents units = %d, want at least 1 (units: %v)", onUnits["incidents"], onUnits)
			}
			for _, other := range tc.others {
				if onUnits[other] < 1 {
					t.Fatalf("feature ON: %s units = %d, want at least 1 (units: %v)", other, onUnits[other], onUnits)
				}
			}
			if got := globalPlanGateTelemetry.snapshotForTest(tc.provider, "incidents", planGateOutcomeFeatureDisabled); got != 0 {
				t.Fatalf("feature ON: feature_disabled count = %d, want 0", got)
			}
			if lines := skipWarnLines(onLogs); len(lines) != 0 {
				t.Fatalf("feature ON: unexpected skip WARN:\n%s", strings.Join(lines, "\n"))
			}

			// Feature OFF, same rows.
			off := startIncidentGateConfig(t, tc.provider, enabled, nil)
			off.setIncidentFeature(t, false)
			offLogs := captureIncidentGateSignals(t)
			offMaterializer, err := NewNativeMaterializer(off.fixture.pool)
			if err != nil {
				t.Fatal(err)
			}
			offPlan, err := materializeAndCommit(t, off.fixture, offMaterializer, off.occurrence)
			if err != nil {
				t.Fatalf("feature OFF with the incident row ON: the other datasets must be planned, got %v", err)
			}
			offUnits := off.mintedUnits(t)
			if offUnits["incidents"] != 0 {
				t.Fatalf("feature OFF: incidents units = %d, want 0 (units: %v)", offUnits["incidents"], offUnits)
			}
			if !equalUnitCounts(offUnits, otherUnits(onUnits)) {
				t.Fatalf("feature OFF: minted units = %v, want the feature-ON units without incidents = %v", offUnits, otherUnits(onUnits))
			}
			wantPlanned := onPlan.PlannedUnits - onUnits["incidents"]
			if offPlan.PlannedUnits != wantPlanned || wantPlanned < 1 {
				t.Fatalf("feature OFF: planned units = %d, want %d (at least 1)", offPlan.PlannedUnits, wantPlanned)
			}
			var totalUnits int
			var status string
			if err := off.fixture.pool.QueryRow(context.Background(),
				`SELECT total_units,status FROM sync_runs WHERE id=$1::uuid`, offPlan.SyncRunID).Scan(&totalUnits, &status); err != nil {
				t.Fatal(err)
			}
			if totalUnits != wantPlanned || status != "planned" {
				t.Fatalf("feature OFF: sync run total_units=%d status=%q, want %d planned", totalUnits, status, wantPlanned)
			}
			if got := globalPlanGateTelemetry.snapshotForTest(tc.provider, "incidents", planGateOutcomeFeatureDisabled); got != 1 {
				t.Fatalf("feature OFF: feature_disabled count for incidents = %d, want 1", got)
			}
			for _, other := range tc.others {
				if got := globalPlanGateTelemetry.snapshotForTest(tc.provider, other, planGateOutcomeFeatureDisabled); got != 0 {
					t.Fatalf("feature OFF: feature_disabled count for %s = %d, want 0", other, got)
				}
			}
			lines := skipWarnLines(offLogs)
			if len(lines) != 1 {
				t.Fatalf("feature OFF: skip WARN lines = %d, want 1:\n%s", len(lines), offLogs.String())
			}
			for _, want := range []string{
				`"level":"WARN"`, `"provider":"` + tc.provider + `"`, `"reason":"feature_disabled"`,
				`"feature_decision_reason":"global_disabled"`, `"skipped_datasets":1`,
				`"skipped_dataset_keys":["incidents"]`, `"integration_id":"` + incidentGateIntegrationID + `"`,
			} {
				if !strings.Contains(lines[0], want) {
					t.Fatalf("skip WARN lacks %s:\n%s", want, lines[0])
				}
			}
		})
	}
}

// A dataset row that is OFF is never planned and never reported: the gate
// reads enabled rows only, so a feature that is off costs such a
// configuration nothing.
func TestNativeMaterializerIncidentFeatureOffWithTheIncidentRowOffPlansTheOtherDatasetsQuietly(t *testing.T) {
	for _, tc := range incidentGateProviders {
		t.Run(tc.provider, func(t *testing.T) {
			config := startIncidentGateConfig(t, tc.provider, tc.others, []string{"incidents"})
			config.setIncidentFeature(t, false)
			logs := captureIncidentGateSignals(t)
			materializer, err := NewNativeMaterializer(config.fixture.pool)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := materializeAndCommit(t, config.fixture, materializer, config.occurrence); err != nil {
				t.Fatal(err)
			}
			units := config.mintedUnits(t)
			if units["incidents"] != 0 {
				t.Fatalf("incidents units = %d, want 0", units["incidents"])
			}
			for _, other := range tc.others {
				if units[other] < 1 {
					t.Fatalf("%s units = %d, want at least 1 (units: %v)", other, units[other], units)
				}
			}
			if got := globalPlanGateTelemetry.snapshotForTest(tc.provider, "incidents", planGateOutcomeFeatureDisabled); got != 0 {
				t.Fatalf("feature_disabled count = %d, want 0", got)
			}
			if lines := skipWarnLines(logs); len(lines) != 0 {
				t.Fatalf("unexpected skip WARN:\n%s", strings.Join(lines, "\n"))
			}
		})
	}
}

// When every dataset the occurrence would plan needs the feature, nothing is
// left: the occurrence stays ineligible, no run and no unit exist, and the
// skip is still counted and written.
func TestNativeMaterializerIncidentFeatureOffWithOnlyGatedDatasetsPlansNothingAndSaysSo(t *testing.T) {
	for _, tc := range []struct {
		name, provider string
		enabled        []string
		// explicit is the manual selector; nil is a scheduled occurrence.
		explicit []string
	}{
		{name: "jira scheduled, incidents is the only enabled row", provider: "jira", enabled: []string{"incidents"}},
		{name: "jira manual selector names only incidents", provider: "jira", enabled: []string{"incidents", "work-items"}, explicit: []string{"incidents"}},
		{name: "gitlab manual selector names only incidents", provider: "gitlab", enabled: []string{"incidents", "commits"}, explicit: []string{"incidents"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := startIncidentGateConfig(t, tc.provider, tc.enabled, nil)
			if tc.explicit != nil {
				config.addManualTrigger(t, SyncModeIncremental, "manual", tc.explicit)
			}
			config.setIncidentFeature(t, false)
			logs := captureIncidentGateSignals(t)
			materializer, err := NewNativeMaterializer(config.fixture.pool)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := materializeAndCommit(t, config.fixture, materializer, config.occurrence); !errors.Is(err, ErrOccurrenceIneligible) {
				t.Fatalf("materialize error = %v, want ErrOccurrenceIneligible", err)
			}
			if units := config.mintedUnits(t); len(units) != 0 {
				t.Fatalf("units = %v, want none", units)
			}
			if runs := config.syncRuns(t); runs != 0 {
				t.Fatalf("sync runs = %d, want 0", runs)
			}
			if got := globalPlanGateTelemetry.snapshotForTest(tc.provider, "incidents", planGateOutcomeFeatureDisabled); got != 1 {
				t.Fatalf("feature_disabled count = %d, want 1", got)
			}
			lines := skipWarnLines(logs)
			if len(lines) != 1 || !strings.Contains(lines[0], `"skipped_dataset_keys":["incidents"]`) || !strings.Contains(lines[0], `"planned_datasets":0`) {
				t.Fatalf("skip WARN lines = %d, want 1 naming incidents with planned_datasets 0:\n%s", len(lines), logs.String())
			}
		})
	}
}

// The manual paths plan through the same gate: "Sync now" (an incremental
// trigger with no selector) and `dho backfill run` (a backfill trigger that
// names its dataset keys).
func TestNativeMaterializerManualAndBackfillTriggersSkipOnlyTheIncidentDatasetWhenTheIncidentFeatureIsOff(t *testing.T) {
	for _, tc := range incidentGateProviders {
		for _, trigger := range []struct {
			name, mode, triggeredBy string
			selector                bool
		}{
			{name: "sync now", mode: SyncModeIncremental, triggeredBy: "manual"},
			{name: "backfill run", mode: SyncModeBackfill, triggeredBy: "backfill", selector: true},
		} {
			t.Run(tc.provider+"/"+trigger.name, func(t *testing.T) {
				enabled := append([]string{"incidents"}, tc.others...)
				var selector []string
				if trigger.selector {
					selector = slices.Clone(enabled)
				}
				plan := func(featureOn bool) (incidentGateConfig, PlanResult, *bytes.Buffer) {
					config := startIncidentGateConfig(t, tc.provider, enabled, nil)
					config.addManualTrigger(t, trigger.mode, trigger.triggeredBy, selector)
					config.setIncidentFeature(t, featureOn)
					logs := captureIncidentGateSignals(t)
					materializer, err := NewNativeMaterializer(config.fixture.pool)
					if err != nil {
						t.Fatal(err)
					}
					result, err := materializeAndCommit(t, config.fixture, materializer, config.occurrence)
					if err != nil {
						t.Fatalf("feature on=%v: %v", featureOn, err)
					}
					return config, result, logs
				}
				on, _, _ := plan(true)
				onUnits := on.mintedUnits(t)
				if onUnits["incidents"] < 1 || len(otherUnits(onUnits)) < 1 {
					t.Fatalf("feature ON: units = %v, want incidents and at least one other dataset", onUnits)
				}
				off, offPlan, logs := plan(false)
				offUnits := off.mintedUnits(t)
				if offUnits["incidents"] != 0 {
					t.Fatalf("feature OFF: incidents units = %d, want 0 (units: %v)", offUnits["incidents"], offUnits)
				}
				if !equalUnitCounts(offUnits, otherUnits(onUnits)) {
					t.Fatalf("feature OFF: minted units = %v, want %v", offUnits, otherUnits(onUnits))
				}
				var triggeredBy string
				if err := off.fixture.pool.QueryRow(context.Background(),
					`SELECT triggered_by FROM sync_runs WHERE id=$1::uuid`, offPlan.SyncRunID).Scan(&triggeredBy); err != nil {
					t.Fatal(err)
				}
				if triggeredBy != trigger.triggeredBy {
					t.Fatalf("sync run triggered_by = %q, want %q", triggeredBy, trigger.triggeredBy)
				}
				if got := globalPlanGateTelemetry.snapshotForTest(tc.provider, "incidents", planGateOutcomeFeatureDisabled); got != 1 {
					t.Fatalf("feature OFF: feature_disabled count = %d, want 1", got)
				}
				if lines := skipWarnLines(logs); len(lines) != 1 {
					t.Fatalf("feature OFF: skip WARN lines = %d, want 1:\n%s", len(lines), logs.String())
				}
			})
		}
	}
}

// github and linear have no dataset that needs the incident feature: the
// feature being off changes nothing for them, and the gate does not read it.
func TestNativeMaterializerIncidentFeatureOffChangesNothingForProvidersWithNoGatedDataset(t *testing.T) {
	for _, tc := range []struct {
		provider string
		datasets []string
	}{
		{provider: "github", datasets: []string{"commits", "prs"}},
		{provider: "linear", datasets: []string{"work-items"}},
	} {
		t.Run(tc.provider, func(t *testing.T) {
			plan := func(featureOn bool) (map[string]int, *bytes.Buffer) {
				config := startIncidentGateConfig(t, tc.provider, tc.datasets, nil)
				config.setIncidentFeature(t, featureOn)
				logs := captureIncidentGateSignals(t)
				materializer, err := NewNativeMaterializer(config.fixture.pool)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := materializeAndCommit(t, config.fixture, materializer, config.occurrence); err != nil {
					t.Fatalf("feature on=%v: %v", featureOn, err)
				}
				return config.mintedUnits(t), logs
			}
			onUnits, _ := plan(true)
			offUnits, logs := plan(false)
			if len(onUnits) == 0 || !equalUnitCounts(onUnits, offUnits) {
				t.Fatalf("units with the feature ON = %v, OFF = %v: want the same, not empty", onUnits, offUnits)
			}
			for _, dataset := range tc.datasets {
				if onUnits[dataset] < 1 {
					t.Fatalf("%s units = %d, want at least 1 (units: %v)", dataset, onUnits[dataset], onUnits)
				}
			}
			if lines := skipWarnLines(logs); len(lines) != 0 {
				t.Fatalf("unexpected skip WARN:\n%s", strings.Join(lines, "\n"))
			}
		})
	}
}

// The other datasets are ON but have nothing to run (the only source is
// off), so the plan holds no unit after the incident dataset is left out. No
// run is created: a run with zero units would be failed as feature_disabled
// at dispatch on every occurrence. With the feature ON the same shape is an
// ordinary zero-unit run, as before.
func TestNativeMaterializerIncidentFeatureOffWithNoUnitLeftCreatesNoRun(t *testing.T) {
	for _, tc := range incidentGateProviders {
		t.Run(tc.provider, func(t *testing.T) {
			plan := func(featureOn bool) (incidentGateConfig, error, *bytes.Buffer) {
				config := startIncidentGateConfig(t, tc.provider, append([]string{"incidents"}, tc.others...), nil)
				if _, err := config.fixture.pool.Exec(context.Background(),
					`UPDATE integration_sources SET is_enabled=FALSE WHERE integration_id=$1::uuid`, config.integrationID); err != nil {
					t.Fatal(err)
				}
				config.setIncidentFeature(t, featureOn)
				logs := captureIncidentGateSignals(t)
				materializer, err := NewNativeMaterializer(config.fixture.pool)
				if err != nil {
					t.Fatal(err)
				}
				_, err = materializeAndCommit(t, config.fixture, materializer, config.occurrence)
				return config, err, logs
			}
			on, err, _ := plan(true)
			if err != nil {
				t.Fatalf("feature ON: %v", err)
			}
			if runs, units := on.syncRuns(t), on.mintedUnits(t); runs != 1 || len(units) != 0 {
				t.Fatalf("feature ON: runs=%d units=%v, want one run with no unit", runs, units)
			}
			off, err, logs := plan(false)
			if !errors.Is(err, ErrOccurrenceIneligible) {
				t.Fatalf("feature OFF: materialize error = %v, want ErrOccurrenceIneligible", err)
			}
			if runs, units := off.syncRuns(t), off.mintedUnits(t); runs != 0 || len(units) != 0 {
				t.Fatalf("feature OFF: runs=%d units=%v, want no run and no unit", runs, units)
			}
			if got := globalPlanGateTelemetry.snapshotForTest(tc.provider, "incidents", planGateOutcomeFeatureDisabled); got != 1 {
				t.Fatalf("feature OFF: feature_disabled count = %d, want 1", got)
			}
			if lines := skipWarnLines(logs); len(lines) != 1 {
				t.Fatalf("feature OFF: skip WARN lines = %d, want 1:\n%s", len(lines), logs.String())
			}
		})
	}
}

// The gate itself, on a live transaction: every left-out dataset is counted
// under its own key, and the feature rows stay locked until the plan's
// transaction ends, so a concurrent change of the feature waits for the plan.
func TestDropCanonicalIncidentDatasetsCountsEachDatasetAndLocksTheFeatureRows(t *testing.T) {
	config := startIncidentGateConfig(t, "pagerduty", nil, nil)
	config.setIncidentFeature(t, false)
	logs := captureIncidentGateSignals(t)
	ctx := context.Background()
	tx, err := config.fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	kept, skipped, err := dropCanonicalIncidentDatasetsWhenFeatureOff(ctx, tx, config.occurrence.OrgID, config.integrationID,
		"pagerduty", []PlanDataset{{Key: "incidents"}, {Key: "services"}, {Key: "users"}}, config.occurrence.ScheduledFor)
	if !errors.Is(err, ErrOccurrenceIneligible) || len(kept) != 0 || skipped != 3 {
		t.Fatalf("kept=%v skipped=%d err=%v, want nothing kept, 3 skipped, ErrOccurrenceIneligible", kept, skipped, err)
	}
	for _, dataset := range []string{"incidents", "services", "users"} {
		if got := globalPlanGateTelemetry.snapshotForTest("pagerduty", dataset, planGateOutcomeFeatureDisabled); got != 1 {
			t.Fatalf("feature_disabled count for %s = %d, want 1", dataset, got)
		}
	}
	lines := skipWarnLines(logs)
	if len(lines) != 1 || !strings.Contains(lines[0], `"skipped_dataset_keys":["incidents","services","users"]`) || !strings.Contains(lines[0], `"skipped_datasets":3`) {
		t.Fatalf("skip WARN = %v, want one line naming the three datasets", lines)
	}
	// A second connection cannot take the feature row while the plan's
	// transaction is open.
	_, err = config.fixture.pool.Exec(ctx,
		`SELECT id FROM public.feature_flags WHERE key='canonical_incident_ingestion' FOR UPDATE NOWAIT`)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "55P03" {
		t.Fatalf("second connection lock attempt: err = %v, want lock_not_available (55P03)", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := config.fixture.pool.Exec(ctx,
		`SELECT id FROM public.feature_flags WHERE key='canonical_incident_ingestion' FOR UPDATE NOWAIT`); err != nil {
		t.Fatalf("the feature row must be free after the plan's transaction ended: %v", err)
	}
}

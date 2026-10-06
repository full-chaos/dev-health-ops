//go:build integration

package sync

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/workitemcontract"
)

// CHAOS-8773: the "ran before" fact is read from live sync_run_units rows by
// the real materializer, through Materialize -> loadMaterializationPlan ->
// PlannerInput -> BuildScheduledPlan. The fixture is a planner-managed github
// parent whose only enabled dataset is "commits", so it has no work-item
// family row.
func TestNativeMaterializerWorkItemFamilyStoppedSignalReadsLiveUnitHistory(t *testing.T) {
	cases := []struct {
		name     string
		seed     func(t *testing.T, fixture materializerFixture)
		wantWarn bool
	}{
		{name: "no earlier unit: count, no WARN", seed: func(*testing.T, materializerFixture) {}},
		{
			name: "earlier successful work-items unit: count and WARN",
			seed: func(t *testing.T, fixture materializerFixture) {
				seedPriorSyncRunUnit(t, fixture, "work-items", "success", `{}`)
			},
			wantWarn: true,
		},
		{
			name: "earlier successful alias unit: count and WARN",
			seed: func(t *testing.T, fixture materializerFixture) {
				seedPriorSyncRunUnit(t, fixture, "work-item-labels", "success", `{}`)
			},
			wantWarn: true,
		},
		{
			name: "earlier FAILED work-items unit only: count, no WARN",
			seed: func(t *testing.T, fixture materializerFixture) {
				seedPriorSyncRunUnit(t, fixture, "work-items", "failed", `{}`)
			},
		},
		{
			name: "earlier successful unit of another dataset: count, no WARN",
			seed: func(t *testing.T, fixture materializerFixture) {
				seedPriorSyncRunUnit(t, fixture, "commits", "success", `{}`)
			},
		},
		{
			name: "earlier successful work-items unit of ANOTHER integration: count, no WARN",
			seed: func(t *testing.T, fixture materializerFixture) {
				seedOtherIntegrationWorkItemsUnit(t, fixture)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fixture := startMaterializerPostgres(t)
			// The shared fixture is a legacy (not planner-managed) config.
			makePlannerManagedParent(t, fixture)
			tc.seed(t, fixture)
			var logs bytes.Buffer
			original := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
			t.Cleanup(func() { slog.SetDefault(original) })
			globalPlanGateTelemetry.resetForTest()

			materializer, err := NewNativeMaterializer(fixture.pool)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := materializeAndCommit(t, fixture, materializer, fixture.occurrence); err != nil {
				t.Fatal(err)
			}
			if got := globalPlanGateTelemetry.snapshotForTest("github", "work-items", planGateOutcomeFamilyNotEnabled); got != 1 {
				t.Fatalf("family_not_enabled count = %d, want 1", got)
			}
			warned := strings.Count(logs.String(), `"msg":"sync.plan.work_item_family_stopped"`)
			if tc.wantWarn && warned != 1 {
				t.Fatalf("stopped WARN count = %d, want 1:\n%s", warned, logs.String())
			}
			if !tc.wantWarn && warned != 0 {
				t.Fatalf("stopped WARN count = %d, want 0:\n%s", warned, logs.String())
			}
			if tc.wantWarn {
				for _, want := range []string{`"provider":"github"`, `"family":"work-items"`, `"integration_id":"00000000-0000-4000-8000-000000001002"`} {
					if !strings.Contains(logs.String(), want) {
						t.Fatalf("WARN lacks %s:\n%s", want, logs.String())
					}
				}
			}
		})
	}
}

// makePlannerManagedParent turns the shared legacy fixture into a
// planner-managed parent: the config flag, and the source marker that
// loadPlanSources requires of a planner-managed config's sources.
func makePlannerManagedParent(t *testing.T, fixture materializerFixture) {
	t.Helper()
	ctx := context.Background()
	if _, err := fixture.pool.Exec(ctx,
		`UPDATE sync_configurations SET planner_managed = TRUE WHERE id = $1::uuid`,
		fixture.occurrence.ConfigID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx,
		`UPDATE integration_sources SET metadata = jsonb_build_object('planner_managed_sync_config_id', $1::text)::json`,
		fixture.occurrence.ConfigID); err != nil {
		t.Fatal(err)
	}
}

// A config that is not a planner-managed parent never reports and never pays
// for the query.
func TestNativeMaterializerWorkItemFamilyStoppedSignalSkipsNonParentConfig(t *testing.T) {
	fixture := startMaterializerPostgres(t)
	seedPriorSyncRunUnit(t, fixture, "work-items", "success", `{}`)
	var logs bytes.Buffer
	original := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(original) })
	globalPlanGateTelemetry.resetForTest()
	materializer, err := NewNativeMaterializer(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := materializeAndCommit(t, fixture, materializer, fixture.occurrence); err != nil {
		t.Fatal(err)
	}
	if got := globalPlanGateTelemetry.snapshotForTest("github", "work-items", planGateOutcomeFamilyNotEnabled); got != 0 {
		t.Fatalf("family_not_enabled count = %d, want 0", got)
	}
	if strings.Contains(logs.String(), "sync.plan.work_item_family_stopped") {
		t.Fatalf("unexpected WARN:\n%s", logs.String())
	}
}

// A manual trigger that names its datasets states exactly what it wants; a
// selector that omits work items is not a stopped family.
func TestNativeMaterializerWorkItemFamilyStoppedSignalSkipsExplicitSelector(t *testing.T) {
	fixture := startMaterializerPostgres(t)
	makePlannerManagedParent(t, fixture)
	seedPriorSyncRunUnit(t, fixture, "work-items", "success", `{}`)
	if _, err := fixture.pool.Exec(context.Background(),
		`INSERT INTO scheduled_sync_occurrences (occurrence_id,identity_version,org_id,sync_config_id,scheduled_job_id,scheduled_for,reconcile_status)
VALUES ($1,$2,$3,$4::uuid,$5::uuid,$6,'pending')`,
		fixture.occurrence.ID, OccurrenceIdentityVersion, fixture.occurrence.OrgID,
		fixture.occurrence.ConfigID, fixture.occurrence.JobID, fixture.occurrence.ScheduledFor); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(context.Background(),
		`INSERT INTO sync_manual_triggers (occurrence_id,mode,since,before,dataset_keys,triggered_by)
VALUES ($1,'incremental',NULL,NULL,ARRAY['commits'],'manual')`,
		fixture.occurrence.ID); err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	original := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(original) })
	globalPlanGateTelemetry.resetForTest()
	materializer, err := NewNativeMaterializer(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := materializeAndCommit(t, fixture, materializer, fixture.occurrence); err != nil {
		t.Fatal(err)
	}
	if got := globalPlanGateTelemetry.snapshotForTest("github", "work-items", planGateOutcomeFamilyNotEnabled); got != 0 {
		t.Fatalf("family_not_enabled count = %d, want 0", got)
	}
	if strings.Contains(logs.String(), "sync.plan.work_item_family_stopped") {
		t.Fatalf("unexpected WARN:\n%s", logs.String())
	}
}

// The query itself, against rows of every shape that must and must not count.
func TestLoadWorkItemUnitsRanBeforeLivePostgres(t *testing.T) {
	fixture := startMaterializerPostgres(t)
	ctx := context.Background()
	const integrationID = "00000000-0000-4000-8000-000000001002"
	ask := func(orgID, integration string) bool {
		t.Helper()
		tx, err := fixture.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		ranBefore, err := loadWorkItemUnitsRanBefore(ctx, tx, orgID, integration)
		if err != nil {
			t.Fatal(err)
		}
		return ranBefore
	}
	if ask(fixture.occurrence.OrgID, integrationID) {
		t.Fatal("no units yet: ran before must be false")
	}
	seedPriorSyncRunUnit(t, fixture, "work-items", "failed", `{}`)
	if ask(fixture.occurrence.OrgID, integrationID) {
		t.Fatal("a failed unit is not an earlier success")
	}
	seedPriorSyncRunUnit(t, fixture, "work-items", "success", `{}`)
	if !ask(fixture.occurrence.OrgID, integrationID) {
		t.Fatal("a successful work-items unit must read as ran before")
	}
	if ask("another-org", integrationID) {
		t.Fatal("another org's integration id must not match")
	}
	// The PR states the probe uses ix_sync_run_units_coverage_scan; plan the
	// same statement with sequential scans disabled and require that index.
	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "SET LOCAL enable_seqscan = off"); err != nil {
		t.Fatal(err)
	}
	rows, err := tx.Query(ctx, "EXPLAIN "+workItemUnitsRanBeforeSQL,
		fixture.occurrence.OrgID, integrationID, workitemcontract.FamilyDatasets())
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		plan.WriteString(line + "\n")
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan.String(), "ix_sync_run_units_coverage_scan") {
		t.Fatalf("probe does not use ix_sync_run_units_coverage_scan:\n%s", plan.String())
	}
}

func seedOtherIntegrationWorkItemsUnit(t *testing.T, fixture materializerFixture) {
	t.Helper()
	ctx := context.Background()
	const (
		otherIntegration = "00000000-0000-4000-8000-0000000030aa"
		otherRun         = "00000000-0000-4000-8000-0000000030bb"
	)
	if _, err := fixture.pool.Exec(ctx, `
INSERT INTO integrations (id,org_id,provider,name,config,is_active,created_at,updated_at)
VALUES ($1::uuid,$2,'github','other','{}'::json,TRUE,now(),now())`,
		otherIntegration, fixture.occurrence.OrgID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `
INSERT INTO sync_runs (id,org_id,integration_id,triggered_by,mode,status,total_units,completed_units,failed_units,created_at)
VALUES ($1::uuid,$2,$3::uuid,'scheduled','incremental','success',1,1,0,now())`,
		otherRun, fixture.occurrence.OrgID, otherIntegration); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,integration_id,source_id,provider,dataset_key,cost_class,mode,status,attempts,result,created_at,updated_at)
VALUES (gen_random_uuid(),$1,$2::uuid,$3::uuid,(SELECT id FROM integration_sources LIMIT 1),
        'github','work-items','medium','incremental','success',1,'{}',now(),now())`,
		fixture.occurrence.OrgID, otherRun, otherIntegration); err != nil {
		t.Fatal(err)
	}
}

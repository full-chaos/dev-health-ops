//go:build integration

package syncdispatchruntime

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/providerunit"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	scheduledsync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/domainroledb"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	closedUnitIntegration = "00000000-0000-4000-8000-000000144900"
	closedUnitSource      = "00000000-0000-4000-8000-000000144901"
)

// A provider-unit job queued for a unit the feature gate failed follows its run: while the run is
// open the job keeps the retry, once the run is closed it completes on its first attempt without
// work. Each unit below is failed by a real writer (the real Dispatch gate, the gate's
// termination with a live sibling, the repository write the worker's entitlement re-check makes).
func TestQueuedJobOfAFeatureDisabledUnitCompletesOnceItsRunIsClosed(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		seedDispatchRoute(t, ctx, pool)
		now := time.Now().UTC()
		rig := newClosedUnitRig(t, pool, now)
		future := now.Add(time.Hour)

		t.Run("dispatch gate fails the dispatching unit and closes the run", func(t *testing.T) {
			const gated = "00000000-0000-4000-8000-000000144101"
			featureID := "00000000-0000-4000-8000-000000144102"
			pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{
				ID: gated, RunID: discoveryTestRun, OrgID: discoveryTestOrg, IntegrationID: discoveryTestIntegration,
				Provider: "pagerduty", DatasetKey: "incidents", CostClass: "rest_core", Mode: "incremental", Status: "dispatching",
			})
			pgseed.SetFeatureFlag(ctx, t, pool, featureID, "canonical_incident_ingestion", "enterprise", true)
			pgseed.OrgOverride(ctx, t, pool, discoveryTestOrg, featureID, false)
			if err := newTestDispatchService(t, pool).Dispatch(ctx, dispatchTestArgs()); err != nil {
				t.Fatalf("Dispatch: %v", err)
			}
			requireFailedWith(t, ctx, pool, gated, featureDisabledErrorCategory)
			requireRunStatus(t, ctx, pool, discoveryTestRun, syncRunStatusFailed)
			before := closedUnitSnapshot(t, ctx, pool, discoveryTestRun)
			for _, queued := range []struct {
				state   string
				attempt int
			}{{"available", 1}, {"pending", 1}, {"retryable", 2}, {"scheduled", 3}} {
				t.Run(queued.state, func(t *testing.T) {
					got := rig.attempt(ctx, discoveryTestOrg, gated, discoveryTestRun, queued.attempt)
					requireCompletedWithoutWorkAs(t, got, before, closedUnitSnapshot(t, ctx, pool, discoveryTestRun), gated, discoveryTestRun,
						closedUnitLine{closedBy: "feature_disabled", provider: "pagerduty", dataset: "incidents"})
				})
			}
		})

		t.Run("gate failure with a live sibling keeps the retry until the run is closed", func(t *testing.T) {
			const runID, gated, sibling = "00000000-0000-4000-8000-000000144111", "00000000-0000-4000-8000-000000144112", "00000000-0000-4000-8000-000000144113"
			owner := "00000000-0000-4000-8000-0000001441ff"
			pgseed.EnsureSyncRun(ctx, t, pool, pgseed.SyncRun{ID: runID, OrgID: closedUnitOrg, IntegrationID: closedUnitIntegration, Status: "dispatching", TotalUnits: 2})
			pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{ID: gated, RunID: runID, OrgID: closedUnitOrg, IntegrationID: closedUnitIntegration, SourceID: closedUnitSource, Status: "dispatching", DatasetKey: "commits"})
			pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{
				ID: sibling, RunID: runID, OrgID: closedUnitOrg, IntegrationID: closedUnitIntegration, SourceID: closedUnitSource, Status: "running", DatasetKey: "prs", LeaseOwner: &owner, LeaseExpiresAt: &future,
			})
			// The gate's own termination, in the shape a lease race leaves: the live sibling stays running.
			withTx(t, ctx, pool, func(tx pgx.Tx) {
				run := &finalizeSyncRun{id: runID, orgID: closedUnitOrg}
				if _, err := terminalizeRun(ctx, tx, run, runTermination{errorCategory: featureDisabledErrorCategory, operator: true}, testFeatureDisabledMessage, now); err != nil {
					t.Fatalf("terminalizeRun: %v", err)
				}
			})
			requireFailedWith(t, ctx, pool, gated, featureDisabledErrorCategory)
			requireRunStatus(t, ctx, pool, runID, "dispatching")
			before := closedUnitSnapshot(t, ctx, pool, runID)
			requireRetriedWithoutWork(t, rig.attempt(ctx, closedUnitOrg, gated, runID, 1), before, closedUnitSnapshot(t, ctx, pool, runID))
			closeRun(t, ctx, pool, runID, sibling)
			before = closedUnitSnapshot(t, ctx, pool, runID)
			got := rig.attempt(ctx, closedUnitOrg, gated, runID, 2)
			requireCompletedWithoutWorkAs(t, got, before, closedUnitSnapshot(t, ctx, pool, runID), gated, runID,
				closedUnitLine{closedBy: "feature_disabled", provider: "github", dataset: "commits"})
		})

		t.Run("worker entitlement re-check keeps the retry while the run is open, then completes", func(t *testing.T) {
			const runID, failed, sibling = "00000000-0000-4000-8000-000000144121", "00000000-0000-4000-8000-000000144122", "00000000-0000-4000-8000-000000144123"
			owner := "00000000-0000-4000-8000-0000001441fe"
			pgseed.EnsureSyncRun(ctx, t, pool, pgseed.SyncRun{ID: runID, OrgID: closedUnitOrg, IntegrationID: closedUnitIntegration, Status: "dispatching", TotalUnits: 2})
			pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{ID: failed, RunID: runID, OrgID: closedUnitOrg, IntegrationID: closedUnitIntegration, SourceID: closedUnitSource, Status: "dispatching", DatasetKey: "commits", CostClass: "medium"})
			pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{
				ID: sibling, RunID: runID, OrgID: closedUnitOrg, IntegrationID: closedUnitIntegration, SourceID: closedUnitSource, Status: "running", DatasetKey: "prs", LeaseOwner: &owner, LeaseExpiresAt: &future,
			})
			// A claim needs the run's resolved credential (Unit.Validate).
			const credentialID = "00000000-0000-4000-8000-000000144902"
			pgseed.Credential(ctx, t, pool, credentialID, closedUnitOrg, "github")
			if _, err := pool.Exec(ctx, `UPDATE sync_runs SET credential_id = $2::uuid WHERE id = $1::uuid`, runID, credentialID); err != nil {
				t.Fatal(err)
			}
			repository, err := providersync.NewPostgresRepository(pool)
			if err != nil {
				t.Fatal(err)
			}
			defer repository.Close()
			claim, err := repository.Claim(ctx, providersync.ClaimRequest{
				UnitID: failed, OrgID: closedUnitOrg, Owner: "00000000-0000-4000-8000-0000001441fd", Now: now, LeaseDuration: time.Minute,
			})
			if err != nil {
				t.Fatalf("Claim: %v", err)
			}
			if err := repository.FailWithCauseClass(ctx, claim, providerunit.FeatureDisabledCategory, providersync.CauseClassUnclassified, nil, now, now.Add(time.Second)); err != nil {
				t.Fatalf("FailWithCauseClass: %v", err)
			}
			requireFailedWith(t, ctx, pool, failed, featureDisabledErrorCategory)
			before := closedUnitSnapshot(t, ctx, pool, runID)
			requireRetriedWithoutWork(t, rig.attempt(ctx, closedUnitOrg, failed, runID, 1), before, closedUnitSnapshot(t, ctx, pool, runID))
			closeRun(t, ctx, pool, runID, sibling)
			before = closedUnitSnapshot(t, ctx, pool, runID)
			got := rig.attempt(ctx, closedUnitOrg, failed, runID, 2)
			requireCompletedWithoutWorkAs(t, got, before, closedUnitSnapshot(t, ctx, pool, runID), failed, runID,
				closedUnitLine{closedBy: "feature_disabled", provider: "github", dataset: "commits"})
		})
	})
}

// Units that only look like a closed feature-gate unit keep today's retry.
func TestQueuedJobOfAUnitThatOnlyLooksFeatureDisabledStillRetries(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	instance := pgschema.Start(ctx, t)
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	now := time.Now().UTC()
	future := now.Add(time.Hour)
	rig := newClosedUnitRig(t, pool, now)
	for index, row := range []struct {
		name   string
		unit   pgseed.SyncRunUnit
		jobOrg string
	}{
		{name: "not failed, feature_disabled category, terminal run",
			unit: pgseed.SyncRunUnit{Status: "retrying", AvailableAt: &future, ResultJSON: `{"error_category": "feature_disabled"}`}},
		{name: "job of another organization",
			unit: pgseed.SyncRunUnit{Status: "failed", ResultJSON: `{"error_category": "feature_disabled"}`}, jobOrg: closedUnitOtherOrg},
		{name: "category with a suffix",
			unit: pgseed.SyncRunUnit{Status: "failed", ResultJSON: `{"error_category": "feature_disabled_x"}`}},
		{name: "category in another case",
			unit: pgseed.SyncRunUnit{Status: "failed", ResultJSON: `{"error_category": "Feature_Disabled"}`}},
		{name: "failed unit with no result",
			unit: pgseed.SyncRunUnit{Status: "failed"}},
	} {
		t.Run(row.name, func(t *testing.T) {
			runID := fmt.Sprintf("00000000-0000-4000-8000-0000001451%02d", index)
			unitID := fmt.Sprintf("00000000-0000-4000-8000-0000001452%02d", index)
			pgseed.EnsureSyncRun(ctx, t, pool, pgseed.SyncRun{ID: runID, OrgID: closedUnitOrg, Status: "failed", TotalUnits: 1})
			unit := row.unit
			unit.ID, unit.RunID, unit.OrgID, unit.DatasetKey = unitID, runID, closedUnitOrg, "commits"
			pgseed.InsertSyncRunUnit(ctx, t, pool, unit)
			jobOrg := row.jobOrg
			if jobOrg == "" {
				jobOrg = closedUnitOrg
			}
			before := closedUnitSnapshot(t, ctx, pool, runID)
			requireRetriedWithoutWork(t, rig.attempt(ctx, jobOrg, unitID, runID, 1), before, closedUnitSnapshot(t, ctx, pool, runID))
		})
	}
}

// The claim and its classification run as the restricted domain role: a feature-gate unit's job
// completes and no statement is refused.
func TestQueuedJobOfAFeatureDisabledUnitCompletesAsTheDomainRole(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	venue := domainroledb.Start(t, ctx, "feature_disabled_closed")
	const runID, unitID = "00000000-0000-4000-8000-000000144131", "00000000-0000-4000-8000-000000144132"
	pgseed.EnsureSyncRun(ctx, t, venue.Admin, pgseed.SyncRun{ID: runID, OrgID: closedUnitOrg, Status: "failed", TotalUnits: 1, FailedUnits: 1})
	pgseed.InsertSyncRunUnit(ctx, t, venue.Admin, pgseed.SyncRunUnit{
		ID: unitID, RunID: runID, OrgID: closedUnitOrg, Status: "failed", DatasetKey: "commits",
		ResultJSON: `{"error_category": "feature_disabled"}`,
	})
	rig := newClosedUnitRig(t, venue.Domain, time.Now().UTC())
	before := closedUnitSnapshot(t, ctx, venue.Admin, runID)
	got := rig.attempt(ctx, closedUnitOrg, unitID, runID, 1)
	requireCompletedWithoutWorkAs(t, got, before, closedUnitSnapshot(t, ctx, venue.Admin, runID), unitID, runID,
		closedUnitLine{closedBy: "feature_disabled", provider: "github", dataset: "commits"})
	if refused := venue.Denials.All(); len(refused) > 0 {
		t.Fatalf("the domain role was refused: %s", strings.Join(refused, "; "))
	}
}

// The premise of closing such a job: re-enabling the feature neither reopens the failed unit nor
// its run. The next occurrence plans a NEW run with NEW units, and the job of the old unit has no
// work left, so it completes without work.
func TestFeatureReEnableMintsANewRunAndTheOldFailedUnitStaysFailed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance := pgschema.Start(ctx, t)
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	const (
		orgID         = "00000000-0000-4000-8000-000000146000"
		integrationID = "00000000-0000-4000-8000-000000146001"
		configID      = "00000000-0000-4000-8000-000000146002"
		sourceID      = "00000000-0000-4000-8000-000000146003"
		datasetID     = "00000000-0000-4000-8000-000000146004"
		jobID         = "00000000-0000-4000-8000-000000146005"
		featureID     = "00000000-0000-4000-8000-000000146006"
		outboxID      = "00000000-0000-4000-8000-000000146007"
	)
	pgseed.Org(ctx, t, pool, orgID, "community")
	pgseed.Integration(ctx, t, pool, integrationID, orgID, "jira")
	pgseed.SetFeatureFlag(ctx, t, pool, featureID, "canonical_incident_ingestion", "community", true)
	for _, statement := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO sync_configurations (id,org_id,name,sync_targets,sync_options,integration_id,is_active,source_id,planner_managed,provider,created_at,updated_at) VALUES ($1::uuid,$2,'config-'||$1::text,'[]'::json,'{"schedule_cron":"0 * * * *"}'::json,$3::uuid,TRUE,NULL,TRUE,'jira',now(),now())`, []any{configID, orgID, integrationID}},
		{fmt.Sprintf(`INSERT INTO integration_sources (id,org_id,integration_id,provider,source_type,external_id,name,full_name,is_enabled,metadata,discovered_at,last_seen_at) VALUES ($1::uuid,$2,$3::uuid,'jira','project','PROJ','PROJ','PROJ',TRUE,'{"planner_managed_sync_config_id":"%s"}'::jsonb,now(),now())`, configID), []any{sourceID, orgID, integrationID}},
		{`INSERT INTO integration_datasets (id,org_id,integration_id,dataset_key,is_enabled,options) VALUES ($1::uuid,$2,$3::uuid,'incidents',TRUE,'{}'::json)`, []any{datasetID, orgID, integrationID}},
		{`INSERT INTO scheduled_jobs (id,org_id,name,sync_config_id,job_type,schedule_cron,timezone,status,is_running,created_at,updated_at) VALUES ($1::uuid,$2,'job-'||$1::text,$3::uuid,'sync','0 * * * *','UTC',0,FALSE,now(),now())`, []any{jobID, orgID, configID}},
	} {
		if _, err := pool.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
	materializer, err := scheduledsync.NewNativeMaterializer(pool)
	if err != nil {
		t.Fatal(err)
	}
	materialize := func(occurrenceID string, scheduledFor time.Time) (scheduledsync.PlanResult, error) {
		if _, err := pool.Exec(ctx, `INSERT INTO scheduled_sync_occurrences (occurrence_id,identity_version,org_id,sync_config_id,scheduled_job_id,scheduled_for,reconcile_status) VALUES ($1,$2,$3,$4::uuid,$5::uuid,$6,'pending') ON CONFLICT (occurrence_id) DO NOTHING`,
			occurrenceID, scheduledsync.OccurrenceIdentityVersion, orgID, configID, jobID, scheduledFor); err != nil {
			t.Fatal(err)
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		plan, err := materializer.Materialize(ctx, tx, scheduledsync.PendingOccurrence{
			ID: occurrenceID, IdentityVersion: scheduledsync.OccurrenceIdentityVersion, OrgID: orgID, ConfigID: configID, JobID: jobID,
			ScheduledFor: scheduledFor, ConfigActive: true, ConfigPlannerManaged: true, JobStatus: 0, JobType: "sync",
		})
		if err != nil {
			_ = tx.Rollback(ctx)
			return scheduledsync.PlanResult{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		return plan, nil
	}
	unitIDs := func(runID string) []string {
		rows, err := pool.Query(ctx, `SELECT id::text FROM sync_run_units WHERE sync_run_id = $1::uuid ORDER BY id`, runID)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var ids []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				t.Fatal(err)
			}
			ids = append(ids, id)
		}
		return ids
	}

	// The feature is on: the occurrence plans a run; the units are planned.
	first, err := materialize("occurrence:v1:feature-disabled-first", time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("first materialize: %v", err)
	}
	oldUnits := unitIDs(first.SyncRunID)
	if len(oldUnits) == 0 {
		t.Fatal("the first occurrence planned no unit")
	}
	for _, id := range oldUnits {
		if _, err := pool.Exec(ctx, `UPDATE sync_run_units SET status = 'dispatching' WHERE id = $1::uuid`, id); err != nil {
			t.Fatal(err)
		}
	}

	// The feature is switched off: the real dispatch gate fails every unit and closes the run.
	pgseed.SetFeatureFlag(ctx, t, pool, featureID, "canonical_incident_ingestion", "community", false)
	pgseed.SyncTransportRoute(ctx, t, pool, "dispatch_sync_run", "river", dispatchRouteGeneration, false, "celery")
	pgseed.SyncDispatchOutbox(ctx, t, pool, outboxID, first.SyncRunID, orgID, "dispatch_sync_run", "dispatched", "river", dispatchRouteGeneration)
	if err := newTestDispatchService(t, pool).Dispatch(ctx, DispatchSyncRunArgs{TransportArgs: TransportArgs{
		Version: ContractVersionV1, OrgID: orgID, RunID: first.SyncRunID, DispatchOutbox: outboxID, RouteGeneration: dispatchRouteGeneration,
	}}); err != nil {
		t.Fatalf("Dispatch with the feature off: %v", err)
	}
	for _, id := range oldUnits {
		requireFailedWith(t, ctx, pool, id, featureDisabledErrorCategory)
	}
	requireRunStatus(t, ctx, pool, first.SyncRunID, syncRunStatusFailed)
	oldRun := closedUnitSnapshot(t, ctx, pool, first.SyncRunID)

	// The feature is switched on again: the next occurrence plans a new run with new unit ids.
	pgseed.SetFeatureFlag(ctx, t, pool, featureID, "canonical_incident_ingestion", "community", true)
	second, err := materialize("occurrence:v1:feature-disabled-second", time.Date(2026, 11, 2, 13, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("second materialize after the re-enable: %v", err)
	}
	if second.SyncRunID == first.SyncRunID {
		t.Fatalf("the re-enable planned into the old run %s", first.SyncRunID)
	}
	newUnits := unitIDs(second.SyncRunID)
	if len(newUnits) == 0 {
		t.Fatal("the second occurrence planned no unit")
	}
	for _, id := range newUnits {
		for _, old := range oldUnits {
			if id == old {
				t.Fatalf("new run reuses old unit %s", id)
			}
		}
		var status string
		if err := pool.QueryRow(ctx, `SELECT status FROM sync_run_units WHERE id = $1::uuid`, id).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status != "planned" {
			t.Fatalf("new unit %s status=%q, want planned", id, status)
		}
	}
	if got := closedUnitSnapshot(t, ctx, pool, first.SyncRunID); got != oldRun {
		t.Fatalf("the re-enable and the new run changed the old run:\nbefore %s\nafter  %s", oldRun, got)
	}
	// The same occurrence planned again over the failed run does not reopen it.
	_, _ = materialize("occurrence:v1:feature-disabled-first", time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC))
	if got := closedUnitSnapshot(t, ctx, pool, first.SyncRunID); got != oldRun {
		t.Fatalf("planning the old occurrence again changed the old run:\nbefore %s\nafter  %s", oldRun, got)
	}

	// The job still queued for an old unit has no work left: it completes without work.
	rig := newClosedUnitRig(t, pool, time.Now().UTC())
	newRun := closedUnitSnapshot(t, ctx, pool, second.SyncRunID)
	got := rig.attempt(ctx, orgID, oldUnits[0], first.SyncRunID, 1)
	requireCompletedWithoutWorkAs(t, got, oldRun, closedUnitSnapshot(t, ctx, pool, first.SyncRunID), oldUnits[0], first.SyncRunID,
		closedUnitLine{closedBy: "feature_disabled", provider: "jira", dataset: "incidents"})
	if after := closedUnitSnapshot(t, ctx, pool, second.SyncRunID); after != newRun {
		t.Fatalf("the old job changed the new run:\nbefore %s\nafter  %s", newRun, after)
	}
}

func requireFailedWith(t *testing.T, ctx context.Context, pool *pgxpool.Pool, unitID, category string) {
	t.Helper()
	var status, got string
	if err := pool.QueryRow(ctx, `SELECT status, COALESCE(result->>'error_category', '') FROM sync_run_units WHERE id = $1::uuid`, unitID).Scan(&status, &got); err != nil {
		t.Fatal(err)
	}
	if status != "failed" || got != category {
		t.Fatalf("unit %s status=%q error_category=%q, want failed/%q", unitID, status, got, category)
	}
}

func requireRunStatus(t *testing.T, ctx context.Context, pool *pgxpool.Pool, runID, want string) {
	t.Helper()
	var got string
	if err := pool.QueryRow(ctx, `SELECT status FROM sync_runs WHERE id = $1::uuid`, runID).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("run %s status=%q, want %q", runID, got, want)
	}
}

// closeRun is what finalize writes once the sibling is done: the sibling succeeds, the run closes.
func closeRun(t *testing.T, ctx context.Context, pool *pgxpool.Pool, runID, siblingID string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `UPDATE sync_run_units SET status = 'success', lease_owner = NULL, lease_expires_at = NULL WHERE id = $1::uuid`, siblingID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE sync_runs SET status = 'partial_failed', completed_at = now(), completed_units = 1, failed_units = 1 WHERE id = $1::uuid`, runID); err != nil {
		t.Fatal(err)
	}
}

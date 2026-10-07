//go:build integration

package sync

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	gosync "sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/synchandoff"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// These tests hold the rule "one scheduled run per configuration at a time"
// against real PostgreSQL. Every age in them is written as an interval from the
// database's now(), the same clock the rule reads, so no case depends on how
// long the test itself took to reach its tick.

const openRunGateOrg = "org-open-run-gate"

func openRunGateID(kind, number int) string {
	return fmt.Sprintf("00000000-0000-4000-8%03d-%012d", kind, number)
}

func startOpenRunGatePostgres(t *testing.T) (context.Context, *pgxpool.Pool, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	pgschema.Apply(ctx, t, pool)
	return ctx, pool, instance.URI
}

// openRunUnit is one unit of a seeded run. Every age is an interval text
// measured back from now(); leaseIn is measured forward (a negative interval is
// an expired lease). Empty means NULL.
type openRunUnit struct {
	status       string
	updatedAgo   string
	heartbeatAgo string
	leaseIn      string
}

// openRunCase seeds one due configuration and what is open for it.
type openRunCase struct {
	name string
	// pendingAgo seeds an occurrence with no run yet, created this long ago.
	pendingAgo string
	// reconcileStatus is that occurrence's state ("pending" when empty).
	reconcileStatus string
	// runOpenedAgo seeds a materialized occurrence whose run was created this
	// long ago, in runStatus ("dispatching" when empty), with units.
	runOpenedAgo string
	runStatus    string
	units        []openRunUnit
	// manual marks the seeded occurrence as a manual trigger's.
	manual bool
	// foreignOrg writes the seeded occurrence under another organization.
	foreignOrg bool
	// otherConfig seeds the open run under a second configuration instead.
	otherConfig bool

	wantSkip   bool
	wantReason OpenRunBoundReason
}

func seedOpenRunCase(ctx context.Context, t *testing.T, pool *pgxpool.Pool, index int, tc openRunCase, dueBase time.Time) (string, string) {
	t.Helper()
	configID, jobID := openRunGateID(1, index), openRunGateID(2, index)
	created := dueBase.Format(time.RFC3339)
	if err := insertPlannerFixture(ctx, pool, plannerFixture{
		configID: configID, jobID: jobID, orgID: openRunGateOrg, plannerManaged: true, createdAt: created,
	}); err != nil {
		t.Fatal(err)
	}
	ownerConfig, ownerJob := configID, jobID
	if tc.otherConfig {
		// The open run belongs to a configuration that is not due (inactive
		// marker), so only the configuration under test is a candidate.
		ownerConfig, ownerJob = openRunGateID(3, index), openRunGateID(4, index)
		if err := insertPlannerFixture(ctx, pool, plannerFixture{
			configID: ownerConfig, jobID: ownerJob, orgID: openRunGateOrg, plannerManaged: true, createdAt: created,
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `UPDATE public.scheduled_jobs SET status = 1 WHERE id = $1::uuid`, ownerJob); err != nil {
			t.Fatal(err)
		}
	}
	if tc.pendingAgo == "" && tc.runOpenedAgo == "" {
		return configID, jobID
	}
	occurrenceID := fmt.Sprintf("seeded-occurrence-%d", index)
	occurrenceOrg := openRunGateOrg
	if tc.foreignOrg {
		occurrenceOrg = "org-open-run-gate-foreign"
	}
	// scheduled_for sits before the due base, so the seeded row never moves the
	// schedule's own base and the configuration stays due at the tick.
	scheduledFor := dueBase.Add(-2 * time.Hour)
	if tc.pendingAgo != "" {
		status := tc.reconcileStatus
		if status == "" {
			status = "pending"
		}
		var errorCode, errorAt any
		if status == "quarantined" {
			errorCode, errorAt = "planner_error", time.Now().UTC()
		}
		if _, err := pool.Exec(ctx, `
INSERT INTO public.scheduled_sync_occurrences
	(occurrence_id, identity_version, org_id, sync_config_id, scheduled_job_id, scheduled_for, created_at,
	 reconcile_status, reconcile_error_code, reconcile_error_at)
VALUES ($1, $2, $3, $4::uuid, $5::uuid, $6, now() - $7::interval, $8, $9, $10)`,
			occurrenceID, OccurrenceIdentityVersion, occurrenceOrg, ownerConfig, ownerJob, scheduledFor,
			tc.pendingAgo, status, errorCode, errorAt); err != nil {
			t.Fatalf("%s: seed pending occurrence: %v", tc.name, err)
		}
	} else {
		runID, jobRunID := openRunGateID(5, index), openRunGateID(6, index)
		integrationID, sourceID := pgseed.EnsureSyncIntegration(ctx, t, pool, openRunGateOrg, openRunGateID(7, 0), openRunGateID(8, 0))
		status := tc.runStatus
		if status == "" {
			status = "dispatching"
		}
		triggeredBy := "schedule"
		if tc.manual {
			triggeredBy = "manual"
		}
		if _, err := pool.Exec(ctx, `
INSERT INTO public.sync_runs (id, org_id, integration_id, triggered_by, mode, status, total_units, completed_units, failed_units, created_at)
VALUES ($1::uuid, $2, $3::uuid, $4, 'incremental', $5, $6, 0, 0, now() - $7::interval)`,
			runID, openRunGateOrg, integrationID, triggeredBy, status, len(tc.units), tc.runOpenedAgo); err != nil {
			t.Fatalf("%s: seed sync run: %v", tc.name, err)
		}
		pgseed.JobRun(ctx, t, pool, jobRunID, ownerJob, 0, "")
		if _, err := pool.Exec(ctx, `
INSERT INTO public.scheduled_sync_occurrences
	(occurrence_id, identity_version, org_id, sync_config_id, scheduled_job_id, scheduled_for, created_at,
	 job_run_id, sync_run_id, reconcile_status)
VALUES ($1, $2, $3, $4::uuid, $5::uuid, $6, now() - $7::interval, $8::uuid, $9::uuid, 'completed')`,
			occurrenceID, OccurrenceIdentityVersion, occurrenceOrg, ownerConfig, ownerJob, scheduledFor,
			tc.runOpenedAgo, jobRunID, runID); err != nil {
			t.Fatalf("%s: seed materialized occurrence: %v", tc.name, err)
		}
		for unitIndex, unit := range tc.units {
			unitID := openRunGateID(100+unitIndex, index)
			pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{
				ID: unitID, RunID: runID, OrgID: openRunGateOrg, IntegrationID: integrationID, SourceID: sourceID,
				DatasetKey: fmt.Sprintf("dataset-%d", unitIndex), Status: unit.status,
			})
			if _, err := pool.Exec(ctx, `
UPDATE public.sync_run_units
SET updated_at = now() - COALESCE($2::interval, interval '0'),
    created_at = now() - $5::interval,
    last_heartbeat_at = now() - $3::interval,
    lease_expires_at = now() + $4::interval,
    lease_owner = CASE WHEN $4::interval IS NULL THEN NULL ELSE 'worker-a' END
WHERE id = $1::uuid`,
				unitID, nullableInterval(unit.updatedAgo), nullableInterval(unit.heartbeatAgo),
				nullableInterval(unit.leaseIn), tc.runOpenedAgo); err != nil {
				t.Fatalf("%s: age unit: %v", tc.name, err)
			}
		}
	}
	if tc.manual {
		if _, err := pool.Exec(ctx, `
INSERT INTO public.sync_manual_triggers (occurrence_id, mode, triggered_by) VALUES ($1, 'incremental', 'manual')`,
			occurrenceID); err != nil {
			t.Fatalf("%s: seed manual trigger: %v", tc.name, err)
		}
	}
	return configID, jobID
}

func nullableInterval(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func TestScheduledTickOpenRunGateDecisionTable(t *testing.T) {
	ctx, pool, _ := startOpenRunGatePostgres(t)
	now := time.Now().UTC()
	// Every configuration was created half an hour before the last cron
	// instant, so each one is due exactly once at the tick.
	dueBase := now.Truncate(time.Hour).Add(-30 * time.Minute)

	done := func(ago string) openRunUnit { return openRunUnit{status: "success", updatedAgo: ago} }
	cases := []openRunCase{
		{name: "no occurrence at all starts a run"},

		// The unmaterialized occurrence: the run row does not exist yet.
		{name: "pending occurrence 1 minute old blocks", pendingAgo: "1 minute", wantSkip: true},
		{name: "retrying occurrence 1 minute old blocks", pendingAgo: "1 minute", reconcileStatus: "retry", wantSkip: true},
		{name: "pending occurrence 1h59m old blocks", pendingAgo: "1 hour 59 minutes", wantSkip: true},
		{name: "pending occurrence 2h01m old is past the progress bound", pendingAgo: "2 hours 1 minute", wantReason: OpenRunNoProgress},
		{name: "pending occurrence 24h01m old is past the age cap", pendingAgo: "24 hours 1 minute", wantReason: OpenRunAgeCap},
		{name: "quarantined occurrence is not open", pendingAgo: "1 minute", reconcileStatus: "quarantined"},

		// Terminal runs never block and are never reported.
		{name: "run success is not open", runOpenedAgo: "10 minutes", runStatus: "success"},
		{name: "run partial_failed is not open", runOpenedAgo: "10 minutes", runStatus: "partial_failed"},
		{name: "run failed is not open", runOpenedAgo: "10 minutes", runStatus: "failed"},

		// Every non-terminal status is open.
		{name: "run planned blocks", runOpenedAgo: "10 minutes", runStatus: "planned", wantSkip: true},
		{name: "run dispatching blocks", runOpenedAgo: "10 minutes", runStatus: "dispatching", wantSkip: true},
		{name: "run running blocks", runOpenedAgo: "10 minutes", runStatus: "running", wantSkip: true},

		// The progress bound. The run's own creation counts as progress.
		{name: "run 1h59m old with no unit blocks", runOpenedAgo: "1 hour 59 minutes", wantSkip: true},
		{name: "run 2h01m old with no unit is past the progress bound", runOpenedAgo: "2 hours 1 minute", wantReason: OpenRunNoProgress},
		{
			name: "a unit that ended 1h59m ago blocks", runOpenedAgo: "5 hours",
			units: []openRunUnit{done("1 hour 59 minutes"), {status: "planned", updatedAgo: "5 hours"}}, wantSkip: true,
		},
		{
			name: "a failed unit 1h59m ago blocks", runOpenedAgo: "5 hours",
			units: []openRunUnit{{status: "failed", updatedAgo: "1 hour 59 minutes"}}, wantSkip: true,
		},
		{
			name: "the last unit ended 2h01m ago: past the progress bound", runOpenedAgo: "5 hours",
			units: []openRunUnit{done("2 hours 1 minute"), done("4 hours"), {status: "planned", updatedAgo: "5 hours"}}, wantReason: OpenRunNoProgress,
		},
		{
			// A planned unit's updated_at is not progress: nothing ended.
			name: "only non-terminal units touched 1 minute ago: past the progress bound", runOpenedAgo: "5 hours",
			units: []openRunUnit{{status: "planned", updatedAgo: "1 minute"}, {status: "dispatching", updatedAgo: "1 minute"}}, wantReason: OpenRunNoProgress,
		},
		{
			name: "a heartbeat 1h59m ago blocks", runOpenedAgo: "5 hours",
			units: []openRunUnit{{status: "running", updatedAgo: "5 hours", heartbeatAgo: "1 hour 59 minutes", leaseIn: "-1 hour"}}, wantSkip: true,
		},
		{
			name: "a heartbeat 2h01m ago with an expired lease: past the progress bound", runOpenedAgo: "5 hours",
			units: []openRunUnit{{status: "running", updatedAgo: "5 hours", heartbeatAgo: "2 hours 1 minute", leaseIn: "-1 minute"}}, wantReason: OpenRunNoProgress,
		},
		{
			name: "a live lease blocks although the heartbeat is old", runOpenedAgo: "5 hours",
			units: []openRunUnit{{status: "running", updatedAgo: "5 hours", heartbeatAgo: "4 hours", leaseIn: "1 minute"}}, wantSkip: true,
		},

		// The hard cap wins over progress.
		{
			name: "run 23h59m old with fresh progress blocks", runOpenedAgo: "23 hours 59 minutes",
			units: []openRunUnit{done("5 minutes")}, wantSkip: true,
		},
		{
			name: "run 24h01m old with fresh progress is past the age cap", runOpenedAgo: "24 hours 1 minute",
			units:      []openRunUnit{done("5 minutes"), {status: "running", updatedAgo: "1 minute", heartbeatAgo: "1 second", leaseIn: "10 minutes"}},
			wantReason: OpenRunAgeCap,
		},
		{name: "run 3 days old with no progress reports the age cap", runOpenedAgo: "3 days", wantReason: OpenRunAgeCap},

		// What is not a scheduled run of THIS configuration never blocks.
		{name: "an open manual run does not block", runOpenedAgo: "10 minutes", manual: true},
		{name: "a pending manual occurrence does not block", pendingAgo: "1 minute", manual: true},
		{name: "another configuration's open run does not block", runOpenedAgo: "10 minutes", otherConfig: true},
		{name: "an occurrence under another organization does not block", runOpenedAgo: "10 minutes", foreignOrg: true},
		{name: "another configuration's pending occurrence does not block", pendingAgo: "1 minute", otherConfig: true},
		{name: "a pending occurrence under another organization does not block", pendingAgo: "1 minute", foreignOrg: true},
	}
	type seeded struct {
		tc       openRunCase
		configID string
		jobID    string
		before   int
	}
	byConfig := map[string]*seeded{}
	for index, tc := range cases {
		configID, jobID := seedOpenRunCase(ctx, t, pool, index+1, tc, dueBase)
		entry := &seeded{tc: tc, configID: configID, jobID: jobID}
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM public.scheduled_sync_occurrences WHERE sync_config_id = $1::uuid`, configID).Scan(&entry.before); err != nil {
			t.Fatal(err)
		}
		byConfig[configID] = entry
	}

	repository, err := newRepositoryWithOwnership(pool, reviewedGoMutationOwnershipPolicy())
	if err != nil {
		t.Fatal(err)
	}
	result, err := repository.HandoffDueResult(ctx, now, maximumSnapshotLimit, NewOccurrenceCoordinator())
	if err != nil {
		t.Fatal(err)
	}
	if result.TimingEligible != len(cases) {
		t.Fatalf("timing eligible = %d, want every one of the %d seeded configurations", result.TimingEligible, len(cases))
	}
	skipped := map[string]OpenRunSkip{}
	for _, skip := range result.SkippedOpenRun {
		skipped[skip.ConfigID] = skip
	}
	minted := map[string]bool{}
	for _, occurrence := range result.HandedOff {
		minted[occurrence.ConfigID] = true
	}
	past := map[string]OpenRunPastBound{}
	for _, entry := range result.OpenRunsPastBound {
		past[entry.ConfigID] = entry
	}
	wantNext := now.Truncate(time.Hour).Add(time.Hour)
	for _, entry := range byConfig {
		tc := entry.tc
		t.Run(tc.name, func(t *testing.T) {
			var after int
			if err := pool.QueryRow(ctx, `SELECT count(*) FROM public.scheduled_sync_occurrences WHERE sync_config_id = $1::uuid`, entry.configID).Scan(&after); err != nil {
				t.Fatal(err)
			}
			var nextRunAt *time.Time
			if err := pool.QueryRow(ctx, `SELECT next_run_at FROM public.scheduled_jobs WHERE id = $1::uuid`, entry.jobID).Scan(&nextRunAt); err != nil {
				t.Fatal(err)
			}
			if nextRunAt == nil || !nextRunAt.Equal(wantNext) {
				t.Fatalf("next_run_at = %v, want the next cron instant %s whether the tick started a run or not", nextRunAt, wantNext)
			}
			skip, wasSkipped := skipped[entry.configID]
			report, reported := past[entry.configID]
			if tc.wantSkip {
				if !wasSkipped || minted[entry.configID] || after != entry.before {
					t.Fatalf("skipped=%v minted=%v occurrences %d -> %d; want the tick skipped and nothing minted", wasSkipped, minted[entry.configID], entry.before, after)
				}
				if skip.OccurrenceID != fmt.Sprintf("seeded-occurrence-%d", indexOfCase(cases, tc.name)+1) {
					t.Fatalf("skip names occurrence %q", skip.OccurrenceID)
				}
				if (skip.SyncRunID == "") != (tc.runOpenedAgo == "") {
					t.Fatalf("skip names sync run %q for a case with runOpenedAgo=%q", skip.SyncRunID, tc.runOpenedAgo)
				}
				if !skip.NextRunAt.Equal(wantNext) {
					t.Fatalf("skip next_run_at = %s, want %s", skip.NextRunAt, wantNext)
				}
				if reported {
					t.Fatalf("a skipped tick was also reported past the bound: %#v", report)
				}
				return
			}
			if wasSkipped || !minted[entry.configID] || after != entry.before+1 {
				t.Fatalf("skipped=%v minted=%v occurrences %d -> %d; want exactly one new occurrence", wasSkipped, minted[entry.configID], entry.before, after)
			}
			if tc.wantReason == "" {
				if reported {
					t.Fatalf("reported past the bound with nothing open: %#v", report)
				}
				return
			}
			if !reported || report.Reason != tc.wantReason || report.OpenRuns != 1 {
				t.Fatalf("past-bound report = %#v (reported=%v), want reason %q for 1 run", report, reported, tc.wantReason)
			}
			if (report.SyncRunID == "") != (tc.runOpenedAgo == "") {
				t.Fatalf("report names sync run %q for a case with runOpenedAgo=%q", report.SyncRunID, tc.runOpenedAgo)
			}
			if report.OpenSeconds < 2*60*60 {
				t.Fatalf("report open_seconds = %d for a run past a 2 h bound", report.OpenSeconds)
			}
		})
	}
}

func indexOfCase(cases []openRunCase, name string) int {
	for index, tc := range cases {
		if tc.name == name {
			return index
		}
	}
	return -1
}

// The whole path through the production reconciler and materializer: a
// configuration with an open run gets no new run at the next three ticks, and
// the one run started after it ends plans its units from their watermarks, so
// that run covers the skipped time.
func TestScheduledTicksSkipWhileARunIsOpenThenOneRunCoversTheGap(t *testing.T) {
	fixture := startMaterializerPostgres(t)
	ctx := context.Background()
	pool, configID := fixture.pool, fixture.occurrence.ConfigID
	hour := time.Now().UTC().Truncate(time.Hour)
	configureMaterializerFixtureAsDue(t, fixture, hour.Add(-30*time.Minute))
	repository, err := newRepositoryWithOwnership(pool, reviewedGoMutationOwnershipPolicy())
	if err != nil {
		t.Fatal(err)
	}
	materializer, err := NewNativeMaterializer(pool)
	if err != nil {
		t.Fatal(err)
	}
	reconciler, err := NewOccurrenceReconciler(pool, materializer)
	if err != nil {
		t.Fatal(err)
	}
	tick := func(at time.Time) HandoffResult {
		t.Helper()
		result, err := repository.HandoffDueResult(ctx, at, 4, NewOccurrenceCoordinator())
		if err != nil {
			t.Fatalf("tick at %s: %v", at, err)
		}
		return result
	}

	first := tick(hour.Add(time.Second))
	if first.Minted() != 1 || len(first.SkippedOpenRun) != 0 {
		t.Fatalf("first tick minted=%d skipped=%d, want one run started", first.Minted(), len(first.SkippedOpenRun))
	}
	// Tick 1 of 3 arrives before the occurrence is materialized: the run row
	// does not exist, the occurrence alone must hold the schedule back.
	skip := tick(hour.Add(time.Hour))
	if len(skip.SkippedOpenRun) != 1 || skip.Minted() != 0 || skip.SkippedOpenRun[0].SyncRunID != "" ||
		skip.SkippedOpenRun[0].OccurrenceID != first.HandedOff[0].ID {
		t.Fatalf("tick before materialization = %#v, want a skip that names the unmaterialized occurrence", skip)
	}
	reconciled, err := reconciler.Reconcile(ctx, hour.Add(time.Hour+time.Minute), 10)
	if err != nil || reconciled.Completed != 1 {
		t.Fatalf("Reconcile() = %#v, %v; want the first occurrence materialized", reconciled, err)
	}
	var firstRun string
	if err := pool.QueryRow(ctx, `SELECT sync_run_id::text FROM public.scheduled_sync_occurrences WHERE occurrence_id = $1`, first.HandedOff[0].ID).Scan(&firstRun); err != nil {
		t.Fatal(err)
	}
	for offset := 2; offset <= 3; offset++ {
		at := hour.Add(time.Duration(offset) * time.Hour)
		skip := tick(at)
		if len(skip.SkippedOpenRun) != 1 || skip.Minted() != 0 || len(skip.HandedOff) != 0 {
			t.Fatalf("tick %d with the run open = %#v, want exactly one skip and nothing minted", offset, skip)
		}
		if got := skip.SkippedOpenRun[0]; got.SyncRunID != firstRun || got.ConfigID != configID || !got.NextRunAt.Equal(at.Add(time.Hour)) {
			t.Fatalf("tick %d skip = %#v, want run %s and next_run_at %s", offset, got, firstRun, at.Add(time.Hour))
		}
		// The reconcile stage of every window finds nothing new to plan.
		if again, err := reconciler.Reconcile(ctx, at.Add(time.Minute), 10); err != nil || again.Completed != 0 {
			t.Fatalf("Reconcile() after a skipped tick = %#v, %v; want nothing to materialize", again, err)
		}
	}
	var runs int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM public.sync_runs`).Scan(&runs); err != nil {
		t.Fatal(err)
	}
	if occurrences := occurrenceCount(t, pool, configID); occurrences != 1 || runs != 1 {
		t.Fatalf("after three ticks with a run open: %d occurrences, %d runs; want 1 and 1", occurrences, runs)
	}

	// The run ends. Its commits unit moved the watermark to the first tick.
	watermark := hour.Add(-10 * time.Minute)
	endScheduledRun(t, pool, firstRun, "success")
	if _, err := pool.Exec(ctx, `UPDATE public.sync_watermarks SET last_synced_at = $2 WHERE org_id = $1 AND dataset_key = 'commits'`, fixture.occurrence.OrgID, watermark); err != nil {
		t.Fatal(err)
	}
	resumeAt := hour.Add(4 * time.Hour)
	resumed := tick(resumeAt)
	if resumed.Minted() != 1 || len(resumed.SkippedOpenRun) != 0 || len(resumed.OpenRunsPastBound) != 0 {
		t.Fatalf("tick after the run ended = %#v, want exactly one run started and nothing reported", resumed)
	}
	// The occurrence stands for the newest due instant, not the first skipped
	// one: the planner ends each unit window at this instant, so the first
	// skipped instant would leave three hours unread.
	if got := resumed.HandedOff[0].ScheduledFor; !got.Equal(resumeAt) {
		t.Fatalf("resumed occurrence scheduled_for = %s, want the newest due instant %s", got, resumeAt)
	}
	reconciled, err = reconciler.Reconcile(ctx, resumeAt.Add(time.Minute), 10)
	if err != nil || reconciled.Completed != 1 {
		t.Fatalf("Reconcile() after the resumed tick = %#v, %v", reconciled, err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM public.sync_runs`).Scan(&runs); err != nil {
		t.Fatal(err)
	}
	if occurrences := occurrenceCount(t, pool, configID); occurrences != 2 || runs != 2 {
		t.Fatalf("after the resumed tick: %d occurrences, %d runs; want 2 and 2", occurrences, runs)
	}
	var since, before *time.Time
	if err := pool.QueryRow(ctx, `
SELECT unit.since_at, unit.before_at
FROM public.sync_run_units AS unit
JOIN public.scheduled_sync_occurrences AS occurrence ON occurrence.sync_run_id = unit.sync_run_id
WHERE occurrence.occurrence_id = $1 AND unit.dataset_key = 'commits'`, resumed.HandedOff[0].ID).Scan(&since, &before); err != nil {
		t.Fatal(err)
	}
	if since == nil || !since.Equal(watermark) {
		t.Fatalf("the resumed run's commits unit starts at %v, want its watermark %s", since, watermark)
	}
	if before == nil || before.Before(resumeAt) {
		t.Fatalf("the resumed run's commits unit ends at %v: it does not cover the three skipped ticks up to %s", before, resumeAt)
	}
	// The schedule is back on its cadence: the next instant is minted as itself.
	endScheduledRun(t, pool, resumedRunID(t, pool, resumed.HandedOff[0].ID), "failed")
	next := tick(resumeAt.Add(time.Hour))
	if next.Minted() != 1 || !next.HandedOff[0].ScheduledFor.Equal(resumeAt.Add(time.Hour)) {
		t.Fatalf("tick after the resumed run = %#v, want one run for %s", next, resumeAt.Add(time.Hour))
	}
}

// blockingCoordinator holds the scheduler transaction open between the open
// run read and its commit, which is the window a second scheduler must not be
// able to decide in.
type blockingCoordinator struct {
	inner   Coordinator
	entered chan struct{}
	release chan struct{}
}

func (coordinator blockingCoordinator) Handoff(ctx context.Context, tx HandoffTransaction, occurrence Occurrence) (HandoffOutcome, error) {
	outcome, err := coordinator.inner.Handoff(ctx, tx, occurrence)
	close(coordinator.entered)
	select {
	case <-coordinator.release:
	case <-ctx.Done():
		return "", ctx.Err()
	}
	return outcome, err
}

func TestTwoSchedulersStartAtMostOneRunForOneConfiguration(t *testing.T) {
	ctx, poolA, uri := startOpenRunGatePostgres(t)
	poolB, err := pgxpool.New(ctx, uri)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(poolB.Close)
	hour := time.Now().UTC().Truncate(time.Hour)
	configID, _ := seedOpenRunCase(ctx, t, poolA, 1, openRunCase{name: "concurrent"}, hour.Add(-30*time.Minute))
	schedulerA, err := newRepositoryWithOwnership(poolA, reviewedGoMutationOwnershipPolicy())
	if err != nil {
		t.Fatal(err)
	}
	schedulerB, err := newRepositoryWithOwnership(poolB, reviewedGoMutationOwnershipPolicy())
	if err != nil {
		t.Fatal(err)
	}

	// Scheduler A has read "no open run" and inserted its occurrence, and has
	// not committed. Scheduler B ticks in that window.
	held := blockingCoordinator{inner: NewOccurrenceCoordinator(), entered: make(chan struct{}), release: make(chan struct{})}
	type outcome struct {
		result HandoffResult
		err    error
	}
	fromA := make(chan outcome, 1)
	go func() {
		result, err := schedulerA.HandoffDueResult(ctx, hour.Add(time.Second), 4, held)
		fromA <- outcome{result, err}
	}()
	select {
	case <-held.entered:
	case early := <-fromA:
		t.Fatalf("scheduler A returned before its handoff was held: %#v, %v", early.result, early.err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	during, err := schedulerB.HandoffDueResult(ctx, hour.Add(time.Second), 4, NewOccurrenceCoordinator())
	if err != nil {
		t.Fatal(err)
	}
	if during.Candidates != 0 || during.Minted() != 0 || len(during.SkippedOpenRun) != 0 {
		t.Fatalf("scheduler B decided for a configuration scheduler A holds: %#v", during)
	}
	close(held.release)
	first := <-fromA
	if first.err != nil || first.result.Minted() != 1 {
		t.Fatalf("scheduler A = %#v, %v; want one run started", first.result, first.err)
	}

	// Both schedulers now tick at once, for three cron instants, with the
	// first occurrence still open. Every instant must be decided exactly once
	// and as a skip.
	const perScheduler = 4
	for offset := 1; offset <= 3; offset++ {
		at := hour.Add(time.Duration(offset) * time.Hour)
		start := make(chan struct{})
		results := make(chan outcome, 2*perScheduler)
		var group gosync.WaitGroup
		for worker := 0; worker < 2*perScheduler; worker++ {
			scheduler := schedulerA
			if worker%2 == 1 {
				scheduler = schedulerB
			}
			group.Add(1)
			go func() {
				defer group.Done()
				<-start
				result, err := scheduler.HandoffDueResult(ctx, at, 4, NewOccurrenceCoordinator())
				results <- outcome{result, err}
			}()
		}
		close(start)
		group.Wait()
		close(results)
		skips, mints := 0, 0
		for got := range results {
			if got.err != nil {
				t.Fatalf("concurrent tick at %s: %v", at, got.err)
			}
			skips += len(got.result.SkippedOpenRun)
			mints += got.result.Minted()
		}
		if skips != 1 || mints != 0 {
			t.Fatalf("instant %s: %d skips and %d runs started across %d concurrent ticks; want exactly 1 skip and 0 runs", at, skips, mints, 2*perScheduler)
		}
	}
	if occurrences := occurrenceCount(t, poolA, configID); occurrences != 1 {
		t.Fatalf("two schedulers left %d occurrences for one configuration with a run open, want 1", occurrences)
	}
}

// A manual trigger is a user action: it is accepted while a scheduled run is
// open, and its own open run never holds the schedule back.
func TestManualTriggerIsAcceptedWhileAScheduledRunIsOpenAndNeverBlocksTheSchedule(t *testing.T) {
	fixture := startMaterializerPostgres(t)
	ctx := context.Background()
	pool, configID := fixture.pool, fixture.occurrence.ConfigID
	hour := time.Now().UTC().Truncate(time.Hour)
	configureMaterializerFixtureAsDue(t, fixture, hour.Add(-30*time.Minute))
	repository, err := newRepositoryWithOwnership(pool, reviewedGoMutationOwnershipPolicy())
	if err != nil {
		t.Fatal(err)
	}
	materializer, err := NewNativeMaterializer(pool)
	if err != nil {
		t.Fatal(err)
	}
	reconciler, err := NewOccurrenceReconciler(pool, materializer)
	if err != nil {
		t.Fatal(err)
	}
	first, err := repository.HandoffDueResult(ctx, hour.Add(time.Second), 4, NewOccurrenceCoordinator())
	if err != nil || first.Minted() != 1 {
		t.Fatalf("first tick = %#v, %v", first, err)
	}
	if reconciled, err := reconciler.Reconcile(ctx, hour.Add(time.Minute), 10); err != nil || reconciled.Completed != 1 {
		t.Fatalf("Reconcile() = %#v, %v", reconciled, err)
	}
	var scheduledRun string
	if err := pool.QueryRow(ctx, `SELECT sync_run_id::text FROM public.scheduled_sync_occurrences WHERE occurrence_id = $1`, first.HandedOff[0].ID).Scan(&scheduledRun); err != nil {
		t.Fatal(err)
	}

	// The user presses "Sync now" with the scheduled run open.
	var trigger synchandoff.Trigger
	if err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		config, err := synchandoff.LoadConfig(ctx, tx, configID)
		if err != nil {
			return err
		}
		trigger, err = synchandoff.Mint(ctx, tx, config, synchandoff.MintInput{Mode: "incremental", TriggeredBy: "manual"}, hour.Add(2*time.Minute))
		return err
	}); err != nil {
		t.Fatalf("manual trigger with a scheduled run open: %v", err)
	}
	if trigger.Existing || trigger.OccurrenceID == "" {
		t.Fatalf("manual trigger = %#v, want a new occurrence", trigger)
	}
	if reconciled, err := reconciler.Reconcile(ctx, hour.Add(3*time.Minute), 10); err != nil || reconciled.Completed != 1 {
		t.Fatalf("Reconcile() of the manual occurrence = %#v, %v; want it planned beside the open scheduled run", reconciled, err)
	}
	var manualRunStatus, manualTriggeredBy string
	if err := pool.QueryRow(ctx, `
SELECT run.status, run.triggered_by FROM public.sync_runs AS run
JOIN public.scheduled_sync_occurrences AS occurrence ON occurrence.sync_run_id = run.id
WHERE occurrence.occurrence_id = $1`, trigger.OccurrenceID).Scan(&manualRunStatus, &manualTriggeredBy); err != nil {
		t.Fatal(err)
	}
	if manualTriggeredBy != "manual" || manualRunStatus == "success" || manualRunStatus == "failed" || manualRunStatus == "partial_failed" {
		t.Fatalf("manual run triggered_by=%q status=%q, want an open manual run", manualTriggeredBy, manualRunStatus)
	}

	// The scheduled run is still what the schedule waits for.
	skip, err := repository.HandoffDueResult(ctx, hour.Add(time.Hour), 4, NewOccurrenceCoordinator())
	if err != nil {
		t.Fatal(err)
	}
	if len(skip.SkippedOpenRun) != 1 || skip.SkippedOpenRun[0].SyncRunID != scheduledRun || skip.Minted() != 0 {
		t.Fatalf("tick with both runs open = %#v, want a skip that names the scheduled run", skip)
	}
	// The scheduled run ends; the manual run stays open. The schedule resumes.
	endScheduledRun(t, pool, scheduledRun, "success")
	resumed, err := repository.HandoffDueResult(ctx, hour.Add(2*time.Hour), 4, NewOccurrenceCoordinator())
	if err != nil {
		t.Fatal(err)
	}
	if resumed.Minted() != 1 || len(resumed.SkippedOpenRun) != 0 || len(resumed.OpenRunsPastBound) != 0 {
		t.Fatalf("tick with only the manual run open = %#v, want one scheduled run started", resumed)
	}
}

// With several open runs, a skip names the newest run that holds the schedule
// back, and a start past the bound names the oldest run that did not end and
// counts all of them.
func TestOpenScheduledRunsReadNamesTheNewestBlockingAndTheOldestPastBoundRun(t *testing.T) {
	ctx, pool, _ := startOpenRunGatePostgres(t)
	hour := time.Now().UTC().Truncate(time.Hour)
	configID, jobID := seedOpenRunCase(ctx, t, pool, 1, openRunCase{name: "several"}, hour.Add(-30*time.Minute))
	integrationID, _ := pgseed.EnsureSyncIntegration(ctx, t, pool, openRunGateOrg, openRunGateID(7, 0), openRunGateID(8, 0))
	seed := func(number int, openedAgo string) string {
		t.Helper()
		runID, jobRunID := openRunGateID(5, number), openRunGateID(6, number)
		if _, err := pool.Exec(ctx, `
INSERT INTO public.sync_runs (id, org_id, integration_id, triggered_by, mode, status, total_units, completed_units, failed_units, created_at)
VALUES ($1::uuid, $2, $3::uuid, 'schedule', 'incremental', 'dispatching', 0, 0, 0, now() - $4::interval)`,
			runID, openRunGateOrg, integrationID, openedAgo); err != nil {
			t.Fatal(err)
		}
		pgseed.JobRun(ctx, t, pool, jobRunID, jobID, 0, "")
		if _, err := pool.Exec(ctx, `
INSERT INTO public.scheduled_sync_occurrences
	(occurrence_id, identity_version, org_id, sync_config_id, scheduled_job_id, scheduled_for, created_at, job_run_id, sync_run_id, reconcile_status)
VALUES ($1, $2, $3, $4::uuid, $5::uuid, $6, now() - $7::interval, $8::uuid, $9::uuid, 'completed')`,
			fmt.Sprintf("several-%d", number), OccurrenceIdentityVersion, openRunGateOrg, configID, jobID,
			hour.Add(-time.Duration(number+2)*time.Hour), openedAgo, jobRunID, runID); err != nil {
			t.Fatal(err)
		}
		return runID
	}
	read := func() openScheduledRuns {
		t.Helper()
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		open, err := readOpenScheduledRuns(ctx, tx, openRunGateOrg, configID)
		if err != nil {
			t.Fatal(err)
		}
		return open
	}

	// Seeded out of age order, so neither answer can come from insertion order.
	oldest := seed(1, "30 hours")
	seed(2, "3 hours")
	seed(3, "26 hours")
	open := read()
	if open.blocking != 0 || open.pastBound != 3 {
		t.Fatalf("open = %#v, want 3 runs past the bound and none blocking", open)
	}
	if open.pastBoundSyncRunID != oldest || open.pastBoundOccurrenceID != "several-1" || open.pastBoundReason != OpenRunAgeCap {
		t.Fatalf("past-bound run = %s (%s, %s), want the oldest run %s with age_cap", open.pastBoundSyncRunID, open.pastBoundOccurrenceID, open.pastBoundReason, oldest)
	}
	if open.pastBoundOpenSeconds < 30*60*60 || open.pastBoundOpenSeconds > 31*60*60 {
		t.Fatalf("past-bound open_seconds = %d, want about 30 hours", open.pastBoundOpenSeconds)
	}

	seed(4, "90 minutes")
	newest := seed(5, "10 minutes")
	seed(6, "50 minutes")
	open = read()
	if open.blocking != 3 || open.pastBound != 3 {
		t.Fatalf("open = %#v, want 3 runs blocking and 3 past the bound", open)
	}
	if open.blockingSyncRunID != newest || open.blockingOccurrenceID != "several-5" {
		t.Fatalf("blocking run = %s (%s), want the newest run %s", open.blockingSyncRunID, open.blockingOccurrenceID, newest)
	}
	if open.blockingOpenSeconds < 10*60 || open.blockingOpenSeconds > 11*60 {
		t.Fatalf("blocking open_seconds = %d, want about 10 minutes", open.blockingOpenSeconds)
	}
	if open.pastBoundSyncRunID != oldest {
		t.Fatalf("past-bound run = %s, want the oldest run %s", open.pastBoundSyncRunID, oldest)
	}
}

var openRunPlanExecutionTime = regexp.MustCompile(`Execution Time: ([0-9.]+) ms`)

// The open run read runs once per due configuration per cron instant. This
// measures it against a ledger shaped like a long-lived deployment: ten hourly
// configurations with 2,000 finished scheduled runs each, and one open run of
// 70 units for the configuration under test.
func TestOpenScheduledRunsReadUsesTheConfigurationIndex(t *testing.T) {
	ctx, pool, _ := startOpenRunGatePostgres(t)
	hour := time.Now().UTC().Truncate(time.Hour)
	integrationID, sourceID := pgseed.EnsureSyncIntegration(ctx, t, pool, openRunGateOrg, openRunGateID(7, 0), openRunGateID(8, 0))
	const configs, history = 10, 2000
	for index := 1; index <= configs; index++ {
		configID, jobID := seedOpenRunCase(ctx, t, pool, index, openRunCase{name: "history"}, hour.Add(-30*time.Minute))
		seeds := []struct {
			statement string
			arguments []any
		}{
			{`INSERT INTO public.job_runs (id, job_id, status, created_at)
			 SELECT md5('job-run-' || $1::text || '-' || n)::uuid, $2::uuid, 0, now() FROM generate_series(1, $3::int) AS n`,
				[]any{configID, jobID, history}},
			{`INSERT INTO public.sync_runs (id, org_id, integration_id, triggered_by, mode, status, total_units, completed_units, failed_units, created_at)
			 SELECT md5('sync-run-' || $1::text || '-' || n)::uuid, $2, $3::uuid, 'schedule', 'incremental', 'success', 0, 0, 0, now() - make_interval(hours => n + 2)
			 FROM generate_series(1, $4::int) AS n`,
				[]any{configID, openRunGateOrg, integrationID, history}},
			{`INSERT INTO public.scheduled_sync_occurrences
				(occurrence_id, identity_version, org_id, sync_config_id, scheduled_job_id, scheduled_for, created_at, job_run_id, sync_run_id, reconcile_status)
			 SELECT 'history-' || $1::text || '-' || n, $2, $3, $1::uuid, $4::uuid, $5::timestamptz - make_interval(hours => n + 2), now() - make_interval(hours => n + 2),
				md5('job-run-' || $1::text || '-' || n)::uuid, md5('sync-run-' || $1::text || '-' || n)::uuid, 'completed'
			 FROM generate_series(1, $6::int) AS n`,
				[]any{configID, OccurrenceIdentityVersion, openRunGateOrg, jobID, hour, history}},
		}
		for _, seed := range seeds {
			if _, err := pool.Exec(ctx, seed.statement, seed.arguments...); err != nil {
				t.Fatalf("seed history: %v", err)
			}
		}
	}
	configID := openRunGateID(1, 1)
	openRun, openJobRun := openRunGateID(5, 1), openRunGateID(6, 1)
	if _, err := pool.Exec(ctx, `
INSERT INTO public.sync_runs (id, org_id, integration_id, triggered_by, mode, status, total_units, completed_units, failed_units, created_at)
VALUES ($1::uuid, $2, $3::uuid, 'schedule', 'incremental', 'dispatching', 70, 0, 0, now() - interval '90 minutes')`,
		openRun, openRunGateOrg, integrationID); err != nil {
		t.Fatal(err)
	}
	pgseed.JobRun(ctx, t, pool, openJobRun, openRunGateID(2, 1), 0, "")
	if _, err := pool.Exec(ctx, `
INSERT INTO public.scheduled_sync_occurrences
	(occurrence_id, identity_version, org_id, sync_config_id, scheduled_job_id, scheduled_for, created_at, job_run_id, sync_run_id, reconcile_status)
VALUES ('open-occurrence', $1, $2, $3::uuid, $4::uuid, $5, now() - interval '90 minutes', $6::uuid, $7::uuid, 'completed')`,
		OccurrenceIdentityVersion, openRunGateOrg, configID, openRunGateID(2, 1), hour.Add(-time.Hour), openJobRun, openRun); err != nil {
		t.Fatal(err)
	}
	for unit := 0; unit < 70; unit++ {
		status := "planned"
		if unit < 20 {
			status = "success"
		}
		pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{
			ID: openRunGateID(100+unit, 1), RunID: openRun, OrgID: openRunGateOrg, IntegrationID: integrationID, SourceID: sourceID,
			DatasetKey: fmt.Sprintf("dataset-%d", unit), Status: status,
		})
	}
	// Units of finished runs, so the unit table is not small enough to read whole.
	if _, err := pool.Exec(ctx, `
INSERT INTO public.sync_run_units (id, org_id, sync_run_id, integration_id, source_id, provider, dataset_key, cost_class, mode, status, attempts, created_at, updated_at)
SELECT md5('unit-' || run.id::text || '-' || n)::uuid, run.org_id, run.id, run.integration_id, $1::uuid, 'github', 'dataset-' || n, 'medium', 'incremental', 'success', 1, run.created_at, run.created_at
FROM (SELECT id, org_id, integration_id, created_at FROM public.sync_runs WHERE status = 'success' ORDER BY id LIMIT 1000) AS run
CROSS JOIN generate_series(1, 20) AS n`, sourceID); err != nil {
		t.Fatalf("seed history units: %v", err)
	}
	if _, err := pool.Exec(ctx, `ANALYZE`); err != nil {
		t.Fatal(err)
	}

	// The read itself answers correctly on this ledger.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	open, err := readOpenScheduledRuns(ctx, tx, openRunGateOrg, configID)
	_ = tx.Rollback(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if open.blocking != 1 || open.pastBound != 0 || open.blockingSyncRunID != openRun {
		t.Fatalf("open runs on the history ledger = %#v, want exactly the one open run blocking", open)
	}

	rows, err := pool.Query(ctx, "EXPLAIN (ANALYZE, BUFFERS) "+schedulerOpenScheduledRunsSQL,
		openRunGateOrg, configID, int64(openRunProgressTTL/time.Second), int64(openRunAgeCap/time.Second))
	if err != nil {
		t.Fatal(err)
	}
	var plan strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		plan.WriteString(line + "\n")
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	t.Logf("open scheduled runs read, %d configurations x %d finished runs:\n%s", configs, history, plan.String())
	for _, table := range []string{"scheduled_sync_occurrences", "sync_runs", "sync_run_units"} {
		if strings.Contains(plan.String(), "Seq Scan on "+table) {
			t.Fatalf("the open run read scans %s sequentially:\n%s", table, plan.String())
		}
	}
	// JIT compilation costs more than the read itself; a plan that starts it
	// has an estimate that follows the size of the ledger.
	if strings.Contains(plan.String(), "JIT:") {
		t.Fatalf("the open run read is costed high enough to start JIT compilation:\n%s", plan.String())
	}
	match := openRunPlanExecutionTime.FindStringSubmatch(plan.String())
	if match == nil {
		t.Fatalf("the plan reports no execution time, so the cost was not measured:\n%s", plan.String())
	}
	milliseconds, err := strconv.ParseFloat(match[1], 64)
	if err != nil {
		t.Fatal(err)
	}
	if milliseconds > 50 {
		t.Fatalf("the open run read took %.1f ms for one configuration, want under 50 ms:\n%s", milliseconds, plan.String())
	}
}

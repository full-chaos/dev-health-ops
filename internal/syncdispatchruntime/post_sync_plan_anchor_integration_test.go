//go:build integration

package syncdispatchruntime

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	anchorOrg         = "30000000-0000-4000-8000-000000000001"
	anchorRun         = "30000000-0000-4000-8000-000000000002"
	anchorOutbox      = "30000000-0000-4000-8000-000000000003"
	anchorIntegration = "30000000-0000-4000-8000-000000000004"
	anchorRepository  = "30000000-0000-4000-8000-000000000005"
	anchorCommitsUnit = "30000000-0000-4000-8000-000000000006"
	anchorItemsUnit   = "30000000-0000-4000-8000-000000000007"
)

// anchorDay is the run's own day in every case below.
var anchorDay = time.Date(2026, 7, 23, 0, 0, 0, 0, time.UTC)

type anchorCase struct {
	startedAt, completedAt *time.Time
	unbounded              bool
	workItemsSince         *time.Time
}

func planAt(t *testing.T, run anchorCase, now time.Time) *PostSyncPlan {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	instance := pgschema.Start(ctx, t)
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createPostSyncTables(t, ctx, pool)
	seedPostSyncGraph(t, ctx, pool, anchorOrg, anchorRun, anchorOutbox, anchorIntegration, anchorRepository,
		anchorCommitsUnit, "commits", "", "")
	if _, err := pool.Exec(ctx, `UPDATE sync_runs SET status = 'success', started_at = $2, completed_at = $3 WHERE id = $1`,
		anchorRun, run.startedAt, run.completedAt); err != nil {
		t.Fatal(err)
	}
	if run.unbounded {
		if _, err := pool.Exec(ctx, `UPDATE sync_run_units SET since_at = NULL, before_at = NULL WHERE id = $1`, anchorCommitsUnit); err != nil {
			t.Fatal(err)
		}
	}
	if run.workItemsSince != nil {
		pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{
			ID: anchorItemsUnit, RunID: anchorRun, OrgID: anchorOrg, IntegrationID: anchorIntegration, SourceID: anchorRepository,
			DatasetKey: "work-items", Status: "success", SinceAt: run.workItemsSince,
		})
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	plan, err := loadPostSyncPlan(ctx, tx, PostSyncArgs{TransportArgs: TransportArgs{
		Version: ContractVersionV1, OrgID: anchorOrg, RunID: anchorRun,
		DispatchOutbox: anchorOutbox, RouteGeneration: postSyncRouteGeneration,
	}}, now)
	if err != nil {
		t.Fatal(err)
	}
	if plan == nil {
		t.Fatal("no plan")
	}
	return plan
}

func at(day time.Time, hour int) *time.Time {
	value := day.Add(time.Duration(hour) * time.Hour)
	return &value
}

// TestPostSyncPlanAnchorsOnRunCompletion pins CHAOS-14115: a post_sync delivery that runs after the run's day
// (late, or re-driven after its River job was exhausted) plans the run's own day from sync_runs.completed_at, never
// the day of its own clock, and never plans complexity for a day that is not today.
func TestPostSyncPlanAnchorsOnRunCompletion(t *testing.T) {
	nextDay := anchorDay.AddDate(0, 0, 1)

	t.Run("N1 unbounded run re-driven one day late plans the run's day", func(t *testing.T) {
		plan := planAt(t, anchorCase{startedAt: at(anchorDay, 8), completedAt: at(anchorDay, 10), unbounded: true}, *at(nextDay, 9))
		if !plan.TargetDay.Equal(*at(anchorDay, 10)) || plan.BackfillDays != 1 {
			t.Fatalf("TargetDay=%s BackfillDays=%d, want the run's completion %s / 1", plan.TargetDay, plan.BackfillDays, *at(anchorDay, 10))
		}
		if plan.Complexity {
			t.Fatal("Complexity planned for a past day: complexity scans today's files")
		}
	})

	t.Run("N2 open work-item window closes at the run's completion", func(t *testing.T) {
		since := anchorDay.AddDate(0, 0, -2)
		plan := planAt(t, anchorCase{startedAt: at(since, 8), completedAt: at(anchorDay, 10), workItemsSince: &since},
			*at(anchorDay.AddDate(0, 0, 3), 9))
		want := []time.Time{since, since.AddDate(0, 0, 1), anchorDay}
		if !slices.EqualFunc(plan.WorkItemWindowDays, want, func(a, b time.Time) bool { return a.Equal(b) }) {
			t.Fatalf("WorkItemWindowDays=%v, want %v", plan.WorkItemWindowDays, want)
		}
	})

	t.Run("N3 unbounded run on its own day plans complexity", func(t *testing.T) {
		plan := planAt(t, anchorCase{startedAt: at(anchorDay, 8), completedAt: at(anchorDay, 10), unbounded: true}, *at(anchorDay, 11))
		if !plan.Complexity || !plan.TargetDay.Equal(*at(anchorDay, 10)) {
			t.Fatalf("Complexity=%t TargetDay=%s, want true / the run's completion", plan.Complexity, plan.TargetDay)
		}
	})

	t.Run("N3 bounded single-day run one day late plans no complexity", func(t *testing.T) {
		plan := planAt(t, anchorCase{startedAt: at(anchorDay, 8), completedAt: at(anchorDay, 10)}, *at(nextDay, 9))
		if plan.Complexity || !plan.TargetDay.Equal(anchorDay) {
			t.Fatalf("Complexity=%t TargetDay=%s, want false / %s", plan.Complexity, plan.TargetDay, anchorDay)
		}
	})

	t.Run("N3 midnight: run done 23:59 D, pass 00:01 D+1 plans D and no complexity", func(t *testing.T) {
		done := anchorDay.Add(23*time.Hour + 59*time.Minute)
		pass := nextDay.Add(time.Minute)
		plan := planAt(t, anchorCase{startedAt: at(anchorDay, 8), completedAt: &done, unbounded: true}, pass)
		if !plan.TargetDay.Equal(done) || plan.Complexity {
			t.Fatalf("TargetDay=%s Complexity=%t, want %s / false (the 00:45 UTC fixed complexity schedule covers D and D+1)",
				plan.TargetDay, plan.Complexity, done)
		}
	})

	t.Run("N4 on-time bounded run is unchanged", func(t *testing.T) {
		plan := planAt(t, anchorCase{startedAt: at(anchorDay, 8), completedAt: at(anchorDay, 10)}, *at(anchorDay, 12))
		if !plan.Complexity || !plan.TargetDay.Equal(anchorDay) || plan.BackfillDays != 1 {
			t.Fatalf("Complexity=%t TargetDay=%s BackfillDays=%d, want true / %s / 1", plan.Complexity, plan.TargetDay, plan.BackfillDays, anchorDay)
		}
	})

	t.Run("N5 a run with no completed_at keeps the delivery clock", func(t *testing.T) {
		now := *at(nextDay, 9)
		plan := planAt(t, anchorCase{startedAt: at(anchorDay, 8), unbounded: true}, now)
		if !plan.TargetDay.Equal(now) || !plan.Complexity {
			t.Fatalf("TargetDay=%s Complexity=%t, want %s / true", plan.TargetDay, plan.Complexity, now)
		}
	})
}

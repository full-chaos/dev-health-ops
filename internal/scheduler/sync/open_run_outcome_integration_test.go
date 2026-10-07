//go:build integration

package sync

import (
	"context"
	gosync "sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// The outcome tests of "one scheduled run per configuration at a time". They
// read only what the system exists to produce -- the run rows and the unit
// windows -- through the production scheduler window, occurrence reconciler and
// materializer, so they state the defect itself: before the rule, every tick
// below started another run beside the open one.

func configureMaterializerFixtureAsDue(t *testing.T, fixture materializerFixture, dueBase time.Time) {
	t.Helper()
	if _, err := fixture.pool.Exec(context.Background(), `
UPDATE public.sync_configurations SET planner_managed = TRUE, created_at = $2, last_sync_at = NULL WHERE id = $1::uuid`,
		fixture.occurrence.ConfigID, dueBase); err != nil {
		t.Fatal(err)
	}
	// A planner-managed configuration plans only the sources that name it.
	if _, err := fixture.pool.Exec(context.Background(), `
UPDATE public.integration_sources SET metadata = json_build_object('planner_managed_sync_config_id', $1::text) WHERE org_id = $2`,
		fixture.occurrence.ConfigID, fixture.occurrence.OrgID); err != nil {
		t.Fatal(err)
	}
}

func occurrenceCount(t *testing.T, pool *pgxpool.Pool, configID string) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(context.Background(), `
SELECT count(*) FROM public.scheduled_sync_occurrences WHERE sync_config_id = $1::uuid`, configID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

// endScheduledRun ends the run as the finalizer leaves the run row: terminal
// status and completed_at. It does NOT stamp the configuration's last_sync_at,
// so these tests hold the case where the schedule's base never moved.
func endScheduledRun(t *testing.T, pool *pgxpool.Pool, runID, status string, endedAt time.Time) {
	t.Helper()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `UPDATE public.sync_run_units SET status = 'success', updated_at = now() WHERE sync_run_id = $1::uuid`, runID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE public.sync_runs SET status = $2, completed_at = $3 WHERE id = $1::uuid`, runID, status, endedAt); err != nil {
		t.Fatal(err)
	}
}

func resumedRunID(t *testing.T, pool *pgxpool.Pool, occurrenceID string) string {
	t.Helper()
	var runID string
	if err := pool.QueryRow(context.Background(), `
SELECT sync_run_id::text FROM public.scheduled_sync_occurrences WHERE occurrence_id = $1`, occurrenceID).Scan(&runID); err != nil {
		t.Fatal(err)
	}
	return runID
}

// A configuration with an open scheduled run gets no second run, however many
// ticks pass, and the run started after it ends covers the whole gap.
func TestScheduledSyncStartsNoSecondRunWhileOneIsOpen(t *testing.T) {
	fixture := startMaterializerPostgres(t)
	ctx := context.Background()
	pool := fixture.pool
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
	// window is one scheduler loop step: hand off what is due, then plan it.
	window := func(at time.Time) {
		t.Helper()
		if _, err := repository.HandoffDueResult(ctx, at, 4, NewOccurrenceCoordinator()); err != nil {
			t.Fatalf("window at %s: %v", at, err)
		}
		if _, err := reconciler.Reconcile(ctx, at, 10); err != nil {
			t.Fatalf("reconcile at %s: %v", at, err)
		}
	}
	scheduledRuns := func() []string {
		t.Helper()
		rows, err := pool.Query(ctx, `SELECT id::text FROM public.sync_runs WHERE triggered_by = 'schedule' ORDER BY created_at, id`)
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
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return ids
	}

	window(hour.Add(time.Second))
	first := scheduledRuns()
	if len(first) != 1 {
		t.Fatalf("the first window started %d scheduled runs, want 1", len(first))
	}
	var units int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM public.sync_run_units WHERE sync_run_id = $1::uuid`, first[0]).Scan(&units); err != nil {
		t.Fatal(err)
	}
	if units == 0 {
		t.Fatal("the fixture run planned no unit, so it cannot show an open run")
	}
	for offset := 1; offset <= 3; offset++ {
		window(hour.Add(time.Duration(offset) * time.Hour))
		if runs := scheduledRuns(); len(runs) != 1 {
			t.Fatalf("after %d ticks with the first run open there are %d scheduled runs, want 1", offset, len(runs))
		}
	}

	watermark := hour.Add(-10 * time.Minute)
	endScheduledRun(t, pool, first[0], "partial_failed", hour.Add(3*time.Hour+40*time.Minute))
	if _, err := pool.Exec(ctx, `UPDATE public.sync_watermarks SET last_synced_at = $2 WHERE org_id = $1 AND dataset_key = 'commits'`, fixture.occurrence.OrgID, watermark); err != nil {
		t.Fatal(err)
	}
	resumeAt := hour.Add(4 * time.Hour)
	window(resumeAt)
	runs := scheduledRuns()
	if len(runs) != 2 {
		t.Fatalf("after the first run ended there are %d scheduled runs, want 2", len(runs))
	}
	var since, before *time.Time
	if err := pool.QueryRow(ctx, `
SELECT since_at, before_at FROM public.sync_run_units
WHERE sync_run_id <> $1::uuid AND dataset_key = 'commits'`, first[0]).Scan(&since, &before); err != nil {
		t.Fatal(err)
	}
	if since == nil || !since.Equal(watermark) {
		t.Fatalf("the next run's commits unit starts at %v, want its watermark %s", since, watermark)
	}
	if before == nil || before.Before(resumeAt) {
		t.Fatalf("the next run's commits unit ends at %v, want the whole gap up to %s", before, resumeAt)
	}
}

// Two schedulers that tick at once, at every instant, still start one run.
func TestConcurrentSchedulersStartNoSecondRunWhileOneIsOpen(t *testing.T) {
	fixture := startMaterializerPostgres(t)
	ctx := context.Background()
	hour := time.Now().UTC().Truncate(time.Hour)
	configureMaterializerFixtureAsDue(t, fixture, hour.Add(-30*time.Minute))
	second, err := pgxpool.New(ctx, fixture.pool.Config().ConnString())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(second.Close)
	var schedulers []*Repository
	for _, pool := range []*pgxpool.Pool{fixture.pool, second} {
		repository, err := newRepositoryWithOwnership(pool, reviewedGoMutationOwnershipPolicy())
		if err != nil {
			t.Fatal(err)
		}
		schedulers = append(schedulers, repository)
	}
	for offset := 0; offset <= 3; offset++ {
		at := hour.Add(time.Duration(offset)*time.Hour + time.Second)
		start := make(chan struct{})
		failures := make(chan error, 8)
		var group gosync.WaitGroup
		for worker := 0; worker < 8; worker++ {
			scheduler := schedulers[worker%2]
			group.Add(1)
			go func() {
				defer group.Done()
				<-start
				if _, err := scheduler.HandoffDueResult(ctx, at, 4, NewOccurrenceCoordinator()); err != nil {
					failures <- err
				}
			}()
		}
		close(start)
		group.Wait()
		close(failures)
		for err := range failures {
			t.Fatalf("concurrent window at %s: %v", at, err)
		}
		if occurrences := occurrenceCount(t, fixture.pool, fixture.occurrence.ConfigID); occurrences != 1 {
			t.Fatalf("after instant %d two schedulers left %d occurrences for one configuration, want 1", offset, occurrences)
		}
	}
}

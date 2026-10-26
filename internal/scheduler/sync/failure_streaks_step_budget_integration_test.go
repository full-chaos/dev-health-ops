//go:build integration

package sync

import (
	"bytes"
	"context"
	"log/slog"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

var streakWarnDuration = regexp.MustCompile(`msg=sync\.materializer\.failure_streaks_unavailable .*failure=(streak_query_failed|row_iteration_failed) .*duration_ms=(\d+)`)

// CHAOS-11427: through the scheduler's own reconcile step, a failure-history
// read that cannot finish ends at its 500 ms bound (not at a share of the
// 15 s step), the plan still completes, and no attempt is spent, in six steps
// in a row. A step with less than the 5 s reserve left skips the read and
// still completes. A table lock held by another session for 1.5 s stands in
// for the slow read.
func TestFailureStreakReadStaysInsideTheSchedulerStepBudget(t *testing.T) {
	ctx := context.Background()
	setupCtx, cancelSetup := context.WithTimeout(ctx, 4*time.Minute)
	defer cancelSetup()
	instance, err := containers.StartPostgres(setupCtx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	pool, err := pgxpool.New(setupCtx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	pgschema.Apply(setupCtx, t, pool)
	configs := seedStreakHarness(setupCtx, t, pool, harnessConfig{integrations: 1, sources: 2, history: 40, activeRuns: 1, activeUnits: 1})
	row := configs[0]

	materializer, err := NewNativeMaterializer(pool)
	if err != nil {
		t.Fatal(err)
	}
	reconciler, err := NewOccurrenceReconciler(pool, materializer)
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	base := time.Now().UTC().Truncate(time.Hour).Add(-24 * time.Hour)
	insert := func(step int) string {
		t.Helper()
		scheduledFor := base.Add(time.Duration(step) * time.Hour)
		occurrence := newOccurrence(row.configID, harnessBigOrg, row.jobID, scheduledFor, scheduledFor, scheduledFor)
		if _, err := pool.Exec(ctx, `
INSERT INTO public.scheduled_sync_occurrences (occurrence_id, identity_version, org_id, sync_config_id, scheduled_job_id, scheduled_for, created_at)
VALUES ($1, $2, $3, $4::uuid, $5::uuid, $6, now())`, occurrence.ID, occurrence.IdentityVersion, harnessBigOrg, row.configID, row.jobID, scheduledFor); err != nil {
			t.Fatal(err)
		}
		return occurrence.ID
	}
	assertCompleted := func(id string, result OccurrenceReconcileResult, err error) {
		t.Helper()
		if err != nil || result.Completed != 1 || result.Retried != 0 || result.Quarantined != 0 {
			t.Fatalf("reconcile = %+v err = %v, want one completed occurrence", result, err)
		}
		var attempts int
		var linked bool
		if err := pool.QueryRow(ctx, `SELECT reconcile_attempt_count, sync_run_id IS NOT NULL FROM public.scheduled_sync_occurrences WHERE occurrence_id = $1`, id).Scan(&attempts, &linked); err != nil {
			t.Fatal(err)
		}
		if attempts != 0 || !linked {
			t.Fatalf("occurrence attempts = %d linked = %t, want 0 and a linked run", attempts, linked)
		}
	}

	const steps = 6
	for step := 0; step < steps; step++ {
		id := insert(step)
		lockTx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := lockTx.Exec(ctx, "LOCK TABLE public.sync_run_units IN ACCESS EXCLUSIVE MODE"); err != nil {
			t.Fatal(err)
		}
		released := make(chan struct{})
		go func() {
			defer close(released)
			time.Sleep(1500 * time.Millisecond)
			_ = lockTx.Rollback(ctx)
		}()
		stepCtx, cancel := context.WithTimeout(ctx, harnessStepTimeout)
		result, err := reconciler.Reconcile(stepCtx, time.Now(), 100)
		cancel()
		<-released
		assertCompleted(id, result, err)
	}
	matches := streakWarnDuration.FindAllStringSubmatch(logs.String(), -1)
	if len(matches) != steps {
		t.Fatalf("failed streak reads = %d, want %d (one per step, each cut off by its own bound); log:\n%s", len(matches), steps, logs.String())
	}
	// 500 ms bound + 100 ms client margin + 300 ms slack. Literal, so the test
	// also runs against code without the bound.
	const limit = 900 * time.Millisecond
	for _, match := range matches {
		ms, _ := strconv.Atoi(match[2])
		if time.Duration(ms)*time.Millisecond > limit {
			t.Fatalf("a failed streak read took %d ms, want at most %s", ms, limit)
		}
	}

	id := insert(steps)
	lowCtx, cancel := context.WithTimeout(ctx, 5050*time.Millisecond)
	result, err := reconciler.Reconcile(lowCtx, time.Now(), 100)
	cancel()
	assertCompleted(id, result, err)
	if !regexp.MustCompile(`failure=skipped_low_budget remaining_ms=\d+`).MatchString(logs.String()) {
		t.Fatalf("no skipped_low_budget WARN with remaining_ms for the low-budget step; log:\n%s", logs.String())
	}
}

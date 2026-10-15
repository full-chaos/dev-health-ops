//go:build integration

package sync

import (
	"context"
	"testing"
	"time"

	gosync "sync"

	"github.com/full-chaos/dev-health-ops/internal/synchandoff"
)

// initialBackfillFixture makes the webhook fixture's configuration the
// planner-managed parent of its integration, inside an organization of tier.
func initialBackfillFixture(t *testing.T, tier string) webhookRequestFixture {
	t.Helper()
	fixture := startWebhookRequestFixture(t)
	ctx := context.Background()
	now := time.Now().UTC()
	if _, err := fixture.pool.Exec(ctx, `
INSERT INTO organizations (id, slug, name, settings, tier, is_active, created_at, updated_at)
VALUES ($1, 'ib', 'ib', '{}', $2, true, $3, $3)`, fixture.org, tier, now); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE sync_configurations SET planner_managed = true, source_id = NULL WHERE id = $1`, fixture.configID); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func (fixture webhookRequestFixture) successRun(t *testing.T, triggeredBy string, completedAt time.Time) {
	t.Helper()
	if _, err := fixture.pool.Exec(context.Background(), `
INSERT INTO sync_runs (id, org_id, integration_id, triggered_by, mode, status, total_units, completed_units, failed_units, created_at, completed_at)
VALUES (gen_random_uuid(), $1, $2::uuid, $3, 'incremental', 'success', 1, 1, 0, $4, $5)`,
		fixture.org, fixture.integrationID, triggeredBy, completedAt.Add(-5*time.Minute), completedAt); err != nil {
		t.Fatal(err)
	}
}

// rollInstant is the roll: before the fixture's first run, after the epoch.
func rollInstant(t *testing.T, _ webhookRequestFixture) time.Time {
	t.Helper()
	return time.Now().UTC().Add(-time.Hour)
}

type initialBackfillState struct {
	occurrences, triggers, jobs int
}

func (fixture webhookRequestFixture) initialBackfillState(t *testing.T) initialBackfillState {
	t.Helper()
	return initialBackfillState{
		occurrences: fixture.count(t, "scheduled_sync_occurrences"),
		triggers:    fixture.count(t, "sync_manual_triggers"),
		jobs:        fixture.count(t, "backfill_jobs"),
	}
}

// A config whose first sync just succeeded gets exactly one backfill of the
// history older than the initial window, wired to the run the occurrence
// plans; a second window, a later success and a second scheduler add nothing.
func TestInitialBackfillEnqueuesOnceAfterTheFirstSuccess(t *testing.T) {
	fixture := initialBackfillFixture(t, "team")
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	completed := now.Add(-10 * time.Minute).Truncate(time.Microsecond)
	fixture.successRun(t, "schedule", completed)

	enqueuer := newInitialBackfillEnqueuer(fixture.pool, rollInstant(t, fixture))
	if err := enqueuer.enqueuePending(ctx, now, 10); err != nil {
		t.Fatal(err)
	}
	if got := fixture.initialBackfillState(t); got != (initialBackfillState{1, 1, 1}) {
		t.Fatalf("after the first window = %+v, want one occurrence, trigger and job", got)
	}
	var scheduledFor time.Time
	var mode, triggeredBy string
	var since, before time.Time
	if err := fixture.pool.QueryRow(ctx, `
SELECT o.scheduled_for, t.mode, t.triggered_by, t.since, t.before
FROM scheduled_sync_occurrences o JOIN sync_manual_triggers t USING (occurrence_id)`).Scan(&scheduledFor, &mode, &triggeredBy, &since, &before); err != nil {
		t.Fatal(err)
	}
	if !scheduledFor.Equal(completed) || mode != "backfill" || triggeredBy != "backfill" {
		t.Fatalf("occurrence = %s %s %s, want %s backfill backfill", scheduledFor, mode, triggeredBy, completed)
	}
	// team tier: 90-day cap, 30-day initial window: the backfill covers days 90..30 before the run.
	if days := before.Sub(since).Hours() / 24; days < 59.9 || days > 60.1 {
		t.Fatalf("window = %s .. %s (%.1f days), want 60 days", since, before, days)
	}
	occurrenceID := synchandoff.OccurrenceIdentity(fixture.configID, completed)
	ids, err := deterministicMaterializationIDs(occurrenceID)
	if err != nil {
		t.Fatal(err)
	}
	var marker, status string
	if err := fixture.pool.QueryRow(ctx, `SELECT celery_task_id, status FROM backfill_jobs`).Scan(&marker, &status); err != nil {
		t.Fatal(err)
	}
	if marker != "sync_run:"+ids.SyncRunID || status != "pending" {
		t.Fatalf("job = %s %s, want sync_run:%s pending", marker, status, ids.SyncRunID)
	}

	if err := enqueuer.enqueuePending(ctx, now.Add(time.Minute), 10); err != nil {
		t.Fatal(err)
	}
	fixture.successRun(t, "schedule", now.Add(time.Minute))
	other := newInitialBackfillEnqueuer(fixture.pool, rollInstant(t, fixture))
	if err := other.enqueuePending(ctx, now.Add(2*time.Minute), 10); err != nil {
		t.Fatal(err)
	}
	if got := fixture.initialBackfillState(t); got != (initialBackfillState{1, 1, 1}) {
		t.Fatalf("after re-sync and a second scheduler = %+v, want still one of each", got)
	}
}

// An installed config (a successful run older than the age bound) is never a
// candidate, however many runs succeed after it: a rollout backfills nothing.
func TestInitialBackfillIgnoresAConfigWithAnEarlierSuccess(t *testing.T) {
	fixture := initialBackfillFixture(t, "team")
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	fixture.successRun(t, "schedule", now.Add(-10*24*time.Hour))
	fixture.successRun(t, "schedule", now.Add(-time.Hour))

	enqueuer := newInitialBackfillEnqueuer(fixture.pool, rollInstant(t, fixture))
	if err := enqueuer.enqueuePending(ctx, now, 10); err != nil {
		t.Fatal(err)
	}
	if got := fixture.initialBackfillState(t); got != (initialBackfillState{}) {
		t.Fatalf("state = %+v, want nothing for a config with an earlier success", got)
	}
}

// A manual backfill's success is not a first sync.
func TestInitialBackfillIgnoresBackfillRuns(t *testing.T) {
	fixture := initialBackfillFixture(t, "team")
	now := time.Now().UTC().Truncate(time.Microsecond)
	fixture.successRun(t, "backfill", now.Add(-time.Hour))

	enqueuer := newInitialBackfillEnqueuer(fixture.pool, rollInstant(t, fixture))
	if err := enqueuer.enqueuePending(context.Background(), now, 10); err != nil {
		t.Fatal(err)
	}
	if got := fixture.initialBackfillState(t); got != (initialBackfillState{}) {
		t.Fatalf("state = %+v, want nothing", got)
	}
}

// Two schedulers racing on one config produce one backfill.
func TestInitialBackfillRaceMakesOneBackfill(t *testing.T) {
	fixture := initialBackfillFixture(t, "team")
	now := time.Now().UTC().Truncate(time.Microsecond)
	fixture.successRun(t, "schedule", now.Add(-10*time.Minute))

	var group gosync.WaitGroup
	start := make(chan struct{})
	errs := make(chan error, 4)
	for range 4 {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			errs <- newInitialBackfillEnqueuer(fixture.pool, rollInstant(t, fixture)).enqueuePending(context.Background(), now, 10)
		}()
	}
	close(start)
	group.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := fixture.initialBackfillState(t); got != (initialBackfillState{1, 1, 1}) {
		t.Fatalf("state after 4 racing schedulers = %+v, want one of each", got)
	}
}

// A tier whose cap does not reach past the initial window has no history to
// backfill: skipped with a closed reason, counted once, nothing minted.
func TestInitialBackfillSkipsWhenTheCapIsNotAboveTheDepth(t *testing.T) {
	fixture := initialBackfillFixture(t, "community")
	now := time.Now().UTC().Truncate(time.Microsecond)
	fixture.successRun(t, "schedule", now.Add(-10*time.Minute))

	enqueuer := newInitialBackfillEnqueuer(fixture.pool, rollInstant(t, fixture))
	for range 2 {
		if err := enqueuer.enqueuePending(context.Background(), now, 10); err != nil {
			t.Fatal(err)
		}
	}
	if got := fixture.initialBackfillState(t); got != (initialBackfillState{}) {
		t.Fatalf("state = %+v, want nothing", got)
	}
	if got := enqueuer.outcomes[InitialBackfillSkipCapNotAboveDepth]; got != 1 {
		t.Fatalf("cap_le_depth counted %d times, want once", got)
	}
}

// A configuration that is not the planner-managed parent is skipped, counted.
func TestInitialBackfillSkipsAConfigThatIsNotPlannerManaged(t *testing.T) {
	fixture := initialBackfillFixture(t, "team")
	now := time.Now().UTC().Truncate(time.Microsecond)
	if _, err := fixture.pool.Exec(context.Background(), `UPDATE sync_configurations SET planner_managed = false WHERE id = $1`, fixture.configID); err != nil {
		t.Fatal(err)
	}
	fixture.successRun(t, "schedule", now.Add(-10*time.Minute))

	enqueuer := newInitialBackfillEnqueuer(fixture.pool, rollInstant(t, fixture))
	if err := enqueuer.enqueuePending(context.Background(), now, 10); err != nil {
		t.Fatal(err)
	}
	if got := fixture.initialBackfillState(t); got != (initialBackfillState{}) {
		t.Fatalf("state = %+v, want nothing", got)
	}
	if got := enqueuer.outcomes[InitialBackfillSkipNotPlanner]; got != 1 {
		t.Fatalf("not_planner_managed counted %d times, want once", got)
	}
}

// A first sync that started before the roll instant is not a candidate, even
// when it completes after it and inside the age bound.
func TestInitialBackfillIgnoresAFirstSyncThatStartedBeforeTheRoll(t *testing.T) {
	fixture := initialBackfillFixture(t, "team")
	now := time.Now().UTC().Truncate(time.Microsecond)
	fixture.successRun(t, "schedule", now.Add(-10*time.Minute)) // created 15 min ago

	enqueuer := newInitialBackfillEnqueuer(fixture.pool, now.Add(-10*time.Minute))
	if err := enqueuer.enqueuePending(context.Background(), now, 10); err != nil {
		t.Fatal(err)
	}
	if got := fixture.initialBackfillState(t); got != (initialBackfillState{}) {
		t.Fatalf("state = %+v, want nothing for a sync that began before the roll", got)
	}
}

// A first success that is days old (an outage, a long first sync) is still
// backfilled once, inside the age bound.
func TestInitialBackfillEnqueuesALateFirstSuccess(t *testing.T) {
	fixture := initialBackfillFixture(t, "team")
	now := time.Now().UTC().Truncate(time.Microsecond)
	fixture.successRun(t, "schedule", now.Add(-3*24*time.Hour))

	enqueuer := newInitialBackfillEnqueuer(fixture.pool, now.Add(-4*24*time.Hour))
	if err := enqueuer.enqueuePending(context.Background(), now, 10); err != nil {
		t.Fatal(err)
	}
	if got := fixture.initialBackfillState(t); got != (initialBackfillState{1, 1, 1}) {
		t.Fatalf("state = %+v, want one of each", got)
	}
}

// A config with a failed attempt before the roll was already syncing: a later
// success is not a first sync.
func TestInitialBackfillIgnoresAConfigWithAPreRollFailedAttempt(t *testing.T) {
	fixture := initialBackfillFixture(t, "team")
	now := time.Now().UTC().Truncate(time.Microsecond)
	if _, err := fixture.pool.Exec(context.Background(), `
INSERT INTO sync_runs (id, org_id, integration_id, triggered_by, mode, status, total_units, completed_units, failed_units, created_at, completed_at)
VALUES (gen_random_uuid(), $1, $2::uuid, 'schedule', 'incremental', 'failed', 1, 0, 1, $3, $3)`,
		fixture.org, fixture.integrationID, now.Add(-3*time.Hour)); err != nil {
		t.Fatal(err)
	}
	fixture.successRun(t, "schedule", now.Add(-10*time.Minute))

	enqueuer := newInitialBackfillEnqueuer(fixture.pool, now.Add(-time.Hour))
	if err := enqueuer.enqueuePending(context.Background(), now, 10); err != nil {
		t.Fatal(err)
	}
	if got := fixture.initialBackfillState(t); got != (initialBackfillState{}) {
		t.Fatalf("state = %+v, want nothing", got)
	}
}

// A source-scoped child configuration is not the planner-managed parent.
func TestInitialBackfillSkipsASourceScopedChild(t *testing.T) {
	fixture := initialBackfillFixture(t, "team")
	now := time.Now().UTC().Truncate(time.Microsecond)
	if _, err := fixture.pool.Exec(context.Background(), `UPDATE sync_configurations SET source_id = $2::uuid WHERE id = $1`, fixture.configID, fixture.sourceID); err != nil {
		t.Fatal(err)
	}
	fixture.successRun(t, "schedule", now.Add(-10*time.Minute))

	enqueuer := newInitialBackfillEnqueuer(fixture.pool, rollInstant(t, fixture))
	if err := enqueuer.enqueuePending(context.Background(), now, 10); err != nil {
		t.Fatal(err)
	}
	if got := fixture.initialBackfillState(t); got != (initialBackfillState{}) {
		t.Fatalf("state = %+v, want nothing", got)
	}
	if got := enqueuer.outcomes[InitialBackfillSkipNotPlanner]; got != 1 {
		t.Fatalf("not_planner_managed counted %d times, want once", got)
	}
}

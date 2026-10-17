//go:build integration

package sync

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// CHAOS-8802 on a real Postgres through the materializer: a source whose
// work-items units failed three times in a row with the time limit is left
// alone for a day, then planned with a shorter window, while its dataset row
// carries the needs_attention stamp. Nothing in planning reads the stamp.
func TestNativeMaterializerBacksOffWorkItemsAfterThreeEqualFailures(t *testing.T) {
	ctx := context.Background()
	fixture := startMaterializerPostgres(t)
	suppressAutoSecurityDataset(t, fixture)
	materializer, err := NewNativeMaterializer(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	failedAt := fixture.occurrence.ScheduledFor.Add(-time.Hour)
	const runID = "00000000-0000-4000-8000-000000008802"
	for _, statement := range []string{`
INSERT INTO integration_datasets (id, org_id, integration_id, dataset_key, is_enabled, options)
VALUES (gen_random_uuid(), $1, (SELECT integration_id FROM sync_configurations LIMIT 1), 'work-items', TRUE, '{}'::jsonb)`, `
INSERT INTO sync_runs (id, org_id, integration_id, triggered_by, mode, status, total_units, completed_units, failed_units, created_at)
VALUES ('` + runID + `'::uuid, $1, (SELECT integration_id FROM sync_configurations LIMIT 1), 'scheduled', 'incremental', 'failed', 3, 0, 3, now())`,
	} {
		if _, err := fixture.pool.Exec(ctx, statement, fixture.occurrence.OrgID); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ {
		if _, err := fixture.pool.Exec(ctx, `
INSERT INTO sync_run_units (
	id, org_id, sync_run_id, integration_id, source_id, provider, dataset_key,
	cost_class, mode, status, attempts, result, created_at, updated_at
) VALUES (
	gen_random_uuid(), $1, $2::uuid,
	(SELECT integration_id FROM sync_configurations LIMIT 1),
	(SELECT id FROM integration_sources LIMIT 1),
	'github', 'work-items', 'medium', 'incremental', 'failed', 5,
	'{"error_category":"time_limit","cause_class":"time_limit"}', $3, $3
)`, fixture.occurrence.OrgID, runID, failedAt.Add(-time.Duration(i)*time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := fixture.pool.Exec(ctx, `
UPDATE integration_datasets
SET unavailable_reason = 'needs_attention:time_limit', unavailable_since = $2, unavailable_last_seen_at = $2
WHERE org_id = $1 AND dataset_key = 'work-items'`, fixture.occurrence.OrgID, failedAt); err != nil {
		t.Fatal(err)
	}

	workItemsUnits := func(syncRunID string) (count int, span time.Duration) {
		t.Helper()
		rows, err := fixture.pool.Query(ctx, `
SELECT since_at, before_at FROM sync_run_units WHERE sync_run_id = $1::uuid AND dataset_key = 'work-items'`, syncRunID)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var since, before time.Time
			if err := rows.Scan(&since, &before); err != nil {
				t.Fatal(err)
			}
			count++
			span = before.Sub(since)
		}
		return count, span
	}
	reason := func() string {
		t.Helper()
		var value string
		if err := fixture.pool.QueryRow(ctx, `
SELECT COALESCE(unavailable_reason, '') FROM integration_datasets WHERE org_id = $1 AND dataset_key = 'work-items'`,
			fixture.occurrence.OrgID).Scan(&value); err != nil {
			t.Fatal(err)
		}
		return value
	}

	within := fixture.occurrence
	within.ID = "occurrence:v1:scheduled:backoff-within-day"
	plan, err := materializeAndCommit(t, fixture, materializer, within)
	if err != nil {
		t.Fatal(err)
	}
	if count, _ := workItemsUnits(plan.SyncRunID); count != 0 {
		t.Fatalf("%d work-items units planned within a day of the third failure, want 0", count)
	}

	due := fixture.occurrence
	due.ID = "occurrence:v1:scheduled:backoff-next-day"
	due.ScheduledFor = fixture.occurrence.ScheduledFor.Add(25 * time.Hour)
	plan, err = materializeAndCommit(t, fixture, materializer, due)
	if err != nil {
		t.Fatal(err)
	}
	count, span := workItemsUnits(plan.SyncRunID)
	if count != 1 {
		t.Fatalf("%d work-items units planned a day after the third failure while the stamp is set, want 1 (the daily try)", count)
	}
	// A cold start spans the initial sync depth (90 days by default); three
	// time-limit failures halve it three times.
	if span <= 0 || span > 12*24*time.Hour {
		t.Fatalf("daily try window = %s, want a halved window of at most 12 days (%s)", span, fmt.Sprint(span))
	}
	if got := reason(); got != "needs_attention:time_limit" {
		t.Fatalf("planning changed the stamp to %q", got)
	}
}

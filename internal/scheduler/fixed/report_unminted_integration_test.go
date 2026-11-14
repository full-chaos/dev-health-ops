//go:build integration

package fixed

import (
	"context"
	"testing"
	"time"
)

// The due query's previous-occurrence column is the newest occurrence's
// created_at, ordered by scheduled_for rather than by created_at, and NULL when
// the report has no occurrence.
func TestDueQueryPreviousOccurrenceCreatedAt(t *testing.T) {
	pool := startScheduledReportPostgres(t)
	seedScheduledReport(t, pool, "0 * * * *", "UTC", time.Date(2026, time.August, 19, 0, 0, 0, 0, time.UTC))
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	dueThrough := time.Date(2026, time.August, 20, 0, 0, 0, 0, time.UTC)

	candidates := func() []dueReportCandidate {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		found, err := reportsProducer(t).lockDueCandidates(ctx, tx, dueThrough, false)
		if err != nil {
			t.Fatal(err)
		}
		return found
	}

	none := candidates()
	if len(none) != 1 || none[0].PreviousOccurrenceCreatedAt != nil {
		t.Fatalf("a report with no occurrence: candidates = %+v, want one with a NULL previous created_at", none)
	}

	older := time.Date(2026, time.August, 19, 22, 0, 0, 0, time.UTC)
	newer := time.Date(2026, time.August, 19, 23, 0, 0, 0, time.UTC)
	// The older cron instant carries the LATER created_at, so ordering by
	// created_at instead of scheduled_for would return the wrong row.
	olderCreated := time.Date(2026, time.August, 19, 23, 30, 0, 0, time.UTC)
	newerCreated := time.Date(2026, time.August, 19, 23, 0, 40, 0, time.UTC)
	for _, row := range []struct{ scheduledFor, createdAt time.Time }{{older, olderCreated}, {newer, newerCreated}} {
		if _, err := pool.Exec(ctx, `
INSERT INTO public.scheduled_report_occurrences
    (occurrence_id, identity_version, org_id, report_id, scheduled_job_id, scheduled_for, created_at)
VALUES ($1, $2, $3, $4::uuid, $5::uuid, $6, $7)`,
			ScheduledReportOccurrenceID(testReportID, row.scheduledFor), scheduledReportOccurrenceIdentityVersion,
			testOrganizationID, testReportID, testJobID, row.scheduledFor, row.createdAt); err != nil {
			t.Fatal(err)
		}
	}
	two := candidates()
	if len(two) != 1 || two[0].PreviousOccurrenceCreatedAt == nil ||
		!two[0].PreviousOccurrenceCreatedAt.Equal(newerCreated) {
		t.Fatalf("a report with two occurrences: candidates = %+v, want the newest occurrence's created_at %s", two, newerCreated)
	}
}

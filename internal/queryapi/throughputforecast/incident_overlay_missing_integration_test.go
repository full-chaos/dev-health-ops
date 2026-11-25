//go:build integration

package throughputforecast

import (
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// Missing is not healthy: with no incident_metrics_daily row in the history window (incidents not ingested), the
// incident overlay reads 0 incidents a week, the same answer as a window whose stored rows count 0 incidents. The WANT:
// the value stays 0 in both and the state tells them apart: NO_ROWS against MEASURED. The control (one stored row with 0
// incidents) keeps the value 0, and a populated window keeps its rate.
func TestIncidentOverlayTellsNoIncidentDataFromAMeasuredZero(t *testing.T) {
	ctx, conn, client := startThroughputForecastSchema(t)
	const org = "org-incident-missing"
	const weeks = 4
	today := time.Now().UTC().Truncate(24 * time.Hour)

	noRows, err := loadIncidentOverlay(ctx, client, org, weeks, today)
	if err != nil {
		t.Fatal(err)
	}

	if err := conn.Exec(ctx, `INSERT INTO incident_metrics_daily (org_id, repo_id, day, incidents_count, computed_at)
        VALUES (?, toUUID('7a000000-0000-4000-8000-000000000001'), ?, 0, now())`, org, today.AddDate(0, 0, -3)); err != nil {
		t.Fatal(err)
	}
	measuredZero, err := loadIncidentOverlay(ctx, client, org, weeks, today)
	if err != nil {
		t.Fatal(err)
	}

	if noRows.value != 0 || measuredZero.value != 0 {
		t.Errorf("value: no row %v, one row of 0 incidents %v; want 0 and 0", noRows.value, measuredZero.value)
	}
	if noRows.state != model.NewestDayStateNoRows {
		t.Errorf("no incident row in the window: state %s, want NO_ROWS", noRows.state)
	}
	if measuredZero.state != model.NewestDayStateMeasured {
		t.Errorf("one stored row with 0 incidents: state %s, want MEASURED", measuredZero.state)
	}

	if err := conn.Exec(ctx, `INSERT INTO incident_metrics_daily (org_id, repo_id, day, incidents_count, computed_at)
        VALUES (?, toUUID('7a000000-0000-4000-8000-000000000003'), ?, 8, now())`, org, today.AddDate(0, 0, -2)); err != nil {
		t.Fatal(err)
	}
	populated, err := loadIncidentOverlay(ctx, client, org, weeks, today)
	if err != nil {
		t.Fatal(err)
	}
	if populated.value <= 0 || populated.state != model.NewestDayStateMeasured {
		t.Errorf("populated window: value %v state %s, want a positive rate and MEASURED", populated.value, populated.state)
	}
}

// Missing is not healthy, review side: with no repo_metrics_daily row carrying a first-review latency in the window,
// the review overlay reads 0 hours, the same answer as a window whose stored rows measure 0 hours. The WANT: the value
// stays 0 in both and the state tells them apart: NO_ROWS against MEASURED. The control (one stored row measuring 0
// hours) keeps the value 0, and a populated window keeps its latency.
func TestReviewOverlayTellsNoReviewDataFromAMeasuredZero(t *testing.T) {
	ctx, conn, client := startThroughputForecastSchema(t)
	const org = "org-review-missing"
	const weeks = 4
	today := time.Now().UTC().Truncate(24 * time.Hour)

	noRows, err := loadReviewOverlay(ctx, client, org, weeks, today)
	if err != nil {
		t.Fatal(err)
	}

	if err := conn.Exec(ctx, `INSERT INTO repo_metrics_daily (repo_id, day, pr_first_review_p50_hours, org_id, computed_at)
        VALUES (toUUID('7a000000-0000-4000-8000-000000000002'), ?, 0, ?, now())`, today.AddDate(0, 0, -3), org); err != nil {
		t.Fatal(err)
	}
	measuredZero, err := loadReviewOverlay(ctx, client, org, weeks, today)
	if err != nil {
		t.Fatal(err)
	}

	if noRows.value != 0 || measuredZero.value != 0 {
		t.Errorf("value: no row %v, one row measuring 0 hours %v; want 0 and 0", noRows.value, measuredZero.value)
	}
	if noRows.state != model.NewestDayStateNoRows {
		t.Errorf("no review row in the window: state %s, want NO_ROWS", noRows.state)
	}
	if measuredZero.state != model.NewestDayStateMeasured {
		t.Errorf("one stored row measuring 0 hours: state %s, want MEASURED", measuredZero.state)
	}

	if err := conn.Exec(ctx, `INSERT INTO repo_metrics_daily (repo_id, day, pr_first_review_p50_hours, org_id, computed_at)
        VALUES (toUUID('7a000000-0000-4000-8000-000000000004'), ?, 6, ?, now())`, today.AddDate(0, 0, -2), org); err != nil {
		t.Fatal(err)
	}
	populated, err := loadReviewOverlay(ctx, client, org, weeks, today)
	if err != nil {
		t.Fatal(err)
	}
	if populated.value != 3 || populated.state != model.NewestDayStateMeasured {
		t.Errorf("populated window: value %v state %s, want 3 (the average of 0 and 6) and MEASURED", populated.value, populated.state)
	}
}

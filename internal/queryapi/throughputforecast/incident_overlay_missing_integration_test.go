//go:build integration

package throughputforecast

import (
	"fmt"
	"testing"
	"time"
)

// Missing is not healthy: with no incident_metrics_daily row in the history window (incidents not ingested), the
// incident overlay reads 0 incidents a week, the same answer as a window whose stored rows count 0 incidents; the
// overlay then serves as inactive either way. The WANT: the no-rows answer differs from the measured-zero answer. The
// control (one stored row with 0 incidents) keeps today's answer.
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

	if got := fmt.Sprint(measuredZero); got != "0" {
		t.Errorf("control, one stored row with 0 incidents: overlay %s, want 0", got)
	}
	if fmt.Sprint(noRows) == fmt.Sprint(measuredZero) {
		t.Errorf("no incident row in the window: overlay %v, the same as a measured 0 (%v): missing incident data reads as no incidents", noRows, measuredZero)
	}
}

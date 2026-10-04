//go:build integration

package operatingreview

import (
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
)

// CHAOS-8115 on a real store: a week whose daily tables hold rows of zeros has
// data, and a week with no row has none. Both give the value 0.
func TestRealClickHouse_HasDataSeparatesStoredZerosFromNoRows(t *testing.T) {
	ctx, admin, client := startOperatingReviewSchema(t)
	const org = "org-8115"
	repoID := "81158115-8115-4115-8115-811581158115"
	week := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC) // the stored week; the week before it holds nothing
	computed := time.Date(2026, 8, 25, 1, 0, 0, 0, time.UTC)

	if err := admin.Exec(ctx, `INSERT INTO deploy_metrics_daily
		(repo_id, day, deployments_count, failed_deployments_count, org_id, computed_at)
		VALUES (?, ?, 0, 0, ?, ?)`, repoID, week, org, computed); err != nil {
		t.Fatalf("insert deploy row: %v", err)
	}
	if err := admin.Exec(ctx, `INSERT INTO incident_metrics_daily
		(repo_id, day, incidents_count, mttr_p50_hours, org_id, computed_at)
		VALUES (?, ?, 0, ?, ?, ?)`, repoID, week, nil, org, computed); err != nil {
		t.Fatalf("insert incident row: %v", err)
	}

	review, err := Resolve(ctx, client, org, nil, graphqldate.New(week))
	if err != nil {
		t.Fatal(err)
	}
	seen := 0
	for _, section := range review.Sections {
		for _, m := range section.Metrics {
			switch m.Key {
			case "deployments_count", "incidents_count":
				seen++
				if m.Value != 0 || !m.HasData {
					t.Errorf("%s: value %v hasData %v, want a stored 0 (hasData true)", m.Key, m.Value, m.HasData)
				}
				if m.Delta.PriorValue != 0 || m.Delta.HasPriorData {
					t.Errorf("%s: prior %v hasPriorData %v, want the 0 placeholder of a week with no row", m.Key, m.Delta.PriorValue, m.Delta.HasPriorData)
				}
			case "mttr_hours", "change_failure_rate", "throughput", "bus_factor", "ktlo_units", "ai_adoption_ratio":
				seen++
				// MTTR is NULL in the stored incident row, no deployment gives no failure rate, and
				// the other tables hold no row for this org.
				if m.HasData || m.Delta.HasPriorData || m.Value != 0 {
					t.Errorf("%s: value %v hasData %v hasPriorData %v, want no data", m.Key, m.Value, m.HasData, m.Delta.HasPriorData)
				}
			}
		}
	}
	if seen != 8 {
		t.Fatalf("checked %d metrics, want 8", seen)
	}
}

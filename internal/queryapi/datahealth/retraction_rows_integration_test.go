//go:build integration

package datahealth

import (
	"context"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/retractionseed"
)

// TestMetricLineageGivesRetractionRowsNoWeight reads the lineage of a work
// item metric and of a team commit metric for two organizations that hold the
// same measurements. One of them also holds the old rows of the retired team
// ids and the retraction row over each (package retractionseed). The row count
// and the time of the newest compute must be the same. An organization of
// retraction rows only has no measured row.
func TestMetricLineageGivesRetractionRowsNoWeight(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	store := retractionseed.Start(ctx, t)
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: store.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()
	reader := &Reader{ClickHouse: client}

	day := store.Days[len(store.Days)-1]
	for _, team := range retractionseed.Teams {
		retractionseed.Retract(ctx, t, store.Conn, retractionseed.RetractionOnlyOrg, day, team, store.OldComputedAt, store.NewComputedAt)
	}

	for _, metric := range []string{"wip", "after_hours_ratio"} {
		control := reader.MetricLineage(ctx, retractionseed.ControlOrg, metric)
		// The control lineage, from the seed: one row for each of four teams
		// on each of seven days, the newest of the second compute.
		if control == nil || control.RowCount == nil || *control.RowCount != 28 || !control.ComputedAt.Equal(store.NewComputedAt) {
			t.Fatalf("control lineage of %s = %+v, want 28 rows computed at %s", metric, control, store.NewComputedAt)
		}
		retracted := reader.MetricLineage(ctx, retractionseed.RetractedOrg, metric)
		if retracted == nil || retracted.RowCount == nil || *retracted.RowCount != *control.RowCount ||
			!retracted.ComputedAt.Equal(control.ComputedAt) {
			t.Errorf("the retraction rows changed the lineage of %s: control %d rows at %s, retracted %+v",
				metric, *control.RowCount, control.ComputedAt, retracted)
		}
		only := reader.MetricLineage(ctx, retractionseed.RetractionOnlyOrg, metric)
		if only != nil && only.RowCount != nil && *only.RowCount != 0 {
			t.Errorf("lineage of %s for retraction rows only counts %d rows, want none", metric, *only.RowCount)
		}
		if only != nil && only.ComputedAt.After(time.Unix(0, 0)) {
			t.Errorf("lineage of %s for retraction rows only is computed at %s, want no compute time", metric, only.ComputedAt)
		}
	}
}

//go:build integration

package datahealth

import (
	"context"
	"fmt"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/retractionseed"
)

// TestMetricLineageRowCountCountsOneRowView seeds work_item_metrics_daily as
// the writer stores it since L2a-2: an 'org' row and a 'scope' row for each
// item day. The served rowCount is the number of item days (the org view),
// a legacy (empty row_view) row counts, and a scope-only row does not.
func TestMetricLineageRowCountCountsOneRowView(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	store := retractionseed.Start(ctx, t)
	conn := store.Conn

	seed := func(org, view string, days int) {
		t.Helper()
		for d := 1; d <= days; d++ {
			stmt := fmt.Sprintf("INSERT INTO work_item_metrics_daily (org_id, day, computed_at, items_completed, row_view) VALUES ('%s', '2026-01-%02d', '2026-01-%02d 03:04:05', 1, '%s')", org, d, d, view)
			if err := conn.Exec(ctx, stmt); err != nil {
				t.Fatalf("seed %s/%s: %v", org, view, err)
			}
		}
	}
	seed("org-both", "org", 3)
	seed("org-both", "scope", 3)
	seed("org-legacy", "", 2)
	seed("org-scope-only", "scope", 2)
	// The shared OrgPredicate convention: a legacy row counts in every view, so a
	// legacy row and an org row of the same item day count twice. Not fixed here.
	seed("org-legacy-same-day", "", 1)
	seed("org-legacy-same-day", "org", 1)

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: store.URI})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	reader := &Reader{ClickHouse: client}

	for _, c := range []struct {
		org  string
		want int
	}{{"org-both", 3}, {"org-legacy", 2}, {"org-scope-only", 0}, {"org-legacy-same-day", 2}} {
		// Kill sites: no view predicate fails org-both (6) and org-scope-only (2);
		// the scope form fails org-scope-only (2) and org-legacy-same-day (1).
		lin := reader.MetricLineage(ctx, c.org, "throughput")
		if lin == nil || lin.RowCount == nil || *lin.RowCount != c.want {
			got := -1
			if lin != nil && lin.RowCount != nil {
				got = *lin.RowCount
			}
			t.Errorf("%s: rowCount = %d, want %d", c.org, got, c.want)
		}
	}
}

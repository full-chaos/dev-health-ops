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

// TestMetricLineageComputedAtIsTheNewestMeasuredRowOfAnyView: the view only
// narrows the row count. The compute time is the newest measured row of the
// organization, whatever its view, so a scope row newer than the org row (or
// the only row) still gives the time of that compute.
func TestMetricLineageComputedAtIsTheNewestMeasuredRowOfAnyView(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	store := retractionseed.Start(ctx, t)

	insert := func(org, view, computedAt string, completed int) {
		t.Helper()
		stmt := fmt.Sprintf("INSERT INTO work_item_metrics_daily (org_id, day, computed_at, items_completed, row_view) VALUES ('%s', '2026-01-01', '%s', %d, '%s')", org, computedAt, completed, view)
		if err := store.Conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("seed %s/%s: %v", org, view, err)
		}
	}
	const t1, t2, t3 = "2026-01-01 00:01:40", "2026-01-01 00:02:30", "2026-01-01 00:03:20"
	insert("scope-only-newer", "scope", t3, 1)
	insert("org-and-newer-scope", "org", t1, 1)
	insert("org-and-newer-scope", "scope", t3, 1)
	insert("retracted-org-live-scope", "org", t1, 1)
	insert("retracted-org-live-scope", "org", t2, 0)
	insert("retracted-org-live-scope", "scope", t3, 1)
	// Control: a legacy-only org keeps the time main served, the newest measured row.
	insert("legacy-only-control", "", t3, 1)

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: store.URI})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	reader := &Reader{ClickHouse: client}

	wantTime := time.Date(2026, 1, 1, 0, 3, 20, 0, time.UTC)
	for _, c := range []struct {
		org  string
		want int
	}{{"scope-only-newer", 0}, {"org-and-newer-scope", 1}, {"retracted-org-live-scope", 0}, {"legacy-only-control", 1}} {
		lin := reader.MetricLineage(ctx, c.org, "throughput")
		if lin == nil || lin.RowCount == nil {
			t.Errorf("%s: no lineage", c.org)
			continue
		}
		// Kill sites: a view predicate in the WHERE shared with the compute time fails the time of all three.
		if !lin.ComputedAt.Equal(wantTime) {
			t.Errorf("%s: computedAt = %s, want %s", c.org, lin.ComputedAt, wantTime)
		}
		if *lin.RowCount != c.want {
			t.Errorf("%s: rowCount = %d, want %d", c.org, *lin.RowCount, c.want)
		}
	}
}

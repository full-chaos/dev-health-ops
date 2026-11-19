//go:build integration

package benchmarking

import (
	"context"
	"math"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The defect_intro_rate series of a group is its new bugs over its new items, not the mean of the stored rates. A row
// with no new item adds 0 to both sums, and a day with no new item in the group has no point, never a 0.
func TestDefectIntroRateSeriesIsPooledOverNewItems(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = ch.Close(context.Background()) }()
	chschema.Apply(ctx, t, ch)
	options, err := stdclickhouse.ParseDSN(ch.URI)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = admin.Close() }()

	const org = "org-defect-pooled"
	day1 := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	day2, day3 := day1.AddDate(0, 0, 1), day1.AddDate(0, 0, 2)
	computed := time.Date(2026, 8, 5, 1, 0, 0, 0, time.UTC)

	row := func(scope string, day time.Time, bugs, items uint32) {
		t.Helper()
		rate := 0.0
		if items > 0 {
			rate = float64(bugs) / float64(items)
		}
		// items_completed 1 keeps every row live: a row of zeros only is a retraction row, not a day with no new item.
		if err := admin.Exec(ctx, `INSERT INTO work_item_metrics_daily
    (org_id, day, provider, work_scope_id, team_id, team_name, items_completed, new_bugs_count, new_items_count, defect_intro_rate, computed_at)
VALUES (?, ?, 'jira', ?, 'team-a', 'team-a', 1, ?, ?, ?, ?)`, org, day, scope, bugs, items, rate, computed); err != nil {
			t.Fatal(err)
		}
	}
	// day1: 1 of 2 and 3 of 10 -> 4/12 (the mean of the rates is 0.4).
	row("scope-a", day1, 1, 2)
	row("scope-b", day1, 3, 10)
	// day2: the same two rows and a row with no new item: it changes nothing.
	row("scope-a", day2, 1, 2)
	row("scope-b", day2, 3, 10)
	row("scope-c", day2, 0, 0)
	// day3: no new item at all: no point.
	row("scope-a", day3, 0, 0)
	row("scope-b", day3, 0, 0)

	loader, err := NewClickHouseLoader(admin, org)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]float64{day1.Format(time.DateOnly): 4.0 / 12, day2.Format(time.DateOnly): 4.0 / 12}
	for _, scope := range []string{ScopeGlobal, ScopeTeam} {
		series, err := loader.FetchMetricSeriesByScope(ctx, "defect_intro_rate", day1, day3, scope)
		if err != nil {
			t.Fatalf("%s: %v", scope, err)
		}
		if len(series) != 1 {
			t.Fatalf("%s: %d series, want 1: %+v", scope, len(series), series)
		}
		for key, points := range series {
			if len(points) != len(want) {
				t.Errorf("%s %s: %+v, want %d points (none on %s)", scope, key, points, len(want), day3.Format(time.DateOnly))
			}
			for _, point := range points {
				if expected, ok := want[point.Day.Format(time.DateOnly)]; !ok || math.Abs(point.Value-expected) > 1e-9 {
					t.Errorf("%s %s: %s = %v, want %v", scope, key, point.Day.Format(time.DateOnly), point.Value, expected)
				}
			}
		}
	}
}

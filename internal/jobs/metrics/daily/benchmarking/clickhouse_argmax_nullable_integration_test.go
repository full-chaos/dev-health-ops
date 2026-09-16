//go:build integration

package benchmarking

import (
	"context"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestFetchMetricSeriesByScopeDropsNewestNullValueDayNotStaleValue is the
// executed proof for FetchMetricSeriesByScope's dynamic ValueColumn
// substitution: several metrics (this test uses "cycle_time_hours" ->
// work_item_metrics_daily.cycle_time_p50_hours, Nullable(Float64)) must not
// let argMax skip a newest-version NULL and resurrect a stale value. Seed
// an older row with a real value and a newer row (by computed_at) with
// cycle_time_p50_hours = NULL, for the same (org_id, team_id, work_scope_id,
// provider, day). The query's own `metric_value IS NOT NULL` filter drops a
// group whose newest value is genuinely NULL; before the fix argMax skipped
// the NULL and the group survived carrying the stale value.
func TestFetchMetricSeriesByScopeDropsNewestNullValueDayNotStaleValue(t *testing.T) {
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
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	admin, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatalf("open ClickHouse admin connection: %v", err)
	}
	defer func() { _ = admin.Close() }()

	day := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	older := time.Date(2026, 8, 1, 1, 0, 0, 0, time.UTC)
	newer := time.Date(2026, 8, 1, 2, 0, 0, 0, time.UTC)
	staleHours := 88.0

	insert := `INSERT INTO work_item_metrics_daily
		(day, provider, work_scope_id, team_id, cycle_time_p50_hours, org_id, computed_at)
		VALUES (?, 'github', 'scope-1', 'team-4547', ?, 'org-4547', ?)`
	if err := admin.Exec(ctx, insert, day, staleHours, older); err != nil {
		t.Fatalf("insert older row: %v", err)
	}
	if err := admin.Exec(ctx, insert, day, nil, newer); err != nil {
		t.Fatalf("insert newer NULL row: %v", err)
	}

	loader, err := NewClickHouseLoader(admin, "org-4547")
	if err != nil {
		t.Fatalf("NewClickHouseLoader: %v", err)
	}
	series, err := loader.FetchMetricSeriesByScope(ctx, "cycle_time_hours", day, day.AddDate(0, 0, 1), ScopeGlobal)
	if err != nil {
		t.Fatalf("FetchMetricSeriesByScope: %v", err)
	}
	for scopeKey, points := range series {
		for _, point := range points {
			if point.Value == staleHours {
				t.Fatalf("series[%q] contains the stale value %v on %v -- argMax skipped the newest row's NULL instead of dropping the group",
					scopeKey, staleHours, point.Day)
			}
		}
	}
	if len(series[ScopeGlobal]) != 0 {
		t.Fatalf("series[%q] = %+v, want empty (the only group's newest value is NULL, so it must be dropped)", ScopeGlobal, series[ScopeGlobal])
	}
}

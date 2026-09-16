//go:build integration

package remaining

import (
	"context"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestLoadSustainabilitySignalsDropsNewestNullCycleTimeDayNotStaleValue is
// the executed proof for work_item_metrics_daily.cycle_time_p50_hours
// (Nullable(Float64)) inside loadSustainabilitySignals's cycleQuery. Seed an
// older row with a real cycle time and a newer row (by computed_at) with
// cycle_time_p50_hours = NULL, for the same (team_id, day, provider,
// work_scope_id). The function drops a day whose value is genuinely NULL
// (see its own doc comment); before the fix argMax skipped the NULL and
// the day survived with the stale value instead of being dropped.
func TestLoadSustainabilitySignalsDropsNewestNullCycleTimeDayNotStaleValue(t *testing.T) {
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
	windowStart := day
	windowEnd := day.AddDate(0, 0, 7)
	older := time.Date(2026, 8, 1, 1, 0, 0, 0, time.UTC)
	newer := time.Date(2026, 8, 1, 2, 0, 0, 0, time.UTC)
	staleHours := 71.0

	insert := `INSERT INTO work_item_metrics_daily
		(day, provider, work_scope_id, team_id, cycle_time_p50_hours, org_id, computed_at)
		VALUES (?, 'github', 'scope-1', 'team-4547', ?, 'org-4547', ?)`
	if err := admin.Exec(ctx, insert, day, staleHours, older); err != nil {
		t.Fatalf("insert older row: %v", err)
	}
	if err := admin.Exec(ctx, insert, day, nil, newer); err != nil {
		t.Fatalf("insert newer NULL row: %v", err)
	}

	loader, err := NewRecommendationsLoader(admin, "org-4547")
	if err != nil {
		t.Fatalf("NewRecommendationsLoader: %v", err)
	}
	_, _, cycleTimes, err := loader.loadSustainabilitySignals(ctx, "team-4547", windowStart, windowEnd)
	if err != nil {
		t.Fatalf("loadSustainabilitySignals: %v", err)
	}
	for _, value := range cycleTimes {
		if value == staleHours {
			t.Fatalf("cycleTimes = %v, contains the stale value %v -- argMax skipped the newest row's NULL instead of dropping the day",
				cycleTimes, staleHours)
		}
	}
	if len(cycleTimes) != 0 {
		t.Fatalf("cycleTimes = %v, want empty (the only day's newest value is NULL, so it must be dropped)", cycleTimes)
	}
}

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

// The cycle-time day list keeps a real gap between measured days. The daily
// table is append-only: a day with no row is missing, not zero, and the slope
// reads the calendar distance between samples. A loader that filled the gap
// (a zero value or an interpolated one) would pass a length-matches and
// increasing-days check, so this asserts the exact days and values. Real
// ClickHouse.
func TestLoadSustainabilitySignalsKeepsARealGapBetweenCycleTimeDays(t *testing.T) {
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
	conn, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatalf("open ClickHouse connection: %v", err)
	}
	defer func() { _ = conn.Close() }()

	day := func(dayOfMonth int) time.Time { return time.Date(2026, 8, dayOfMonth, 0, 0, 0, 0, time.UTC) }
	computedAt := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	// Measured on the 1st, 2nd, 6th and 7th. The 3rd, 4th and 5th have no row at all.
	seeds := []struct {
		dayOfMonth int
		hours      float64
	}{{1, 10.0}, {2, 12.0}, {6, 30.0}, {7, 34.0}}
	for _, seed := range seeds {
		if err := conn.Exec(ctx, `INSERT INTO work_item_metrics_daily
			(day, provider, work_scope_id, team_id, cycle_time_p50_hours, org_id, computed_at)
			VALUES (?, 'github', 'scope-gap', 'team-gap', ?, 'org-gap', ?)`,
			day(seed.dayOfMonth), seed.hours, computedAt); err != nil {
			t.Fatalf("insert day %d: %v", seed.dayOfMonth, err)
		}
	}

	loader, err := NewRecommendationsLoader(conn, "org-gap")
	if err != nil {
		t.Fatalf("NewRecommendationsLoader: %v", err)
	}
	_, _, cycleTimes, cycleDays, err := loader.loadSustainabilitySignals(ctx, "team-gap", day(1), day(8))
	if err != nil {
		t.Fatalf("loadSustainabilitySignals: %v", err)
	}

	wantDays := []time.Time{day(1), day(2), day(6), day(7)}
	wantHours := []float64{10.0, 12.0, 30.0, 34.0}
	if len(cycleDays) != len(wantDays) {
		t.Fatalf("cycle-time days = %v, want exactly the four measured days %v: a filled gap adds days that were never measured", cycleDays, wantDays)
	}
	for index, want := range wantDays {
		if !cycleDays[index].Equal(want) {
			t.Errorf("cycle-time day %d = %s, want %s (full list %v)", index, cycleDays[index].Format("2006-01-02"), want.Format("2006-01-02"), cycleDays)
		}
	}
	if !sameFloats(cycleTimes, wantHours) {
		t.Errorf("cycle times = %v, want %v: no zero or interpolated value for the unmeasured days", cycleTimes, wantHours)
	}
}

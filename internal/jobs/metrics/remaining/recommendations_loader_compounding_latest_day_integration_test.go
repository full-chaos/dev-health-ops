//go:build integration

package remaining

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-9713: the persisted team score is taken from the latest DAY in the
// window, not from the row with the latest computed_at. An old day that was
// backfilled after the latest day carries the newest computed_at and must not
// win. Real ClickHouse.
func TestRecommendationsLoaderCompoundingRiskLatestDayBeatsLaterBackfill(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)
	dsn, err := containers.ClickHouseHTTPDSN(ctx, instance)
	if err != nil {
		t.Fatal(err)
	}
	conn := openLoaderClickHouse(t, ctx, dsn)

	const org = "org-latest-day"
	insert := func(team, day, score, severity, computedAt string) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO compounding_risk_daily
(org_id, day, scope, scope_id, compounding_risk, severity, churn_norm, complexity_norm, ownership_norm, review_norm,
 w_churn, w_complexity, w_ownership, w_review, threshold_elevated, threshold_high, computed_at)
SELECT '`+org+`', toDate('`+day+`'), 'team', '`+team+`', `+score+`, '`+severity+`', 0.8, 0.6, 0.5, 0.7,
 0.3, 0.3, 0.2, 0.2, 0.4, 0.65, toDateTime('`+computedAt+`')`); err != nil {
			t.Fatal(err)
		}
	}
	// The latest day (10-12) was computed on 10-12; the older day (10-01) was
	// backfilled on 10-14, so it carries the newest computed_at.
	insert("team-mobile", "2026-10-12", "0.246", "low", "2026-10-12 09:00:00")
	insert("team-mobile", "2026-10-01", "0.545", "elevated", "2026-10-14 09:00:00")
	// Within one day, computed_at still breaks the tie.
	insert("team-retied", "2026-10-12", "0.1", "low", "2026-10-12 09:00:00")
	insert("team-retied", "2026-10-12", "0.7", "high", "2026-10-12 10:00:00")

	loader, err := NewRecommendationsLoader(conn, org)
	if err != nil {
		t.Fatal(err)
	}
	start, end := time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC), time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC)

	score, known, severity, _, _, _, err := loader.loadCompoundingRiskPersisted(ctx, "team-mobile", start, end)
	if err != nil {
		t.Fatal(err)
	}
	if !known || score != 0.246 || severity != "low" {
		t.Errorf("team-mobile: score %v (known %v) severity %q, want 0.246 low from the latest day 2026-10-12", score, known, severity)
	}
	score, known, severity, _, _, _, err = loader.loadCompoundingRiskPersisted(ctx, "team-retied", start, end)
	if err != nil {
		t.Fatal(err)
	}
	if !known || score != 0.7 || severity != "high" {
		t.Errorf("team-retied: score %v (known %v) severity %q, want 0.7 high from the later computed_at within the day", score, known, severity)
	}
}

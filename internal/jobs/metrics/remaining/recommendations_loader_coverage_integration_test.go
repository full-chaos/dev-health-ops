//go:build integration

package remaining

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The loader reads the coverage of the persisted team score from the same row
// (CHAOS-6545): the weights of the present component norms over all four, and
// the names of those inputs, only beside a score. Real ClickHouse.
func TestRecommendationsLoaderReadsTheCoverageOfAPartialTeamScore(t *testing.T) {
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

	const org = "org-coverage"
	insert := func(team, score, severity, churn, complexity, ownership, review string) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO compounding_risk_daily
(org_id, day, scope, scope_id, compounding_risk, severity, churn_norm, complexity_norm, ownership_norm, review_norm,
 w_churn, w_complexity, w_ownership, w_review, threshold_elevated, threshold_high, computed_at)
SELECT '`+org+`', toDate('2026-08-20'), 'team', '`+team+`', `+score+`, '`+severity+`', `+churn+`, `+complexity+`, `+ownership+`, `+review+`,
 0.3, 0.3, 0.2, 0.2, 0.4, 0.65, toDateTime('2026-08-20 09:00:00')`); err != nil {
			t.Fatal(err)
		}
	}
	insert("team-partial", "0.7", "high", "0.8", "NULL", "0.5", "NULL")
	insert("team-full", "0.7", "high", "0.8", "0.6", "0.5", "0.7")
	insert("team-old", "NULL", "unknown", "0.8", "0.6", "NULL", "NULL")

	loader, err := NewRecommendationsLoader(conn, org)
	if err != nil {
		t.Fatal(err)
	}
	start, end := time.Date(2026, 8, 17, 0, 0, 0, 0, time.UTC), time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	read := func(team string) MetricsSnapshot {
		t.Helper()
		_, _, _, coverage, known, inputs, err := loader.loadCompoundingRiskPersisted(ctx, team, start, end)
		if err != nil {
			t.Fatal(err)
		}
		return MetricsSnapshot{CompoundingRiskCoverage: coverage, CompoundingRiskCoverageKnown: known, CompoundingRiskInputs: inputs}
	}
	if got := read("team-partial"); !got.CompoundingRiskCoverageKnown || got.CompoundingRiskCoverage < 0.5-1e-12 || got.CompoundingRiskCoverage > 0.5+1e-12 ||
		len(got.CompoundingRiskInputs) != 2 || got.CompoundingRiskInputs[0] != "churn" || got.CompoundingRiskInputs[1] != "ownership concentration" {
		t.Errorf("partial team: coverage %v (known %v) inputs %v, want 0.5 from churn and ownership concentration",
			got.CompoundingRiskCoverage, got.CompoundingRiskCoverageKnown, got.CompoundingRiskInputs)
	}
	if got := read("team-full"); !got.CompoundingRiskCoverageKnown || got.CompoundingRiskCoverage != 1.0 || len(got.CompoundingRiskInputs) != 4 {
		t.Errorf("full team: coverage %v inputs %v, want 1 from all four", got.CompoundingRiskCoverage, got.CompoundingRiskInputs)
	}
	if got := read("team-old"); got.CompoundingRiskCoverageKnown || len(got.CompoundingRiskInputs) != 0 {
		t.Errorf("an old row has no score and so no coverage: %v %v", got.CompoundingRiskCoverage, got.CompoundingRiskInputs)
	}
}

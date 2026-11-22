package compoundingrisk

import (
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// CHAOS-14473: repo-m moves from t1 to t2 after the selected day. The derived
// team rows of that day keep it under t1, as the writer of the stored team
// rows does (ownership as of the day).
func TestDerivedTeamRowsKeepAMovedRepositoryUnderItsOwnerOfTheDay(t *testing.T) {
	now := sweepNow()
	store := &sweepStore{world: sweepMovedFixture("derived", now)}
	got, err := Resolve(context.Background(), store, sweepOrg,
		&model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeTeam, TrendDays: 30}, now)
	if err != nil {
		t.Fatal(err)
	}
	for _, st := range store.unknown {
		t.Fatalf("a statement the model does not know:\n%s", st)
	}
	scores := map[string]string{}
	for _, r := range got.Rows {
		scores[r.ScopeID] = sweepNum(r.Score)
	}
	if scores["t1"] != "0.375" || scores["t2"] != "0.625" {
		t.Fatalf("derived rows t1=%s t2=%s; want t1=0.375 (repo-a, repo-c and repo-m, its owner on the selected day) and t2=0.625 (repo-x only)",
			scores["t1"], scores["t2"])
	}
}

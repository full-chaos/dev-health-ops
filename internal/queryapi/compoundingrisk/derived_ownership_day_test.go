package compoundingrisk

import (
	"context"
	"testing"
	"time"

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

// The day pick keeps the ownership of now (the selected day is not known
// before it): t2 owns repo-m now, and repo-m's newest row is a day later than
// repo-x's. The later day is picked; on it t2 owned only repo-x, which has no
// row that day, so the answer has no row.
func TestDayPickOfNamedTeamsUsesTheOwnershipOfNow(t *testing.T) {
	now := sweepNow()
	world := sweepMovedFixture("derived", now)
	later := world.risk[len(world.risk)-1]
	later.day, later.score = time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC), sweepF(.5)
	world.risk = append(world.risk, later)
	store := &sweepStore{world: world}
	got, err := Resolve(context.Background(), store, sweepOrg,
		&model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeTeam, TeamIds: []string{"t2"}, TrendDays: 30}, now)
	if err != nil {
		t.Fatal(err)
	}
	for _, st := range store.unknown {
		t.Fatalf("a statement the model does not know:\n%s", st)
	}
	var rows, trend []string
	for _, r := range got.Rows {
		rows = append(rows, r.ScopeID+"@"+r.Day.String()+"="+sweepNum(r.Score))
	}
	for _, p := range got.Trend {
		trend = append(trend, p.Day.String()+"="+sweepNum(p.Score))
	}
	if len(rows) != 0 || len(trend) != 1 || trend[0] != "2026-03-09=0.625" {
		t.Fatalf("rows %v trend %v; want the later day 2026-03-10 picked by the ownership of now: no row (repo-x has none that day) and the trend of repo-x only [2026-03-09=0.625]",
			rows, trend)
	}
}

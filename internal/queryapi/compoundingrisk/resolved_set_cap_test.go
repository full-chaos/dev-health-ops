package compoundingrisk

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// CHAOS-14480: team big owns maxRows+1 repositories big-000..big-500; big-500
// sorts last. A set the team scope resolves is not a request list and is never
// cut at maxRows.

func bigRepoID(i int) string { return fmt.Sprintf("big-%03d", i) }

func bigRiskRow(day time.Time, id string, score float64, now time.Time) sweepRiskRow {
	return sweepRiskRow{day: day, scope: scopeRepo, scopeID: id, severity: "low", score: sweepF(score), churn: sweepF(score),
		wChurn: .4, wComplexity: .3, wOwnership: .2, wReview: .1, thresholdElevated: .4, thresholdHigh: .7,
		computedAt: time.Date(now.Year(), now.Month(), now.Day(), 1, 0, 0, 0, time.UTC)}
}

// bigTeamWorld: every repository scores 0.25 on the day before now except
// big-500 at 1. With sharedName, big-499 and big-500 share one full name.
func bigTeamWorld(now time.Time, sharedName bool) sweepWorld {
	d := time.Date(now.Year(), now.Month(), now.Day()-1, 0, 0, 0, 0, time.UTC)
	w := sweepWorld{}
	var owns []string
	for i := 0; i <= maxRows; i++ {
		id := bigRepoID(i)
		name := "acme/" + id
		if sharedName && i >= maxRows-1 {
			name = "acme/shared"
		}
		score := .25
		if i == maxRows {
			score = 1
		}
		w.repos = append(w.repos, sweepRepo{id, name})
		owns = append(owns, id)
		w.risk = append(w.risk, bigRiskRow(d, id, score, now))
	}
	w.teams = []sweepTeam{{id: "big", name: "Big", active: true, owns: owns}}
	return w
}

func bigResolve(t *testing.T, world sweepWorld, repoIDs []string, now time.Time) (rows, trend []string) {
	t.Helper()
	store := &sweepStore{world: world}
	got, err := Resolve(context.Background(), store, sweepOrg, &model.CompoundingRiskFilterInput{
		Breakout: model.CompoundingRiskScopeTeam, TeamIds: []string{"big"}, RepoIds: repoIDs, TrendDays: 30,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	for _, st := range store.unknown {
		t.Fatalf("a statement the model does not know:\n%s", st)
	}
	for _, r := range got.Rows {
		rows = append(rows, r.ScopeID+"@"+r.Day.String()+"="+sweepNum(r.Score))
	}
	for _, p := range got.Trend {
		trend = append(trend, p.Day.String()+"="+sweepNum(p.Score))
	}
	return rows, trend
}

func TestNamedTeamWithMoreThan500OwnedRepositoriesKeepsEveryRepository(t *testing.T) {
	now := sweepNow()
	tokens := make([]string, 0, maxRows)
	for i := 0; i < maxRows-1; i++ {
		tokens = append(tokens, bigRepoID(i))
	}
	tokens = append(tokens, "acme/shared")
	want := sweepNum(sweepF(126.0 / 501))
	for _, c := range []struct {
		name       string
		sharedName bool
		repoIDs    []string
	}{
		{"501-owned-no-repoIds", false, nil},
		{"500-tokens-resolve-to-501", true, tokens},
	} {
		t.Run(c.name, func(t *testing.T) {
			rows, trend := bigResolve(t, bigTeamWorld(now, c.sharedName), c.repoIDs, now)
			if len(rows) != 1 || rows[0] != "big@2026-03-09="+want {
				t.Errorf("rows %v; want [big@2026-03-09=%s] (the mean over all 501 repositories; 0.25 is the mean without big-500)", rows, want)
			}
			if len(trend) != 1 || trend[0] != "2026-03-09="+want {
				t.Errorf("trend %v; want [2026-03-09=%s] (the mean over all 501 repositories)", trend, want)
			}
		})
	}
}

// The stored team row of big covers all 501 repositories; repoIds keeps 500 of
// them, so the row is derived from the 500 selected rows.
func TestNamedTeamWithMoreThan500OwnedRepositoriesNarrowedByRepoIdsDerivesTheRow(t *testing.T) {
	now := sweepNow()
	world := bigTeamWorld(now, false)
	stored := bigRiskRow(time.Date(2026, 3, 9, 0, 0, 0, 0, time.UTC), "big", .9, now)
	stored.scope, stored.severity = scopeTeam, "high"
	world.risk = append(world.risk, stored)
	var keep []string
	for i := 1; i <= maxRows; i++ {
		keep = append(keep, bigRepoID(i))
	}
	want := sweepNum(sweepF(125.75 / 500))
	rows, trend := bigResolve(t, world, keep, now)
	if len(rows) != 1 || rows[0] != "big@2026-03-09="+want || len(trend) != 1 || trend[0] != "2026-03-09="+want {
		t.Fatalf("rows %v trend %v; want rows [big@2026-03-09=%s] and trend [2026-03-09=%s] (derived from big-001..big-500; 0.9 is the stored row of all 501)",
			rows, trend, want, want)
	}
}

// Only big-500 has a row on the newest day: the day pick of the named team
// sees it.
func TestNamedTeamWithMoreThan500OwnedRepositoriesPicksTheDayOfTheLastRepository(t *testing.T) {
	now := sweepNow()
	world := bigTeamWorld(now, false)
	older := time.Date(2026, 3, 8, 0, 0, 0, 0, time.UTC)
	world.risk = world.risk[:0]
	for i := 0; i < maxRows; i++ {
		world.risk = append(world.risk, bigRiskRow(older, bigRepoID(i), .25, now))
	}
	world.risk = append(world.risk, bigRiskRow(time.Date(2026, 3, 9, 0, 0, 0, 0, time.UTC), bigRepoID(maxRows), 1, now))
	rows, trend := bigResolve(t, world, nil, now)
	if len(rows) != 1 || rows[0] != "big@2026-03-09=1" || len(trend) != 2 || trend[0] != "2026-03-08=0.25" || trend[1] != "2026-03-09=1" {
		t.Fatalf("rows %v trend %v; want rows [big@2026-03-09=1] (the newest day, big-500 only) and trend [2026-03-08=0.25 2026-03-09=1]", rows, trend)
	}
}

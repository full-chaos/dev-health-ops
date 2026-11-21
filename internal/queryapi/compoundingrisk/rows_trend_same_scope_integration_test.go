//go:build integration

package compoundingrisk

import (
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// One answer, one repository set: with no team named, the rows (derived from
// repository rows) and the trend (repository rows) must describe the same
// repositories. One repository per team and distinct scores make the set
// visible: the mean of the row scores equals the trend of the row's day only
// when both cover the same repositories.
func TestRealClickHouse_TeamBreakoutNoTeamRowsAndTrendCoverSameRepositories(t *testing.T) {
	ctx, conn, client := startStore(t)
	now := time.Now().UTC()
	repo(t, ctx, conn, "org-1", rA, "acme/web")
	repo(t, ctx, conn, "org-1", rB, "acme/api")
	exec(t, ctx, conn, `INSERT INTO teams (id, name, org_id, repo_patterns, updated_at) VALUES ('t1', 'Platform', 'org-1', [], now64(6)), ('p1', 'Patterns', 'org-1', ['acme/api'], now64(6))`)
	own(t, ctx, conn, "org-1", "t1", rA, "acme/web")
	put(t, ctx, conn, risk{org: "org-1", scope: "repo", id: rA, day: day(now, 1), score: "0.2", sev: "low"})
	put(t, ctx, conn, risk{org: "org-1", scope: "repo", id: rB, day: day(now, 1), score: "0.8", sev: "high"})
	got, err := Resolve(ctx, client, "org-1", &model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeTeam, TrendDays: 30}, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Trend) != 1 || got.Trend[0].Score == nil {
		t.Fatalf("trend %#v, want one scored day", got.Trend)
	}
	var sum float64
	for _, r := range got.Rows {
		if r.Score == nil {
			t.Fatalf("row %s has no score", r.ScopeID)
		}
		sum += *r.Score
	}
	if len(got.Rows) == 0 {
		t.Fatalf("rows empty beside a trend of %v", *got.Trend[0].Score)
	}
	if mean := sum / float64(len(got.Rows)); mean-*got.Trend[0].Score > 1e-9 || *got.Trend[0].Score-mean > 1e-9 {
		t.Errorf("rows cover other repositories than the trend: rows %#v mean %v, trend %v", got.Rows, mean, *got.Trend[0].Score)
	}
}

func TestRealClickHouse_PatternsOnlyTeamNoTeamRowsAndTrendBothAnswer(t *testing.T) {
	ctx, conn, client := startStore(t)
	now := time.Now().UTC()
	repo(t, ctx, conn, "org-1", rA, "acme/web")
	exec(t, ctx, conn, `INSERT INTO teams (id, name, org_id, repo_patterns, updated_at) VALUES ('p1', 'Patterns', 'org-1', ['acme/*'], now64(6))`)
	put(t, ctx, conn, risk{org: "org-1", scope: "repo", id: rA, day: day(now, 1), score: "0.5", sev: "elevated"})
	got, err := Resolve(ctx, client, "org-1", &model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeTeam, TrendDays: 30}, now)
	if err != nil {
		t.Fatal(err)
	}
	if (len(got.Rows) == 0) != (len(got.Trend) == 0) {
		t.Errorf("one answer, two scopes: rows=%d trend=%d", len(got.Rows), len(got.Trend))
	}
}

func trendMap(got *model.CompoundingRiskResult) map[string]float64 {
	out := map[string]float64{}
	for _, p := range got.Trend {
		out[p.Day.String()] = *p.Score
	}
	return out
}

// The no-team bucket is served with the label the web already uses for it.
func TestRealClickHouse_TeamBreakoutNoTeamUnassignedRowLabel(t *testing.T) {
	ctx, conn, client := startStore(t)
	now := time.Now().UTC()
	repo(t, ctx, conn, "org-1", rA, "acme/web")
	repo(t, ctx, conn, "org-1", rB, "acme/api")
	exec(t, ctx, conn, `INSERT INTO teams (id, name, org_id, updated_at) VALUES ('t1', 'Platform', 'org-1', now64(6))`)
	own(t, ctx, conn, "org-1", "t1", rA, "acme/web")
	put(t, ctx, conn, risk{org: "org-1", scope: "repo", id: rA, day: day(now, 1), score: "0.2", sev: "low"})
	put(t, ctx, conn, risk{org: "org-1", scope: "repo", id: rB, day: day(now, 1), score: "0.8", sev: "high"})
	got, err := Resolve(ctx, client, "org-1", &model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeTeam, TrendDays: 30}, now)
	if err != nil {
		t.Fatal(err)
	}
	byID := map[string]model.CompoundingRiskPoint{}
	for _, r := range got.Rows {
		byID[r.ScopeID] = r
	}
	if len(byID) != 2 || byID["t1"].ScopeLabel != "Platform" || byID["unassigned"].ScopeLabel != "Unassigned team" || *byID["unassigned"].Score != 0.8 {
		t.Fatalf("rows %#v, want t1 Platform and unassigned 'Unassigned team' 0.8", got.Rows)
	}
}

// A repoIds filter with no team named: the unowned repository is the
// unassigned row and the trend covers the same repository.
func TestRealClickHouse_TeamBreakoutNoTeamRepoFilterRowsAndTrendSame(t *testing.T) {
	ctx, conn, client := startStore(t)
	now := time.Now().UTC()
	repo(t, ctx, conn, "org-1", rA, "acme/web")
	repo(t, ctx, conn, "org-1", rB, "acme/api")
	exec(t, ctx, conn, `INSERT INTO teams (id, name, org_id, updated_at) VALUES ('t1', 'Platform', 'org-1', now64(6))`)
	own(t, ctx, conn, "org-1", "t1", rA, "acme/web")
	put(t, ctx, conn, risk{org: "org-1", scope: "repo", id: rA, day: day(now, 1), score: "0.2", sev: "low"})
	put(t, ctx, conn, risk{org: "org-1", scope: "repo", id: rB, day: day(now, 1), score: "0.8", sev: "high"})
	got, err := Resolve(ctx, client, "org-1", &model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeTeam, RepoIds: []string{rB}, TrendDays: 30}, now)
	if err != nil {
		t.Fatal(err)
	}
	tr := trendMap(got)
	if len(got.Rows) != 1 || got.Rows[0].ScopeID != "unassigned" || *got.Rows[0].Score != 0.8 || len(tr) != 1 || tr[day(now, 1).Format("2006-01-02")] != 0.8 {
		t.Fatalf("rows %#v trend %v, want one unassigned row 0.8 and trend 0.8", got.Rows, tr)
	}
}

// Every repository owned: no unassigned row. A named team: rows and trend over
// its owned repository only (the unowned repository is not listed).
func TestRealClickHouse_TeamBreakoutNoUnassignedRowWhenOwnedOrNamed(t *testing.T) {
	ctx, conn, client := startStore(t)
	now := time.Now().UTC()
	repo(t, ctx, conn, "org-1", rA, "acme/web")
	repo(t, ctx, conn, "org-1", rB, "acme/api")
	exec(t, ctx, conn, `INSERT INTO teams (id, name, org_id, updated_at) VALUES ('t1', 'Platform', 'org-1', now64(6))`)
	own(t, ctx, conn, "org-1", "t1", rA, "acme/web")
	put(t, ctx, conn, risk{org: "org-1", scope: "repo", id: rA, day: day(now, 1), score: "0.2", sev: "low"})
	put(t, ctx, conn, risk{org: "org-1", scope: "repo", id: rB, day: day(now, 1), score: "0.8", sev: "high"})
	named, err := Resolve(ctx, client, "org-1", &model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeTeam, TeamIds: []string{"t1"}, TrendDays: 30}, now)
	if err != nil {
		t.Fatal(err)
	}
	if tr := trendMap(named); len(named.Rows) != 1 || named.Rows[0].ScopeID != "t1" || *named.Rows[0].Score != 0.2 || len(tr) != 1 || tr[day(now, 1).Format("2006-01-02")] != 0.2 {
		t.Fatalf("named team: rows %#v trend %v", named.Rows, trendMap(named))
	}
	own(t, ctx, conn, "org-1", "t1", rB, "acme/api")
	all, err := Resolve(ctx, client, "org-1", &model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeTeam, TrendDays: 30}, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(all.Rows) != 1 || all.Rows[0].ScopeID != "t1" {
		t.Fatalf("every repository owned: rows %#v, want t1 only", all.Rows)
	}
}

// Boundary: with a stored team row the answer is what main answered before
// this change: the stored row only (no unassigned row), the trend over every
// repository (including the unowned one), the stored row's own label.
func TestRealClickHouse_StoredTeamRowPathUnchangedBoundary(t *testing.T) {
	ctx, conn, client := startStore(t)
	now := time.Now().UTC()
	repo(t, ctx, conn, "org-1", rA, "acme/web")
	repo(t, ctx, conn, "org-1", rB, "acme/api")
	exec(t, ctx, conn, `INSERT INTO teams (id, name, org_id, updated_at) VALUES ('t1', 'Platform', 'org-1', now64(6))`)
	own(t, ctx, conn, "org-1", "t1", rA, "acme/web")
	put(t, ctx, conn, risk{org: "org-1", scope: "team", id: "t1", day: day(now, 1), score: "0.5", sev: "elevated"})
	put(t, ctx, conn, risk{org: "org-1", scope: "repo", id: rA, day: day(now, 1), score: "0.2", sev: "low"})
	put(t, ctx, conn, risk{org: "org-1", scope: "repo", id: rB, day: day(now, 1), score: "0.8", sev: "high"})
	got, err := Resolve(ctx, client, "org-1", &model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeTeam, TrendDays: 30}, now)
	if err != nil {
		t.Fatal(err)
	}
	tr := trendMap(got)
	if len(got.Rows) != 1 || got.Rows[0].ScopeID != "t1" || got.Rows[0].ScopeLabel != "Platform" || *got.Rows[0].Score != 0.5 || len(tr) != 1 || tr[day(now, 1).Format("2006-01-02")] != 0.5 {
		t.Fatalf("stored-row path changed: rows %#v trend %v", got.Rows, tr)
	}
}

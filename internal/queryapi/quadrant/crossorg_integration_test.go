//go:build integration

// CHAOS-7239: two orgs share one repo_id (see internal/testsupport/crossorg).
// The repo-grain quadrant joins repo_metrics_daily to repos; it must read
// only the calling org's metric rows and repos row.
//
// External test package: crossorg derives person ids through this package,
// so an in-package test importing crossorg would be an import cycle.
package quadrant_test

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/quadrant"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

func TestRepoGrainQuadrantReturnsOnlyTheCallingOrgsRowsForASharedRepoID(t *testing.T) {
	ctx := context.Background()
	admin, client := crossorg.Start(ctx, t)
	f := crossorg.Default()
	crossorg.SeedRepos(ctx, t, admin, f)

	day := time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)
	for _, row := range []struct {
		org       string
		loc       uint32
		prsMerged uint32
	}{
		{f.OrgA, 10, 2},
		{f.OrgB, 1000, 50},
	} {
		crossorg.Exec(ctx, t, admin, `
            INSERT INTO repo_metrics_daily (org_id, repo_id, day, total_loc_touched, prs_merged, computed_at)
            VALUES (?, ?, ?, ?, ?, now())`,
			row.org, f.RepoID, day, row.loc, row.prsMerged)
	}

	start := time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC)
	resp, err := quadrant.BuildResponse(ctx, client, f.OrgA, quadrant.Params{
		Type: "churn_throughput", ScopeType: "org", Bucket: "week", StartDate: &start, EndDate: &end,
	})
	if err != nil {
		t.Fatalf("BuildResponse: %v", err)
	}
	if len(resp.Points) != 1 || resp.Points[0].X != 10 || resp.Points[0].Y != 2 {
		t.Fatalf("org A points = %+v; want one point x=10 y=2 (org B's rows must not count)", resp.Points)
	}
}

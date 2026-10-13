//go:build integration

package home

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestHomeTeamScopeSumsARepositoryAtTheOwnersShare pins CHAOS-9413 (D5988, D5990) on the Home tile of a SUMMED
// repository metric (churn), against a real ClickHouse: a team scope reads its own linked share of each
// repository's total, never the whole repository.
//
//   - repo-lead (churn 100): team-main 0.6, team-co 0.4
//   - repo-tie  (churn  50): team-main 0.5, team-tie 0.5 (D5990)
//   - repo-solo (churn  10): team-main 1.0
func TestHomeTeamScopeSumsARepositoryAtTheOwnersShare(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := newHomeTestClickHouse(ctx, t)

	const org = "home-owner-share-org"
	synced := time.Date(2026, 9, 1, 5, 0, 0, 0, time.UTC)
	computed := time.Date(2026, 9, 1, 6, 0, 0, 0, time.UTC)
	day := time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)
	ownedFrom := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	asOf := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)

	type owner struct {
		team    string
		primary uint8
		share   float64
	}
	for _, repo := range []struct {
		name   string
		churn  float64
		owners []owner
	}{
		{"acme/repo-lead", 100, []owner{{"team-main", 1, 0.6}, {"team-co", 2, 0.4}}},
		{"acme/repo-tie", 50, []owner{{"team-main", 1, 0.5}, {"team-tie", 1, 0.5}}},
		{"acme/repo-solo", 10, []owner{{"team-main", 1, 1.0}}},
	} {
		id := uuid.New()
		if err := admin.Exec(ctx, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, 'github', ?, ?, ?)`,
			id, repo.name, org, synced, synced); err != nil {
			t.Fatalf("seed repo %s: %v", repo.name, err)
		}
		if err := admin.Exec(ctx, `INSERT INTO repo_metrics_daily (org_id, repo_id, day, total_loc_touched, computed_at) VALUES (?, ?, ?, ?, ?)`,
			org, id, day, repo.churn, computed); err != nil {
			t.Fatalf("seed churn %s: %v", repo.name, err)
		}
		for _, o := range repo.owners {
			if err := admin.Exec(ctx, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at, share)
				VALUES (?, 'github', ?, ?, ?, 'exact', 'inferred', ?, 0, 0, ?, NULL, ?, ?)`,
				org, o.team, id, repo.name, o.primary, ownedFrom, ownedFrom, o.share); err != nil {
				t.Fatalf("seed ownership %s/%s: %v", repo.name, o.team, err)
			}
		}
	}

	var churn metricSpec
	for _, spec := range metrics {
		if spec.Metric == "churn" {
			churn = spec
		}
	}
	start, end := day.AddDate(0, 0, -1), day.AddDate(0, 0, 2)
	read := func(teams ...string) MetricDelta {
		t.Helper()
		delta, err := computeMetricDelta(ctx, client, churn, start, end, start.AddDate(0, 0, -7), start,
			Filters{Scope: ScopeFilter{Level: "team", IDs: teams}}, org, asOf)
		if err != nil {
			t.Fatalf("churn for %v: %v", teams, err)
		}
		return delta
	}

	for _, tc := range []struct {
		name  string
		teams []string
		want  float64
	}{
		{"co-owner at 0.4 reads 0.4 of the repository", []string{"team-co"}, 40},
		{"main owner reads its own 0.6, half of the tie and all of its solo repository", []string{"team-main"}, 60 + 25 + 10},
		{"a tied owner reads its equal half", []string{"team-tie"}, 25},
		{"the owners of one repository read it once", []string{"team-main", "team-co"}, 100 + 25 + 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := read(tc.teams...); math.Abs(got.Value-tc.want) > 1e-6 || !got.HasData {
				t.Errorf("churn = %+v, want %v", got, tc.want)
			}
		})
	}
}

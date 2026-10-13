//go:build integration

package explain

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

// TestExplainTeamScopeSumsARepositoryAtTheOwnersShare pins CHAOS-9413 (D5988, D5990) on the explained value, drivers
// and contributors of a SUMMED repository metric (churn). repo-lead (churn 100): team-main 0.6, team-co 0.4.
// repo-tie (50): team-main 0.5, team-tie 0.5. repo-solo (10): team-main 1.0.
func TestExplainTeamScopeSumsARepositoryAtTheOwnersShare(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := crossorg.Start(ctx, t)
	const org = "explain-owner-share-org"
	synced := time.Date(2026, 9, 1, 5, 0, 0, 0, time.UTC)
	computed := time.Date(2026, 9, 1, 6, 0, 0, 0, time.UTC)
	from := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC)
	ownedFrom := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	day := time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)
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
		crossorg.Exec(ctx, t, admin, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, 'github', ?, ?, ?)`,
			id, repo.name, org, synced, synced)
		crossorg.Exec(ctx, t, admin, `INSERT INTO repo_metrics_daily (org_id, repo_id, day, total_loc_touched, computed_at) VALUES (?, ?, ?, ?, ?)`,
			org, id, day, repo.churn, computed)
		for _, o := range repo.owners {
			crossorg.Exec(ctx, t, admin, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at, share)
				VALUES (?, 'github', ?, ?, ?, 'exact', 'inferred', ?, 0, 0, ?, NULL, ?, ?)`,
				org, o.team, id, repo.name, o.primary, ownedFrom, ownedFrom, o.share)
		}
	}
	_ = from
	_ = to

	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("reader: %v", err)
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
			response, err := BuildExplainResponse(ctx, reader, org, Params{
				Metric: "churn", ScopeLevel: "team", ScopeIDs: tc.teams,
				StartDay: day.AddDate(0, 0, -1), EndDay: day.AddDate(0, 0, 2),
				CompareStart: day.AddDate(0, 0, -8), CompareEnd: day.AddDate(0, 0, -1),
			})
			if err != nil {
				t.Fatalf("explain %v: %v", tc.teams, err)
			}
			if math.Abs(response.Value-tc.want) > 1e-6 {
				t.Errorf("value = %v, want %v", response.Value, tc.want)
			}
			var contributed float64
			for _, c := range response.Contributors {
				contributed += c.Value
			}
			if math.Abs(contributed-tc.want) > 1e-6 {
				t.Errorf("contributors sum to %v, want %v (they must add up to the headline)", contributed, tc.want)
			}
		})
	}
}

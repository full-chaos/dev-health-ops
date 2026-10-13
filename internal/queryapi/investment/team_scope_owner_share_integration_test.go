//go:build integration

package investment

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

// TestSunburstTeamScopeWeighsARepositoryByTheOwnersShare pins CHAOS-9413 (D5988, D5990) on the Investment sunburst:
// a team scope gives each owning team its own linked share of a repository's effort, never the whole repository.
// repo-lead (effort 100): team-main 0.6, team-co 0.4. repo-tie (50): team-main 0.5, team-tie 0.5. repo-solo (10): team-main 1.0.
func TestSunburstTeamScopeWeighsARepositoryByTheOwnersShare(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := crossorg.Start(ctx, t)
	const org = "sunburst-owner-share-org"
	synced := time.Date(2026, 9, 1, 5, 0, 0, 0, time.UTC)
	computed := time.Date(2026, 9, 1, 6, 0, 0, 0, time.UTC)
	from := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC)
	ownedFrom := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	asOf := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)

	type owner struct {
		team    string
		primary uint8
		share   float64
	}
	for i, repo := range []struct {
		name   string
		effort float64
		owners []owner
	}{
		{"acme/repo-lead", 100, []owner{{"team-main", 1, 0.6}, {"team-co", 2, 0.4}}},
		{"acme/repo-tie", 50, []owner{{"team-main", 1, 0.5}, {"team-tie", 1, 0.5}}},
		{"acme/repo-solo", 10, []owner{{"team-main", 1, 1.0}}},
	} {
		id := uuid.New()
		crossorg.Exec(ctx, t, admin, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, 'github', ?, ?, ?)`,
			id, repo.name, org, synced, synced)
		crossorg.Exec(ctx, t, admin, `INSERT INTO work_unit_investments (work_unit_id, from_ts, to_ts, repo_id, provider, effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json, structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status, categorization_errors_json, categorization_model_version, categorization_input_hash, categorization_run_id, computed_at, org_id)
			VALUES (?, ?, ?, ?, 'github', 'churn_loc', ?, map('maintenance', 1.0), map('maintenance.debt', 1.0), '{}', 0.7, 'moderate', 'ok', '{}', 'v1', 'hash', 'run-1', ?, ?)`,
			"wu-"+string(rune('a'+i)), from, to, id, repo.effort, computed, org)
		for _, o := range repo.owners {
			crossorg.Exec(ctx, t, admin, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at, share)
				VALUES (?, 'github', ?, ?, ?, 'exact', 'inferred', ?, 0, 0, ?, NULL, ?, ?)`,
				org, o.team, id, repo.name, o.primary, ownedFrom, ownedFrom, o.share)
		}
	}

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
			condition, bindings := teamscope.RepoCondition(org, "repo_id", tc.teams, asOf)
			rows, err := reader.FetchInvestmentSunburst(ctx, SunburstFilters{
				OrgID: org, StartTS: from.AddDate(0, 0, -1), EndTS: to.AddDate(0, 0, 1), Limit: 100,
				TeamScopeCondition: condition, TeamScopeBindings: bindings, TeamIDs: tc.teams, TeamAsOf: asOf,
			})
			if err != nil {
				t.Fatalf("sunburst %v: %v", tc.teams, err)
			}
			var got float64
			for _, row := range rows {
				got += row.Value
			}
			if math.Abs(got-tc.want) > 1e-3 {
				t.Errorf("sunburst = %v, want %v", got, tc.want)
			}
		})
	}
}

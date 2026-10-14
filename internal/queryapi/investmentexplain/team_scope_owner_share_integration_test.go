//go:build integration

package investmentexplain

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// TestInvestmentTeamScopeWeighsARepositoryByTheOwnersShare pins CHAOS-9413 (D5988, D5990) on the Investment page's
// own sums, in the measure the reads take (COUNT), against a real ClickHouse: a team scope gives each owning team ITS OWN linked share of a
// repository's effort, never the whole repository.
//
//   - repo-lead (effort 100): team-main 0.6 (is_primary 1), team-co 0.4 (is_primary 2)
//   - repo-tie  (effort  50): team-main 0.5 and team-tie 0.5, both is_primary 1 (D5990)
//   - repo-solo (effort  10): team-main alone, share 1.0
//
// The teams of one repository read it once between them: main + co on repo-lead is one unit in COUNT,
// never 1.4.
func TestInvestmentTeamScopeWeighsARepositoryByTheOwnersShare(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := newInvestmentExplainTestClickHouse(ctx, t)

	const org = "investmentexplain-owner-share-org"
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
	repos := []struct {
		name   string
		effort float64
		owners []owner
	}{
		{"acme/repo-lead", 100, []owner{{"team-main", 1, 0.6}, {"team-co", 2, 0.4}}},
		{"acme/repo-tie", 50, []owner{{"team-main", 1, 0.5}, {"team-tie", 1, 0.5}}},
		{"acme/repo-solo", 10, []owner{{"team-main", 1, 1.0}}},
	}
	for i, repo := range repos {
		id := uuid.New()
		if err := admin.Exec(ctx, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, 'github', ?, ?, ?)`,
			id, repo.name, org, synced, synced); err != nil {
			t.Fatalf("seed repo %s: %v", repo.name, err)
		}
		if err := admin.Exec(ctx, `INSERT INTO work_unit_investments (work_unit_id, from_ts, to_ts, repo_id, provider, effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json, structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status, categorization_errors_json, categorization_model_version, categorization_input_hash, categorization_run_id, computed_at, org_id)
			VALUES (?, ?, ?, ?, 'github', 'churn_loc', ?, map('maintenance', 1.0), map('maintenance.debt', 1.0), '{}', 0.7, 'moderate', 'ok', '{}', 'v1', 'hash', 'run-1', ?, ?)`,
			"wu-"+string(rune('a'+i)), from, to, id, repo.effort, computed, org); err != nil {
			t.Fatalf("seed work unit %s: %v", repo.name, err)
		}
		for _, o := range repo.owners {
			if err := admin.Exec(ctx, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at, share)
				VALUES (?, 'github', ?, ?, ?, 'exact', 'inferred', ?, 0, 0, ?, NULL, ?, ?)`,
				org, o.team, id, repo.name, o.primary, ownedFrom, ownedFrom, o.share); err != nil {
				t.Fatalf("seed ownership %s/%s: %v", repo.name, o.team, err)
			}
		}
	}

	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	read := func(measure string, teams ...string) (breakdown, themeEffort float64) {
		t.Helper()
		condition, bindings := teamscope.RepoCondition(org, "repo_id", teams, asOf)
		filters := BreakdownFilters{
			OrgID: org, StartTS: from.AddDate(0, 0, -1), EndTS: to.AddDate(0, 0, 1),
			TeamScopeCondition: condition, TeamScopeBindings: bindings, TeamIDs: teams, TeamAsOf: asOf,
			Measure: measure,
		}
		rows, err := reader.FetchInvestmentBreakdown(ctx, filters)
		if err != nil {
			t.Fatalf("breakdown %v: %v", teams, err)
		}
		for _, row := range rows {
			breakdown += row.Value
		}
		themes, err := reader.FetchInvestmentThemeEffort(ctx, filters)
		if err != nil {
			t.Fatalf("theme effort %v: %v", teams, err)
		}
		for _, row := range themes {
			themeEffort += row.Value
		}
		return breakdown, themeEffort
	}

	// Each repository holds one unit: COUNT reads the owners' shares of it (1 per unit).
	for _, tc := range []struct {
		name  string
		teams []string
		count float64
	}{
		{"co-owner at 0.4 reads 0.4 of the repository", []string{"team-co"}, 0.4},
		{"main owner reads its own 0.6 on the lead repository and 0.5 of the tie and all of its solo repository", []string{"team-main"}, 0.6 + 0.5 + 1},
		{"a tied owner reads its equal half", []string{"team-tie"}, 0.5},
		{"the owners of one repository read it once", []string{"team-main", "team-co"}, 1 + 0.5 + 1},
		{"every owner of every repository reads each once", []string{"team-main", "team-co", "team-tie"}, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			breakdown, themeValue := read(MeasureCount, tc.teams...)
			if math.Abs(breakdown-tc.count) > 1e-3 {
				t.Errorf("COUNT breakdown = %v, want %v", breakdown, tc.count)
			}
			if math.Abs(themeValue-tc.count) > 1e-3 {
				t.Errorf("COUNT theme value = %v, want %v", themeValue, tc.count)
			}
		})
	}
}

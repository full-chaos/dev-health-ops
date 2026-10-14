//go:build integration

package analytics

import (
	"context"
	"encoding/json"
	"math"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chquery"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-9221 (D5947): on a real ClickHouse, a team scope of the analytics theme readers is the team's OWNED
// repositories read over the unit's repository ROWS; the work-item attribution vote is not read, and a unit with
// no own repo_id is in a repository's scope through work_unit_repo_effort.
//
//	U1: own repo_id = repo A (team A owns A), no attribution at all          -> IN team A's scope (the vote lost it)
//	U2: own repo_id NULL, repository row = repo A                             -> IN team A's scope AND repository A's scope
//	U3: own repo_id = repo B (team B owns B), evidence attributed to team A   -> NOT in team A's scope (the vote is not read)
func TestTeamScopeIsTheOwnedRepositoriesOverTheUnitsRepositoryRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = ch.Close(context.Background()) })
	chschema.Apply(ctx, t, ch)
	options, err := stdclickhouse.ParseDSN(ch.URI)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client, err := chquery.NewProductionClient(ch.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	const org = "org-9221"
	const repoA, repoB = "92210001-9221-4221-8221-922192219221", "92210002-9221-4221-8221-922192219221"
	at := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	exec := func(statement string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, statement, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, statement)
		}
	}
	for _, r := range [][2]string{{repoA, "acme/a"}, {repoB, "acme/b"}} {
		exec(`INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, 'github', ?, ?, ?)`, r[0], r[1], org, at, at)
	}
	owns := func(team, repo, name string) {
		exec(`INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, 'github', ?, ?, ?, 'exact', 'native', 1, 100, 0, ?, NULL, ?)`,
			org, team, repo, name, at.AddDate(-1, 0, 0), at)
	}
	owns("team-a", repoA, "acme/a")
	owns("team-b", repoB, "acme/b")
	evidence := func(pr string) string {
		raw, _ := json.Marshal(map[string]any{"issues": []string{}, "prs": []string{pr}, "commits": []string{}, "edges": []string{}})
		return string(raw)
	}
	unit := func(id string, own any, themes, subs map[string]float64, ev string, effort float64) {
		exec(`INSERT INTO work_unit_investments (work_unit_id, from_ts, to_ts, repo_id, effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json, structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status, categorization_model_version, categorization_run_id, computed_at, org_id)
VALUES (?, ?, ?, ?, 'churn_loc', ?, ?, ?, ?, 0.8, 'high', 'ok', 'jev-test', 'run-1', ?, ?)`, id, at.Add(-24*time.Hour), at, own, effort, themes, subs, ev, at.Add(time.Hour), org)
	}
	feature := map[string]float64{"feature_delivery": 1}
	featureSub := map[string]float64{"feature_delivery.roadmap": 1}
	var nilRepo *string
	unit("u1", repoA, feature, featureSub, evidence(repoA+"#pr1"), 10)
	unit("u2", nilRepo, feature, featureSub, evidence("none#pr2"), 10)
	exec(`INSERT INTO work_unit_repo_effort (work_unit_id, repo_id, effort_metric, effort_value, allocation_weight, allocation_source, categorization_run_id, computed_at, org_id) VALUES ('u2', ?, 'churn_loc', 10, 1.0, 'pr_refs', 'run-1', ?, ?)`, repoA, at.Add(time.Hour), org)
	unit("u3", repoB, feature, featureSub, evidence(repoB+"#pr3"), 10)
	// UX touches both repositories: effort 100 allocated A 60 / B 40, own repo_id = A. Its repositories must sum to
	// its effort, never twice it (the invariant of CHAOS-9221).
	unit("ux", repoA, feature, featureSub, evidence(repoA+"#pr4"), 100)
	for repo, v := range map[string]float64{repoA: 60, repoB: 40} {
		exec(`INSERT INTO work_unit_repo_effort (work_unit_id, repo_id, effort_metric, effort_value, allocation_weight, allocation_source, categorization_run_id, computed_at, org_id) VALUES ('ux', ?, 'churn_loc', ?, ?, 'pr_refs', 'run-1', ?, ?)`, repo, v, v/100, at.Add(time.Hour), org)
	}
	// The attribution vote of U3's evidence names team A: the vote must not put U3 in team A's scope.
	exec(`INSERT INTO work_item_team_attributions (org_id, repo_id, work_item_id, team_id, team_name, source, is_primary, confidence, computed_at) VALUES (?, ?, ?, 'team-a', 'team-a', 'linked_issue', 1, 'high', ?)`,
		org, repoB, "ghpr:acme/b#3", at)

	dates := &model.DateRangeInput{StartDate: graphqldate.New(at.AddDate(0, 0, -3)), EndDate: graphqldate.New(at.AddDate(0, 0, 3))}
	measure := func(m model.MeasureInput, filters *model.FilterInput) float64 {
		t.Helper()
		result, err := Resolve(ctx, client, org, model.AnalyticsRequestInput{
			Filters:    filters,
			Breakdowns: []model.BreakdownRequestInput{{Dimension: model.DimensionInputTheme, Measure: m, DateRange: dates, TopN: 10}},
		})
		if err != nil {
			t.Fatal(err)
		}
		var total float64
		for _, b := range result.Breakdowns {
			for _, item := range b.Items {
				if item.Key == "feature_delivery" && item.Value != nil {
					total += *item.Value
				}
			}
		}
		return total
	}
	count := func(filters *model.FilterInput) float64 { return measure(model.MeasureInputCount, filters) }
	team := func(id string) *model.FilterInput {
		return &model.FilterInput{Scope: &model.ScopeFilterInput{Level: model.ScopeLevelInputTeam, Ids: []string{id}}}
	}
	repo := func(id string) *model.FilterInput {
		return &model.FilterInput{Scope: &model.ScopeFilterInput{Level: model.ScopeLevelInputRepo, Ids: []string{id}}}
	}
	check := func(label string, got, want float64) {
		t.Helper()
		if math.Abs(got-want) > 1e-6 {
			t.Errorf("%s = %v units, want %v", label, got, want)
		}
	}
	// Units: UX counts 0.6 / 0.4 (its allocation weights), so the repositories and the teams sum to the org.
	check("org units", count(nil), 4)
	check("team A (owned repo A: U1 + U2 + 0.6 of UX; never U3 by the vote)", count(team("team-a")), 2.6)
	check("team B (owned repo B: U3 + 0.4 of UX)", count(team("team-b")), 1.4)
	check("repository A scope (own column U1, repository row U2, 0.6 of UX)", count(repo(repoA)), 2.6)
	check("repository B scope", count(repo(repoB)), 1.4)
	check("units: team A + team B = the org", count(team("team-a"))+count(team("team-b")), count(nil))

	// An org WITH a complete membership run (prod and bigboy have one): the readers keep only the units of the
	// latest run, and the team / repository predicates still resolve inside it. U1 and U2 are members; U3 is not.
	exec(`INSERT INTO work_unit_membership_runs (org_id, run_id, completed_at) VALUES (?, 'run-m1', ?)`, org, at.Add(2*time.Hour))
	for _, id := range []string{"u1", "u2", "ux"} {
		exec(`INSERT INTO work_unit_membership (org_id, node_type, node_id, work_unit_id, category_kind, category, weight, is_dominant, categorization_status, computed_at, run_id) VALUES (?, 'work_item', ?, ?, 'theme', 'feature_delivery', 1, 1, 'ok', ?, 'run-m1')`, org, "node-"+id, id, at.Add(2*time.Hour))
	}
	check("marker org: team A (U1, U2, 0.6 of UX in the run)", count(team("team-a")), 2.6)
	check("marker org: team B (U3 is not in the latest run; 0.4 of UX is)", count(team("team-b")), 0.4)
	check("marker org: repository A scope", count(repo(repoA)), 2.6)

	// Evidence quality at team scope follows the same rule.
	stats, err := Resolve(ctx, client, org, model.AnalyticsRequestInput{
		Filters:    team("team-a"),
		Breakdowns: []model.BreakdownRequestInput{{Dimension: model.DimensionInputTheme, Measure: model.MeasureInputCount, DateRange: dates, TopN: 10}},
	})
	if err != nil || stats.EvidenceQualityStats == nil || stats.EvidenceQualityStats.Total != 3 {
		t.Fatalf("evidence quality at team A scope = %+v, %v; want total 3 (U1, U2, UX: units in scope, not shares)", stats.EvidenceQualityStats, err)
	}
}

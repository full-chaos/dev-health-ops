//go:build integration

package analytics

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chquery"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-9264: teamCoverage from repository OWNERSHIP, against a real engine. Four units, one per case of the rule:
//
//	u-owned-novote   repository A is owned by team-a; no attribution row names a team     -> assigned
//	u-unowned-voted  repository C has no owner; an attribution row votes a team            -> unassigned
//	u-two-teams      repository A (team-a) and repository B (team-b), 50/50 effort         -> assigned, counted ONCE
//	u-unowned-plain  repository C, no vote                                                 -> unassigned
//
// total 4.0, assigned 2.0 -> teamCoverage 0.5. The vote would have said 1.0/4.0. The chart's TEAM dimension is the
// vote and stays the vote: its node for the voted team is still there with the voted unit.
func TestTeamCoverageReadsOwnershipNotTheVote(t *testing.T) {
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

	const org = "org-9264"
	const repoA, repoB, repoC = "92640001-9264-4264-8264-926492649264", "92640002-9264-4264-8264-926492649264", "92640003-9264-4264-8264-926492649264"
	at := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	exec := func(statement string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, statement, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, statement)
		}
	}
	for _, r := range [][2]string{{repoA, "acme/a"}, {repoB, "acme/b"}, {repoC, "acme/c"}} {
		exec(`INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, 'github', ?, ?, ?)`, r[0], r[1], org, at, at)
	}
	owns := func(team, repo, name string) {
		exec(`INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, 'github', ?, ?, ?, 'exact', 'native', 1, 100, 0, ?, NULL, ?)`,
			org, team, repo, name, at.AddDate(-1, 0, 0), at)
	}
	owns("team-a", repoA, "acme/a")
	owns("team-b", repoB, "acme/b")
	themes := map[string]float64{"feature_delivery": 1}
	subs := map[string]float64{"feature_delivery.roadmap": 1}
	unit := func(id, repo, issue string, effort float64) {
		raw, _ := json.Marshal(map[string]any{"issues": []string{issue}, "prs": []string{}, "commits": []string{}, "edges": []string{}})
		exec(`INSERT INTO work_unit_investments (work_unit_id, from_ts, to_ts, repo_id, effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json, structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status, categorization_model_version, categorization_run_id, computed_at, org_id)
VALUES (?, ?, ?, ?, 'churn_loc', ?, ?, ?, ?, 0.8, 'high', 'ok', 'jev-test', 'run-1', ?, ?)`, id, at.Add(-24*time.Hour), at, repo, effort, themes, subs, string(raw), at.Add(time.Hour), org)
	}
	repoRow := func(id, repo string, effort float64) {
		exec(`INSERT INTO work_unit_repo_effort (work_unit_id, repo_id, effort_metric, effort_value, allocation_weight, allocation_source, categorization_run_id, computed_at, org_id) VALUES (?, ?, 'churn_loc', ?, ?, 'pr_refs', 'run-1', ?, ?)`, id, repo, effort, 0.5, at.Add(time.Hour), org)
	}
	unit("u-owned-novote", repoA, "linear:NOVOTE-1", 10)
	unit("u-unowned-voted", repoC, "linear:VOTED-1", 10)
	unit("u-two-teams", repoA, "linear:TWO-1", 10)
	unit("u-unowned-plain", repoC, "linear:PLAIN-1", 10)
	repoRow("u-owned-novote", repoA, 10)
	repoRow("u-unowned-voted", repoC, 10)
	repoRow("u-two-teams", repoA, 5)
	repoRow("u-two-teams", repoB, 5)
	repoRow("u-unowned-plain", repoC, 10)
	// Only the unit in the unowned repository has a vote.
	exec(`INSERT INTO work_item_team_attributions (org_id, repo_id, work_item_id, provider, team_id, team_name, source, is_primary, confidence, evidence, computed_at) VALUES (?, ?, 'linear:VOTED-1', 'linear', 'team-vote', 'Vote Team', 'native_team', 1, 'high', '', ?)`, org, repoC, at)

	dates := &model.DateRangeInput{StartDate: graphqldate.New(at.AddDate(0, 0, -3)), EndDate: graphqldate.New(at.AddDate(0, 0, 3))}
	reqInput := model.SankeyRequestInput{
		Path:      []model.DimensionInput{model.DimensionInputTeam, model.DimensionInputTheme},
		Measure:   model.MeasureInputCount,
		DateRange: dates,
		MaxNodes:  16,
		MaxEdges:  100,
	}
	req, err := SankeyRequestFromInput(reqInput)
	if err != nil {
		t.Fatal(err)
	}
	got := resolveSankeyCoverage(ctx, client, org, req, 60, true, nil)
	if got == nil {
		t.Fatal("coverage is nil for a window that holds units")
	}
	if math.Abs(got.TeamCoverage-0.5) > 1e-9 {
		t.Errorf("TeamCoverage = %v, want 0.5 (owned: u-owned-novote and u-two-teams once; the vote would say 0.25)", got.TeamCoverage)
	}
	// The fields that are not the team's are what they were: every unit has a repository.
	if math.Abs(got.RepoCoverage-1.0) > 1e-9 {
		t.Errorf("RepoCoverage = %v, want 1.0", got.RepoCoverage)
	}

	// The chart's TEAM dimension is unchanged: it is still the vote.
	out, err := Resolve(ctx, client, org, model.AnalyticsRequestInput{Sankey: &reqInput})
	if err != nil {
		t.Fatal(err)
	}
	var voted float64
	var total float64
	for _, e := range out.Sankey.Edges {
		if !strings.HasPrefix(e.Source, "TEAM:") || e.Value == nil {
			continue
		}
		total += *e.Value
		if strings.Contains(e.Source, "Vote Team") || strings.Contains(e.Source, "team-vote") {
			voted += *e.Value
		}
	}
	if voted != 1 {
		t.Errorf("chart TEAM node of the voted team carries %v units, want 1 (the chart still reads the vote)", voted)
	}
	if total != 4 {
		t.Errorf("chart TEAM edges carry %v units, want 4", total)
	}
}

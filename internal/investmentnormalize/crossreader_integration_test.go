//go:build integration

package investmentnormalize

import (
	"context"
	"math"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/analytics"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/home"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/operatingreview"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chquery"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-9206 round 129 r1 P1 (1) and (2), closed by an executed proof: the SAME normalized rows are visible
// in Home, the Operating Review and the analytics THEME breakdown, with asserted values (not "non-empty").
// One seeded set (a week of three completed items, legacy keys only, no categorized unit):
//
//	security/vulnerability, 3 units -> risk; product/general "fix: ..." 1 unit -> quality (estimated);
//	infra/k8s, 2 units -> maintenance.
func TestNormalizedRowsAreVisibleInHomeOperatingReviewAndAnalytics(t *testing.T) {
	readersScenario(t, false)
}

// CHAOS-9227 fix 3: the SAME readers on an organization WITH a membership run (the marker of every real
// organization). The normalized rows have no work_unit_membership rows (membership is projected from the work
// graph), so the shared scope filter dropped every one of them, and writing them moved the scope state to
// scoped_projection_lag. They must be visible next to a canonical unit, and the scope state must not move.
func TestNormalizedRowsAreVisibleToReadersOfAnOrgWithAMembershipRun(t *testing.T) {
	readersScenario(t, true)
}

func readersScenario(t *testing.T, withMarker bool) {
	t.Helper()
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

	org := "org-9206-readers"
	if withMarker {
		org = "org-9227-readers-marker"
	}
	const repo = "92060003-9206-4206-8206-920692069206"
	week := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC) // a Monday
	completed := week.Add(60 * time.Hour)                // Wednesday 12:00
	day := time.Date(completed.Year(), completed.Month(), completed.Day(), 0, 0, 0, 0, time.UTC)
	exec := func(statement string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, statement, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, statement)
		}
	}
	exec(`INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, 'acme/readers', 'github', ?, ?, ?)`, repo, org, completed, completed)
	item := func(id, title string, points *float64, area, stream string) {
		exec(`INSERT INTO work_items (repo_id, work_item_id, provider, title, description, type, status, status_raw, project_key, project_id,
 assignees, reporter, created_at, updated_at, started_at, completed_at, closed_at, labels, story_points, sprint_id, sprint_name,
 parent_id, epic_id, url, last_synced, org_id)
VALUES (?, ?, 'jira', ?, NULL, 'pr', 'done', 'Done', 'P', 'p', [], 'r', ?, ?, NULL, ?, NULL, [], ?, '', '', '', '', '', ?, ?)`,
			repo, id, title, week, completed, completed, points, completed, org)
		exec(`INSERT INTO investment_classifications_daily (repo_id, day, artifact_type, artifact_id, provider, investment_area, project_stream, confidence, rule_id, computed_at, org_id)
VALUES (?, ?, 'work_item', ?, 'jira', ?, ?, 1.0, 'r', ?, ?)`, repo, day, id, area, stream, completed, org)
	}
	three, two := 3.0, 2.0
	item("R-1", "Rotate the keys", &three, "security", "vulnerability")
	item("R-2", "fix(api): crash on login", nil, "product", "general")
	item("R-3", "Upgrade the cluster", &two, "infra", "k8s")

	if withMarker {
		// A canonical unit of 5 feature_delivery effort in the same week, a membership row for it, and a COMPLETE
		// membership run that finished after the unit was computed: the organization is scoped, with no lag.
		exec(`INSERT INTO work_unit_investments (work_unit_id, from_ts, to_ts, repo_id, effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json, structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status, categorization_model_version, categorization_run_id, computed_at, org_id)
VALUES ('wu-canon', ?, ?, ?, 'churn_loc', 5, ?, ?, ?, 0.8, 'high', 'ok', 'jev-test', 'run-1', ?, ?)`,
			week.Add(24*time.Hour), completed, repo, map[string]float64{"feature_delivery": 1.0}, map[string]float64{"feature_delivery.roadmap": 1.0}, `{"issues":[],"prs":[]}`, completed.Add(time.Minute), org)
		exec(`INSERT INTO work_unit_membership (org_id, node_type, node_id, work_unit_id, category_kind, category, weight, is_dominant, categorization_status, computed_at, run_id)
VALUES (?, 'issue', 'node-1', 'wu-canon', 'theme', 'feature_delivery', 1.0, 1, 'ok', ?, 'run-m1')`, org, completed.Add(2*time.Minute))
		exec(`INSERT INTO work_unit_membership_runs (org_id, run_id, completed_at) VALUES (?, 'run-m1', ?)`, org, completed.Add(3*time.Minute))
	}
	window := Options{Org: org, From: day.AddDate(0, 0, -1), To: day.AddDate(0, 0, 1), RunID: "normalized_legacy:readers"}
	records, efforts, report, err := Plan(ctx, conn, window)
	if err != nil || report.Rows != 3 {
		t.Fatalf("plan = %+v, %v; want 3 rows", report, err)
	}
	if err := Apply(ctx, conn, window, records, efforts, report); err != nil {
		t.Fatal(err)
	}
	near := func(label string, got, want float64) {
		t.Helper()
		if math.Abs(got-want) > 1e-3 {
			t.Errorf("%s = %v, want %v", label, got, want)
		}
	}

	// Home: the tile of the week's effort per theme (risk 3, maintenance 2, quality 1: 50% / 33.3% / 16.7%).
	endDate := week.AddDate(0, 0, 7)
	response, err := home.BuildResponse(ctx, client, nil, org, home.Filters{
		Time:  home.TimeFilter{RangeDays: 7, CompareDays: 7, EndDate: &endDate},
		Scope: home.ScopeFilter{Level: "org"},
	}, endDate.Add(time.Hour))
	if err != nil {
		t.Fatalf("home: %v", err)
	}
	tile := map[string]home.ReworkThemeAllocation{}
	for _, row := range response.ReworkThemeAllocation {
		tile[row.Theme] = row
	}
	canon, themes := 0.0, 3
	if withMarker {
		canon, themes = 5, 4 // the canonical unit's feature_delivery effort sits beside the normalized rows
		near("home feature_delivery effort", tile["feature_delivery"].Allocation, canon)
	}
	if len(tile) != themes {
		t.Fatalf("home tile themes = %v, want %d (risk, maintenance, quality%s)", tile, themes, map[bool]string{true: ", feature_delivery", false: ""}[withMarker])
	}
	near("home risk effort", tile["risk"].Allocation, 3)
	near("home maintenance effort", tile["maintenance"].Allocation, 2)
	near("home quality effort", tile["quality"].Allocation, 1)
	near("home risk share", tile["risk"].AllocationPct, 100*3/(6+canon))

	// Operating Review: the investment section of the week.
	review, err := operatingreview.ResolveInput(ctx, client, org, nil, nil, graphqldate.New(week))
	if err != nil {
		t.Fatalf("operating review: %v", err)
	}
	effort := map[string]float64{}
	for _, section := range review.Sections {
		if section.Key != "investment" {
			continue
		}
		for _, m := range section.Metrics {
			if !m.HasData {
				t.Errorf("%s: hasData false for a week with normalized rows", m.Key)
			}
			effort[m.Key] = m.Value
		}
	}
	near("review risk_effort", effort["risk_effort"], 3)
	near("review maintenance_effort", effort["maintenance_effort"], 2)
	near("review quality_effort", effort["quality_effort"], 1)
	near("review feature_delivery_effort", effort["feature_delivery_effort"], canon)

	// Analytics: the THEME breakdown counts the units (one each).
	dates := &model.DateRangeInput{StartDate: graphqldate.New(week), EndDate: graphqldate.New(week.AddDate(0, 0, 7))}
	result, err := analytics.Resolve(ctx, client, org, model.AnalyticsRequestInput{
		Breakdowns: []model.BreakdownRequestInput{{Dimension: model.DimensionInputTheme, Measure: model.MeasureInputCount, DateRange: dates, TopN: 10}},
	})
	if err != nil {
		t.Fatalf("analytics: %v", err)
	}
	units := map[string]float64{}
	for _, b := range result.Breakdowns {
		for _, item := range b.Items {
			if item.Value != nil {
				units[item.Key] = *item.Value
			}
		}
	}
	for _, theme := range []string{"risk", "maintenance", "quality"} {
		near("analytics units of "+theme, units[theme], 1)
	}
	if withMarker {
		near("analytics units of feature_delivery", units["feature_delivery"], 1)
	}
	if len(units) != themes {
		t.Errorf("analytics themes = %v, want exactly %d", units, themes)
	}
	// Writing normalized rows must not move the scope state: a marker that is as new as every canonical row
	// is "scoped" with no lag, however many normalized rows are written after it.
	if withMarker {
		state, err := analytics.FetchInvestmentMembershipScopeState(ctx, client, org, 60)
		if err != nil {
			t.Fatalf("scope state: %v", err)
		}
		if state.ScopeMode != "scoped" || state.LagSeconds != 0 {
			t.Errorf("scope state after the apply = %s / lag %d s, want scoped / 0", state.ScopeMode, state.LagSeconds)
		}
	}
}

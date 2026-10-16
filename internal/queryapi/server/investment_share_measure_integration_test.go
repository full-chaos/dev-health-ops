//go:build integration

package server

// CHAOS-9622 (D5999: lines of code are not a measure of effort). Every surface that serves an investment share
// reads the work-unit count, the one the Investment page reads, or ISSUE_SIZE where the surface carries its sizing
// coverage (REST /investment, CHAOS-9809); with no sized unit the two are equal: REST /investment, REST
// /investment/sunburst, the Home tile "Investment by theme" and the Operating Review investment section. The fixture is built so that COUNT
// and effort_value (the changed lines of code) give very different shares: on the unfixed code every assertion
// below is red.
//
//	wu-big    repo A  1000 lines  feature_delivery.roadmap 1.0
//	wu-split  repo A    10 lines  maintenance.debt 0.5, risk.security 0.5
//	wu-small  repo B    10 lines  maintenance.debt 1.0
//
// COUNT:        feature_delivery 1, maintenance 1.5, risk 0.5  (total 3)    -> 33.3 / 50 / 16.7 percent
// effort_value: feature_delivery 1000, maintenance 15, risk 5  (total 1020) -> 98.0 / 1.5 / 0.5 percent
// Repository A alone: COUNT 1 / 0.5 / 0.5 (total 2); effort_value 1000 / 5 / 5.

import (
	"context"
	"encoding/json"
	"math"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/analytics"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/home"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/investment"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/investmentexplain"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

const (
	measureOrg   = "org-9622-measure"
	measureRepoA = "96220001-9622-4622-8622-962296229622"
	measureRepoB = "96220002-9622-4622-8622-962296229622"
)

func measureSeedUnit(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, id, repo string, lines float64, distribution map[string]float64) {
	t.Helper()
	themes := map[string]float64{}
	for key, share := range distribution {
		for i := range key {
			if key[i] == '.' {
				themes[key[:i]] += share
				break
			}
		}
	}
	// The unit ends 2026-01-06 12:00, inside the week of 2026-01-05 and the window [2026-01-05, 2026-01-12).
	to := time.Date(2026, 1, 6, 12, 0, 0, 0, time.UTC)
	evidence, _ := json.Marshal(map[string]any{"issues": []string{}, "prs": []string{}, "commits": []string{}, "edges": []string{}})
	if err := admin.Exec(ctx, `INSERT INTO work_unit_investments
(work_unit_id, from_ts, to_ts, repo_id, effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json, structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status, categorization_model_version, categorization_run_id, computed_at, org_id)
VALUES (?, ?, ?, ?, 'churn_loc', ?, ?, ?, ?, 0.8, 'high', 'ok', 'jev-test', 'run-1', ?, ?)`,
		id, to.Add(-24*time.Hour), to, repo, lines, themes, distribution, string(evidence), to.Add(time.Hour), measureOrg); err != nil {
		t.Fatalf("seed unit %s: %v", id, err)
	}
}

func measureShares(values map[string]float64) map[string]float64 {
	total := 0.0
	for _, v := range values {
		total += v
	}
	out := make(map[string]float64, len(values))
	for k, v := range values {
		if total > 0 {
			out[k] = v / total * 100
		}
	}
	return out
}

func measureAssertShares(t *testing.T, surface string, got, want map[string]float64) {
	t.Helper()
	// The shares are Float32 in the shared read (the Investment page's own narrowing).
	for key, w := range want {
		if math.Abs(got[key]-w) > 0.01 {
			t.Errorf("%s: %s = %.3f%%, want %.3f%% (all: %v)", surface, key, got[key], w, got)
		}
	}
	for key, g := range got {
		if _, ok := want[key]; !ok && g > 0.01 {
			t.Errorf("%s: unexpected theme %s = %.3f%%", surface, key, g)
		}
	}
}

func TestInvestmentSharesOfEverySurfaceAreTheCountTheInvestmentPageReads(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = instance.Close(context.Background()) }()
	chschema.Apply(ctx, t, instance)
	options, err := stdclickhouse.ParseDSN(instance.URI)
	if err != nil {
		t.Fatalf("parse DSN: %v", err)
	}
	admin, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatalf("open ClickHouse: %v", err)
	}
	defer func() { _ = admin.Close() }()
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(newUnrestrictedReadClickHouseOptions(instance.URI))
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	synced := time.Date(2026, 1, 7, 0, 0, 0, 0, time.UTC)
	for repo, name := range map[string]string{measureRepoA: "acme/a", measureRepoB: "acme/b"} {
		if err := admin.Exec(ctx, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, 'github', ?, ?, ?)`, repo, name, measureOrg, synced, synced); err != nil {
			t.Fatalf("seed repo: %v", err)
		}
	}
	measureSeedUnit(ctx, t, admin, "wu-big", measureRepoA, 1000, map[string]float64{"feature_delivery.roadmap": 1})
	measureSeedUnit(ctx, t, admin, "wu-split", measureRepoA, 10, map[string]float64{"maintenance.debt": 0.5, "risk.security": 0.5})
	measureSeedUnit(ctx, t, admin, "wu-small", measureRepoB, 10, map[string]float64{"maintenance.debt": 1})

	start := time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 1, 12, 0, 0, 0, 0, time.UTC)
	dateRange := &model.DateRangeInput{StartDate: graphqldate.New(start), EndDate: graphqldate.New(end)}

	cases := []struct {
		name       string
		scopeLevel string
		scopeIDs   []string
		want       map[string]float64 // theme -> COUNT
		wantTotal  float64
	}{
		{"organization", "org", nil, map[string]float64{"feature_delivery": 1, "maintenance": 1.5, "risk": 0.5}, 3},
		{"repository A", "repo", []string{measureRepoA}, map[string]float64{"feature_delivery": 1, "maintenance": 0.5, "risk": 0.5}, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wantShares := measureShares(tc.want)
			analyticsScope := &model.FilterInput{}
			if tc.scopeLevel == "repo" {
				analyticsScope.Scope = &model.ScopeFilterInput{Level: model.ScopeLevelInputRepo, Ids: tc.scopeIDs}
			}

			// The Investment page: the COUNT breakdown by theme.
			useInvestment := true
			page, err := analytics.Resolve(ctx, client, measureOrg, model.AnalyticsRequestInput{
				Breakdowns:    []model.BreakdownRequestInput{{Dimension: model.DimensionInputTheme, Measure: model.MeasureInputCount, DateRange: dateRange, TopN: 10}},
				UseInvestment: &useInvestment,
				Filters:       analyticsScope,
			})
			if err != nil {
				t.Fatalf("page: %v", err)
			}
			pageValues := map[string]float64{}
			for _, item := range page.Breakdowns[0].Items {
				if item.Value != nil {
					pageValues[item.Key] = *item.Value
				}
			}
			measureAssertShares(t, "page (the reference: COUNT)", measureShares(pageValues), wantShares)

			// REST /investment: theme_distribution, measure and denominator.
			reader, err := investment.NewReader(client)
			if err != nil {
				t.Fatal(err)
			}
			rest, err := investment.BuildResponse(ctx, reader, measureOrg, investment.Params{OrgID: measureOrg, StartTS: start, EndTS: end, ScopeLevel: tc.scopeLevel, ScopeIDs: tc.scopeIDs})
			if err != nil {
				t.Fatalf("REST /investment: %v", err)
			}
			restValues := map[string]float64{}
			for theme, value := range rest.ThemeDistribution.All() {
				restValues[theme] = value
			}
			measureAssertShares(t, "REST /investment", measureShares(restValues), measureShares(pageValues))
			measureAssertShares(t, "REST /investment (expected COUNT)", measureShares(restValues), wantShares)
			// CHAOS-9809: REST /investment reads ISSUE_SIZE (it carries its sizing coverage); no unit of this fixture has a
			// size, so every unit counts 1 and the numbers are the COUNT's.
			if rest.Measure != investmentexplain.RESTInvestmentMeasure {
				t.Errorf("REST /investment measure = %q, want %q", rest.Measure, investmentexplain.RESTInvestmentMeasure)
			}
			if rest.SizingCoverage == nil || *rest.SizingCoverage != 0 || rest.SizingNote == "" {
				t.Errorf("REST /investment sizing = (%v, %q), want 0 sized and the mostly-unsized sentence", rest.SizingCoverage, rest.SizingNote)
			}
			if rest.Denominator == nil || math.Abs(*rest.Denominator-tc.wantTotal) > 1e-3 {
				t.Errorf("REST /investment denominator = %v, want %v (the sum of the themes)", rest.Denominator, tc.wantTotal)
			}
			for theme, want := range tc.want {
				// numerator / denominator is the share: the value of the distribution IS the numerator.
				if got, _ := rest.ThemeDistribution.Get(theme); math.Abs(got-want) > 1e-3 {
					t.Errorf("REST /investment numerator of %s = %v, want %v", theme, got, want)
				}
			}

			// REST /investment/sunburst: its slices roll up to the same theme shares.
			slices, err := investment.BuildSunburstResponse(ctx, reader, measureOrg, investment.SunburstParams{OrgID: measureOrg, StartTS: start, EndTS: end, ScopeLevel: tc.scopeLevel, ScopeIDs: tc.scopeIDs, Limit: 100})
			if err != nil {
				t.Fatalf("REST /investment/sunburst: %v", err)
			}
			sunburstValues := map[string]float64{}
			for _, slice := range slices {
				sunburstValues[slice.Theme] += slice.Value
			}
			measureAssertShares(t, "REST /investment/sunburst", measureShares(sunburstValues), measureShares(pageValues))

			// The Home tile: allocation_pct per theme (dated by the unit's END day, D5933: the unit ends inside the window).
			f := home.Filters{Time: home.TimeFilter{RangeDays: 7, CompareDays: 7, EndDate: &end}}
			if tc.scopeLevel == "repo" {
				f.Scope = home.ScopeFilter{Level: "repo", IDs: tc.scopeIDs}
			}
			tile, err := home.BuildResponse(ctx, client, nil, measureOrg, f, end.Add(12*time.Hour))
			if err != nil {
				t.Fatalf("home: %v", err)
			}
			tileShares := map[string]float64{}
			tileAllocation := map[string]float64{}
			for _, row := range tile.ReworkThemeAllocation {
				tileShares[row.Theme] = row.AllocationPct
				tileAllocation[row.Theme] = row.Allocation
			}
			measureAssertShares(t, "Home tile allocation_pct", tileShares, measureShares(pageValues))
			for theme, want := range tc.want {
				if math.Abs(tileAllocation[theme]-want) > 1e-3 {
					t.Errorf("Home tile allocation of %s = %v, want the COUNT %v", theme, tileAllocation[theme], want)
				}
			}
		})
	}

	// The Operating Review reads the same measure through the same shared reader; its metrics are pinned by value in
	// the operatingreview package (operatingreview_investment_canonical_integration_test.go), not imported here.

	// No read can fall back to a weight: a read that names no measure is refused.
	explainReader, err := investmentexplain.NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	empty := investmentexplain.BreakdownFilters{OrgID: measureOrg, StartTS: start, EndTS: end}
	if _, err := explainReader.FetchInvestmentBreakdown(ctx, empty); err == nil {
		t.Error("FetchInvestmentBreakdown with no measure returned no error")
	}
	if _, err := explainReader.FetchInvestmentThemeEffort(ctx, empty); err == nil {
		t.Error("FetchInvestmentThemeEffort with no measure returned no error")
	}

	// Lines of code are no measure of Investment (CHAOS-9695): a read that names CHURN_LOC is refused too.
	if _, err := explainReader.FetchInvestmentThemeEffort(ctx, investmentexplain.BreakdownFilters{OrgID: measureOrg, StartTS: start, EndTS: end, Measure: "CHURN_LOC"}); err == nil {
		t.Error("FetchInvestmentThemeEffort with measure CHURN_LOC returned no error")
	}
}

//go:build integration

package investmentexplain

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/analytics"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
)

// pageMixSeed is the frozen parameter set of the work units below: the same rows on every run.
const pageMixSeed = 9555

// TestExplainSharesEqualTheInvestmentPagesMix pins CHAOS-9555 against a real ClickHouse: for one
// window, the shares and the sums an explanation serves equal the mix the Investment page's
// InvestmentBreakdown reads (THEME and SUBCATEGORY, measure COUNT), whatever the effort of each unit is.
// The units' effort_value spans 1 to 5000 so a share weighted by effort differs from the page's.
func TestExplainSharesEqualTheInvestmentPagesMix(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := newInvestmentExplainTestClickHouse(ctx, t)

	const org = "investmentexplain-page-mix-parity-org"
	rng := rand.New(rand.NewSource(pageMixSeed))
	computed := time.Date(2026, 10, 1, 6, 0, 0, 0, time.UTC)
	windowStart := time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC)
	windowEnd := time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC)

	subcategories := units.SortedSubcategories[:]
	for i := 0; i < 80; i++ {
		first := subcategories[rng.Intn(len(subcategories))]
		second := subcategories[rng.Intn(len(subcategories))]
		share := math.Round(rng.Float64()*100) / 100
		subcategoryJSON := map[string]float64{first: share}
		subcategoryJSON[second] += 1 - share
		themeJSON := map[string]float64{}
		for key, value := range subcategoryJSON {
			themeJSON[units.ThemeOf(key)] += value
		}
		from := windowStart.AddDate(0, 0, rng.Intn(12))
		to := from.AddDate(0, 0, 1+rng.Intn(2))
		effort := float64(1 + rng.Intn(5000))
		repoID := uuid.New()
		if err := admin.Exec(ctx, `INSERT INTO work_unit_investments (work_unit_id, from_ts, to_ts, repo_id, provider, effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json, structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status, categorization_errors_json, categorization_model_version, categorization_input_hash, categorization_run_id, computed_at, org_id)
			VALUES (?, ?, ?, ?, 'github', 'churn_loc', ?, ?, ?, '{}', 0.7, 'moderate', 'ok', '{}', 'v1', 'hash', 'run-1', ?, ?)`,
			fmt.Sprintf("wu-%03d", i), from, to, repoID, effort, themeJSON, subcategoryJSON, computed, org); err != nil {
			t.Fatalf("seed work unit %d: %v", i, err)
		}
	}

	// The page: InvestmentBreakdown, THEME and SUBCATEGORY, COUNT, the page's scope TEAM with no ids.
	useInvestment := true
	breakdown := func(dimension model.DimensionInput) map[string]float64 {
		result, err := analytics.Resolve(ctx, client, org, model.AnalyticsRequestInput{
			UseInvestment: &useInvestment,
			Filters:       &model.FilterInput{Scope: &model.ScopeFilterInput{Level: model.ScopeLevelInputTeam, Ids: []string{}}},
			Breakdowns: []model.BreakdownRequestInput{{
				Dimension: dimension, Measure: model.MeasureInputCount, TopN: 100,
				// the page names its last day, inclusive (D6101); the explain below reads [windowStart, windowEnd)
				DateRange: &model.DateRangeInput{StartDate: graphqldate.New(windowStart), EndDate: graphqldate.New(windowEnd.AddDate(0, 0, -1))},
			}},
		})
		if err != nil {
			t.Fatalf("page breakdown %s: %v", dimension, err)
		}
		values := map[string]float64{}
		for _, item := range result.Breakdowns[0].Items {
			if item.Value != nil {
				values[item.Key] = *item.Value
			}
		}
		return values
	}
	pageThemes := breakdown(model.DimensionInputTheme)
	pageSubcategories := breakdown(model.DimensionInputSubcategory)
	pageTotal := 0.0
	for _, value := range pageThemes {
		pageTotal += value
	}
	if len(pageSubcategories) < 5 || pageTotal < 70 {
		t.Fatalf("the seeded page mix is too thin to prove anything: %d subcategories, total %v", len(pageSubcategories), pageTotal)
	}

	keys := make([]string, 0, len(pageSubcategories))
	for key := range pageSubcategories {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if len(keys) > 8 {
		keys = keys[:8]
	}
	findings := make([]any, 0, len(keys))
	for _, key := range keys {
		findings = append(findings, map[string]any{
			"finding": "this work appears in the mix",
			"evidence": map[string]any{
				"theme": units.ThemeOf(key), "subcategory": key, "share_pct": 1.0,
				"delta_pct_points": nil, "evidence_quality_mean": nil, "evidence_quality_band": nil,
			},
		})
	}
	completion, err := json.Marshal(map[string]any{
		"summary":      "Work appears spread across the mix.",
		"top_findings": findings,
		"confidence": map[string]any{
			"level": "moderate", "quality_mean": 0.55, "quality_stddev": 0.25,
			"band_mix": map[string]any{"high": 1, "moderate": 0, "low": 1, "very_low": 0, "unknown": 0}, "drivers": []any{},
		},
		"what_to_check_next": []any{map[string]any{"action": "Open the evidence", "why": "It shows the mix", "where": "Evidence panel"}},
		"anti_claims":        []any{"This does not rank people."},
	})
	if err != nil {
		t.Fatal(err)
	}

	reader, err := NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	// The request the web sends before CHAOS-9615 names no measure; the page's measure is the default.
	opts := ExplainInvestmentMixOptions{
		OrgID: org, StartTS: windowStart, EndTS: windowEnd, ScopeLevel: "team",
		LLMProvider: "openai", Now: explainFixtureNow, FiltersForCacheKey: defaultMetricFilterForCacheKey(),
	}
	var calls int
	got, err := reader.ExplainInvestmentMix(ctx, nil, alwaysAvailable, countingComplete(string(completion), &calls), opts)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status == nil || *got.Status != "valid" || len(got.TopFindings) != len(keys) {
		t.Fatalf("explanation status/findings = %v/%d, want valid with %d findings", got.Status, len(got.TopFindings), len(keys))
	}
	for _, finding := range got.TopFindings {
		key := *finding.Evidence.Subcategory
		wantShare := roundOneDecimal(pageSubcategories[key] / pageTotal * 100)
		if finding.Evidence.SharePct != wantShare {
			t.Errorf("%s: explain share_pct = %v, the page's mix gives %v", key, finding.Evidence.SharePct, wantShare)
		}
		if finding.Evidence.Numerator == nil || math.Abs(*finding.Evidence.Numerator-pageSubcategories[key]) > 1e-3 {
			t.Errorf("%s: explain numerator = %v, the page's sum is %v", key, finding.Evidence.Numerator, pageSubcategories[key])
		}
		if finding.Evidence.Denominator == nil || math.Abs(*finding.Evidence.Denominator-pageTotal) > 1e-3 {
			t.Errorf("%s: explain denominator = %v, the page's total is %v", key, finding.Evidence.Denominator, pageTotal)
		}
		// The request names no measure: the explanation reads its default, ISSUE_SIZE (CHAOS-9809); no unit of this
		// fixture has a size, so every unit counts 1 and the shares are the page's COUNT shares.
		if finding.Evidence.Measure != ExplanationDefaultMeasure {
			t.Errorf("%s: measure = %q, want %q", key, finding.Evidence.Measure, ExplanationDefaultMeasure)
		}
	}
}

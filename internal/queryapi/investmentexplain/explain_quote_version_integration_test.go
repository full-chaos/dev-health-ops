//go:build integration

package investmentexplain

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

// TestExplainIsRecomputedAfterAQuoteOnlyWrite pins CHAOS-9582 against a real ClickHouse. The work-unit row is not
// touched between the two identical requests; only a quote of its run is written (the state a run leaves when it dies
// between its quote write and its work-unit write, materialize.go). The answer built from the old quotes must not be
// replayed.
func TestExplainIsRecomputedAfterAQuoteOnlyWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := newInvestmentExplainTestClickHouse(ctx, t)

	const org = "investmentexplain-quote-version-org"
	computed := time.Date(2026, 10, 1, 6, 0, 0, 0, time.UTC)
	windowStart := time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC)
	windowEnd := time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC)

	subcategory := units.SortedSubcategories[0]
	theme := units.ThemeOf(subcategory)
	if err := admin.Exec(ctx, `INSERT INTO work_unit_investments (work_unit_id, from_ts, to_ts, repo_id, provider, effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json, structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status, categorization_errors_json, categorization_model_version, categorization_input_hash, categorization_run_id, computed_at, org_id)
		VALUES (?, ?, ?, ?, 'github', 'churn_loc', 10, ?, ?, '{}', 0.7, 'moderate', 'ok', '{}', 'v1', 'hash', 'run-1', ?, ?)`,
		"wu-1", windowStart.AddDate(0, 0, 2), windowStart.AddDate(0, 0, 3), uuid.New(),
		map[string]float64{theme: 1}, map[string]float64{subcategory: 1}, computed, org); err != nil {
		t.Fatalf("seed work unit: %v", err)
	}
	insertQuote := func(quote string, at time.Time) {
		t.Helper()
		if err := admin.Exec(ctx, `INSERT INTO work_unit_investment_quotes (work_unit_id, quote, source_type, source_id, computed_at, categorization_run_id, org_id)
			VALUES ('wu-1', ?, 'pr_body', ?, ?, 'run-1', ?)`, quote, quote, at, org); err != nil {
			t.Fatalf("seed quote: %v", err)
		}
	}
	insertQuote("the first quote", computed)

	completion, err := json.Marshal(map[string]any{
		"summary": "Work appears to lean toward one area.",
		"top_findings": []any{map[string]any{
			"finding": "this work appears in the mix",
			"evidence": map[string]any{
				"theme": theme, "subcategory": subcategory, "share_pct": 100.0,
				"delta_pct_points": nil, "evidence_quality_mean": nil, "evidence_quality_band": nil,
			},
		}},
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
	writer, err := NewCacheWriter(admin)
	if err != nil {
		t.Fatal(err)
	}
	opts := ExplainInvestmentMixOptions{
		OrgID: org, StartTS: windowStart, EndTS: windowEnd,
		LLMProvider: "openai", Now: explainFixtureNow, FiltersForCacheKey: defaultMetricFilterForCacheKey(),
	}
	var calls int
	explain := func() {
		t.Helper()
		if _, err := reader.ExplainInvestmentMix(ctx, writer, alwaysAvailable, countingComplete(string(completion), &calls), opts); err != nil {
			t.Fatal(err)
		}
	}

	explain()
	if calls != 1 {
		t.Fatalf("first explain made %d model calls, want 1", calls)
	}
	explain()
	if calls != 1 {
		t.Fatalf("second identical explain made %d model calls in total, want 1 (the cache must serve it); the test cannot tell staleness from a dead cache otherwise", calls)
	}

	insertQuote("a quote the first answer never saw", computed.Add(time.Hour))
	explain()
	if calls != 2 {
		t.Fatalf("after a quote-only write the explain made %d model calls in total, want 2: the cached answer was replayed over changed quotes", calls)
	}
}

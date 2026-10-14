package investmentexplain

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"math"
	"regexp"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
)

// mixAgreementClient serves one breakdown (the rows the Investment mix reads),
// an optional cached explanation, and the work-unit fixtures of the golden client.
type mixAgreementClient struct {
	breakdown      [][]any
	cachedJSON     string
	cachedAt       time.Time
	cacheReadCount *int
	dataVersion    string
}

func (c mixAgreementClient) Query(ctx context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	switch {
	case strings.Contains(statement, "ARRAY JOIN CAST(subcategory_distribution_json") && strings.Contains(statement, "subcategory_kv.1 AS subcategory"):
		return &fixtureRowScanner{rows: c.breakdown}, nil
	case strings.Contains(statement, "toString(max(computed_at))"):
		return &fixtureRowScanner{rows: [][]any{{c.dataVersion, "4"}}}, nil
	case strings.Contains(statement, "FROM investment_explanations"):
		if c.cacheReadCount != nil {
			*c.cacheReadCount++
		}
		if c.cachedJSON == "" {
			return &fixtureRowScanner{}, nil
		}
		return &fixtureRowScanner{rows: [][]any{{"key", c.cachedJSON, "openai", nil, c.cachedAt, "org-9555"}}}, nil
	}
	return explainFixtureClient{}.Query(ctx, statement, bindings)
}

// mixRows is a mix with Feature Delivery 19.2 %, Quality 31.4 %, Operational 49.4 %.
func mixRows() [][]any {
	return [][]any{
		{"operational.support", "operational", 49.4},
		{"quality.bugfix", "quality", 31.4},
		{"feature_delivery.roadmap", "feature_delivery", 10.0},
		{"feature_delivery.customer", "feature_delivery", 9.2},
	}
}

func completionWithKeys(share float64) string {
	body := map[string]any{
		"summary": "Effort appears to lean toward feature_delivery.roadmap, per theme_distribution_top.",
		"top_findings": []any{map[string]any{
			"finding": "feature_delivery.roadmap work appears to lead the mix",
			"evidence": map[string]any{
				"theme": "feature_delivery", "subcategory": "feature_delivery.roadmap", "share_pct": share,
				"delta_pct_points": nil, "evidence_quality_mean": nil, "evidence_quality_band": nil,
			},
		}},
		"confidence": map[string]any{
			"level": "moderate", "quality_mean": 0.55, "quality_stddev": 0.25,
			"band_mix": map[string]any{"high": 1, "moderate": 0, "low": 1, "very_low": 0, "unknown": 0}, "drivers": []any{},
		},
		"what_to_check_next": []any{map[string]any{
			"action": "Inspect subcategory_distribution_top", "why": "It leans on operational.support",
			"where": "theme_distribution_top panel for feature_delivery.roadmap",
		}},
		"anti_claims": []any{"This does not rank feature_delivery.customer above quality.bugfix."},
	}
	encoded, _ := json.Marshal(body)
	return string(encoded)
}

var rawKeyPattern = regexp.MustCompile(`[a-z]+_[a-z_]+|(?:feature_delivery|operational|maintenance|quality|risk)\.[a-z_]+`)

func explainOpts() ExplainInvestmentMixOptions {
	return ExplainInvestmentMixOptions{
		OrgID: "org-9555", StartTS: time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC), EndTS: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		LLMProvider: "openai", Now: explainFixtureNow, FiltersForCacheKey: defaultMetricFilterForCacheKey(),
	}
}

func alwaysAvailable(context.Context, string, string) bool { return true }

func countingComplete(text string, calls *int) CompleteFunc {
	return func(context.Context, string, string, string) (categorize.CompletionResult, string, string, error) {
		*calls++
		return categorize.CompletionResult{Text: text, Model: "fake"}, "openai", "gpt-5-mini", nil
	}
}

func TestExplainFindingSharesAgreeWithMixAndLeakNoRawKeys(t *testing.T) {
	rows := mixRows()
	reader, err := NewReader(mixAgreementClient{breakdown: rows})
	if err != nil {
		t.Fatal(err)
	}
	// The model reports 64 in its own share_pct field; the served share is the mix's.
	var calls int
	got, err := reader.ExplainInvestmentMix(context.Background(), nil, alwaysAvailable, countingComplete(completionWithKeys(64), &calls), explainOpts())
	if err != nil {
		t.Fatal(err)
	}
	if got.Status == nil || *got.Status != "valid" {
		t.Fatalf("status = %v, want valid", got.Status)
	}

	var breakdown []BreakdownRow
	for _, r := range rows {
		breakdown = append(breakdown, BreakdownRow{Subcategory: r[0].(string), Theme: r[1].(string), Value: r[2].(float64)})
	}
	themes, subcategories := buildInvestmentBreakdownDistributions(breakdown)
	themeTotal := 0.0
	for _, kv := range themes.Items() {
		themeTotal += kv.Value
	}
	wantSubcategory := roundOneDecimal(sharesPct(subcategories.Items(), themeTotal)["feature_delivery.roadmap"])
	wantTheme := roundOneDecimal(sharesPct(themes.Items(), themeTotal)["feature_delivery"])
	if wantTheme != 19.2 || wantSubcategory != 10.0 {
		t.Fatalf("fixture mix shares = %v / %v, want 19.2 / 10.0", wantTheme, wantSubcategory)
	}
	if share := got.TopFindings[0].Evidence.SharePct; share != wantSubcategory {
		t.Fatalf("finding share = %v, want the mix's %v", share, wantSubcategory)
	}
	if !strings.Contains(got.TopFindings[0].Finding, "~10% of work") {
		t.Fatalf("finding text = %q, want the mix share", got.TopFindings[0].Finding)
	}

	encoded, err := EncodeInvestmentMixExplanation(got)
	if err != nil {
		t.Fatal(err)
	}
	var served struct {
		Summary  string `json:"summary"`
		Findings []struct {
			Finding  string                 `json:"finding"`
			Evidence map[string]interface{} `json:"evidence"`
		} `json:"top_findings"`
		Actions []struct{ Action, Why, Where string } `json:"what_to_check_next"`
		Claims  []string                              `json:"anti_claims"`
	}
	if err := json.Unmarshal([]byte(encoded), &served); err != nil {
		t.Fatal(err)
	}
	texts := []string{served.Summary, served.Findings[0].Finding}
	for _, a := range served.Actions {
		texts = append(texts, a.Action, a.Why, a.Where)
	}
	texts = append(texts, served.Claims...)
	for _, text := range texts {
		if raw := rawKeyPattern.FindString(text); raw != "" {
			t.Errorf("served text %q carries raw key %q", text, raw)
		}
	}
	if !strings.Contains(served.Actions[0].Where, "theme distribution panel for Feature Delivery · Roadmap") {
		t.Errorf("where = %q, want display names", served.Actions[0].Where)
	}
	if served.Findings[0].Evidence["theme"] != "feature_delivery" || served.Findings[0].Evidence["subcategory"] != "feature_delivery.roadmap" {
		t.Errorf("evidence keys must stay canonical, got %v", served.Findings[0].Evidence)
	}
}

func cachedExplanationJSON(t *testing.T, share float64) string {
	t.Helper()
	status := "valid"
	subcategory := "feature_delivery.roadmap"
	encoded, err := EncodeInvestmentMixExplanation(InvestmentMixExplanation{
		Summary: "cached",
		TopFindings: []InvestmentMixFinding{{
			Finding:  "roadmap work appears to lead the mix (~64% of effort).",
			Evidence: InvestmentMixFindingEvidence{Theme: "feature_delivery", Subcategory: &subcategory, SharePct: share},
		}},
		Confidence: InvestmentMixConfidence{Level: "moderate", BandMix: BandMix{}, Drivers: []string{}},
		Status:     &status,
	})
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func TestExplainCachedFindingThatDisagreesWithMixIsRecomputed(t *testing.T) {
	reader, err := NewReader(mixAgreementClient{breakdown: mixRows(), cachedJSON: cachedExplanationJSON(t, 64), cachedAt: explainFixtureNow})
	if err != nil {
		t.Fatal(err)
	}
	var calls int
	got, err := reader.ExplainInvestmentMix(context.Background(), nil, alwaysAvailable, countingComplete(completionWithKeys(64), &calls), explainOpts())
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("model calls = %d, want 1: a cached 64 %% against a 10 %% mix must be recomputed", calls)
	}
	if share := got.TopFindings[0].Evidence.SharePct; share != 10.0 {
		t.Fatalf("served share = %v, want 10.0", share)
	}
}

func TestExplainCachedFindingThatAgreesWithMixIsServed(t *testing.T) {
	reader, err := NewReader(mixAgreementClient{breakdown: mixRows(), cachedJSON: cachedExplanationJSON(t, 10.0000001), cachedAt: explainFixtureNow})
	if err != nil {
		t.Fatal(err)
	}
	var calls int
	got, err := reader.ExplainInvestmentMix(context.Background(), nil, alwaysAvailable, countingComplete(completionWithKeys(10), &calls), explainOpts())
	if err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatalf("model calls = %d, want 0: a cached answer that agrees with the mix is served", calls)
	}
	if got.Summary != "cached" || got.TopFindings[0].Evidence.SharePct != 10.0 {
		t.Fatalf("served = %+v, want the cached answer with a rounded share", got)
	}
}

func TestPresentExplanationRoundsToOneDecimalWithoutTouchingTheInput(t *testing.T) {
	delta := 3.14159
	status := "valid"
	in := InvestmentMixExplanation{
		TopFindings: []InvestmentMixFinding{{Evidence: InvestmentMixFindingEvidence{Theme: "quality", SharePct: 31.4285714, DeltaPctPoints: &delta}}},
		Status:      &status,
	}
	out := PresentExplanation(in)
	if got := out.TopFindings[0].Evidence.SharePct; got != 31.4 {
		t.Errorf("share = %v, want 31.4", got)
	}
	if got := *out.TopFindings[0].Evidence.DeltaPctPoints; got != 3.1 {
		t.Errorf("delta = %v, want 3.1", got)
	}
	if math.Abs(in.TopFindings[0].Evidence.SharePct-31.4285714) > 1e-12 || *in.TopFindings[0].Evidence.DeltaPctPoints != 3.14159 {
		t.Errorf("input mutated: %+v", in.TopFindings[0].Evidence)
	}
}

func TestDisplayTextNames(t *testing.T) {
	cases := map[string]string{
		"feature_delivery.roadmap leads":                  "Feature Delivery · Roadmap leads",
		"operational.incident_response and risk.security": "Operational / Support · Incident Response and Risk / Security · Security",
		"see theme_distribution_top":                      "see theme distribution",
		"the feature_delivery theme":                      "the Feature Delivery theme",
		"quality work appears to lean":                    "quality work appears to lean",
		"missing_evidence_metadata":                       "missing evidence metadata",
		"":                                                "",
	}
	for in, want := range cases {
		if got := displayText(in); got != want {
			t.Errorf("displayText(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestPayloadCarriesDisplayLabels(t *testing.T) {
	payload := buildExplainPayload(explainPayloadInput{
		TotalEffort:      100,
		TopThemes:        []keyValue{{Key: "feature_delivery", Value: 19.2}},
		TopSubcategories: []keyValue{{Key: "feature_delivery.roadmap", Value: 10}},
	})
	theme := payload["theme_distribution_top"].([]any)[0].(map[string]any)
	subcategory := payload["subcategory_distribution_top"].([]any)[0].(map[string]any)
	if theme["label"] != "Feature Delivery" || subcategory["label"] != "Feature Delivery · Roadmap" {
		t.Fatalf("labels = %v / %v", theme["label"], subcategory["label"])
	}
}

func TestExplainModelDigitsInProseAreRejectedNotServed(t *testing.T) {
	reader, err := NewReader(mixAgreementClient{breakdown: mixRows()})
	if err != nil {
		t.Fatal(err)
	}
	text := strings.Replace(completionWithKeys(10), "work appears to lead the mix", "work dominates the mix (~64 % of effort)", 1)
	var calls int
	got, err := reader.ExplainInvestmentMix(context.Background(), nil, alwaysAvailable, countingComplete(text, &calls), explainOpts())
	if err != nil {
		t.Fatal(err)
	}
	if got.Status == nil || *got.Status != "invalid_llm_output" {
		t.Fatalf("status = %v, want invalid_llm_output: digits in model prose must never be served", got.Status)
	}
}

func TestRawKeyCountCountsLeaks(t *testing.T) {
	e := InvestmentMixExplanation{
		Summary:         "feature_delivery.roadmap leads",
		WhatToCheckNext: []InvestmentMixActionItem{{Action: "a", Why: "w", Where: "theme_distribution_top"}},
	}
	if got := rawKeyCount(e); got < 2 {
		t.Fatalf("rawKeyCount = %d, want at least 2", got)
	}
	if got := rawKeyCount(PresentExplanation(e)); got != 0 {
		t.Fatalf("rawKeyCount after present = %d, want 0", got)
	}
}

func TestCacheKeyChangesWithWindowAndMix(t *testing.T) {
	themes := newOrderedFloatMap()
	themes.Add("quality", 50)
	subcategories := newOrderedFloatMap()
	opts := explainOpts()
	base := mixCacheFilters(opts, themes, subcategories, "v1/4")
	keyOf := func(f any) string {
		k, err := ComputeCacheKey(CacheKeyInput{Filters: f, OrgID: "o"})
		if err != nil {
			t.Fatal(err)
		}
		return k
	}
	moved := opts
	moved.EndTS = moved.EndTS.AddDate(0, 0, 1)
	if keyOf(base) == keyOf(mixCacheFilters(moved, themes, subcategories, "v1/4")) {
		t.Error("key must change with the resolved window")
	}
	changed := newOrderedFloatMap()
	changed.Add("quality", 50)
	changed.Add("risk", 10)
	if keyOf(base) == keyOf(mixCacheFilters(opts, changed, subcategories, "v1/4")) {
		t.Error("key must change with the mix")
	}
	if keyOf(base) != keyOf(mixCacheFilters(opts, themes, subcategories, "v1/4")) {
		t.Error("key must be stable for the same window and mix")
	}
}

func TestExplainLogsRawKeysFoundInProse(t *testing.T) {
	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	reader, err := NewReader(mixAgreementClient{breakdown: mixRows()})
	if err != nil {
		t.Fatal(err)
	}
	var calls int
	if _, err := reader.ExplainInvestmentMix(context.Background(), nil, alwaysAvailable, countingComplete(completionWithKeys(10), &calls), explainOpts()); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "raw_keys_in_prose") {
		t.Fatalf("no raw_keys_in_prose log line, got: %s", buf.String())
	}
}

func TestCacheKeyChangesWithTheDataVersionAlone(t *testing.T) {
	themes := newOrderedFloatMap()
	themes.Add("quality", 50)
	subcategories := newOrderedFloatMap()
	keyOf := func(version string) string {
		k, err := ComputeCacheKey(CacheKeyInput{Filters: mixCacheFilters(explainOpts(), themes, subcategories, version), OrgID: "o"})
		if err != nil {
			t.Fatal(err)
		}
		return k
	}
	if keyOf("2026-10-13 10:00:00/4") == keyOf("2026-10-13 11:00:00/4") {
		t.Error("key must change when only the newest write changes")
	}
	if keyOf("2026-10-13 10:00:00/4") == keyOf("2026-10-13 10:00:00/5") {
		t.Error("key must change when only the row count changes")
	}
}

func TestEvidenceChangeWithSameMixIsRecomputed(t *testing.T) {
	// Same mix, same cached finding share; the work-unit data version moved, so the
	// key differs and nothing is read back from the cache row of the old version.
	row := func(version string) mixAgreementClient {
		return mixAgreementClient{breakdown: mixRows(), cachedJSON: cachedExplanationJSON(t, 10.0), cachedAt: explainFixtureNow, dataVersion: version}
	}
	keyFor := func(version string) string {
		reader, _ := NewReader(row(version))
		themes, subcategories, err := reader.fetchMixDistributions(context.Background(), explainOpts())
		if err != nil {
			t.Fatal(err)
		}
		got, err := reader.FetchInvestmentDataVersion(context.Background(), "org-9555")
		if err != nil {
			t.Fatal(err)
		}
		k, _ := ComputeCacheKey(CacheKeyInput{Filters: mixCacheFilters(explainOpts(), themes, subcategories, got), OrgID: "org-9555"})
		return k
	}
	if keyFor("2026-10-13 10:00:00") == keyFor("2026-10-13 12:00:00") {
		t.Fatal("same mix, newer work units: the cache key must differ")
	}
}

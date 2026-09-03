// Package investmentexplain -- this file is explain_investment_mix's own
// orchestration/assembly layer (CHAOS-4977 step 6b): theme/subcategory
// validation, LLM availability, cache read, build_investment_response +
// build_work_unit_investments, local aggregation (band_counts,
// dominant_counts, quotes_by_subcategory, top_themes/top_subcategories/
// quality_drivers), payload/prompt construction, provider.Complete,
// strict-parse with its fallback construction, best-effort token-usage
// persist, and cache write.
package investmentexplain

import (
	"context"
	"errors"
	"math"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

// CompleteFunc is CompleteInvestmentMixExplanation's own signature,
// extracted as a type so ExplainInvestmentMix can take one as a
// parameter -- real callers (the eventual REST handler, step 5a) pass
// CompleteInvestmentMixExplanation itself; tests substitute a fake that
// returns a fixed, "recorded" completion, the seam
// categorize.NewProviderFromEnv("mock") cannot provide (MockProvider's
// real investment-mix-explanation response never satisfies the strict
// parser -- see provider_test.go -- so there is no way to exercise the
// VALID path through the real mock provider at all, by design).
type CompleteFunc func(ctx context.Context, requestedProvider, requestedModel, fullPrompt string) (result categorize.CompletionResult, resolvedProvider string, resolvedModel string, err error)

// ExplainInvestmentMixOptions bundles explain_investment_mix's parameters
// (investment_mix_explain.py:177-187).
//
// RepoIDs is the CALLER-resolved repo-id list -- resolve_repo_filter_ids
// (api/services/filtering.py:95-110) is NOT ported here, matching this
// port's existing boundary (BreakdownFilters.RepoIDs, reader.go): it
// needs its own ClickHouse (repo lookups) and Postgres (team->repo
// resolution) reads this package doesn't otherwise touch, and every
// reader in this package already treats repo-id resolution as the
// caller's job.
//
// FiltersForCacheKey is whatever filters.model_dump(mode="json") would
// have produced -- see ComputeCacheKey's own doc comment.
type ExplainInvestmentMixOptions struct {
	OrgID              string
	StartTS            time.Time
	EndTS              time.Time
	RepoIDs            []string
	WorkCategory       []string
	Theme              string
	Subcategory        string
	FiltersForCacheKey any
	LLMProvider        string
	LLMModel           string
	ForceRefresh       bool
	Now                time.Time
}

// ErrUnknownTheme/ErrUnknownSubcategory port explain_investment_mix's
// `raise ValueError("Unknown theme"/"Unknown subcategory")`.
var (
	ErrUnknownTheme       = errors.New("investmentexplain: unknown theme")
	ErrUnknownSubcategory = errors.New("investmentexplain: unknown subcategory")
)

// buildInvestmentBreakdownDistributions ports build_investment_response's
// local aggregation (services/investment.py:214-225) over
// fetch_investment_breakdown's rows -- theme_distribution and
// subcategory_distribution, in FIRST-SEEN order (the rows themselves are
// ORDER BY value DESC, so first-seen order is "highest single
// contribution first", not alphabetical or insertion-into-CH order).
func buildInvestmentBreakdownDistributions(rows []BreakdownRow) (themes, subcategories *orderedFloatMap) {
	themes = newOrderedFloatMap()
	subcategories = newOrderedFloatMap()
	for _, row := range rows {
		if row.Theme != "" && row.Value > 0 {
			themes.Add(row.Theme, row.Value)
		}
		if row.Subcategory != "" && strings.Contains(row.Subcategory, ".") && row.Value > 0 {
			subcategories.Add(row.Subcategory, row.Value)
		}
	}
	return themes, subcategories
}

// llmUnavailableExplanation ports explain_investment_mix's early-return
// InvestmentMixExplanation when is_llm_available is false
// (investment_mix_explain.py:204-218).
func llmUnavailableExplanation() InvestmentMixExplanation {
	status := "llm_unavailable"
	return InvestmentMixExplanation{
		Summary:     "",
		TopFindings: []InvestmentMixFinding{},
		Confidence: InvestmentMixConfidence{
			Level:   "unknown",
			BandMix: BandMix{},
			Drivers: []string{},
		},
		WhatToCheckNext: []InvestmentMixActionItem{},
		AntiClaims:      []string{},
		Status:          &status,
	}
}

// invalidLLMOutputExplanation ports explain_investment_mix's fallback
// InvestmentMixExplanation construction on a parse failure
// (investment_mix_explain.py:455-495).
func invalidLLMOutputExplanation(topThemes []keyValue, totalEffort float64, confidenceLevel string, qualityMean, qualityStddev *float64, bandCounts BandMix, qualityDrivers []string) InvestmentMixExplanation {
	fallbackFindings := make([]InvestmentMixFinding, 0, 2)
	for _, kv := range topThemes[:min(2, len(topThemes))] {
		pct := 0.0
		if totalEffort != 0 {
			pct = kv.Value / totalEffort * 100
		}
		theme := kv.Key
		fallbackFindings = append(fallbackFindings, InvestmentMixFinding{
			Finding: "Effort appears concentrated in " + theme + " (~" + formatPercentZeroDecimals(pct) + "% of total).",
			Evidence: InvestmentMixFindingEvidence{
				Theme:               theme,
				SharePct:            pct,
				EvidenceQualityMean: qualityMean,
			},
		})
	}
	status := "invalid_llm_output"
	return InvestmentMixExplanation{
		Summary:     "This mix suggests effort leans toward the leading themes shown, with subcategories providing the specific intent behind that allocation.",
		TopFindings: fallbackFindings,
		Confidence: InvestmentMixConfidence{
			Level:         confidenceLevel,
			QualityMean:   qualityMean,
			QualityStddev: qualityStddev,
			BandMix:       bandCounts,
			Drivers:       qualityDrivers,
		},
		WhatToCheckNext: []InvestmentMixActionItem{
			{
				Action: "Review the largest subcategories",
				Why:    "They drive the overall theme distribution",
				Where:  "Subcategory breakdown panel",
			},
		},
		AntiClaims: []string{
			"This does not measure individual productivity.",
			"This does not assign intent or correctness to any work.",
		},
		Status: &status,
	}
}

// ExplainInvestmentMix ports explain_investment_mix
// (investment_mix_explain.py:177-556) -- see this file's package doc
// comment for scope. record_explanation_parse/llm_call_metrics
// (telemetry-only, no output effect) are NOT ported, matching this
// package's treatment of record_stale_investment_membership_scope
// elsewhere.
//
// writer may be nil -- both the token-usage persist and the cache write
// are best-effort in Python (a 2s-timeout swallow-all for token usage;
// a bare try/except logger.debug for the cache write, investment_mix_
// explain.py:532-554), so a nil writer (or either write itself failing)
// degrades to "did not persist" rather than failing the request, exactly
// matching that swallow-everything contract.
func (reader *Reader) ExplainInvestmentMix(ctx context.Context, writer *CacheWriter, complete CompleteFunc, opts ExplainInvestmentMixOptions) (InvestmentMixExplanation, error) {
	theme := opts.Theme
	subcategory := opts.Subcategory
	if theme != "" && !units.IsTheme(theme) {
		return InvestmentMixExplanation{}, ErrUnknownTheme
	}
	if subcategory != "" && !units.IsSubcategory(subcategory) {
		return InvestmentMixExplanation{}, ErrUnknownSubcategory
	}
	if theme != "" && subcategory != "" && units.ThemeOf(subcategory) != theme {
		theme = units.ThemeOf(subcategory)
	}

	if !IsLLMAvailable(opts.LLMProvider, opts.OrgID) {
		return llmUnavailableExplanation(), nil
	}

	var themePtr, subcategoryPtr *string
	if theme != "" {
		themePtr = &theme
	}
	if subcategory != "" {
		subcategoryPtr = &subcategory
	}
	cacheKey, err := ComputeCacheKey(CacheKeyInput{
		Filters:     opts.FiltersForCacheKey,
		Theme:       themePtr,
		Subcategory: subcategoryPtr,
		OrgID:       opts.OrgID,
	})
	if err != nil {
		return InvestmentMixExplanation{}, err
	}

	if !opts.ForceRefresh && opts.LLMProvider != "mock" {
		cached, err := reader.ReadInvestmentExplanation(ctx, cacheKey, opts.OrgID)
		if err == nil && cached != nil {
			if explanation, decodeErr := DecodeInvestmentMixExplanation(cached.ExplanationJSON); decodeErr == nil {
				return explanation, nil
			}
		}
	}

	breakdownFilter := BreakdownFilters{
		OrgID:   opts.OrgID,
		StartTS: opts.StartTS,
		EndTS:   opts.EndTS,
		RepoIDs: opts.RepoIDs,
	}
	breakdownRows, err := reader.FetchInvestmentBreakdown(ctx, breakdownFilter)
	if err != nil {
		return InvestmentMixExplanation{}, err
	}
	themeDistribution, subcategoryDistribution := buildInvestmentBreakdownDistributions(breakdownRows)

	filteredSubcategoryItems := subcategoryDistribution.Items()
	if theme != "" {
		filtered := make([]keyValue, 0, len(filteredSubcategoryItems))
		for _, kv := range filteredSubcategoryItems {
			if strings.HasPrefix(kv.Key, theme+".") {
				filtered = append(filtered, kv)
			}
		}
		filteredSubcategoryItems = filtered
	}
	if subcategory != "" {
		filtered := make([]keyValue, 0, 1)
		for _, kv := range filteredSubcategoryItems {
			if kv.Key == subcategory {
				filtered = append(filtered, kv)
			}
		}
		filteredSubcategoryItems = filtered
	}

	themeFilters, subcategoryFilters := splitCategoryFilters(opts.WorkCategory)
	unitOpts := BuildWorkUnitInvestmentsOptions{
		OrgID:              opts.OrgID,
		StartTS:            opts.StartTS,
		EndTS:              opts.EndTS,
		RepoIDs:            opts.RepoIDs,
		Limit:              200,
		IncludeText:        true,
		ThemeFilters:       themeFilters,
		SubcategoryFilters: subcategoryFilters,
	}
	allUnits, err := reader.BuildWorkUnitInvestments(ctx, unitOpts)
	if err != nil {
		return InvestmentMixExplanation{}, err
	}

	filteredUnits := make([]WorkUnitInvestment, 0, len(allUnits))
	for _, unit := range allUnits {
		if theme != "" {
			if v, ok := keyValueGet(unit.Investment.Themes, theme); !ok || v <= 0 {
				continue
			}
		}
		if subcategory != "" {
			if v, ok := keyValueGet(unit.Investment.Subcategories, subcategory); !ok || v <= 0 {
				continue
			}
		}
		filteredUnits = append(filteredUnits, unit)
	}

	bandCounts := BandMix{}
	dominantCounts := newOrderedIntMap()
	quotesBySubcategory := map[string][]string{}

	for _, unit := range filteredUnits {
		band := "very_low"
		if unit.EvidenceQuality.Band != nil && *unit.EvidenceQuality.Band != "" {
			band = *unit.EvidenceQuality.Band
		}
		bandCounts = addBandCount(bandCounts, band)

		dominant, hasDominant := dominantSubcategory(unit.Investment.Subcategories)
		dominantKey := "unassigned"
		if hasDominant {
			dominantKey = dominant
			dominantCounts.Add(dominant, 1)
		}

		for _, entry := range unit.Evidence.Textual {
			quoteRaw, _ := entry["quote"].(string)
			quote := strings.TrimSpace(quoteRaw)
			if quote == "" {
				continue
			}
			quotesBySubcategory[dominantKey] = append(quotesBySubcategory[dominantKey], quote)
		}
	}

	topThemes := topItems(themeDistribution.Items(), 8)
	topSubcategories := topItems(filteredSubcategoryItems, 12)
	dominantCountItems := make([]keyValue, len(dominantCounts.keys))
	for i, key := range dominantCounts.keys {
		dominantCountItems[i] = keyValue{Key: key, Value: float64(dominantCounts.values[key])}
	}
	topCounts := topItems(dominantCountItems, 10)

	sampleQuotes := make([]sampleQuoteEntry, 0, 6)
	for _, kv := range topCounts[:min(6, len(topCounts))] {
		quotes := quotesBySubcategory[kv.Key]
		if len(quotes) > 3 {
			quotes = quotes[:3]
		}
		if len(quotes) > 0 {
			sampleQuotes = append(sampleQuotes, sampleQuoteEntry{Subcategory: kv.Key, Quotes: quotes})
		}
	}

	totalEffort := 0.0
	for _, kv := range themeDistribution.Items() {
		totalEffort += kv.Value
	}
	totalUnits := len(filteredUnits)

	qualityValues := make([]float64, 0, len(filteredUnits))
	for _, unit := range filteredUnits {
		if unit.EvidenceQuality.Value != nil {
			qualityValues = append(qualityValues, *unit.EvidenceQuality.Value)
		}
	}
	var qualityMean, qualityStddev *float64
	if len(qualityValues) > 0 {
		mean := sumFloat64(qualityValues) / float64(len(qualityValues))
		qualityMean = &mean
		if len(qualityValues) > 1 {
			variance := 0.0
			for _, v := range qualityValues {
				variance += (v - mean) * (v - mean)
			}
			variance /= float64(len(qualityValues))
			stddev := math.Sqrt(variance)
			qualityStddev = &stddev
		}
	}

	bandTotal := 0
	for _, bc := range bandCounts {
		bandTotal += bc.Count
	}
	unknownCount, _ := bandCounts.Get("unknown")
	lowCount, _ := bandCounts.Get("low")
	veryLowCount, _ := bandCounts.Get("very_low")
	var qualityDrivers []string
	if bandTotal > 0 {
		if float64(unknownCount)/float64(bandTotal) > 0.3 {
			qualityDrivers = append(qualityDrivers, "missing_evidence_metadata")
		}
		if float64(lowCount+veryLowCount)/float64(bandTotal) > 0.5 {
			qualityDrivers = append(qualityDrivers, "weak_cross_links")
		}
	}
	if qualityMean != nil && *qualityMean < 0.4 {
		qualityDrivers = append(qualityDrivers, "low_text_signal")
	}
	if qualityStddev != nil && *qualityStddev > 0.25 {
		qualityDrivers = append(qualityDrivers, "high_uncertainty_spread")
	}

	payload := buildExplainPayload(explainPayloadInput{
		Theme:            opts.Theme,
		Subcategory:      opts.Subcategory,
		TotalEffort:      totalEffort,
		TopThemes:        topThemes,
		TopSubcategories: topSubcategories,
		WorkUnitCount:    totalUnits,
		TopCounts:        topCounts,
		BandCounts:       bandCounts,
		QualityMean:      qualityMean,
		QualityStddev:    qualityStddev,
		QualityDrivers:   qualityDrivers,
		SampleQuotes:     sampleQuotes,
	})

	promptText := LoadPrompt()
	fullPrompt, err := BuildPrompt(promptText, payload)
	if err != nil {
		return InvestmentMixExplanation{}, err
	}

	completion, resolvedProvider, resolvedModel, err := complete(ctx, opts.LLMProvider, opts.LLMModel, fullPrompt)
	if err != nil {
		return InvestmentMixExplanation{}, err
	}

	if opts.LLMProvider != "mock" && writer != nil {
		completionModel := completion.Model
		if completionModel == "" {
			completionModel = resolvedModel
		}
		if record, ok := BuildLLMTokenUsageRecord(TokenUsageInput{
			OrgID:        opts.OrgID,
			Provider:     resolvedProvider,
			Model:        &completionModel,
			InputTokens:  completion.InputTokens,
			OutputTokens: completion.OutputTokens,
		}, opts.Now); ok {
			// Best-effort: a write failure here must not fail the request,
			// matching _persist_investment_mix_token_usage's own
			// swallow-everything contract (a real 2s asyncio.wait_for
			// timeout belongs at the CALLER's goroutine/context boundary,
			// not inside this synchronous aggregation function).
			_ = writer.WriteLLMTokenUsage(ctx, record)
		}
	}

	confidenceLevel := determineConfidenceLevel(qualityMean, qualityStddev)
	themeSharesPct := sharesPct(themeDistribution.Items(), totalEffort)
	subcategorySharesPct := sharesPct(filteredSubcategoryItems, totalEffort)

	fallbackBandMixMap := make(map[string]int, len(bandCounts))
	for _, bc := range bandCounts {
		fallbackBandMixMap[bc.Band] = bc.Count
	}

	parseResult := ParseInvestmentMixResponse(completion.Text, ParseOptions{
		ThemeSharesPct:       themeSharesPct,
		SubcategorySharesPct: subcategorySharesPct,
		FallbackLevel:        confidenceLevel,
		FallbackBandMix:      fallbackBandMixMap,
		FallbackDrivers:      qualityDrivers,
		FallbackMean:         qualityMean,
		FallbackStddev:       qualityStddev,
	})

	if parseResult.Output == nil {
		return invalidLLMOutputExplanation(topThemes, totalEffort, confidenceLevel, qualityMean, qualityStddev, bandCounts, qualityDrivers), nil
	}

	result := explainOutputToExplanation(*parseResult.Output)

	if opts.LLMProvider != "mock" && writer != nil {
		if explanationJSON, encodeErr := EncodeInvestmentMixExplanation(result); encodeErr == nil {
			// llm_provider/llm_model here are the RAW request parameters
			// (investment_mix_explain.py:544-545: `llm_provider=llm_provider,
			// llm_model=llm_model`), NOT resolved_llm_provider/
			// resolved_llm_model or completion.model -- confirmed against
			// the real function's own cache-write call, not assumed from
			// the earlier logging/token-usage call sites, which DO use
			// the resolved values and are a different pair of variables
			// entirely despite the similar names.
			var llmModel *string
			if opts.LLMModel != "" {
				llmModel = &opts.LLMModel
			}
			// Best-effort, same reasoning as the token-usage write above.
			_ = writer.WriteInvestmentExplanation(ctx, InvestmentExplanationRecord{
				CacheKey:        cacheKey,
				ExplanationJSON: explanationJSON,
				LLMProvider:     opts.LLMProvider,
				LLMModel:        llmModel,
				ComputedAt:      opts.Now,
				OrgID:           opts.OrgID,
			})
		}
	}

	return result, nil
}

func addBandCount(bandCounts BandMix, band string) BandMix {
	for i, bc := range bandCounts {
		if bc.Band == band {
			bandCounts[i].Count++
			return bandCounts
		}
	}
	return append(bandCounts, BandCount{Band: band, Count: 1})
}

func sumFloat64(values []float64) float64 {
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum
}

func sharesPct(items []keyValue, totalEffort float64) map[string]float64 {
	out := make(map[string]float64, len(items))
	for _, kv := range items {
		if totalEffort != 0 {
			out[kv.Key] = kv.Value / totalEffort * 100
		} else {
			out[kv.Key] = 0.0
		}
	}
	return out
}

func explainOutputToExplanation(output InvestmentMixExplainOutput) InvestmentMixExplanation {
	findings := make([]InvestmentMixFinding, len(output.TopFindings))
	for i, f := range output.TopFindings {
		findings[i] = InvestmentMixFinding{
			Finding: f.Finding,
			Evidence: InvestmentMixFindingEvidence{
				Theme:               f.Evidence.Theme,
				Subcategory:         f.Evidence.Subcategory,
				SharePct:            f.Evidence.SharePct,
				DeltaPctPoints:      f.Evidence.DeltaPctPoints,
				EvidenceQualityMean: f.Evidence.EvidenceQualityMean,
				EvidenceQualityBand: f.Evidence.EvidenceQualityBand,
			},
		}
	}
	actions := make([]InvestmentMixActionItem, len(output.WhatToCheckNext))
	for i, a := range output.WhatToCheckNext {
		actions[i] = InvestmentMixActionItem{Action: a.Action, Why: a.Why, Where: a.Where}
	}
	bandMix := make(BandMix, 0, len(output.Confidence.BandMix))
	for _, band := range []string{"high", "moderate", "low", "very_low", "unknown"} {
		if count, ok := output.Confidence.BandMix[band]; ok {
			bandMix = append(bandMix, BandCount{Band: band, Count: count})
		}
	}
	status := "valid"
	return InvestmentMixExplanation{
		Summary:     output.Summary,
		TopFindings: findings,
		Confidence: InvestmentMixConfidence{
			Level:         output.Confidence.Level,
			QualityMean:   output.Confidence.QualityMean,
			QualityStddev: output.Confidence.QualityStddev,
			BandMix:       bandMix,
			Drivers:       output.Confidence.Drivers,
		},
		WhatToCheckNext: actions,
		AntiClaims:      output.AntiClaims,
		Status:          &status,
	}
}

type orderedIntMap struct {
	keys   []string
	values map[string]int
}

func newOrderedIntMap() *orderedIntMap {
	return &orderedIntMap{values: map[string]int{}}
}

func (m *orderedIntMap) Add(key string, delta int) {
	if _, exists := m.values[key]; !exists {
		m.keys = append(m.keys, key)
	}
	m.values[key] += delta
}

type sampleQuoteEntry struct {
	Subcategory string
	Quotes      []string
}

type explainPayloadInput struct {
	Theme            string
	Subcategory      string
	TotalEffort      float64
	TopThemes        []keyValue
	TopSubcategories []keyValue
	WorkUnitCount    int
	TopCounts        []keyValue
	BandCounts       BandMix
	QualityMean      *float64
	QualityStddev    *float64
	QualityDrivers   []string
	SampleQuotes     []sampleQuoteEntry
}

// buildExplainPayload ports the `payload` dict construction
// (investment_mix_explain.py:345-373).
func buildExplainPayload(in explainPayloadInput) map[string]any {
	themeTop := make([]any, len(in.TopThemes))
	for i, kv := range in.TopThemes {
		pct := 0.0
		if in.TotalEffort != 0 {
			pct = kv.Value / in.TotalEffort
		}
		themeTop[i] = map[string]any{"theme": kv.Key, "value": kv.Value, "pct": pct}
	}
	subcategoryTop := make([]any, len(in.TopSubcategories))
	for i, kv := range in.TopSubcategories {
		pct := 0.0
		if in.TotalEffort != 0 {
			pct = kv.Value / in.TotalEffort
		}
		subcategoryTop[i] = map[string]any{"subcategory": kv.Key, "value": kv.Value, "pct": pct}
	}
	dominantTop := make([]any, len(in.TopCounts))
	for i, kv := range in.TopCounts {
		dominantTop[i] = map[string]any{"subcategory": kv.Key, "count": int(kv.Value)}
	}
	bandCounts := map[string]any{}
	for _, bc := range in.BandCounts {
		bandCounts[bc.Band] = bc.Count
	}
	sampleQuotes := make([]any, len(in.SampleQuotes))
	for i, entry := range in.SampleQuotes {
		quotes := make([]any, len(entry.Quotes))
		for j, q := range entry.Quotes {
			quotes[j] = q
		}
		sampleQuotes[i] = map[string]any{"subcategory": entry.Subcategory, "quotes": quotes}
	}
	qualityDrivers := make([]any, len(in.QualityDrivers))
	for i, d := range in.QualityDrivers {
		qualityDrivers[i] = d
	}

	var theme, subcategory any
	if in.Theme != "" {
		theme = in.Theme
	}
	if in.Subcategory != "" {
		subcategory = in.Subcategory
	}

	return map[string]any{
		"focus":                        map[string]any{"theme": theme, "subcategory": subcategory},
		"total_effort":                 in.TotalEffort,
		"theme_distribution_top":       themeTop,
		"subcategory_distribution_top": subcategoryTop,
		"work_unit_count":              in.WorkUnitCount,
		"work_unit_dominant_subcategory_counts_top": dominantTop,
		"evidence_quality_band_counts":              bandCounts,
		"evidence_quality_mean":                     optionalFloatToAny(in.QualityMean),
		"evidence_quality_stddev":                   optionalFloatToAny(in.QualityStddev),
		"quality_drivers":                           qualityDrivers,
		"evidence_quote_samples":                    sampleQuotes,
	}
}

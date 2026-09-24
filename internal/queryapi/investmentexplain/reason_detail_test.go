package investmentexplain

import (
	"encoding/json"
	"strings"
	"testing"
)

// buildValidExplainDoc returns a fresh, from-scratch map that
// ParseInvestmentMixResponse accepts as ParseStatusValid against
// reasonDetailTestOptions below -- every reason-detail test case starts
// from a fresh copy of this (never a shared one: maps are reference
// types, so a shared instance would let one case's mutation bleed into
// another's) and changes exactly one thing, so a failure can only be
// caused by the rule under test.
func buildValidExplainDoc() map[string]any {
	return map[string]any{
		"summary": "Effort this period leans toward delivery work across the team.",
		"top_findings": []any{
			map[string]any{
				"finding": "Delivery work makes up most of the observed effort",
				"evidence": map[string]any{
					"theme":                 "delivery",
					"subcategory":           "delivery.velocity",
					"share_pct":             40.0,
					"delta_pct_points":      nil,
					"evidence_quality_mean": nil,
					"evidence_quality_band": nil,
				},
			},
		},
		"confidence": map[string]any{
			"level":          "moderate",
			"quality_mean":   nil,
			"quality_stddev": nil,
			"band_mix": map[string]any{
				"high": 1, "moderate": 2, "low": 0, "very_low": 0, "unknown": 0,
			},
			"drivers": []any{},
		},
		"what_to_check_next": []any{
			map[string]any{
				"action": "Review the delivery subcategories",
				"why":    "They drive the overall split",
				"where":  "Breakdown panel",
			},
		},
		"anti_claims": []any{
			"This does not measure individual productivity.",
		},
	}
}

func reasonDetailTestOptions() ParseOptions {
	return ParseOptions{
		ThemeSharesPct:       map[string]float64{"delivery": 40.0},
		SubcategorySharesPct: map[string]float64{"delivery.velocity": 40.0},
		FallbackLevel:        "moderate",
		FallbackBandMix:      BandMix{},
	}
}

func docText(t *testing.T, doc map[string]any) string {
	t.Helper()
	encoded, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal test doc: %v", err)
	}
	return string(encoded)
}

// TestParseInvestmentMixResponseValidDocSanityCheck confirms
// buildValidExplainDoc actually parses as ParseStatusValid against
// reasonDetailTestOptions -- every other test in this file mutates a copy
// of it to break exactly one rule, so if the baseline itself doesn't
// parse clean, every "mutate one thing" case below would be meaningless.
func TestParseInvestmentMixResponseValidDocSanityCheck(t *testing.T) {
	result := ParseInvestmentMixResponse(docText(t, buildValidExplainDoc()), reasonDetailTestOptions())
	if result.Status != ParseStatusValid {
		t.Fatalf("status = %q, reason = %q (rule %q, path %q), want valid", result.Status, result.Reason, result.ReasonRule, result.ReasonPath)
	}
}

// TestParseInvestmentMixResponseReasonRuleNamesTheFailingRule is the
// red-first test for the actual telemetry gap: before ParseResult grew
// ReasonRule/ReasonPath/ReasonSnippet, a top_findings[i]/what_to_check_
// next[i]/confidence rejection collapsed to a bare "... failed
// validation" with no way to tell which of the many content rules
// actually fired, or on what value -- exactly the gap that made a real
// prod rejection (reason="top_findings[2] failed validation") useless
// for triage. Each case here mutates exactly one field of a document that
// otherwise parses clean and asserts ReasonRule/ReasonPath name that
// specific field/rule, and ReasonSnippet carries the offending value.
func TestParseInvestmentMixResponseReasonRuleNamesTheFailingRule(t *testing.T) {
	cases := []struct {
		name       string
		mutate     func(doc map[string]any)
		wantStatus ParseStatus
		wantRule   string
		wantPath   string
		wantHas    string // substring expected in ReasonSnippet
	}{
		{
			name:       "summary is not a string",
			mutate:     func(doc map[string]any) { doc["summary"] = 5 },
			wantStatus: ParseStatusInvalidLLMOutput,
			wantRule:   "summary_not_string",
			wantPath:   "summary",
		},
		{
			name:       "summary is blank",
			mutate:     func(doc map[string]any) { doc["summary"] = "   " },
			wantStatus: ParseStatusInvalidLLMOutput,
			wantRule:   "summary_blank",
			wantPath:   "summary",
		},
		{
			name:       "summary too long",
			mutate:     func(doc map[string]any) { doc["summary"] = strings.Repeat("a", 1001) },
			wantStatus: ParseStatusInvalidLLMOutput,
			wantRule:   "summary_too_long",
			wantPath:   "summary",
		},
		{
			name:       "summary contains a digit",
			mutate:     func(doc map[string]any) { doc["summary"] = "There were 3 themes this period" },
			wantStatus: ParseStatusInvalidLLMOutput,
			wantRule:   "summary_digit",
			wantPath:   "summary",
			wantHas:    "3",
		},
		{
			name:       "top_findings is not a list",
			mutate:     func(doc map[string]any) { doc["top_findings"] = "oops" },
			wantStatus: ParseStatusInvalidLLMOutput,
			wantRule:   "top_findings_not_list",
			wantPath:   "top_findings",
		},
		{
			name: "top_findings has more than 10 entries",
			mutate: func(doc map[string]any) {
				many := make([]any, 11)
				for i := range many {
					many[i] = (doc["top_findings"].([]any))[0]
				}
				doc["top_findings"] = many
			},
			wantStatus: ParseStatusInvalidLLMOutput,
			wantRule:   "top_findings_too_many",
			wantPath:   "top_findings",
		},
		{
			name: "confidence level invalid",
			mutate: func(doc map[string]any) {
				doc["confidence"].(map[string]any)["level"] = "extreme"
			},
			wantStatus: ParseStatusInvalidLLMOutput,
			wantRule:   "confidence_level_invalid",
			wantPath:   "confidence.level",
			wantHas:    "extreme",
		},
		{
			name: "confidence band_mix value invalid",
			mutate: func(doc map[string]any) {
				doc["confidence"].(map[string]any)["band_mix"].(map[string]any)["high"] = -1
			},
			wantStatus: ParseStatusInvalidLLMOutput,
			wantRule:   "confidence_band_mix_value_invalid",
			wantPath:   "confidence.band_mix.high",
		},
		{
			name: "confidence driver invalid",
			mutate: func(doc map[string]any) {
				doc["confidence"].(map[string]any)["drivers"] = []any{""}
			},
			wantStatus: ParseStatusInvalidLLMOutput,
			wantRule:   "confidence_driver_invalid",
			wantPath:   "confidence.drivers[0]",
		},
		{
			name: "finding is not an object",
			mutate: func(doc map[string]any) {
				doc["top_findings"] = []any{"oops"}
			},
			wantStatus: ParseStatusInvalidLLMOutput,
			wantRule:   "finding_not_object",
			wantPath:   "top_findings[0].$",
			wantHas:    "oops",
		},
		{
			name: "finding keys mismatch",
			mutate: func(doc map[string]any) {
				finding := (doc["top_findings"].([]any))[0].(map[string]any)
				finding["extra"] = "surprise"
			},
			wantStatus: ParseStatusInvalidLLMOutput,
			wantRule:   "finding_keys_mismatch",
			wantPath:   "top_findings[0].$",
		},
		{
			name: "finding text blank",
			mutate: func(doc map[string]any) {
				finding := (doc["top_findings"].([]any))[0].(map[string]any)
				finding["finding"] = "   "
			},
			wantStatus: ParseStatusInvalidLLMOutput,
			wantRule:   "finding_blank",
			wantPath:   "top_findings[0].finding",
		},
		{
			name: "finding text too long",
			mutate: func(doc map[string]any) {
				finding := (doc["top_findings"].([]any))[0].(map[string]any)
				finding["finding"] = strings.Repeat("a", 501)
			},
			wantStatus: ParseStatusInvalidLLMOutput,
			wantRule:   "finding_too_long",
			wantPath:   "top_findings[0].finding",
		},
		{
			name: "finding text contains a digit",
			mutate: func(doc map[string]any) {
				finding := (doc["top_findings"].([]any))[0].(map[string]any)
				finding["finding"] = "Delivery work is 2x the rest"
			},
			wantStatus: ParseStatusInvalidLLMOutput,
			wantRule:   "finding_digit",
			wantPath:   "top_findings[0].finding",
			wantHas:    "2x",
		},
		{
			name: "evidence is not an object",
			mutate: func(doc map[string]any) {
				finding := (doc["top_findings"].([]any))[0].(map[string]any)
				finding["evidence"] = "oops"
			},
			wantStatus: ParseStatusInvalidLLMOutput,
			wantRule:   "evidence_not_object",
			wantPath:   "top_findings[0].evidence",
		},
		{
			name: "evidence keys mismatch",
			mutate: func(doc map[string]any) {
				finding := (doc["top_findings"].([]any))[0].(map[string]any)
				evidence := finding["evidence"].(map[string]any)
				evidence["extra"] = "surprise"
			},
			wantStatus: ParseStatusInvalidLLMOutput,
			wantRule:   "evidence_keys_mismatch",
			wantPath:   "top_findings[0].evidence",
		},
		{
			name: "theme unknown",
			mutate: func(doc map[string]any) {
				finding := (doc["top_findings"].([]any))[0].(map[string]any)
				evidence := finding["evidence"].(map[string]any)
				evidence["theme"] = "not_a_real_theme"
				evidence["subcategory"] = nil
			},
			wantStatus: ParseStatusInvalidLLMOutput,
			wantRule:   "theme_unknown",
			wantPath:   "top_findings[0].evidence.theme",
			wantHas:    "not_a_real_theme",
		},
		{
			name: "subcategory unknown",
			mutate: func(doc map[string]any) {
				finding := (doc["top_findings"].([]any))[0].(map[string]any)
				evidence := finding["evidence"].(map[string]any)
				evidence["subcategory"] = "delivery.not_a_real_subcategory"
			},
			wantStatus: ParseStatusInvalidLLMOutput,
			wantRule:   "subcategory_unknown",
			wantPath:   "top_findings[0].evidence.subcategory",
		},
		{
			name: "subcategory invalid type",
			mutate: func(doc map[string]any) {
				finding := (doc["top_findings"].([]any))[0].(map[string]any)
				evidence := finding["evidence"].(map[string]any)
				evidence["subcategory"] = 5
			},
			wantStatus: ParseStatusInvalidLLMOutput,
			wantRule:   "subcategory_invalid_type",
			wantPath:   "top_findings[0].evidence.subcategory",
		},
		{
			name: "share_pct invalid",
			mutate: func(doc map[string]any) {
				finding := (doc["top_findings"].([]any))[0].(map[string]any)
				evidence := finding["evidence"].(map[string]any)
				evidence["share_pct"] = 150.0
			},
			wantStatus: ParseStatusInvalidLLMOutput,
			wantRule:   "share_pct_invalid",
			wantPath:   "top_findings[0].evidence.share_pct",
		},
		{
			name: "delta_pct_points invalid",
			mutate: func(doc map[string]any) {
				finding := (doc["top_findings"].([]any))[0].(map[string]any)
				evidence := finding["evidence"].(map[string]any)
				evidence["delta_pct_points"] = 500.0
			},
			wantStatus: ParseStatusInvalidLLMOutput,
			wantRule:   "delta_pct_points_invalid",
			wantPath:   "top_findings[0].evidence.delta_pct_points",
		},
		{
			name: "evidence_quality_mean invalid",
			mutate: func(doc map[string]any) {
				finding := (doc["top_findings"].([]any))[0].(map[string]any)
				evidence := finding["evidence"].(map[string]any)
				evidence["evidence_quality_mean"] = 5.0
			},
			wantStatus: ParseStatusInvalidLLMOutput,
			wantRule:   "evidence_quality_mean_invalid",
			wantPath:   "top_findings[0].evidence.evidence_quality_mean",
		},
		{
			name: "evidence_quality_band invalid",
			mutate: func(doc map[string]any) {
				finding := (doc["top_findings"].([]any))[0].(map[string]any)
				evidence := finding["evidence"].(map[string]any)
				evidence["evidence_quality_band"] = "sky_high"
			},
			wantStatus: ParseStatusInvalidLLMOutput,
			wantRule:   "evidence_quality_band_invalid",
			wantPath:   "top_findings[0].evidence.evidence_quality_band",
			wantHas:    "sky_high",
		},
		{
			name:       "what_to_check_next is not a list",
			mutate:     func(doc map[string]any) { doc["what_to_check_next"] = "oops" },
			wantStatus: ParseStatusInvalidLLMOutput,
			wantRule:   "what_to_check_next_not_list",
			wantPath:   "what_to_check_next",
		},
		{
			name: "action is not an object",
			mutate: func(doc map[string]any) {
				doc["what_to_check_next"] = []any{"oops"}
			},
			wantStatus: ParseStatusInvalidLLMOutput,
			wantRule:   "action_not_object",
			wantPath:   "what_to_check_next[0].$",
		},
		{
			name: "action field blank",
			mutate: func(doc map[string]any) {
				action := (doc["what_to_check_next"].([]any))[0].(map[string]any)
				action["why"] = "   "
			},
			wantStatus: ParseStatusInvalidLLMOutput,
			wantRule:   "action_field_blank",
			wantPath:   "what_to_check_next[0].why",
		},
		{
			name: "action field too long",
			mutate: func(doc map[string]any) {
				action := (doc["what_to_check_next"].([]any))[0].(map[string]any)
				action["action"] = strings.Repeat("a", 201)
			},
			wantStatus: ParseStatusInvalidLLMOutput,
			wantRule:   "action_field_too_long",
			wantPath:   "what_to_check_next[0].action",
		},
		{
			name: "action field digit",
			mutate: func(doc map[string]any) {
				action := (doc["what_to_check_next"].([]any))[0].(map[string]any)
				action["where"] = "Panel 2"
			},
			wantStatus: ParseStatusInvalidLLMOutput,
			wantRule:   "action_field_digit",
			wantPath:   "what_to_check_next[0].where",
		},
		{
			name:       "anti_claims is not a list",
			mutate:     func(doc map[string]any) { doc["anti_claims"] = "oops" },
			wantStatus: ParseStatusInvalidLLMOutput,
			wantRule:   "anti_claims_not_list",
			wantPath:   "anti_claims",
		},
		{
			name:       "anti_claim not a string",
			mutate:     func(doc map[string]any) { doc["anti_claims"] = []any{5} },
			wantStatus: ParseStatusInvalidLLMOutput,
			wantRule:   "anti_claim_invalid",
			wantPath:   "anti_claims[0]",
		},
		{
			name: "anti_claim contains a digit after stripping",
			mutate: func(doc map[string]any) {
				doc["anti_claims"] = []any{"This covers 2 quarters of work."}
			},
			wantStatus: ParseStatusInvalidLLMOutput,
			wantRule:   "anti_claim_digit",
			wantPath:   "anti_claims[0]",
			wantHas:    "2",
		},
		{
			name: "forbidden language in the narrative",
			mutate: func(doc map[string]any) {
				doc["summary"] = "This is the leading theme this period."
			},
			wantStatus: ParseStatusForbiddenLanguage,
			wantRule:   "forbidden_language:is",
			wantPath:   "narrative",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			doc := buildValidExplainDoc()
			tc.mutate(doc)
			result := ParseInvestmentMixResponse(docText(t, doc), reasonDetailTestOptions())
			if result.Status != tc.wantStatus {
				t.Fatalf("status = %q, want %q (reason %q, rule %q)", result.Status, tc.wantStatus, result.Reason, result.ReasonRule)
			}
			if result.ReasonRule != tc.wantRule {
				t.Fatalf("rule = %q, want %q (reason %q)", result.ReasonRule, tc.wantRule, result.Reason)
			}
			if result.ReasonPath != tc.wantPath {
				t.Fatalf("path = %q, want %q", result.ReasonPath, tc.wantPath)
			}
			if tc.wantHas != "" && !strings.Contains(result.ReasonSnippet, tc.wantHas) {
				t.Fatalf("snippet = %q, want it to contain %q", result.ReasonSnippet, tc.wantHas)
			}
		})
	}
}

// TestParseInvestmentMixResponseSubcategoryThemeMismatchRule exercises the
// one rule that needs a second theme to construct a genuine mismatch
// (the base fixture's finding.theme and evidence.subcategory already
// agree, by construction) -- kept separate from the table above rather
// than forcing that shared fixture to carry a second theme every other
// case doesn't need.
func TestParseInvestmentMixResponseSubcategoryThemeMismatchRule(t *testing.T) {
	doc := buildValidExplainDoc()
	finding := (doc["top_findings"].([]any))[0].(map[string]any)
	evidence := finding["evidence"].(map[string]any)
	evidence["theme"] = "quality"
	evidence["subcategory"] = "delivery.velocity"

	opts := reasonDetailTestOptions()
	opts.ThemeSharesPct["quality"] = 10.0

	result := ParseInvestmentMixResponse(docText(t, doc), opts)
	if result.Status != ParseStatusInvalidLLMOutput {
		t.Fatalf("status = %q, want %q (reason %q)", result.Status, ParseStatusInvalidLLMOutput, result.Reason)
	}
	if result.ReasonRule != "subcategory_theme_mismatch" {
		t.Fatalf("rule = %q, want subcategory_theme_mismatch", result.ReasonRule)
	}
	if result.ReasonPath != "top_findings[0].evidence.subcategory" {
		t.Fatalf("path = %q, want top_findings[0].evidence.subcategory", result.ReasonPath)
	}
}

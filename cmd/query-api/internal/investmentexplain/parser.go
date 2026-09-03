package investmentexplain

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// extractJSONObject ports llm/json_utils.py's extract_json_object
// (find the first "{" and the last "}" in the trimmed text, parse that
// slice, require the result to be a JSON object). Decoded via
// json.Decoder.UseNumber() so every number leaf comes back as
// json.Number rather than float64 -- see isPythonInt's doc comment for
// why that distinction matters to valid_confidence's band_mix check.
//
// KNOWN DIVERGENCE, deliberately not closed: CPython's json.loads accepts
// bare NaN/Infinity/-Infinity tokens by default (parse_constant), so
// {"share_pct": NaN} parses successfully in Python and is THEN rejected by
// finite_number's math.isfinite check -- reaching invalid_llm_output.
// encoding/json rejects those tokens as a syntax error outright, reaching
// invalid_json instead. Both paths reject the response; only the specific
// ParseStatus differs, in a case that requires an LLM to emit an
// non-JSON-standard bare token. Not worth a hand-rolled permissive
// tokenizer to preserve one enum value in an already-rejected response.
func extractJSONObject(text string) (map[string]any, bool) {
	if pythonparity.Strip(text) == "" {
		return nil, false
	}
	candidate := pythonparity.Strip(text)
	start := strings.Index(candidate, "{")
	end := strings.LastIndex(candidate, "}")
	if start == -1 || end == -1 || end < start {
		return nil, false
	}
	jsonStr := candidate[start : end+1]

	decoder := json.NewDecoder(bytes.NewReader([]byte(jsonStr)))
	decoder.UseNumber()
	var parsed any
	if err := decoder.Decode(&parsed); err != nil {
		return nil, false
	}
	obj, ok := parsed.(map[string]any)
	return obj, ok
}

// ParseInvestmentMixResponse ports parse_investment_mix_response
// (investment_mix_parser.py:43-133) exactly, field order and all.
func ParseInvestmentMixResponse(text string, opts ParseOptions) ParseResult {
	parsed, ok := extractJSONObject(text)
	if !ok {
		return ParseResult{Status: ParseStatusInvalidJSON}
	}
	if !exactKeySetFromSet(parsed, topLevelKeys) {
		return ParseResult{Status: ParseStatusInvalidLLMOutput}
	}

	summary, summaryOK := parsed["summary"].(string)
	rawFindings, findingsOK := parsed["top_findings"].([]any)
	rawActions, actionsOK := parsed["what_to_check_next"].([]any)
	rawAntiClaims, antiClaimsIsList := parsed["anti_claims"].([]any)

	if !summaryOK || pythonparity.Strip(summary) == "" || pythonLen(summary) > 1000 || hasUnicodeDigit(summary) {
		return ParseResult{Status: ParseStatusInvalidLLMOutput}
	}
	if !findingsOK || len(rawFindings) > 10 || !validConfidence(parsed["confidence"]) {
		return ParseResult{Status: ParseStatusInvalidLLMOutput}
	}

	findingOpts := parseFindingOptions{
		themeSharesPct:       opts.ThemeSharesPct,
		subcategorySharesPct: opts.SubcategorySharesPct,
		qualityMean:          opts.FallbackMean,
		qualityBand:          opts.FallbackQualityBand,
	}
	findings := make([]Finding, 0, len(rawFindings))
	for _, item := range rawFindings {
		finding, ok := parseFinding(item, findingOpts)
		if !ok {
			return ParseResult{Status: ParseStatusInvalidLLMOutput}
		}
		findings = append(findings, finding)
	}
	if !actionsOK || len(rawActions) > 10 {
		return ParseResult{Status: ParseStatusInvalidLLMOutput}
	}

	actions := make([]ActionItem, 0, len(rawActions))
	for _, item := range rawActions {
		action, ok := parseAction(item)
		if !ok {
			return ParseResult{Status: ParseStatusInvalidLLMOutput}
		}
		actions = append(actions, action)
	}

	if !antiClaimsIsList {
		return ParseResult{Status: ParseStatusInvalidLLMOutput}
	}
	for _, claim := range rawAntiClaims {
		claimString, isString := claim.(string)
		if !isString || pythonLen(claimString) > 300 {
			return ParseResult{Status: ParseStatusInvalidLLMOutput}
		}
	}
	antiClaims := stringList(parsed["anti_claims"])
	if len(rawAntiClaims) > 10 {
		return ParseResult{Status: ParseStatusInvalidLLMOutput}
	}
	for _, claim := range antiClaims {
		if pythonLen(claim) > 300 || hasUnicodeDigit(claim) {
			return ParseResult{Status: ParseStatusInvalidLLMOutput}
		}
	}
	if len(antiClaims) != len(rawAntiClaims) {
		return ParseResult{Status: ParseStatusInvalidLLMOutput}
	}

	output := InvestmentMixExplainOutput{
		Summary:     pythonparity.Strip(summary),
		TopFindings: findings,
		Confidence: Confidence{
			Level:         opts.FallbackLevel,
			QualityMean:   opts.FallbackMean,
			QualityStddev: opts.FallbackStddev,
			BandMix:       cloneBandMix(opts.FallbackBandMix),
			Drivers:       cloneStrings(opts.FallbackDrivers),
		},
		WhatToCheckNext: actions,
		AntiClaims:      antiClaims,
	}

	narrative := make([]string, 0, 2+len(findings)*1+len(actions)*3+len(antiClaims))
	narrative = append(narrative, output.Summary)
	for _, finding := range findings {
		narrative = append(narrative, finding.Finding)
	}
	for _, action := range actions {
		narrative = append(narrative, action.Action, action.Why, action.Where)
	}
	narrative = append(narrative, antiClaims...)
	if containsForbiddenLanguage(strings.Join(narrative, " ")) {
		return ParseResult{Status: ParseStatusForbiddenLanguage}
	}

	return ParseResult{Status: ParseStatusValid, Output: &output}
}

// ParseOptions bundles parse_investment_mix_response's keyword-only
// parameters (investment_mix_parser.py:43-54).
type ParseOptions struct {
	ThemeSharesPct       map[string]float64
	SubcategorySharesPct map[string]float64
	FallbackLevel        string // defaults to "unknown", matching Python's default
	FallbackQualityBand  *string
	FallbackBandMix      map[string]int
	FallbackDrivers      []string
	FallbackMean         *float64
	FallbackStddev       *float64
}

func cloneBandMix(m map[string]int) map[string]int {
	if len(m) == 0 {
		return map[string]int{}
	}
	out := make(map[string]int, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func cloneStrings(s []string) []string {
	if len(s) == 0 {
		return []string{}
	}
	out := make([]string, len(s))
	copy(out, s)
	return out
}

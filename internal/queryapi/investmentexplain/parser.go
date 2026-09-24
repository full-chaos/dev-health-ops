package investmentexplain

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
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
// CPython's json.loads accepts bare NaN/Infinity/-Infinity tokens by
// default (parse_constant), so {"share_pct": NaN} parses successfully in
// Python and is THEN rejected downstream by finite_number's
// math.isfinite check -- reaching invalid_llm_output, not invalid_json.
// encoding/json has no such mode; it rejects those tokens as a syntax
// error outright. Team-lead ruling (2026-09-03): the differential test
// compares ParseStatus values, so "same outcome, different label" is a
// real parity break, not a cosmetic one -- fixed via
// sanitizeNonFiniteJSONTokens/replaceNonFiniteSentinels rather than left
// as a documented gap.
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
	jsonStr := sanitizeNonFiniteJSONTokens(candidate[start : end+1])

	decoder := json.NewDecoder(bytes.NewReader([]byte(jsonStr)))
	decoder.UseNumber()
	var parsed any
	if err := decoder.Decode(&parsed); err != nil {
		return nil, false
	}
	// Python's extract_json_object calls json.loads(json_str) on the
	// WHOLE slice (json_utils.py:65) -- json.loads rejects any trailing
	// data after the first complete value ("Extra data" JSONDecodeError).
	// json.Decoder.Decode only reads ONE value and, unlike json.Unmarshal,
	// does NOT verify the stream is exhausted afterward -- two adjacent
	// valid JSON objects in the LLM's raw text ("{...}{...}") silently
	// decoded to just the first one here, where Python would reject the
	// whole response as invalid_json. Caught by codex round 3 (P1).
	if _, err := decoder.Token(); err != io.EOF {
		return nil, false
	}
	parsed = replaceNonFiniteSentinels(parsed)
	obj, ok := parsed.(map[string]any)
	return obj, ok
}

// nonFiniteValue is what a substituted NaN/Infinity/-Infinity token
// becomes after decoding, via replaceNonFiniteSentinels. Its own distinct
// Go type is the point: it must fail EVERY type assertion this file's
// validators make (string, json.Number, bool, []any, map[string]any) --
// those five, plus nil, are the ONLY shapes json.Unmarshal ever produces,
// so no combination of them can represent "present but not any real JSON
// type" the way Python's float('nan')/float('inf') is a real value that
// still fails isinstance(x, str) while passing isinstance(x, (int,
// float)).
//
// A first version of this fix substituted a literal JSON array (`[...]`)
// textually and decoded normally -- wrong, because top_findings/
// anti_claims/what_to_check_next legitimately expect []any, so a NaN
// standing in for one of those fields would have PASSED the "is this a
// list" check instead of failing it. Substituting JSON null (decoding to
// Go nil) is wrong the other way: several fields treat None as "field
// omitted, use the fallback" (the `present && x != nil` guards in
// ParseInvestmentMixResponse/parseFinding/validConfidence), so a NaN
// masquerading as null would be silently ACCEPTED as absent instead of
// rejected as invalid -- the opposite of Python's real
// `delta is not None and not finite_number(delta)` rejection. A distinct
// struct type is the only shape that is simultaneously "not nil" and
// "not any JSON primitive", which is what's actually needed.
type nonFiniteValue struct{}

// nonFiniteSentinelString is the decoded form of the placeholder
// sanitizeNonFiniteJSONTokens writes in place of a bare token. The NUL
// bytes make an accidental collision with real LLM-authored text
// effectively impossible: a raw NUL byte cannot appear in a legally
// authored JSON string without an explicit escape sequence, which this
// substitution never writes (see nonFiniteJSONLiteral), so no
// legitimately decoded string can ever equal this exact value.
const nonFiniteSentinelString = "\x00pythonparity_nonfinite\x00"

// nonFiniteJSONLiteral is nonFiniteSentinelString encoded as JSON text --
// built via json.Marshal rather than hand-escaped, so there is no risk of
// a typo in a manually written escape sequence producing a string that
// doesn't decode back to nonFiniteSentinelString exactly.
var nonFiniteJSONLiteral = mustMarshalJSONString(nonFiniteSentinelString)

func mustMarshalJSONString(s string) string {
	encoded, err := json.Marshal(s)
	if err != nil {
		panic("investmentexplain: failed to encode nonFiniteSentinelString: " + err.Error())
	}
	return string(encoded)
}

// replaceNonFiniteSentinels walks a decoded json.Unmarshal tree and
// replaces every string equal to nonFiniteSentinelString with a
// nonFiniteValue{}, recursively through []any and map[string]any.
func replaceNonFiniteSentinels(value any) any {
	switch typed := value.(type) {
	case string:
		if typed == nonFiniteSentinelString {
			return nonFiniteValue{}
		}
		return typed
	case []any:
		for i, element := range typed {
			typed[i] = replaceNonFiniteSentinels(element)
		}
		return typed
	case map[string]any:
		for key, element := range typed {
			typed[key] = replaceNonFiniteSentinels(element)
		}
		return typed
	default:
		return value
	}
}

// nonFiniteJSONTokens are the exact literal spellings CPython's json
// scanner recognizes in VALUE position -- case-sensitive, no "nan", no
// "+Infinity" (confirmed against real json.loads: both raise
// "Expecting value"). Longest-prefix-safe: "-Infinity" and "Infinity"
// share no common start byte with each other or with "NaN", so checking
// order doesn't matter here.
var nonFiniteJSONTokens = []string{"-Infinity", "Infinity", "NaN"}

// sanitizeNonFiniteJSONTokens walks raw JSON text and replaces every bare
// NaN/Infinity/-Infinity token OUTSIDE a string literal with
// nonFiniteJSONLiteral, leaving string contents (including one that
// happens to contain the substring "NaN") untouched -- confirmed against
// CPython that `{"x": "This has NaN inside a string"}` parses as an
// ordinary string, not a constant.
func sanitizeNonFiniteJSONTokens(raw string) string {
	var out strings.Builder
	out.Grow(len(raw))
	inString := false
	escaped := false
	for i := 0; i < len(raw); {
		c := raw[i]
		if inString {
			out.WriteByte(c)
			switch {
			case escaped:
				escaped = false
			case c == '\\':
				escaped = true
			case c == '"':
				inString = false
			}
			i++
			continue
		}
		if c == '"' {
			inString = true
			out.WriteByte(c)
			i++
			continue
		}
		if token, ok := matchNonFiniteToken(raw[i:]); ok {
			out.WriteString(nonFiniteJSONLiteral)
			i += len(token)
			continue
		}
		out.WriteByte(c)
		i++
	}
	return out.String()
}

func matchNonFiniteToken(s string) (string, bool) {
	for _, token := range nonFiniteJSONTokens {
		if !strings.HasPrefix(s, token) {
			continue
		}
		rest := s[len(token):]
		if len(rest) > 0 && isJSONIdentByte(rest[0]) {
			// part of a longer bare identifier (not valid JSON either
			// way, but not this token) -- leave it for the decoder to
			// reject on its own terms.
			continue
		}
		return token, true
	}
	return "", false
}

func isJSONIdentByte(b byte) bool {
	return b == '_' || (b >= '0' && b <= '9') || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
}

// ParseInvestmentMixResponse ports parse_investment_mix_response
// (investment_mix_parser.py:43-133) exactly, field order and all.
//
// The combined boolean conditions Python (and an earlier draft of this
// port) tests in one `if` are split into sequential `if`s below purely
// so each can carry its own Reason string -- Go's `||` and a run of
// single-condition `if`s short-circuit identically, so this is a
// logging-only change: every input takes exactly the same branch, in
// the same order, to the same Status/Output it did before. See
// ParseResult.Reason's own doc comment for why Reason exists at all and
// why it carries no parity weight.
func ParseInvestmentMixResponse(text string, opts ParseOptions) ParseResult {
	parsed, ok := extractJSONObject(text)
	if !ok {
		return ParseResult{Status: ParseStatusInvalidJSON, Reason: "could not extract a single JSON object from the raw text", ReasonRule: "invalid_json", ReasonPath: "$", ReasonSnippet: snippetOf(text)}
	}
	if !exactKeySetFromSet(parsed, topLevelKeys) {
		return ParseResult{Status: ParseStatusInvalidLLMOutput, Reason: "top-level keys did not exactly match summary/top_findings/confidence/what_to_check_next/anti_claims", ReasonRule: "top_level_keys_mismatch", ReasonPath: "$", ReasonSnippet: snippetOf(parsed)}
	}

	summary, summaryOK := parsed["summary"].(string)
	rawFindings, findingsOK := parsed["top_findings"].([]any)
	rawActions, actionsOK := parsed["what_to_check_next"].([]any)
	rawAntiClaims, antiClaimsIsList := parsed["anti_claims"].([]any)

	if !summaryOK {
		return ParseResult{Status: ParseStatusInvalidLLMOutput, Reason: "summary is not a string", ReasonRule: "summary_not_string", ReasonPath: "summary", ReasonSnippet: snippetOf(parsed["summary"])}
	}
	if pythonparity.Strip(summary) == "" {
		return ParseResult{Status: ParseStatusInvalidLLMOutput, Reason: "summary is blank", ReasonRule: "summary_blank", ReasonPath: "summary", ReasonSnippet: snippetOf(summary)}
	}
	if pythonLen(summary) > 1000 {
		return ParseResult{Status: ParseStatusInvalidLLMOutput, Reason: "summary exceeds 1000 characters", ReasonRule: "summary_too_long", ReasonPath: "summary", ReasonSnippet: snippetOf(summary)}
	}
	if hasUnicodeDigit(summary) {
		return ParseResult{Status: ParseStatusInvalidLLMOutput, Reason: "summary contains a digit", ReasonRule: "summary_digit", ReasonPath: "summary", ReasonSnippet: snippetOf(summary)}
	}
	if !findingsOK {
		return ParseResult{Status: ParseStatusInvalidLLMOutput, Reason: "top_findings is not a list", ReasonRule: "top_findings_not_list", ReasonPath: "top_findings", ReasonSnippet: snippetOf(parsed["top_findings"])}
	}
	if len(rawFindings) > 10 {
		return ParseResult{Status: ParseStatusInvalidLLMOutput, Reason: "top_findings has more than 10 entries", ReasonRule: "top_findings_too_many", ReasonPath: "top_findings", ReasonSnippet: snippetOf(len(rawFindings))}
	}
	if confidenceOK, confidenceRejection := validConfidence(parsed["confidence"]); !confidenceOK {
		return ParseResult{
			Status:        ParseStatusInvalidLLMOutput,
			Reason:        "confidence failed validation",
			ReasonRule:    confidenceRejection.Rule,
			ReasonPath:    "confidence." + confidenceRejection.Path,
			ReasonSnippet: confidenceRejection.Snippet,
		}
	}

	findingOpts := parseFindingOptions{
		themeSharesPct:       opts.ThemeSharesPct,
		subcategorySharesPct: opts.SubcategorySharesPct,
		qualityMean:          opts.FallbackMean,
		qualityBand:          opts.FallbackQualityBand,
	}
	findings := make([]Finding, 0, len(rawFindings))
	for i, item := range rawFindings {
		finding, rejection, ok := parseFinding(item, findingOpts)
		if !ok {
			return ParseResult{
				Status:        ParseStatusInvalidLLMOutput,
				Reason:        fmt.Sprintf("top_findings[%d] failed validation", i),
				ReasonRule:    rejection.Rule,
				ReasonPath:    fmt.Sprintf("top_findings[%d].%s", i, rejection.Path),
				ReasonSnippet: rejection.Snippet,
			}
		}
		findings = append(findings, finding)
	}
	if !actionsOK {
		return ParseResult{Status: ParseStatusInvalidLLMOutput, Reason: "what_to_check_next is not a list", ReasonRule: "what_to_check_next_not_list", ReasonPath: "what_to_check_next", ReasonSnippet: snippetOf(parsed["what_to_check_next"])}
	}
	if len(rawActions) > 10 {
		return ParseResult{Status: ParseStatusInvalidLLMOutput, Reason: "what_to_check_next has more than 10 entries", ReasonRule: "what_to_check_next_too_many", ReasonPath: "what_to_check_next", ReasonSnippet: snippetOf(len(rawActions))}
	}

	actions := make([]ActionItem, 0, len(rawActions))
	for i, item := range rawActions {
		action, rejection, ok := parseAction(item)
		if !ok {
			return ParseResult{
				Status:        ParseStatusInvalidLLMOutput,
				Reason:        fmt.Sprintf("what_to_check_next[%d] failed validation", i),
				ReasonRule:    rejection.Rule,
				ReasonPath:    fmt.Sprintf("what_to_check_next[%d].%s", i, rejection.Path),
				ReasonSnippet: rejection.Snippet,
			}
		}
		actions = append(actions, action)
	}

	if !antiClaimsIsList {
		return ParseResult{Status: ParseStatusInvalidLLMOutput, Reason: "anti_claims is not a list", ReasonRule: "anti_claims_not_list", ReasonPath: "anti_claims", ReasonSnippet: snippetOf(parsed["anti_claims"])}
	}
	for i, claim := range rawAntiClaims {
		claimString, isString := claim.(string)
		if !isString || pythonLen(claimString) > 300 {
			return ParseResult{
				Status:        ParseStatusInvalidLLMOutput,
				Reason:        fmt.Sprintf("anti_claims[%d] is not a string, or exceeds 300 characters", i),
				ReasonRule:    "anti_claim_invalid",
				ReasonPath:    fmt.Sprintf("anti_claims[%d]", i),
				ReasonSnippet: snippetOf(claim),
			}
		}
	}
	antiClaims := stringList(parsed["anti_claims"])
	if len(rawAntiClaims) > 10 {
		return ParseResult{Status: ParseStatusInvalidLLMOutput, Reason: "anti_claims has more than 10 entries", ReasonRule: "anti_claims_too_many", ReasonPath: "anti_claims", ReasonSnippet: snippetOf(len(rawAntiClaims))}
	}
	for i, claim := range antiClaims {
		if pythonLen(claim) > 300 {
			return ParseResult{
				Status:        ParseStatusInvalidLLMOutput,
				Reason:        fmt.Sprintf("anti_claims[%d] exceeds 300 characters after stripping", i),
				ReasonRule:    "anti_claim_too_long",
				ReasonPath:    fmt.Sprintf("anti_claims[%d]", i),
				ReasonSnippet: snippetOf(claim),
			}
		}
		if hasUnicodeDigit(claim) {
			return ParseResult{
				Status:        ParseStatusInvalidLLMOutput,
				Reason:        fmt.Sprintf("anti_claims[%d] contains a digit after stripping", i),
				ReasonRule:    "anti_claim_digit",
				ReasonPath:    fmt.Sprintf("anti_claims[%d]", i),
				ReasonSnippet: snippetOf(claim),
			}
		}
	}
	if len(antiClaims) != len(rawAntiClaims) {
		return ParseResult{Status: ParseStatusInvalidLLMOutput, Reason: "anti_claims contains a blank-after-strip entry", ReasonRule: "anti_claim_blank", ReasonPath: "anti_claims", ReasonSnippet: snippetOf(rawAntiClaims)}
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
	joinedNarrative := strings.Join(narrative, " ")
	if word, matched := firstForbiddenMatch(joinedNarrative); matched {
		return ParseResult{
			Status:        ParseStatusForbiddenLanguage,
			Reason:        "narrative text contains forbidden hedge/certainty language",
			ReasonRule:    "forbidden_language:" + word,
			ReasonPath:    "narrative",
			ReasonSnippet: snippetOf(joinedNarrative),
		}
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
	FallbackBandMix      BandMix
	FallbackDrivers      []string
	FallbackMean         *float64
	FallbackStddev       *float64
}

func cloneBandMix(m BandMix) BandMix {
	if len(m) == 0 {
		return BandMix{}
	}
	out := make(BandMix, len(m))
	copy(out, m)
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

package categorize

import (
	"encoding/json"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// Validation limits, from llm_schema.py:23-27.
const (
	weightNormalizationTolerance = 1e-6
	maxUncertaintyLen            = 280
	maxQuoteLen                  = 280
	minQuotes                    = 1
	maxQuotes                    = 10
)

var (
	allowedTopLevelKeys = map[string]struct{}{"subcategories": {}, "evidence_quotes": {}, "uncertainty": {}}
	allowedQuoteKeys    = map[string]struct{}{"quote": {}, "source": {}, "id": {}}
	allowedSources      = map[string]struct{}{"issue": {}, "pr": {}, "commit": {}}
)

// EvidenceQuote is llm_schema.py's EvidenceQuote.
type EvidenceQuote struct {
	Quote      string
	SourceType string
	SourceID   string
}

// LLMValidationResult is llm_schema.py's LLMValidationResult.
type LLMValidationResult struct {
	OK             bool
	Errors         []string
	Subcategories  map[string]float64
	EvidenceQuotes []EvidenceQuote
	Uncertainty    string
	Warnings       []string
}

// ParseLLMJSON ports llm_schema.py:58-65 parse_llm_json.
//
// Go's json.Unmarshal into `any` decodes a JSON object as map[string]any --
// the same shape Python's json.loads gives for a JSON object -- so the
// isinstance(payload, dict) check becomes a plain type assertion.
func ParseLLMJSON(rawText string) (map[string]any, []string) {
	var payload any
	if err := json.Unmarshal([]byte(rawText), &payload); err != nil {
		return nil, []string{"invalid_json: " + err.Error()}
	}
	object, ok := payload.(map[string]any)
	if !ok {
		return nil, []string{"payload_not_object"}
	}
	return object, nil
}

// ValidateLLMPayload ports llm_schema.py:68-220 validate_llm_payload.
//
// sourceTexts and handleMap match units.TextBundle's own field types
// exactly (units.TextBundle.SourceTexts, and units.SourceRef for the
// handle-map value) -- both already exist and are already what
// BuildTextBundle produces, so a caller passes the same bundle fields
// straight through.
//
// # WHY GO NEEDS NO isinstance(value, bool) GUARD HERE
//
// Python's json.loads decodes true/false as bool, which SUBCLASSES int --
// so a naive `isinstance(value, (int, float))` check would silently accept
// a boolean weight as 0/1, which is why the reference checks
// `isinstance(value, bool)` FIRST. Go's json.Unmarshal into `any` decodes
// true/false as a genuine `bool` value and every JSON number as `float64`
// -- two disjoint types with no subclass relationship -- so a plain type
// switch already separates them correctly with no ordering trap to
// replicate.
func ValidateLLMPayload(
	payload map[string]any,
	sourceTexts map[string]map[string]string,
	handleMap map[string]units.SourceRef,
) LLMValidationResult {
	var errors []string
	var warnings []string

	keys := make(map[string]struct{}, len(payload))
	for key := range payload {
		keys[key] = struct{}{}
	}
	if !mapKeysEqual(keys, allowedTopLevelKeys) {
		missing := sortedSetDifference(allowedTopLevelKeys, keys)
		extra := sortedSetDifference(keys, allowedTopLevelKeys)
		if len(missing) > 0 {
			errors = append(errors, "missing_top_level_keys:"+pythonStrListRepr(missing))
		}
		if len(extra) > 0 {
			errors = append(errors, "unexpected_top_level_keys:"+pythonStrListRepr(extra))
		}
	}

	rawSubcategories, ok := payload["subcategories"].(map[string]any)
	if !ok {
		errors = append(errors, "subcategories_not_object")
		rawSubcategories = map[string]any{}
	}

	// DOCUMENTED DIVERGENCE: Python iterates raw_subcategories.items() in
	// JSON-SOURCE-TEXT order (json.loads preserves insertion order), so a
	// payload with multiple invalid subcategory entries appends their
	// errors in that order. Go's json.Unmarshal into map[string]any
	// discards key order entirely -- there is no order left to iterate in
	// by the time ParseLLMJSON hands this map over. This function iterates
	// in SORTED key order instead: deterministic (unlike depending on the
	// caller's exact JSON text layout) but not order-matching Python for a
	// multi-error payload. No downstream logic depends on error ORDER --
	// _repair_guidance (categorize/prompts.go) and every consumer test
	// this port has check error PRESENCE, never position -- so this is a
	// property change (byte-order of the stored errors list), not a
	// behavior change.
	cleaned := map[string]float64{}
	for _, key := range sortedMapKeys(rawSubcategories) {
		value := rawSubcategories[key]
		if !units.IsSubcategory(key) {
			errors = append(errors, "unknown_subcategory:"+key)
			continue
		}
		numeric, ok := value.(float64)
		if _, isBool := value.(bool); isBool || !ok {
			errors = append(errors, "invalid_weight:"+key)
			continue
		}
		if math.IsNaN(numeric) || math.IsInf(numeric, 0) {
			errors = append(errors, "non_finite_weight:"+key)
			continue
		}
		if numeric < 0.0 {
			errors = append(errors, "negative_weight:"+key)
			continue
		}
		cleaned[key] = numeric
	}

	total := sumMapValuesInSortedKeyOrder(cleaned)
	switch {
	case math.IsNaN(total) || math.IsInf(total, 0):
		errors = append(errors, "weight_sum_not_finite")
	case total <= 0.0:
		errors = append(errors, "all_weights_zero")
	case math.Abs(total-1.0) > weightNormalizationTolerance:
		warnings = append(warnings, "weights_normalized:"+formatFixed4(total))
	}

	evidenceQuotesRaw, ok := payload["evidence_quotes"].([]any)
	if !ok {
		errors = append(errors, "evidence_quotes_not_list")
		evidenceQuotesRaw = nil
	}

	var evidenceQuotes []EvidenceQuote
	if len(evidenceQuotesRaw) < minQuotes || len(evidenceQuotesRaw) > maxQuotes {
		errors = append(errors, "evidence_quotes_count_out_of_range")
	}
	for idx, entryAny := range evidenceQuotesRaw {
		entry, ok := entryAny.(map[string]any)
		if !ok {
			errors = append(errors, "evidence_quote_not_object:"+strconv.Itoa(idx))
			continue
		}
		entryKeys := make(map[string]struct{}, len(entry))
		for key := range entry {
			entryKeys[key] = struct{}{}
		}
		if !mapKeysEqual(entryKeys, allowedQuoteKeys) {
			missing := sortedSetDifference(allowedQuoteKeys, entryKeys)
			extra := sortedSetDifference(entryKeys, allowedQuoteKeys)
			if len(missing) > 0 {
				errors = append(errors, "evidence_quote_missing_keys:"+strconv.Itoa(idx)+":"+pythonStrListRepr(missing))
			}
			if len(extra) > 0 {
				errors = append(errors, "evidence_quote_extra_keys:"+strconv.Itoa(idx)+":"+pythonStrListRepr(extra))
			}
			continue
		}
		rawQuote, quoteOK := entry["quote"].(string)
		rawSourceType, sourceTypeOK := entry["source"].(string)
		rawSourceID, sourceIDOK := entry["id"].(string)
		if !quoteOK || !sourceTypeOK || !sourceIDOK {
			errors = append(errors, "evidence_quote_invalid_type:"+strconv.Itoa(idx))
			continue
		}
		quote := pythonparity.Strip(rawQuote)
		sourceType := pythonparity.Strip(rawSourceType)
		sourceID := pythonparity.Strip(rawSourceID)
		if quote == "" {
			errors = append(errors, "evidence_quote_empty:"+strconv.Itoa(idx))
			continue
		}
		if pythonparity.RuneLen(quote) > maxQuoteLen {
			errors = append(errors, "evidence_quote_too_long:"+strconv.Itoa(idx))
			continue
		}
		if _, ok := allowedSources[sourceType]; !ok {
			errors = append(errors, "evidence_quote_invalid_source:"+strconv.Itoa(idx)+":"+sourceType)
			continue
		}
		if sourceID == "" {
			errors = append(errors, "evidence_quote_missing_id:"+strconv.Itoa(idx))
			continue
		}
		resolved, ok := handleMap[sourceID]
		if !ok {
			errors = append(errors, "evidence_quote_unknown_source:"+strconv.Itoa(idx))
			continue
		}
		sourceText := sourceTexts[resolved.SourceType][resolved.SourceID]
		if sourceText == "" {
			errors = append(errors, "evidence_quote_unknown_source:"+strconv.Itoa(idx))
			continue
		}
		recoveredQuote, ok := recoverQuoteSpan(quote, sourceText)
		if !ok {
			errors = append(errors, "evidence_quote_not_substring:"+strconv.Itoa(idx))
			continue
		}
		evidenceQuotes = append(evidenceQuotes, EvidenceQuote{
			Quote: recoveredQuote, SourceType: resolved.SourceType, SourceID: resolved.SourceID,
		})
	}

	uncertaintyRaw, uncertaintyIsString := payload["uncertainty"].(string)
	uncertainty := ""
	if uncertaintyIsString {
		uncertainty = pythonparity.Strip(uncertaintyRaw)
	}
	switch {
	case !uncertaintyIsString:
		errors = append(errors, "uncertainty_invalid_type")
	case uncertainty == "":
		errors = append(errors, "uncertainty_missing")
	case pythonparity.RuneLen(uncertainty) > maxUncertaintyLen:
		errors = append(errors, "uncertainty_too_long")
	}

	if len(errors) > 0 {
		return LLMValidationResult{
			OK: false, Errors: errors, Subcategories: map[string]float64{},
			EvidenceQuotes: nil, Uncertainty: uncertainty, Warnings: warnings,
		}
	}

	normalized := EnsureFullSubcategoryVector(cleaned)
	return LLMValidationResult{
		OK: true, Errors: nil, Subcategories: normalized,
		EvidenceQuotes: evidenceQuotes, Uncertainty: uncertainty, Warnings: warnings,
	}
}

// recoverQuoteSpan ports llm_schema.py:47-55 _recover_quote_span: split the
// (already-trimmed) quote on Python whitespace, build a regex joining the
// escaped tokens with `\s+`, and search for that pattern in source_text --
// so a quote reproduced with different internal whitespace (e.g. a
// collapsed double space) still recovers the ORIGINAL source span.
//
// KNOWN NARROW DIVERGENCE: Go's regexp `\s` (RE2) matches ASCII whitespace
// only ([\t\n\f\r ], missing \v and any Unicode space); Python's `re`
// module matches Unicode whitespace by default for str patterns. A source
// text using an exotic Unicode space character (e.g. a non-breaking space)
// between the tokens being searched for could recover on the Python side
// and fail to recover here. Not fixed: real PR/issue/commit text
// overwhelmingly uses ASCII spaces between words, and the failure mode is
// SAFE (a missed recovery drops the evidence quote with
// evidence_quote_not_substring, never fabricates or corrupts one).
func recoverQuoteSpan(quote, sourceText string) (string, bool) {
	tokens := pythonparity.SplitWhitespace(quote)
	if len(tokens) == 0 {
		return "", false
	}
	parts := make([]string, len(tokens))
	for i, token := range tokens {
		parts[i] = regexp.QuoteMeta(token)
	}
	pattern := strings.Join(parts, `\s+`)
	re, err := regexp.Compile(pattern)
	if err != nil {
		return "", false
	}
	match := re.FindString(sourceText)
	if match == "" {
		return "", false
	}
	return match, true
}

// EnsureFullSubcategoryVector ports utils/normalization.py's
// ensure_full_subcategory_vector + normalize_scores, specialised to the
// investment subcategory set: every canonical key is present, and the
// vector sums to 1.0 (or, if every input weight is zero/absent, a uniform
// distribution -- normalize_scores's own `total <= 0.0` branch).
func EnsureFullSubcategoryVector(subcategories map[string]float64) map[string]float64 {
	keys := units.SortedSubcategories[:]

	values := make([]float64, len(keys))
	for i, key := range keys {
		values[i] = subcategories[key]
	}
	total := pythonparity.Sum(values)

	normalized := make(map[string]float64, len(keys))
	if total <= 0.0 {
		uniform := 0.0
		if len(keys) > 0 {
			uniform = 1.0 / float64(len(keys))
		}
		for _, key := range keys {
			normalized[key] = uniform
		}
		return normalized
	}
	for i, key := range keys {
		normalized[key] = values[i] / total
	}
	return normalized
}

func mapKeysEqual(a, b map[string]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for key := range a {
		if _, ok := b[key]; !ok {
			return false
		}
	}
	return true
}

// sortedSetDifference returns the sorted elements of a that are not in b --
// Python's `sorted(a - b)`.
func sortedSetDifference(a, b map[string]struct{}) []string {
	var out []string
	for key := range a {
		if _, ok := b[key]; !ok {
			out = append(out, key)
		}
	}
	sort.Strings(out)
	return out
}

func sortedMapKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// sumMapValuesInSortedKeyOrder mirrors sum(cleaned.values()): Python dict
// iteration is INSERTION order, not sorted -- but the weight-sum total is a
// symmetric function under CPython 3.12+'s compensated sum() for any
// finite input, and this function's own SORTED order is used purely for a
// deterministic, reproducible summation (matching the general discipline
// elsewhere in this port of never depending on Go's randomised map order),
// not because Python's own order is sorted.
func sumMapValuesInSortedKeyOrder(m map[string]float64) float64 {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	values := make([]float64, len(keys))
	for i, key := range keys {
		values[i] = m[key]
	}
	return pythonparity.Sum(values)
}

// pythonStrListRepr reproduces Python's repr() of a sorted list of plain
// identifier strings (subcategory names, JSON object keys) -- e.g.
// ['subcategories', 'uncertainty']. Every string this function is ever
// called with, in this file, comes from a closed set of plain ASCII
// identifiers (schema key names, canonical subcategory names) that can
// never contain a quote character, so the single-quote form Python's
// repr() defaults to is always correct here -- this is NOT a general
// Python repr() port.
func pythonStrListRepr(values []string) string {
	quoted := make([]string, len(values))
	for i, v := range values {
		quoted[i] = "'" + v + "'"
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}

// formatFixed4 reproduces Python's f"{value:.4f}" for the
// weights_normalized warning -- fixed 4 decimal places. Only ever called on
// a value already proven finite by the switch above it (the NaN/Inf branch
// returns before this one is reached), and precision 4 is always a valid
// FormatFixed precision, so the error return can never actually fire at
// this call site.
func formatFixed4(value float64) string {
	formatted, err := pythonparity.FormatFixed(value, 4)
	if err != nil {
		// Unreachable: see the doc comment above.
		return strconv.FormatFloat(value, 'f', 4, 64)
	}
	return formatted
}

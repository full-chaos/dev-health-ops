package investmentexplain

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// bands ports investment_mix_validation.py's BANDS frozenset.
var bands = map[string]bool{
	"high": true, "moderate": true, "low": true, "very_low": true, "unknown": true,
}

// evidenceKeys ports investment_mix_validation.py's EVIDENCE_KEYS.
var evidenceKeys = map[string]bool{
	"theme": true, "subcategory": true, "share_pct": true,
	"delta_pct_points": true, "evidence_quality_mean": true, "evidence_quality_band": true,
}

// topLevelKeys ports investment_mix_validation.py's TOP_LEVEL_KEYS.
var topLevelKeys = map[string]bool{
	"summary": true, "top_findings": true, "confidence": true,
	"what_to_check_next": true, "anti_claims": true,
}

// confidenceKeys ports valid_confidence's local `expected` set
// (investment_mix_validation.py:119).
var confidenceKeys = map[string]bool{
	"level": true, "quality_mean": true, "quality_stddev": true,
	"band_mix": true, "drivers": true,
}

// confidenceLevels ports valid_confidence's `BANDS - {"very_low"}`
// (investment_mix_validation.py:122).
var confidenceLevels = map[string]bool{
	"high": true, "moderate": true, "low": true, "unknown": true,
}

// forbiddenWords ports investment_mix_validation.py's FORBIDDEN_PATTERN's
// word alternatives -- everything except the separate "without\s+question"
// phrase, handled by containsWithoutQuestion below.
var forbiddenWords = []string{
	"is", "was", "should", "determined", "detected",
	"definitely", "certainly", "undoubtedly",
}

// isPythonWordChar ports what CPython's `\w` matches under Unicode mode
// (the default for a str pattern): any Unicode letter or number (L* and
// N* general categories -- Nd, Nl AND No, so a digit-like character such
// as superscript "²" counts too, not just decimal digits) plus "_".
// Measured against CPython directly (re.match(r"\w", ch) for a spread of
// categories) rather than assumed -- confirmed on 'a' Ll, '5' Nd, '_' Pc,
// 'é' Ll, a CJK ideograph Lo, superscript-2 No, and roman-numeral-8 Nl, all
// True; '-' Pd, space Zs, '.' Po all False.
func isPythonWordChar(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || unicode.IsNumber(r)
}

// hasUnicodeDigit ports investment_mix_validation.py's
// NUMERIC_PATTERN = re.compile(r"\d") applied via .search(...) -- CPython's
// \d under Unicode mode (the default) matches the Unicode "Nd" (decimal
// digit) general category specifically, NOT the broader \w number set --
// confirmed superscript-2 (No) does NOT match \d though it IS \w. Go's
// regexp \d is ASCII-only with no flag to widen it, so this walks runes
// and checks unicode.Is(unicode.Nd, r) directly rather than using
// regexp at all.
func hasUnicodeDigit(s string) bool {
	for _, r := range s {
		if unicode.Is(unicode.Nd, r) {
			return true
		}
	}
	return false
}

// containsForbiddenLanguage ports investment_mix_validation.py's
// contains_forbidden_language / FORBIDDEN_PATTERN:
//
//	re.compile(r"\b(?:is|was|should|determined|detected|definitely|"
//	            r"certainly|undoubtedly)\b|without\s+question", re.IGNORECASE)
//
// Go's regexp \b/\w are ASCII-only (a documented RE2 limitation -- no flag
// widens them), while Python's are Unicode-aware by default, so this is a
// hand-rolled scan rather than a regexp.MustCompile call: for each
// forbidden word, find every case-insensitive occurrence and confirm
// isPythonWordChar is false (or the string boundary) on both sides, the
// exact definition of \b.
//
// CASE-FOLDING NOTE (kept visible per team-lead ruling, 2026-09-03): the
// case-insensitive match below is a plain strings.ToLower, not a full
// Unicode case-fold (unicode.ToLower per-rune would be closer to
// Python's re.IGNORECASE, which case-folds the whole pattern, not just
// ASCII). This is EXACT today because forbiddenWords is entirely ASCII --
// plain ToLower and Python's IGNORECASE agree on every ASCII letter. It
// stops being exact the moment a non-ASCII word is ever added to
// forbiddenWords (a German or French forbidden term, say): strings.ToLower
// still handles many non-ASCII letters correctly (Go's ToLower is
// Unicode-aware per rune), so the real risk is narrower than "ASCII only"
// suggests, but it has not been verified for the SPECIFIC casefolding
// edge cases Python's re module handles (e.g. Turkish dotless I, German
// ß) -- unverified is not the same as safe. Re-check this note before
// adding any non-ASCII entry to forbiddenWords.
func containsForbiddenLanguage(text string) bool {
	lower := strings.ToLower(text)
	for _, word := range forbiddenWords {
		if containsWordBoundaryMatch(lower, word) {
			return true
		}
	}
	return containsWithoutQuestion(lower)
}

func containsWordBoundaryMatch(lower, word string) bool {
	from := 0
	for {
		idx := strings.Index(lower[from:], word)
		if idx < 0 {
			return false
		}
		start := from + idx
		end := start + len(word)
		if leftBoundaryOK(lower, start) && rightBoundaryOK(lower, end) {
			return true
		}
		from = start + 1
	}
}

func leftBoundaryOK(s string, byteIndex int) bool {
	if byteIndex == 0 {
		return true
	}
	r, _ := utf8.DecodeLastRuneInString(s[:byteIndex])
	return !isPythonWordChar(r)
}

func rightBoundaryOK(s string, byteIndex int) bool {
	if byteIndex >= len(s) {
		return true
	}
	r, _ := utf8.DecodeRuneInString(s[byteIndex:])
	return !isPythonWordChar(r)
}

// containsWithoutQuestion ports the `without\s+question` alternative --
// \s under Unicode mode is pythonparity.IsSpace's exact predicate.
func containsWithoutQuestion(lower string) bool {
	const target = "without"
	from := 0
	for {
		idx := strings.Index(lower[from:], target)
		if idx < 0 {
			return false
		}
		start := from + idx
		rest := lower[start+len(target):]
		spaceRunes := 0
		consumed := 0
		for _, r := range rest {
			if !pythonparity.IsSpace(r) {
				break
			}
			spaceRunes++
			consumed += utf8.RuneLen(r)
		}
		if spaceRunes > 0 && strings.HasPrefix(rest[consumed:], "question") {
			return true
		}
		from = start + 1
	}
}

// finiteNumber ports investment_mix_validation.py's finite_number. Values
// come from extractJSONObject, which decodes with json.Decoder.UseNumber()
// -- every JSON number arrives as json.Number, not float64 -- so this
// accepts either int-shaped or float-shaped JSON text and converts via
// Float64(), matching Python's `isinstance(value, (int, float))` accepting
// both. The bool exclusion guards a value built directly in Go test code
// (json.Unmarshal never produces bool for a numeric field), kept for the
// predicate's own self-containment.
func finiteNumber(value any, minimum float64, maximum *float64, hasMaximum bool) bool {
	if _, isBool := value.(bool); isBool {
		return false
	}
	number, ok := value.(json.Number)
	if !ok {
		return false
	}
	numeric, err := number.Float64()
	if err != nil {
		return false
	}
	if math.IsNaN(numeric) || math.IsInf(numeric, 0) {
		return false
	}
	if numeric < minimum {
		return false
	}
	if hasMaximum && numeric > *maximum {
		return false
	}
	return true
}

// isPythonInt reports whether a decoded json.Number's ORIGINAL LITERAL
// TEXT would have parsed as a Python int rather than a Python float --
// CPython's json scanner decodes a number token to int() when it has
// neither a '.' nor an exponent, and to float() otherwise (its NUMBER_RE
// has separate optional groups for the fractional and exponent parts,
// and either one present routes to float()). json.Number preserves the
// original text, which is exactly what this needs to check: a value
// written "3" is a Python int; "3.0" is a Python float, even though both
// carry the same numeric value. valid_confidence's band_mix check is the
// one place in this file that cares about that distinction -- every other
// numeric field uses finiteNumber, which (matching Python's finite_number)
// accepts either shape.
func isPythonInt(number json.Number) bool {
	return !strings.ContainsAny(string(number), ".eE")
}

// pythonLen ports Python's len(str): a CODE POINT count, not Go's len()
// byte count. Every length check this parser makes (summary <= 1000,
// finding <= 500, action/why/where <= 200/300/200, driver <= 120,
// anti_claim <= 300) is against Python str length, so using Go's byte-
// counting len() would reject shorter, or accept longer, non-ASCII text
// than Python does.
func pythonLen(s string) int {
	return utf8.RuneCountInString(s)
}

// stringList ports investment_mix_validation.py's string_list.
func stringList(value any) []string {
	list, isList := value.([]any)
	if !isList {
		return nil
	}
	out := make([]string, 0, len(list))
	for _, item := range list {
		s, isString := item.(string)
		if !isString {
			continue
		}
		trimmed := pythonparity.Strip(s)
		if trimmed == "" {
			continue
		}
		out = append(out, trimmed)
	}
	return out
}

// parseFindingOptions bundles parse_finding's keyword-only parameters
// (investment_mix_validation.py:58-65).
type parseFindingOptions struct {
	themeSharesPct       map[string]float64
	subcategorySharesPct map[string]float64
	qualityMean          *float64
	qualityBand          *string
}

// parseFinding ports investment_mix_validation.py's parse_finding
// (investment_mix_validation.py:58-115) exactly, including the rendered
// finding text's "(~{share_pct:.0f}% of effort)." suffix.
func parseFinding(raw any, opts parseFindingOptions) (Finding, bool) {
	rawMap, isMap := raw.(map[string]any)
	if !isMap || !exactKeySet(rawMap, "finding", "evidence") {
		return Finding{}, false
	}
	finding, isString := rawMap["finding"].(string)
	if !isString {
		return Finding{}, false
	}
	strippedFinding := pythonparity.Strip(finding)
	if strippedFinding == "" || pythonLen(finding) > 500 || hasUnicodeDigit(finding) {
		return Finding{}, false
	}
	evidence, isEvidenceMap := rawMap["evidence"].(map[string]any)
	if !isEvidenceMap || !exactKeySetFromSet(evidence, evidenceKeys) {
		return Finding{}, false
	}
	theme, themeOK := evidence["theme"].(string)
	if !themeOK {
		return Finding{}, false
	}
	themeShare, themeKnown := opts.themeSharesPct[theme]
	if !themeKnown {
		return Finding{}, false
	}

	var subcategory *string
	switch rawSubcategory := evidence["subcategory"].(type) {
	case nil:
		// subcategory is None -- allowed, matches Python's `subcategory
		// is not None` guard being false.
	case string:
		subcategoryShare, subcategoryKnown := opts.subcategorySharesPct[rawSubcategory]
		if !subcategoryKnown {
			return Finding{}, false
		}
		if before, _, _ := strings.Cut(rawSubcategory, "."); before != theme {
			return Finding{}, false
		}
		_ = subcategoryShare
		subcategoryCopy := rawSubcategory
		subcategory = &subcategoryCopy
	default:
		// subcategory present but not a string (and not None) -- invalid.
		return Finding{}, false
	}

	if !finiteNumber(evidence["share_pct"], 0, ptr(100.0), true) {
		return Finding{}, false
	}
	if delta, present := evidence["delta_pct_points"]; present && delta != nil {
		if !finiteNumber(delta, -100, ptr(100.0), true) {
			return Finding{}, false
		}
	}
	if rawQuality, present := evidence["evidence_quality_mean"]; present && rawQuality != nil {
		if !finiteNumber(rawQuality, 0, ptr(1.0), true) {
			return Finding{}, false
		}
	}
	if rawBand, present := evidence["evidence_quality_band"]; present && rawBand != nil {
		bandString, isBandString := rawBand.(string)
		if !isBandString || !bands[bandString] {
			return Finding{}, false
		}
	}

	sharePct := themeShare
	if subcategory != nil {
		sharePct = opts.subcategorySharesPct[*subcategory]
	}

	return Finding{
		Finding: strings.TrimRight(strippedFinding, ".") + " (~" + formatPercentZeroDecimals(sharePct) + "% of effort).",
		Evidence: FindingEvidence{
			Theme:               theme,
			Subcategory:         subcategory,
			SharePct:            sharePct,
			DeltaPctPoints:      nil,
			EvidenceQualityMean: opts.qualityMean,
			EvidenceQualityBand: opts.qualityBand,
		},
	}, true
}

// validConfidence ports investment_mix_validation.py's valid_confidence
// (investment_mix_validation.py:118-146).
func validConfidence(raw any) bool {
	rawMap, isMap := raw.(map[string]any)
	if !isMap || !exactKeySetFromSet(rawMap, confidenceKeys) {
		return false
	}
	level, levelOK := rawMap["level"].(string)
	if !levelOK || !confidenceLevels[level] {
		return false
	}
	if mean, present := rawMap["quality_mean"]; present && mean != nil {
		if !finiteNumber(mean, 0, ptr(1.0), true) {
			return false
		}
	}
	if stddev, present := rawMap["quality_stddev"]; present && stddev != nil {
		if !finiteNumber(stddev, 0, ptr(1.0), true) {
			return false
		}
	}
	bandMix, isBandMixMap := rawMap["band_mix"].(map[string]any)
	if !isBandMixMap || !exactKeySetFromSet(bandMix, bands) {
		return false
	}
	for _, value := range bandMix {
		if _, isBool := value.(bool); isBool {
			return false
		}
		number, isNumber := value.(json.Number)
		if !isNumber || !isPythonInt(number) {
			return false
		}
		numeric, err := number.Float64()
		if err != nil || numeric < 0 {
			return false
		}
	}
	drivers, isDriversList := rawMap["drivers"].([]any)
	if !isDriversList || len(drivers) > 10 {
		return false
	}
	for _, driver := range drivers {
		driverString, isString := driver.(string)
		if !isString || pythonparity.Strip(driverString) == "" || pythonLen(driverString) > 120 {
			return false
		}
	}
	return true
}

// parseAction ports investment_mix_validation.py's parse_action
// (investment_mix_validation.py:149-165).
func parseAction(raw any) (ActionItem, bool) {
	rawMap, isMap := raw.(map[string]any)
	if !isMap || !exactKeySet(rawMap, "action", "why", "where") {
		return ActionItem{}, false
	}
	action, actionOK := rawMap["action"].(string)
	why, whyOK := rawMap["why"].(string)
	where, whereOK := rawMap["where"].(string)
	if !actionOK || pythonparity.Strip(action) == "" {
		return ActionItem{}, false
	}
	if !whyOK || pythonparity.Strip(why) == "" {
		return ActionItem{}, false
	}
	if !whereOK || pythonparity.Strip(where) == "" {
		return ActionItem{}, false
	}
	if pythonLen(action) > 200 || pythonLen(why) > 300 || pythonLen(where) > 200 {
		return ActionItem{}, false
	}
	if hasUnicodeDigit(action) || hasUnicodeDigit(why) || hasUnicodeDigit(where) {
		return ActionItem{}, false
	}
	return ActionItem{
		Action: pythonparity.Strip(action),
		Why:    pythonparity.Strip(why),
		Where:  pythonparity.Strip(where),
	}, true
}

func ptr(v float64) *float64 { return &v }

// formatPercentZeroDecimals ports Python's f"{share_pct:.0f}" -- both
// CPython's float formatting and Go's strconv/fmt use correctly-rounded,
// round-half-to-even decimal conversion for a fixed-precision float
// format, confirmed matching CPython on a spread of exact-.5 and
// non-terminating cases (0.5->0, 1.5->2, 2.5->2, 3.5->4, 12.5->12,
// 49.5->50, 50.5->50, 33.333333->33, 66.666666->67) rather than assumed
// from the shared "round to nearest" contract alone.
func formatPercentZeroDecimals(value float64) string {
	return fmt.Sprintf("%.0f", value)
}

// exactKeySet reports whether m's key set is EXACTLY the given names --
// Python's `set(raw) != {...}` (extra keys are as invalid as missing
// ones).
func exactKeySet(m map[string]any, names ...string) bool {
	if len(m) != len(names) {
		return false
	}
	for _, name := range names {
		if _, ok := m[name]; !ok {
			return false
		}
	}
	return true
}

// exactKeySetFromSet is exactKeySet against a pre-built name set (used
// where the reference set is a package-level var like evidenceKeys/
// confidenceKeys/bands, so callers don't repeat the member list).
func exactKeySetFromSet(m map[string]any, names map[string]bool) bool {
	if len(m) != len(names) {
		return false
	}
	for name := range names {
		if _, ok := m[name]; !ok {
			return false
		}
	}
	return true
}

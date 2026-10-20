package investmentexplain

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
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

// reasonSnippetHeadRunes caps snippetOf's output: a rejection detail's snippet
// targets one single offending field, not the entire response.
const reasonSnippetHeadRunes = 80

// validationRejection is the structured companion to the plain bool
// parseFinding/parseAction/validConfidence returned before this fix: Rule
// names WHICH specific check failed, Path locates the offending field
// relative to the item being validated (e.g. "evidence.theme", to be
// prefixed by the caller with "top_findings[i]."), and Snippet is the
// REDACTED head of the offending raw value. See ParseResult's own doc
// comment (types.go) for why this carries no parity weight.
type validationRejection struct {
	Rule    string
	Path    string
	Snippet string
}

// snippetOf renders value -- whatever shape a rejected JSON field held:
// string, json.Number, bool, nil, a nested list/map, or the nonFiniteValue
// sentinel -- as a short REDACTED head for a log line. A real string is
// used as-is; everything else falls back to fmt.Sprint so a non-string
// offender (e.g. top_findings[2].evidence.theme holding the number 5)
// still shows something rather than silently vanishing from the log.
func snippetOf(value any) string {
	s, isString := value.(string)
	if !isString {
		s = fmt.Sprint(value)
	}
	return redactedOutputHead(s, reasonSnippetHeadRunes)
}

// parseFindingOptions bundles parse_finding's keyword-only parameters
// (investment_mix_validation.py:58-65).
type parseFindingOptions struct {
	themeSharesPct       map[string]float64
	subcategorySharesPct map[string]float64
	shareNoun            string
	qualityMean          *float64
	qualityBand          *string
}

// parseFinding ports investment_mix_validation.py's parse_finding
// (investment_mix_validation.py:58-115) exactly, including the rendered
// finding text's "(~{share_pct:.0f}% of effort)." suffix. The bool return
// is unchanged parity behavior; validationRejection is the Go-only
// addition (see its own doc comment) describing WHY a false came back.
func parseFinding(raw any, opts parseFindingOptions) (Finding, validationRejection, bool) {
	rawMap, isMap := raw.(map[string]any)
	if !isMap {
		return Finding{}, validationRejection{Rule: "finding_not_object", Path: "$", Snippet: snippetOf(raw)}, false
	}
	if !exactKeySet(rawMap, "finding", "evidence") {
		return Finding{}, validationRejection{Rule: "finding_keys_mismatch", Path: "$", Snippet: snippetOf(rawMap)}, false
	}
	finding, isString := rawMap["finding"].(string)
	if !isString {
		return Finding{}, validationRejection{Rule: "finding_not_string", Path: "finding", Snippet: snippetOf(rawMap["finding"])}, false
	}
	strippedFinding := pythonparity.Strip(finding)
	if strippedFinding == "" {
		return Finding{}, validationRejection{Rule: "finding_blank", Path: "finding", Snippet: snippetOf(finding)}, false
	}
	if pythonLen(finding) > 500 {
		return Finding{}, validationRejection{Rule: "finding_too_long", Path: "finding", Snippet: snippetOf(finding)}, false
	}
	if hasUnicodeDigit(finding) {
		return Finding{}, validationRejection{Rule: "finding_digit", Path: "finding", Snippet: snippetOf(finding)}, false
	}
	evidence, isEvidenceMap := rawMap["evidence"].(map[string]any)
	if !isEvidenceMap {
		return Finding{}, validationRejection{Rule: "evidence_not_object", Path: "evidence", Snippet: snippetOf(rawMap["evidence"])}, false
	}
	if !exactKeySetFromSet(evidence, evidenceKeys) {
		return Finding{}, validationRejection{Rule: "evidence_keys_mismatch", Path: "evidence", Snippet: snippetOf(evidence)}, false
	}
	theme, themeOK := evidence["theme"].(string)
	if !themeOK {
		return Finding{}, validationRejection{Rule: "theme_not_string", Path: "evidence.theme", Snippet: snippetOf(evidence["theme"])}, false
	}
	theme, themeShare, themeKnown := resolveCanonicalKey(theme, opts.themeSharesPct, themeLabel)
	if !themeKnown {
		return Finding{}, validationRejection{Rule: "theme_unknown", Path: "evidence.theme", Snippet: snippetOf(theme)}, false
	}

	var subcategory *string
	switch rawSubcategory := evidence["subcategory"].(type) {
	case nil:
		// subcategory is None -- allowed, matches Python's `subcategory
		// is not None` guard being false.
	case string:
		rawSubcategory, subcategoryShare, subcategoryKnown := resolveCanonicalKey(rawSubcategory, opts.subcategorySharesPct, subcategoryLabel)
		if !subcategoryKnown {
			return Finding{}, validationRejection{Rule: "subcategory_unknown", Path: "evidence.subcategory", Snippet: snippetOf(rawSubcategory)}, false
		}
		if before, _, _ := strings.Cut(rawSubcategory, "."); before != theme {
			return Finding{}, validationRejection{Rule: "subcategory_theme_mismatch", Path: "evidence.subcategory", Snippet: snippetOf(rawSubcategory)}, false
		}
		_ = subcategoryShare
		subcategoryCopy := rawSubcategory
		subcategory = &subcategoryCopy
	default:
		// subcategory present but not a string (and not None) -- invalid.
		return Finding{}, validationRejection{Rule: "subcategory_invalid_type", Path: "evidence.subcategory", Snippet: snippetOf(rawSubcategory)}, false
	}

	if !finiteNumber(evidence["share_pct"], 0, ptr(100.0), true) {
		return Finding{}, validationRejection{Rule: "share_pct_invalid", Path: "evidence.share_pct", Snippet: snippetOf(evidence["share_pct"])}, false
	}
	if delta, present := evidence["delta_pct_points"]; present && delta != nil {
		if !finiteNumber(delta, -100, ptr(100.0), true) {
			return Finding{}, validationRejection{Rule: "delta_pct_points_invalid", Path: "evidence.delta_pct_points", Snippet: snippetOf(delta)}, false
		}
	}
	if rawQuality, present := evidence["evidence_quality_mean"]; present && rawQuality != nil {
		if !finiteNumber(rawQuality, 0, ptr(1.0), true) {
			return Finding{}, validationRejection{Rule: "evidence_quality_mean_invalid", Path: "evidence.evidence_quality_mean", Snippet: snippetOf(rawQuality)}, false
		}
	}
	if rawBand, present := evidence["evidence_quality_band"]; present && rawBand != nil {
		bandString, isBandString := rawBand.(string)
		if !isBandString || !bands[bandString] {
			return Finding{}, validationRejection{Rule: "evidence_quality_band_invalid", Path: "evidence.evidence_quality_band", Snippet: snippetOf(rawBand)}, false
		}
	}

	sharePct := themeShare
	if subcategory != nil {
		sharePct = opts.subcategorySharesPct[*subcategory]
	}
	// The share appended below is the theme's when the evidence names no subcategory. A sentence whose subject is
	// one listed subcategory of the theme is repaired to that subcategory, so the share sits beside its own subject;
	// a sentence that names several keeps no percent, since no single share is its own.
	repairedSubject, omitShare := false, false
	if subcategory == nil {
		switch named := namedSubcategoryKeys(strippedFinding, theme, opts.subcategorySharesPct); len(named) {
		case 0:
		case 1:
			repaired := named[0]
			subcategory = &repaired
			sharePct = opts.subcategorySharesPct[repaired]
			repairedSubject = true
		default:
			omitShare = true
		}
	}

	noun := opts.shareNoun
	if noun == "" {
		noun = "effort"
	}
	text := strings.TrimRight(strippedFinding, ".") + " (~" + formatPercentZeroDecimals(sharePct) + "% of " + noun + ")."
	if omitShare {
		text = strings.TrimRight(strippedFinding, ".") + "."
	}
	return Finding{
		repairedSubject: repairedSubject,
		Finding:         text,
		Evidence: FindingEvidence{
			Theme:               theme,
			Subcategory:         subcategory,
			SharePct:            sharePct,
			DeltaPctPoints:      nil,
			EvidenceQualityMean: opts.qualityMean,
			EvidenceQualityBand: opts.qualityBand,
		},
	}, validationRejection{}, true
}

// namedSubcategoryKeys returns the subcategories of theme that the finding's sentence names, in key order. The
// theme's own label is removed first: "Quality / Reliability" holds the word of a subcategory without naming it.
func namedSubcategoryKeys(finding, theme string, subcategoryShares map[string]float64) []string {
	text := " " + normalizeKeyForm(finding) + " "
	owns := []string{normalizeKeyForm(themeLabel(theme)), normalizeKeyForm(theme)}
	for _, part := range strings.Split(themeLabel(theme), "/") {
		owns = append(owns, normalizeKeyForm(part))
	}
	sort.Slice(owns, func(i, j int) bool { return len(owns[i]) > len(owns[j]) })
	for _, own := range owns {
		if own != "" {
			text = strings.ReplaceAll(text, " "+own+" ", " ")
		}
	}
	keys := make([]string, 0, len(subcategoryShares))
	for key := range subcategoryShares {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var named []string
	for _, key := range keys {
		before, leaf, found := strings.Cut(key, ".")
		if !found || before != theme {
			continue
		}
		if form := normalizeKeyForm(leaf); form != "" && strings.Contains(text, " "+form+" ") {
			named = append(named, key)
		}
	}
	return named
}

// normalizeKeyForm lowercases s and collapses every run of non-alphanumeric
// characters to one space, so "Quality / Reliability", "quality-reliability"
// and "quality_reliability" share one form.
func normalizeKeyForm(s string) string {
	var b strings.Builder
	pendingSpace := false
	for _, r := range strings.ToLower(s) {
		if unicode.IsLetter(r) || unicode.IsNumber(r) {
			if pendingSpace && b.Len() > 0 {
				b.WriteByte(' ')
			}
			pendingSpace = false
			b.WriteRune(r)
			continue
		}
		pendingSpace = true
	}
	return b.String()
}

// resolveCanonicalKey maps raw to one of the canonical keys in shares. An
// exact key wins. Otherwise raw is matched, in normalized form, against each
// key and its display label; a form shared by two keys is ambiguous and stays
// unresolved. A raw value that names no key in shares stays unknown, so the
// caller still rejects it.
func resolveCanonicalKey(raw string, shares map[string]float64, label func(string) string) (string, float64, bool) {
	if share, ok := shares[raw]; ok {
		return raw, share, true
	}
	want := normalizeKeyForm(raw)
	if want == "" {
		return raw, 0, false
	}
	match := ""
	for key := range shares {
		if normalizeKeyForm(key) != want && normalizeKeyForm(label(key)) != want {
			continue
		}
		if match != "" {
			return raw, 0, false
		}
		match = key
	}
	if match == "" {
		return raw, 0, false
	}
	return match, shares[match], true
}

// validConfidence ports investment_mix_validation.py's valid_confidence
// (investment_mix_validation.py:118-146). The bool return is unchanged
// parity behavior; validationRejection is the Go-only addition (see its
// own doc comment) describing WHY a false came back.
func validConfidence(raw any) (bool, validationRejection) {
	rawMap, isMap := raw.(map[string]any)
	if !isMap {
		return false, validationRejection{Rule: "confidence_not_object", Path: "$", Snippet: snippetOf(raw)}
	}
	if !exactKeySetFromSet(rawMap, confidenceKeys) {
		return false, validationRejection{Rule: "confidence_keys_mismatch", Path: "$", Snippet: snippetOf(rawMap)}
	}
	level, levelOK := rawMap["level"].(string)
	if !levelOK || !confidenceLevels[level] {
		return false, validationRejection{Rule: "confidence_level_invalid", Path: "level", Snippet: snippetOf(rawMap["level"])}
	}
	if mean, present := rawMap["quality_mean"]; present && mean != nil {
		if !finiteNumber(mean, 0, ptr(1.0), true) {
			return false, validationRejection{Rule: "confidence_quality_mean_invalid", Path: "quality_mean", Snippet: snippetOf(mean)}
		}
	}
	if stddev, present := rawMap["quality_stddev"]; present && stddev != nil {
		if !finiteNumber(stddev, 0, ptr(1.0), true) {
			return false, validationRejection{Rule: "confidence_quality_stddev_invalid", Path: "quality_stddev", Snippet: snippetOf(stddev)}
		}
	}
	bandMix, isBandMixMap := rawMap["band_mix"].(map[string]any)
	if !isBandMixMap {
		return false, validationRejection{Rule: "confidence_band_mix_not_object", Path: "band_mix", Snippet: snippetOf(rawMap["band_mix"])}
	}
	if !exactKeySetFromSet(bandMix, bands) {
		return false, validationRejection{Rule: "confidence_band_mix_keys_mismatch", Path: "band_mix", Snippet: snippetOf(bandMix)}
	}
	for band, value := range bandMix {
		if _, isBool := value.(bool); isBool {
			return false, validationRejection{Rule: "confidence_band_mix_value_invalid", Path: "band_mix." + band, Snippet: snippetOf(value)}
		}
		number, isNumber := value.(json.Number)
		if !isNumber || !isPythonInt(number) {
			return false, validationRejection{Rule: "confidence_band_mix_value_invalid", Path: "band_mix." + band, Snippet: snippetOf(value)}
		}
		// Non-negativity is checked on the LITERAL DIGIT STRING, not via
		// number.Float64() -- Python's int is arbitrary-precision
		// (investment_mix_validation.py:133's check is
		// isinstance(value, int) and value >= 0, no range limit at all),
		// so a genuinely huge non-negative integer (e.g. 1000 digits) is
		// a VALID band_mix count on the Python side. float64's ~1.8e308
		// range rejected such a value with strconv.ErrRange here, turning
		// a well-formed LLM response into invalid_llm_output where Python
		// would accept it -- caught by codex round 3 (P2). isPythonInt
		// above already confirms the literal has no '.'/'e'/'E', so
		// checking for a leading '-' is sufficient and exact.
		if strings.HasPrefix(string(number), "-") {
			return false, validationRejection{Rule: "confidence_band_mix_value_invalid", Path: "band_mix." + band, Snippet: snippetOf(value)}
		}
	}
	drivers, isDriversList := rawMap["drivers"].([]any)
	if !isDriversList {
		return false, validationRejection{Rule: "confidence_drivers_invalid", Path: "drivers", Snippet: snippetOf(rawMap["drivers"])}
	}
	if len(drivers) > 10 {
		return false, validationRejection{Rule: "confidence_drivers_too_many", Path: "drivers", Snippet: snippetOf(len(drivers))}
	}
	for i, driver := range drivers {
		driverString, isString := driver.(string)
		if !isString || pythonparity.Strip(driverString) == "" || pythonLen(driverString) > 120 {
			return false, validationRejection{Rule: "confidence_driver_invalid", Path: fmt.Sprintf("drivers[%d]", i), Snippet: snippetOf(driver)}
		}
	}
	return true, validationRejection{}
}

// parseAction ports investment_mix_validation.py's parse_action
// (investment_mix_validation.py:149-165). The bool return is unchanged
// parity behavior; validationRejection is the Go-only addition (see its
// own doc comment) describing WHY a false came back.
func parseAction(raw any) (ActionItem, validationRejection, bool) {
	rawMap, isMap := raw.(map[string]any)
	if !isMap {
		return ActionItem{}, validationRejection{Rule: "action_not_object", Path: "$", Snippet: snippetOf(raw)}, false
	}
	if !exactKeySet(rawMap, "action", "why", "where") {
		return ActionItem{}, validationRejection{Rule: "action_keys_mismatch", Path: "$", Snippet: snippetOf(rawMap)}, false
	}
	action, actionOK := rawMap["action"].(string)
	why, whyOK := rawMap["why"].(string)
	where, whereOK := rawMap["where"].(string)
	if !actionOK || pythonparity.Strip(action) == "" {
		return ActionItem{}, validationRejection{Rule: "action_field_blank", Path: "action", Snippet: snippetOf(rawMap["action"])}, false
	}
	if !whyOK || pythonparity.Strip(why) == "" {
		return ActionItem{}, validationRejection{Rule: "action_field_blank", Path: "why", Snippet: snippetOf(rawMap["why"])}, false
	}
	if !whereOK || pythonparity.Strip(where) == "" {
		return ActionItem{}, validationRejection{Rule: "action_field_blank", Path: "where", Snippet: snippetOf(rawMap["where"])}, false
	}
	if pythonLen(action) > 200 {
		return ActionItem{}, validationRejection{Rule: "action_field_too_long", Path: "action", Snippet: snippetOf(action)}, false
	}
	if pythonLen(why) > 300 {
		return ActionItem{}, validationRejection{Rule: "action_field_too_long", Path: "why", Snippet: snippetOf(why)}, false
	}
	if pythonLen(where) > 200 {
		return ActionItem{}, validationRejection{Rule: "action_field_too_long", Path: "where", Snippet: snippetOf(where)}, false
	}
	if hasUnicodeDigit(action) {
		return ActionItem{}, validationRejection{Rule: "action_field_digit", Path: "action", Snippet: snippetOf(action)}, false
	}
	if hasUnicodeDigit(why) {
		return ActionItem{}, validationRejection{Rule: "action_field_digit", Path: "why", Snippet: snippetOf(why)}, false
	}
	if hasUnicodeDigit(where) {
		return ActionItem{}, validationRejection{Rule: "action_field_digit", Path: "where", Snippet: snippetOf(where)}, false
	}
	return ActionItem{
		Action: pythonparity.Strip(action),
		Why:    pythonparity.Strip(why),
		Where:  pythonparity.Strip(where),
	}, validationRejection{}, true
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

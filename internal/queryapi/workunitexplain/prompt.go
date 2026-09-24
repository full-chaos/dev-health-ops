// Package workunitexplain ports the per-work-unit LLM explanation chain:
// llm/explainers/work_unit_explainer.py (allowed-input extraction, the
// canonical prompt, the language validator) and
// api/services/work_unit_explain.py (the orchestration and the response
// parser). It explains a PRECOMPUTED investment view and never computes
// one: the only values that reach a prompt are the ones
// extract_allowed_inputs admits, and the ClickHouse reads behind them
// belong to investmentexplain's already-ported
// (*Reader).BuildWorkUnitInvestments, which this package consumes rather
// than re-queries.
package workunitexplain

import (
	"fmt"
	"sort"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// canonicalExplanationPrompt is work_unit_explainer.py's
// CANONICAL_EXPLANATION_PROMPT verbatim, including its blank lines: the
// module's own comment marks it "USE VERBATIM", and the prompt text is the
// contract with the model, so a reflowed copy is a different prompt.
const canonicalExplanationPrompt = `You are explaining a precomputed investment view.

You are not allowed to:
- Recalculate scores
- Change categories
- Introduce new conclusions
- Be conversational (no "Hello", "As an AI", or interactive follow-ups)

Explain the investment view in three distinct sections:

1. **SUMMARY**: Provide a high-level narrative (max 3 sentences) using probabilistic language (appears, leans, suggests) explaining why the work leans toward the primary categories.
2. **REASONS**: List the specific evidence (structural, contextual, textual) that contributed most to this interpretation.
3. **UNCERTAINTY**: Disclose where uncertainty exists based on the evidence quality and evidence mix.

Always include evidence quality level and limits.`

// forbiddenWords is work_unit_explainer.py's FORBIDDEN_WORDS. Sorted here
// rather than a set because validateExplanationLanguage's output order is
// observable in Go and is not in Python (a frozenset's iteration order is
// hash-randomized per process) -- and because that output only ever
// reaches a log line, never a response, an arbitrary-but-stable order is
// strictly more useful than reproducing the randomization.
var forbiddenWords = []string{"detected", "determined", "is", "was"}

// evidenceSummary is _summarize_evidence's return value
// (work_unit_explainer.py:100-163) with its three optional sub-dicts
// modelled as presence flags plus fields. The `density`/`provenanceScore`
// flags exist because build_explanation_prompt tests membership for those
// two (`if "density" in struct`) while reading span_days/score through
// .get(..., 0) -- so an absent span_days is indistinguishable from zero on
// the reference plane, and is not tracked separately here either.
type evidenceSummary struct {
	hasStructural   bool
	structuralCount int
	structuralTypes []string
	hasDensity      bool
	density         float64
	hasProvenance   bool
	provenanceScore float64

	hasContextual      bool
	contextualCount    int
	contextualSpanDays float64
	contextualScore    float64

	hasTextual        bool
	textualMatchCount int
	textualCategories []string
}

// explanationInputs is work_unit_explainer.py's ExplanationInputs -- the
// ONLY values allowed to reach the model. Raw events, text blobs, code
// diffs and heuristic formulas are absent by construction, not by
// filtering later: nothing else is ever put in here.
type explanationInputs struct {
	workUnitID          string
	timeRangeStart      string
	timeRangeEnd        string
	timeSpanDays        float64
	categories          []categoryWeight
	evidenceQualityVal  float64
	evidenceQualityBand string
	evidenceSummary     evidenceSummary
}

type categoryWeight struct {
	name   string
	weight float64
}

// pythonFloat ports the CPython float() constructor for the JSON-decoded
// value types a structural-evidence entry can hold. A nil, map or slice
// value raises TypeError in Python, which api/main.py's own outer
// `except Exception` turns into a 503; an error here carries that same
// fate to the caller rather than substituting a zero the reference plane
// never produces.
func pythonFloat(value any) (float64, error) {
	switch typed := value.(type) {
	case float64:
		return typed, nil
	case int:
		return float64(typed), nil
	case bool:
		// A Python bool IS an int, so float(True) is 1.0 rather than an error.
		if typed {
			return 1, nil
		}
		return 0, nil
	case string:
		parsed, ok := pythonparity.ParseFloat(typed)
		if !ok {
			return 0, fmt.Errorf("could not convert string to float: %q", typed)
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("float() argument must be a string or a real number, not %T", value)
	}
}

// evidenceValue ports `float(item.get(key, 0))`: an ABSENT key yields the
// literal 0 default, while a key present with a null value reaches
// float(None) and raises -- the two are different on the reference plane
// and are kept different here.
func evidenceValue(item map[string]any, key string) (float64, error) {
	raw, present := item[key]
	if !present {
		return 0, nil
	}
	return pythonFloat(raw)
}

func itemString(item map[string]any, key string) (string, bool) {
	raw, present := item[key]
	if !present {
		return "", false
	}
	text, isString := raw.(string)
	return text, isString
}

// summarizeEvidence ports _summarize_evidence (work_unit_explainer.py:
// 100-163): counts and type names only, never raw text content.
//
// structuralTypes is SORTED, where Python builds it as `list({...})` from
// a set comprehension -- a set of strings whose iteration order CPython
// randomizes per process, so the reference plane has no stable order to
// reproduce and this list's order is not a parity property. It reaches the
// prompt's "types: a, b" line and nothing else.
func summarizeEvidence(evidence Evidence) (evidenceSummary, error) {
	var summary evidenceSummary

	if len(evidence.Structural) > 0 {
		summary.hasStructural = true
		summary.structuralCount = len(evidence.Structural)
		seen := map[string]struct{}{}
		for _, item := range evidence.Structural {
			entryType, isString := itemString(item, "type")
			if !isString {
				// Python's str(item.get("type", "unknown")) stringifies
				// whatever is there; only an ABSENT key yields "unknown".
				if raw, present := item["type"]; present {
					entryType = fmt.Sprintf("%v", raw)
				} else {
					entryType = "unknown"
				}
			}
			if _, already := seen[entryType]; !already {
				seen[entryType] = struct{}{}
				summary.structuralTypes = append(summary.structuralTypes, entryType)
			}
		}
		sort.Strings(summary.structuralTypes)

		// Python scans every entry and assigns unconditionally, so the LAST
		// matching entry wins for each of the two keys.
		for _, item := range evidence.Structural {
			entryType, _ := itemString(item, "type")
			if entryType == "graph_density" {
				value, err := evidenceValue(item, "value")
				if err != nil {
					return evidenceSummary{}, err
				}
				summary.hasDensity = true
				summary.density = value
			}
			if entryType == "provenance" {
				value, err := evidenceValue(item, "value")
				if err != nil {
					return evidenceSummary{}, err
				}
				summary.hasProvenance = true
				summary.provenanceScore = value
			}
		}
	}

	if len(evidence.Contextual) > 0 {
		summary.hasContextual = true
		summary.contextualCount = len(evidence.Contextual)
		for _, item := range evidence.Contextual {
			entryType, _ := itemString(item, "type")
			if entryType != "time_range" {
				continue
			}
			spanDays, err := evidenceValue(item, "span_days")
			if err != nil {
				return evidenceSummary{}, err
			}
			score, err := evidenceValue(item, "score")
			if err != nil {
				return evidenceSummary{}, err
			}
			summary.contextualSpanDays = spanDays
			summary.contextualScore = score
		}
	}

	if len(evidence.Textual) > 0 {
		categoryCounts := map[string]struct{}{}
		var categoryOrder []string
		matchCount := 0
		for _, item := range evidence.Textual {
			// `item.get("phrase") or item.get("keyword") or item.get("quote")`
			// -- an `or` chain returns the LAST operand when every one of them
			// is falsy, so an entry carrying only an EMPTY quote string still
			// yields "" (which `is not None`) and still counts as a match.
			_, phraseNotNone := chainedString(item, "phrase", "keyword", "quote")
			if phraseNotNone {
				matchCount++
			}
			category, _ := chainedString(item, "subcategory", "category")
			if category != "" {
				if _, already := categoryCounts[category]; !already {
					categoryCounts[category] = struct{}{}
					categoryOrder = append(categoryOrder, category)
				}
			}
		}
		if matchCount > 0 || len(categoryOrder) > 0 {
			summary.hasTextual = true
			summary.textualMatchCount = matchCount
			// dict insertion order, which CPython preserves -- NOT sorted,
			// unlike structuralTypes above, because this one IS deterministic
			// on the reference plane and therefore is a parity property.
			summary.textualCategories = categoryOrder
		}
	}

	return summary, nil
}

// chainedString evaluates Python's `a or b or c` over the named keys and
// reports whether the result is `not None`. Absent and null both read as
// Python's None; every other value is stringified the way an f-string
// would, since only a string ever reaches these keys in practice.
func chainedString(item map[string]any, keys ...string) (value string, notNone bool) {
	for index, key := range keys {
		raw, present := item[key]
		if !present || raw == nil {
			if index == len(keys)-1 {
				return "", false
			}
			continue
		}
		text, isString := raw.(string)
		if !isString {
			text = fmt.Sprintf("%v", raw)
		}
		if text == "" && index < len(keys)-1 {
			// Falsy: the `or` chain moves on.
			continue
		}
		return text, true
	}
	return "", false
}

// extractAllowedInputs ports extract_allowed_inputs
// (work_unit_explainer.py:59-98) together with explain_work_unit's own
// pre-call normalisation of the two evidence-quality values
// (work_unit_explain.py:91-97): a null value reads as 0.0 and a null or
// empty band reads as "unknown", for the PROMPT only -- the explanation's
// own evidence_quality_limits line reads the unnormalised values instead.
func extractAllowedInputs(unit WorkUnit) (explanationInputs, error) {
	summary, err := summarizeEvidence(unit.Evidence)
	if err != nil {
		return explanationInputs{}, err
	}

	var qualityValue float64
	if unit.EvidenceQualityValue != nil {
		qualityValue = *unit.EvidenceQualityValue
	}
	qualityBand := "unknown"
	if unit.EvidenceQualityBand != nil && *unit.EvidenceQualityBand != "" {
		qualityBand = *unit.EvidenceQualityBand
	}

	categories := make([]categoryWeight, 0, len(unit.Themes))
	for _, theme := range unit.Themes {
		categories = append(categories, categoryWeight{name: theme.Key, weight: theme.Value})
	}
	// `sorted(inputs.categories.items())` (build_explanation_prompt) sorts
	// by key; theme keys are unique, so the value half of the tuple never
	// breaks a tie.
	sort.Slice(categories, func(i, j int) bool { return categories[i].name < categories[j].name })

	return explanationInputs{
		workUnitID:     unit.WorkUnitID,
		timeRangeStart: pythonparity.IsoformatUTC(unit.TimeRangeStart),
		timeRangeEnd:   pythonparity.IsoformatUTC(unit.TimeRangeEnd),
		// (end - start).total_seconds() / 86400, reproduced through Go's own
		// Duration arithmetic -- the two agree to about fifteen significant
		// digits and not bit for bit, which the ".1f" interpolation below
		// absorbs entirely.
		timeSpanDays:        unit.TimeRangeEnd.Sub(unit.TimeRangeStart).Seconds() / 86400,
		categories:          categories,
		evidenceQualityVal:  qualityValue,
		evidenceQualityBand: qualityBand,
		evidenceSummary:     summary,
	}, nil
}

// formatPercent ports an f-string's ".2%"/".0%" conversion: the value is
// multiplied by 100 and rendered with the requested fixed precision, then
// a literal percent sign is appended.
func formatPercent(value float64, precision int) (string, error) {
	text, err := pythonparity.FormatFixed(value*100, precision)
	if err != nil {
		return "", err
	}
	return text + "%", nil
}

// buildExplanationPrompt ports build_explanation_prompt
// (work_unit_explainer.py:166-243) -- the canonical prompt followed by the
// work unit's own data section.
func buildExplanationPrompt(inputs explanationInputs) (string, error) {
	categoryLines := make([]string, 0, len(inputs.categories))
	for _, category := range inputs.categories {
		share, err := formatPercent(category.weight, 2)
		if err != nil {
			return "", err
		}
		categoryLines = append(categoryLines, fmt.Sprintf("  - %s: %s", category.name, share))
	}

	var evidenceLines []string
	summary := inputs.evidenceSummary
	if summary.hasStructural {
		evidenceLines = append(evidenceLines, fmt.Sprintf(
			"  - Structural evidence: %d items, types: %s",
			summary.structuralCount, strings.Join(summary.structuralTypes, ", ")))
		if summary.hasDensity {
			density, err := pythonparity.FormatFixed(summary.density, 2)
			if err != nil {
				return "", err
			}
			evidenceLines = append(evidenceLines, fmt.Sprintf("    - Graph density: %s", density))
		}
		if summary.hasProvenance {
			provenance, err := pythonparity.FormatFixed(summary.provenanceScore, 2)
			if err != nil {
				return "", err
			}
			evidenceLines = append(evidenceLines, fmt.Sprintf("    - Provenance score: %s", provenance))
		}
	}
	if summary.hasContextual {
		spanDays, err := pythonparity.FormatFixed(summary.contextualSpanDays, 1)
		if err != nil {
			return "", err
		}
		score, err := pythonparity.FormatFixed(summary.contextualScore, 2)
		if err != nil {
			return "", err
		}
		evidenceLines = append(evidenceLines, fmt.Sprintf(
			"  - Contextual evidence: span %s days, score %s", spanDays, score))
	}
	if summary.hasTextual {
		evidenceLines = append(evidenceLines, fmt.Sprintf(
			"  - Textual phrases cited: %d matches.", summary.textualMatchCount))
	}

	evidenceText := "  (no evidence details)"
	if len(evidenceLines) > 0 {
		evidenceText = strings.Join(evidenceLines, "\n")
	}

	spanText, err := pythonparity.FormatFixed(inputs.timeSpanDays, 1)
	if err != nil {
		return "", err
	}
	qualityText, err := pythonparity.FormatFixed(inputs.evidenceQualityVal, 2)
	if err != nil {
		return "", err
	}

	dataSection := fmt.Sprintf(`
---
WORK UNIT DATA (precomputed, do not recalculate):

Work Unit ID: %s
Time Range: %s to %s
Time Span: %s days

Investment Vector:
%s

Evidence Quality: %s (%s)

Evidence Summary:
%s
---

Based on the above precomputed investment view, explain why this work leans toward certain categories.
Use probabilistic language (appears, leans, suggests). Never use definitive language (is, was, detected).
`,
		inputs.workUnitID,
		inputs.timeRangeStart, inputs.timeRangeEnd,
		spanText,
		strings.Join(categoryLines, "\n"),
		qualityText, inputs.evidenceQualityBand,
		evidenceText,
	)

	return canonicalExplanationPrompt + dataSection, nil
}

// validateExplanationLanguage ports validate_explanation_language
// (work_unit_explainer.py:246-259). Its result is LOGGED and never
// returned to a caller of the endpoint -- explain_work_unit's own comment:
// "We log but don't reject - the violations are informational" -- so
// strings.ToLower stands in for CPython's str.lower() here without the
// full-Unicode fold a wire-visible value would need.
func validateExplanationLanguage(text string) []string {
	words := map[string]struct{}{}
	for _, word := range pythonparity.SplitWhitespace(strings.ToLower(text)) {
		words[word] = struct{}{}
	}
	var violations []string
	for _, forbidden := range forbiddenWords {
		if _, found := words[forbidden]; found {
			violations = append(violations, fmt.Sprintf("Forbidden word found: '%s'", forbidden))
		}
	}
	return violations
}

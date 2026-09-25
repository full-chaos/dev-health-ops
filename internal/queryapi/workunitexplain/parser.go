package workunitexplain

import (
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"

	"fmt"
	"regexp"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// bandUncertaintyText is _extract_uncertainty's band table
// (work_unit_explain.py:300-306), including its fallback: a band the table
// does not name reads the "moderate" entry.
var bandUncertaintyText = map[string]string{
	"high":     "With high evidence quality, uncertainty appears minimal but results should still be interpreted probabilistically.",
	"moderate": "Moderate evidence quality suggests meaningful uncertainty exists in the categorization.",
	"low":      "Low evidence quality indicates significant uncertainty; these results should be treated as tentative.",
	"very_low": "Very low evidence quality indicates high uncertainty; categorization leans toward estimates only.",
}

// bulletPattern ports `re.findall(r"[-•]\s*(.+?)(?=\n|$)", ...)`.
//
// The reference pattern's trailing lookahead is not expressible in RE2, and
// does not need to be: without re.DOTALL, `.` excludes the newline, so a
// lazy `.+?` bounded by "the next newline or the end" reaches exactly the
// end of its own line -- which is what a greedy `[^\n]+` reaches too. Go's
// regexp keeps leftmost-FIRST submatch semantics (not POSIX
// leftmost-longest), so the preceding greedy `\s*` backtracks here the same
// way the reference's does.
var bulletPattern = regexp.MustCompile("[-•]\\s*([^\n]+)")

// sectionHeaderPattern builds the header half of _extract_section's pattern
// (work_unit_explain.py:228-242): `\*\*HEADER[:\*]*\*\*`. The reference
// interpolates the header without escaping it; every header it is called
// with is regex-inert, so quoting here changes no result while removing the
// hazard.
func sectionHeaderPattern(header string) *regexp.Regexp {
	return regexp.MustCompile(`(?is)\*\*` + regexp.QuoteMeta(header) + `[:\*]*\*\*`)
}

// extractSectionByHeader ports _extract_section's header branch.
//
// WHY THIS IS NOT ONE REGEXP: the reference pattern ends
// `\s*(.*?)(?=\n\n|\*\*|$)` under re.DOTALL, and RE2 has no lookahead. The
// lookahead is what bounds the lazy group, so it is reproduced directly
// instead: the greedy `\s*` consumes every whitespace character after the
// header (it wins outright, because some group length always satisfies the
// lookahead -- the `$` alternative can never fail), and the group then runs
// to the first "\n\n" or "**" after that point, or to the end of the text.
// The reference's `$` also matches just BEFORE a single trailing newline,
// one character earlier than the end; .strip() erases that difference, so
// the end of the text is used for both.
func extractSectionByHeader(text, header, fallback string) string {
	location := sectionHeaderPattern(header).FindStringIndex(text)
	if location == nil {
		return fallback
	}

	runes := []rune(text[location[1]:])
	start := 0
	for start < len(runes) && pythonparity.IsSpace(runes[start]) {
		start++
	}
	remainder := string(runes[start:])

	end := len(remainder)
	if index := strings.Index(remainder, "\n\n"); index >= 0 && index < end {
		end = index
	}
	if index := strings.Index(remainder, "**"); index >= 0 && index < end {
		end = index
	}
	return pythonparity.Strip(remainder[:end])
}

// extractFirstParagraph ports _extract_section's header-less branch: the
// first non-blank "\n\n"-separated paragraph, stripped.
func extractFirstParagraph(text, fallback string) string {
	for _, paragraph := range strings.Split(text, "\n\n") {
		stripped := pythonparity.Strip(paragraph)
		if stripped != "" {
			return stripped
		}
	}
	return fallback
}

// extractEvidenceHighlights ports _extract_evidence_highlights
// (work_unit_explain.py:267-285). The three keyword-driven fallbacks are
// appended in the reference's own order, and the final `or` guarantees a
// non-empty list even when none of the keywords appear.
func extractEvidenceHighlights(text string) []string {
	var highlights []string

	section := extractSectionByHeader(text, "Evidence Highlights", "")
	if section != "" {
		for _, match := range bulletPattern.FindAllStringSubmatch(section, -1) {
			bullet := pythonparity.Strip(match[1])
			if bullet != "" {
				highlights = append(highlights, bullet)
			}
		}
	}

	if len(highlights) == 0 {
		lowered := strings.ToLower(text)
		if strings.Contains(lowered, "structural") {
			highlights = append(highlights, "Structural evidence appears most significant")
		}
		if strings.Contains(lowered, "contextual") {
			highlights = append(highlights, "Contextual evidence provides corroboration")
		}
		if strings.Contains(lowered, "textual") {
			highlights = append(highlights, "Textual phrases align with the investment mix")
		}
	}

	if len(highlights) == 0 {
		return []string{"Structural evidence appears most significant"}
	}
	return highlights
}

// extractUncertainty ports _extract_uncertainty (work_unit_explain.py:
// 287-306): the "Uncertainty Disclosure" section, then the bare
// "Uncertainty" section, then the band table.
func extractUncertainty(text, band string) string {
	if section := extractSectionByHeader(text, "Uncertainty Disclosure", ""); section != "" {
		return section
	}
	if section := extractSectionByHeader(text, "Uncertainty", ""); section != "" {
		return section
	}
	if entry, named := bandUncertaintyText[band]; named {
		return entry
	}
	return bandUncertaintyText["moderate"]
}

// extractEvidenceQualityLimits ports _extract_evidence_quality_limits
// (work_unit_explain.py:308-321).
//
// A NULL evidence_quality.value reaching the default branch is an error
// here because it is an error on the reference plane: that branch formats
// the value with ".0%", and format(None, ".0%") raises TypeError, which
// the endpoint's own outer handler turns into a 503. The caller propagates
// that rather than substituting a number the reference never emits. Note
// that a null BAND does not raise -- it interpolates as the four-character
// text "None" -- so the two nulls behave differently and are kept
// different.
func extractEvidenceQualityLimits(text string, unit WorkUnit) (string, error) {
	if section := extractSectionByHeader(text, "Evidence Quality Limits", ""); section != "" {
		return section, nil
	}

	if unit.EvidenceQualityValue == nil {
		return "", fmt.Errorf("unsupported format string passed to NoneType.__format__: evidence_quality.value is null")
	}
	share, err := formatPercent(*unit.EvidenceQualityValue, 0)
	if err != nil {
		return "", err
	}

	band := "None"
	if unit.EvidenceQualityBand != nil {
		band = *unit.EvidenceQualityBand
	}
	return fmt.Sprintf(
		"With %s evidence quality (%s), these results should be interpreted as probabilistic indicators. "+
			"The categorization suggests tendencies rather than definitive classifications.",
		band, share), nil
}

// parseLLMResponse ports _parse_llm_response (work_unit_explain.py:
// 180-226), including the order its six field values are derived in --
// several of them read `reasons_text or raw_response` and
// `uncertainty_text or raw_response`, so which sections matched changes
// what the later fields are computed from.
func parseLLMResponse(rawResponse string, unit WorkUnit) (Explanation, error) {
	summary := extractSectionByHeader(rawResponse, "SUMMARY", "")
	reasonsText := extractSectionByHeader(rawResponse, "REASONS", "")
	uncertaintyText := extractSectionByHeader(rawResponse, "UNCERTAINTY", "")

	if summary == "" {
		summary = extractFirstParagraph(rawResponse, pythonparity.TruncateRunes(rawResponse, 500))
	}

	rationaleSource := reasonsText
	if rationaleSource == "" {
		rationaleSource = rawResponse
	}

	categoryRationale := pyjson.NewOrderedMap[string]()
	analysisSection := extractSectionByHeader(rationaleSource, "Category Analysis", "")
	for _, theme := range unit.Themes {
		// `rf"{re.escape(category)}[^.]*\."` -- the category name followed by
		// any run of non-period characters and then one period. lru_cache'd on
		// the reference plane purely for speed; compiled per call here, which
		// has no observable effect.
		pattern := regexp.MustCompile(`(?i)` + regexp.QuoteMeta(theme.Key) + `[^.]*\.`)
		// `if matches:` on re.findall's result -- a found match, not a
		// non-empty one. FindStringIndex distinguishes "no match" from a
		// match that happens to be empty; this pattern cannot match empty
		// (it requires a literal period), but keying on the index rather
		// than the text keeps that independent of the pattern.
		if location := pattern.FindStringIndex(rationaleSource); location != nil {
			categoryRationale.Set(theme.Key, pythonparity.Strip(rationaleSource[location[0]:location[1]]))
			continue
		}
		if analysisSection != "" {
			categoryRationale.Set(theme.Key, "Category appears in overall analysis.")
			continue
		}
		categoryRationale.Set(theme.Key, "Category leaning based on structural evidence.")
	}

	band := ""
	if unit.EvidenceQualityBand != nil {
		band = *unit.EvidenceQualityBand
	}
	if band == "" {
		band = "unknown"
	}

	uncertainty := uncertaintyText
	if uncertainty == "" {
		uncertainty = extractUncertainty(rawResponse, band)
	}

	limitsSource := uncertaintyText
	if limitsSource == "" {
		limitsSource = rawResponse
	}
	limits, err := extractEvidenceQualityLimits(limitsSource, unit)
	if err != nil {
		return Explanation{}, err
	}

	return Explanation{
		WorkUnitID:            unit.WorkUnitID,
		AIGenerated:           true,
		Summary:               summary,
		CategoryRationale:     categoryRationale,
		EvidenceHighlights:    extractEvidenceHighlights(rationaleSource),
		UncertaintyDisclosure: uncertainty,
		EvidenceQualityLimits: limits,
	}, nil
}

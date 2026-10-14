package investmentexplain

import (
	"math"
	"regexp"
	"strings"
)

// themeDisplayLabels are the display names of the five canonical themes
// (docs/reference/taxonomies/investment.md, "Display label").
var themeDisplayLabels = map[string]string{
	"feature_delivery": "Feature Delivery",
	"operational":      "Operational / Support",
	"maintenance":      "Maintenance / Tech Debt",
	"quality":          "Quality / Reliability",
	"risk":             "Risk / Security",
}

// payloadFieldLabels name the fields of the prompt payload (buildExplainPayload)
// and the derived quality drivers in words. A model that echoes one of them in
// prose would otherwise show a raw field name to the customer.
var payloadFieldLabels = map[string]string{
	"theme_distribution_top":                    "theme distribution",
	"subcategory_distribution_top":              "subcategory distribution",
	"work_unit_dominant_subcategory_counts_top": "dominant subcategory counts",
	"evidence_quality_band_counts":              "evidence quality bands",
	"evidence_quality_mean":                     "mean evidence quality",
	"evidence_quality_stddev":                   "evidence quality spread",
	"evidence_quote_samples":                    "evidence quote samples",
	"quality_drivers":                           "quality drivers",
	"work_unit_count":                           "work unit count",
	"total_effort":                              "total effort",
	"total_work":                                "total work",
	"total_changed_lines":                       "total changed lines",
}

var (
	subcategoryKeyPattern = regexp.MustCompile(`\b(feature_delivery|operational|maintenance|quality|risk)\.([a-z][a-z0-9_]*)\b`)
	snakeCasePattern      = regexp.MustCompile(`\b[a-z][a-z0-9]*(?:_[a-z0-9]+)+\b`)
)

// themeLabel returns the canonical display label of a theme key.
func themeLabel(theme string) string {
	if label, ok := themeDisplayLabels[theme]; ok {
		return label
	}
	return titleWords(theme)
}

// subcategoryLabel returns "<theme label> · <Leaf>" for "theme.leaf".
func subcategoryLabel(subcategory string) string {
	theme, leaf, found := strings.Cut(subcategory, ".")
	if !found {
		return titleWords(subcategory)
	}
	return themeLabel(theme) + " · " + titleWords(leaf)
}

func titleWords(key string) string {
	words := strings.Fields(strings.ReplaceAll(key, "_", " "))
	for i, word := range words {
		words[i] = strings.ToUpper(word[:1]) + word[1:]
	}
	return strings.Join(words, " ")
}

// displayText replaces the raw keys and field names inside customer-facing
// prose with display names. Order matters: dotted subcategory keys first (their
// theme prefix is itself a key), then payload field names, then the remaining
// theme key with an underscore, then any other snake_case token as plain words.
// A bare one-word theme key ("quality", "risk") is ordinary English and stays.
func displayText(text string) string {
	if text == "" {
		return text
	}
	text = subcategoryKeyPattern.ReplaceAllStringFunc(text, subcategoryLabel)
	text = snakeCasePattern.ReplaceAllStringFunc(text, func(token string) string {
		if label, ok := payloadFieldLabels[token]; ok {
			return label
		}
		if label, ok := themeDisplayLabels[token]; ok {
			return label
		}
		return strings.ReplaceAll(token, "_", " ")
	})
	return text
}

func displayTexts(texts []string) []string {
	if texts == nil {
		return nil
	}
	out := make([]string, len(texts))
	for i, text := range texts {
		out[i] = displayText(text)
	}
	return out
}

// roundOneDecimal rounds a percentage to the one decimal the contract serves.
func roundOneDecimal(value float64) float64 {
	return math.Round(value*10) / 10
}

func roundOneDecimalPtr(value *float64) *float64 {
	if value == nil {
		return nil
	}
	rounded := roundOneDecimal(*value)
	return &rounded
}

// PresentExplanation is the contract boundary of the explain answer: display
// names in every customer-facing text field and percentages rounded to one
// decimal. It returns a copy and never touches the persisted (cached) value.
// evidence.theme and evidence.subcategory stay canonical keys: they are
// contract fields, not prose.
func PresentExplanation(explanation InvestmentMixExplanation) InvestmentMixExplanation {
	out := explanation
	out.Summary = displayText(explanation.Summary)
	out.AntiClaims = displayTexts(explanation.AntiClaims)

	if explanation.TopFindings != nil {
		out.TopFindings = make([]InvestmentMixFinding, len(explanation.TopFindings))
		for i, finding := range explanation.TopFindings {
			finding.Finding = displayText(finding.Finding)
			finding.Evidence.SharePct = roundOneDecimal(finding.Evidence.SharePct)
			finding.Evidence.DeltaPctPoints = roundOneDecimalPtr(finding.Evidence.DeltaPctPoints)
			out.TopFindings[i] = finding
		}
	}
	if explanation.WhatToCheckNext != nil {
		out.WhatToCheckNext = make([]InvestmentMixActionItem, len(explanation.WhatToCheckNext))
		for i, action := range explanation.WhatToCheckNext {
			out.WhatToCheckNext[i] = InvestmentMixActionItem{
				Action: displayText(action.Action),
				Why:    displayText(action.Why),
				Where:  displayText(action.Where),
			}
		}
	}
	return out
}

// rawKeyCount counts the raw keys and field names in the prose of an explanation,
// the tokens displayText replaces. A non-zero count means the model ignored the
// prompt rule; the answer is still served with display names.
func rawKeyCount(explanation InvestmentMixExplanation) int {
	texts := []string{explanation.Summary}
	texts = append(texts, explanation.AntiClaims...)
	for _, finding := range explanation.TopFindings {
		texts = append(texts, finding.Finding)
	}
	for _, action := range explanation.WhatToCheckNext {
		texts = append(texts, action.Action, action.Why, action.Where)
	}
	count := 0
	for _, text := range texts {
		count += len(snakeCasePattern.FindAllString(text, -1))
		count += len(subcategoryKeyPattern.FindAllString(text, -1))
	}
	return count
}

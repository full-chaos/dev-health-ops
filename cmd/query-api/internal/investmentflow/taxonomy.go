// Taxonomy label/key helpers -- ports dev_health_ops/core/taxonomy.py's
// _title_case, format_theme_label, format_subcategory_label,
// normalize_theme_key and split_category_filters, the pure-logic pieces
// api/services/investment_flow.py imports directly. THEMES membership
// reuses internal/jobs/workgraph/units' own already-transcribed taxonomy
// port (SortedThemes/IsTheme, dev_health_ops/investment_taxonomy.py)
// rather than a second copy of that registry; THEME_LABELS has no
// existing Go port anywhere in this repo (grepped, see this package's own
// PR notes) so it is transcribed here, the same "definition, not a
// derived table" reasoning units.taxonomy.go's own doc comment gives for
// its two sorted slices.
package investmentflow

import (
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

// themeLabels ports THEME_LABELS (core/taxonomy.py:22-28) verbatim.
var themeLabels = map[string]string{
	"feature_delivery": "Feature Delivery",
	"operational":      "Operational / Support",
	"maintenance":      "Maintenance / Tech Debt",
	"quality":          "Quality / Reliability",
	"risk":             "Risk / Security",
}

// themeKeysByLabel ports THEME_KEYS_BY_LABEL (core/taxonomy.py:31-33):
// the reverse mapping, lowercase label -> theme key.
var themeKeysByLabel = buildThemeKeysByLabel()

func buildThemeKeysByLabel() map[string]string {
	out := make(map[string]string, len(themeLabels))
	for key, label := range themeLabels {
		out[strings.ToLower(label)] = key
	}
	return out
}

// titleCase ports _title_case (core/taxonomy.py:36-37): Python's
// str.title() upper-cases the first letter of every "word" (a maximal
// run of alphabetic characters) and lower-cases every other letter in
// that run -- NOT the same as Go's strings.Title (deprecated, splits on
// whitespace only) or a naive per-space Title. strings.ToTitle-per-rune
// with a word-boundary tracker reproduces str.title()'s exact rule for
// the ASCII underscore/hyphen/space-delimited keys this taxonomy ever
// feeds it (confirmed: every SortedThemes/SortedSubcategories leaf is
// ASCII lowercase with '_' separators only).
func titleCase(value string) string {
	replaced := strings.ReplaceAll(strings.ReplaceAll(value, "_", " "), "-", " ")
	trimmed := strings.TrimSpace(replaced)
	if trimmed == "" {
		return trimmed
	}
	var b strings.Builder
	b.Grow(len(trimmed))
	prevIsLetter := false
	for _, r := range trimmed {
		isLetter := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
		switch {
		case isLetter && !prevIsLetter:
			b.WriteRune(toUpperASCII(r))
		case isLetter:
			b.WriteRune(toLowerASCII(r))
		default:
			b.WriteRune(r)
		}
		prevIsLetter = isLetter
	}
	return b.String()
}

func toUpperASCII(r rune) rune {
	if r >= 'a' && r <= 'z' {
		return r - ('a' - 'A')
	}
	return r
}

func toLowerASCII(r rune) rune {
	if r >= 'A' && r <= 'Z' {
		return r + ('a' - 'A')
	}
	return r
}

// formatThemeLabel ports format_theme_label (core/taxonomy.py:40-48).
func formatThemeLabel(themeKey string) string {
	key := strings.ToLower(strings.TrimSpace(themeKey))
	if label, ok := themeLabels[key]; ok {
		return label
	}
	return titleCase(themeKey)
}

// formatSubcategoryLabel ports format_subcategory_label (core/taxonomy.py:
// 51-59).
func formatSubcategoryLabel(subcategoryKey string) string {
	idx := strings.Index(subcategoryKey, ".")
	if idx < 0 {
		return titleCase(subcategoryKey)
	}
	theme, sub := subcategoryKey[:idx], subcategoryKey[idx+1:]
	return titleCase(theme) + " · " + titleCase(sub)
}

// normalizeThemeKey ports normalize_theme_key (core/taxonomy.py:62-74).
// present is false for Python's None return (both the nil-input and the
// no-match cases collapse to that single Go zero value, matching every
// call site's own "empty means absent" usage -- drill_category and theme
// are always treated as optional strings, never distinguished from "not
// found" downstream).
func normalizeThemeKey(themeKey *string) (key string, present bool) {
	if themeKey == nil {
		return "", false
	}
	raw := strings.TrimSpace(*themeKey)
	if raw == "" {
		return "", false
	}
	lowered := strings.ToLower(raw)
	if units.IsTheme(lowered) {
		return lowered, true
	}
	if mapped, ok := themeKeysByLabel[lowered]; ok {
		return mapped, true
	}
	return "", false
}

// splitCategoryFilters ports split_category_filters (core/taxonomy.py:
// 77-105): a value with a dot is a subcategory (its theme prefix is also
// added to themes); a value with none is a theme. Both returned slices
// are deduplicated, ordered by first appearance -- Go's map-seen +
// append-in-order idiom reproduces Python's dict.fromkeys ordering
// exactly.
func splitCategoryFilters(workCategory []string) (themes, subcategories []string) {
	seenThemes := map[string]bool{}
	seenSubcategories := map[string]bool{}
	for _, category := range workCategory {
		trimmed := strings.TrimSpace(category)
		if trimmed == "" {
			continue
		}
		if idx := strings.Index(trimmed, "."); idx >= 0 {
			if !seenSubcategories[trimmed] {
				seenSubcategories[trimmed] = true
				subcategories = append(subcategories, trimmed)
			}
			theme := trimmed[:idx]
			if !seenThemes[theme] {
				seenThemes[theme] = true
				themes = append(themes, theme)
			}
		} else {
			if !seenThemes[trimmed] {
				seenThemes[trimmed] = true
				themes = append(themes, trimmed)
			}
		}
	}
	return themes, subcategories
}

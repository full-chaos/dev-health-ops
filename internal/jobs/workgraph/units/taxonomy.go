package units

import "strings"

// The canonical investment taxonomy, ported from
// dev_health_ops/investment_taxonomy.py.
//
// # WHY SORTED SLICES AND NOT A MAP
//
// The reference holds THEMES and SUBCATEGORIES as `set[str]`. Python's set
// iteration order for strings depends on PYTHONHASHSEED and so varies between
// interpreter runs; Go randomises map iteration deliberately. Neither plane has
// a stable "natural" order, so any ordered artifact built by ranging a map would
// be nondeterministic in the Go plane exactly as it would be in the Python one.
//
// Every order-sensitive consumer in the reference goes through `sorted()`. The
// port therefore stores the sorted sequence as the primary form and derives
// membership from it, rather than storing a map and sorting at each use. That
// makes the deterministic path the default one and the nondeterministic path
// unavailable, instead of merely discouraged.
//
// The lists are transcribed rather than generated because they are the
// definition of the taxonomy, not a derived table -- there is no upstream to
// generate them from. tests/fixtures/generate_investment_taxonomy_golden.py
// imports the reference registry and the golden test compares against it, so a
// divergence fails rather than drifts.

// SortedThemes is THEMES in the only order any consumer uses.
var SortedThemes = [...]string{
	"feature_delivery",
	"maintenance",
	"operational",
	"quality",
	"risk",
}

// SortedSubcategories is SUBCATEGORIES in sorted order.
//
// Sorted as byte strings, matching Python's default `sorted()` on str, which
// compares by code point. Every key here is ASCII, so byte order and code-point
// order coincide and the two planes cannot disagree. If a non-ASCII key is ever
// added, that equivalence must be rechecked -- Go's sort.Strings compares bytes,
// while Python compares code points, and the two differ above U+007F.
var SortedSubcategories = [...]string{
	"feature_delivery.customer",
	"feature_delivery.enablement",
	"feature_delivery.roadmap",
	"maintenance.debt",
	"maintenance.refactor",
	"maintenance.upgrade",
	"operational.incident_response",
	"operational.on_call",
	"operational.support",
	"quality.bugfix",
	"quality.reliability",
	"quality.testing",
	"risk.compliance",
	"risk.security",
	"risk.vulnerability",
}

// themeSet and subcategorySet back the membership predicates. They are never
// ranged over -- only indexed -- so their iteration order cannot leak into
// output.
var (
	themeSet         = indexStrings(SortedThemes[:])
	subcategorySet   = indexStrings(SortedSubcategories[:])
	subcategoryTheme = buildSubcategoryTheme()
)

func indexStrings(values []string) map[string]struct{} {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	return set
}

// buildSubcategoryTheme ports SUBCATEGORY_TO_THEME, which the reference derives
// as `{s: s.split(".", 1)[0] for s in SUBCATEGORIES}`.
//
// split(".", 1)[0] is "everything before the FIRST dot", which for a key with no
// dot is the whole string. SplitN(key, ".", 2)[0] has exactly that behaviour,
// including the no-dot case, so the two agree without a special case. Note this
// is NOT the same as rsplit(".", 1)[-1], which metrics.py uses for the LEAF --
// the two differ for any key with more than one dot, and reaching for the wrong
// one produces a plausible-looking value rather than an error.
func buildSubcategoryTheme() map[string]string {
	mapping := make(map[string]string, len(SortedSubcategories))
	for _, key := range SortedSubcategories {
		mapping[key] = strings.SplitN(key, ".", 2)[0]
	}
	return mapping
}

// IsTheme reports whether key is a canonical theme.
func IsTheme(key string) bool {
	_, ok := themeSet[key]
	return ok
}

// IsSubcategory reports whether key is a canonical subcategory.
func IsSubcategory(key string) bool {
	_, ok := subcategorySet[key]
	return ok
}

// ThemeOf ports investment_taxonomy.theme_of.
//
// # THE EMPTY STRING IS THE CONTRACT, NOT A FAILURE SIGNAL
//
// The reference is `SUBCATEGORY_TO_THEME.get(subcategory_key, "")`. An unknown
// key yields the EMPTY STRING and does not raise. That includes cases a reader
// might expect to be special: a theme name passed where a subcategory is
// expected, a key differing only in case, and a key with surrounding whitespace.
// There is no case folding and no stripping.
//
// The idiomatic Go signature here would be `(string, bool)` or `(string, error)`,
// and adopting either would be a behaviour change rather than a translation: a
// caller that currently writes "" into a column would start seeing an error, and
// callers that MEANT to write "" would have to reconstruct it. The single return
// is deliberate. Membership, when a caller genuinely needs it, is IsSubcategory.
func ThemeOf(subcategoryKey string) string {
	return subcategoryTheme[subcategoryKey]
}

// PromptCategoryList ports categorization_prompts.py:88,
// `", ".join(sorted(SUBCATEGORIES))`.
//
// This string is embedded in the categorization prompt, the prompt drives
// categorization, and categorization feeds the input_hash that decides whether a
// unit is sent to the LLM at all. A reordering or a separator change here is not
// cosmetic: it changes every input_hash, misses every skip-existing lookup, and
// re-categorises the whole corpus at full cost, silently. Hence the verbatim
// pin in the golden corpus.
func PromptCategoryList() string {
	return strings.Join(SortedSubcategories[:], ", ")
}

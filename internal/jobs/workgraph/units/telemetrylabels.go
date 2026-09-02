package units

import (
	"strings"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// Fixed-cardinality labels for investment LLM telemetry, ported from
// llm_telemetry_labels.py.
//
// These are metric label values. Their whole purpose is bounded cardinality, so
// a divergence does not fail loudly -- it produces a DIFFERENT BUCKET, and the
// two planes' dashboards disagree while both look healthy.

// pythonLower reproduces Python's str.lower().
//
// # THIS IS NOT strings.ToLower
//
// str.lower() applies the full Unicode lowercase mapping including the
// SpecialCasing rules; strings.ToLower maps rune by rune. Measured divergences:
//
//	"ΟΔΟΣ".lower() -> "οδος"        final sigma, context-sensitive
//	                  strings.ToLower gives "οδοσ" -- no such rule
//	"İ".lower()    -> "i" + U+0307, TWO code points
//	                  unicode.ToLower gives one, dropping the combining dot
//
// cases.Lower(language.Und) is correct BY DESIGN, not by observed agreement, and
// the distinction matters because the second kind evaporates on a dependency
// bump. CPython's str.lower() is locale-INDEPENDENT by definition: it applies
// the default Unicode mapping and never the Turkish (dotless i) or Lithuanian
// tailorings, whatever the process locale. Und is the caser with no tailoring,
// so it is the only correct choice here -- not merely the one that currently
// agrees. A future x/text release changing a DEFAULT would be a bug in x/text;
// one changing a tailoring cannot reach us.
//
// (Raised by lane-pathb-go: my first wording stated the agreement without the
// reason, which reads as a coincidence a reader has no way to re-derive.)
//
// # A MEASURED, BOUNDED DIVERGENCE THAT IS CONTAINED RATHER THAN IGNORED
//
// x/text is not a complete substitute either. Its Final_Sigma lookahead is
// BOUNDED where CPython's is not, and the boundary is exactly 31 case-ignorable
// runes. Found by a codex round; reproduced here before being accepted:
//
//	("A\u03a3" + "." * n + "B").lower()      n <= 30      n >= 31
//	  CPython 3.14.7                          sigma        sigma      (medial)
//	  x/text cases.Lower(Und)                 sigma        FINAL      <- differs
//
// Final_Sigma says a capital sigma is final only when it is NOT followed by a
// cased letter, skipping case-ignorable characters in between. With 31 dots and
// then "B" the sigma is not final, and CPython sees that at any distance;
// x/text's scan gives up and concludes "no following cased letter".
//
// This is NOT accepted on the grounds that a 31-dot model name is implausible.
// Implausibility is not a measurement, and this lane has been wrong that way
// before. It is accepted because it is CONTAINED, provably:
//
//   - both sigma spellings are non-ASCII;
//   - every entry in every allow-list here is ASCII;
//   - every prefix ModelBucket tests is ASCII.
//
// So a string differing only in which sigma it carries takes the same branch and
// lands in the same bucket, whichever form appears.
//
// TestSigmaFormCannotChangeABucket asserts exactly that, and is the reason this
// stays acceptable. It fails the moment the containment does -- if pythonLower
// is exported, if a non-ASCII entry joins an allow-list, or if a non-ASCII
// prefix is added. At that point this comment stops being a justification and
// the Final_Sigma rule has to be implemented directly.
//
// A caser is not safe for concurrent use, so one is constructed per call rather
// than shared. These are called once per telemetry emission, not in a loop over
// rows; if that changes, use a sync.Pool rather than a package-level caser.
func pythonLower(value string) string {
	return cases.Lower(language.Und).String(value)
}

// Bounded ports llm_telemetry_labels.bounded.
//
// Note it does NOT strip or lower -- its callers do that first. So the empty
// string is not "absent", it is simply a value in no allow-list, and becomes the
// default. Reproduced rather than "improved" because a caller that passes an
// unstripped value should get the default, which is the signal that the caller
// forgot.
func Bounded(value string, allowed map[string]struct{}, defaultValue string) string {
	if _, ok := allowed[value]; ok {
		return value
	}
	return defaultValue
}

// Allow-lists, ported from the frozensets. Held as maps because they are only
// ever membership-tested, never ranged -- a frozenset's iteration order varies
// with PYTHONHASHSEED and a Go map's is randomised, so neither may feed an
// ordered artifact. The sorted slices exist for tests and for any caller that
// needs a stable listing.
var (
	PromptKinds             = indexStrings(SortedPromptKinds[:])
	Stages                  = indexStrings(SortedStages[:])
	PromptVersions          = indexStrings(SortedPromptVersions[:])
	Providers               = indexStrings(SortedProviders[:])
	CategorizationStatuses  = indexStrings(SortedCategorizationStatuses[:])
	ParseStatuses           = indexStrings(SortedParseStatuses[:])
	ValidationErrorFamilies = indexStrings(SortedValidationErrorFamilies[:])
)

var SortedPromptKinds = [...]string{
	"investment_categorize",
	"investment_mix_explain",
}

var SortedStages = [...]string{"initial", "repair", "request"}

var SortedPromptVersions = [...]string{
	"investment-categorization-v2",
	"investment-mix-explain-v2",
}

var SortedProviders = [...]string{
	"anthropic", "gemini", "lmstudio", "local", "mock", "none", "ollama",
	"openai", "qwen", "qwen-lmstudio", "qwen-local", "unknown",
}

var SortedCategorizationStatuses = [...]string{
	"insufficient_evidence", "invalid_llm_output", "llm_task_failed",
	"no_text_sources", "ok", "repaired",
}

var SortedParseStatuses = [...]string{
	"forbidden_language", "invalid_json", "invalid_llm_output", "valid",
}

var SortedValidationErrorFamilies = [...]string{
	"all_weights_zero",
	"evidence_quote_empty",
	"evidence_quote_extra_keys",
	"evidence_quote_invalid_source",
	"evidence_quote_invalid_type",
	"evidence_quote_missing_id",
	"evidence_quote_missing_keys",
	"evidence_quote_not_object",
	"evidence_quote_not_substring",
	"evidence_quote_too_long",
	"evidence_quote_unknown_source",
	"evidence_quotes_count_out_of_range",
	"evidence_quotes_not_list",
	"invalid_json",
	"invalid_weight",
	"missing_top_level_keys",
	"negative_weight",
	"non_finite_weight",
	"payload_not_object",
	"subcategories_not_object",
	"uncertainty_invalid_type",
	"uncertainty_missing",
	"uncertainty_too_long",
	"unexpected_top_level_keys",
	"unknown_subcategory",
	"weight_overflow",
	"weight_sum_not_finite",
}

// ProviderBucket ports provider_bucket: `bounded(provider.strip().lower(), PROVIDERS)`.
//
// The strip is pythonparity.Strip, NOT strings.TrimSpace. str.strip() uses
// str.isspace(), which is 29 code points including U+001C-U+001F.
//
// This is the OPPOSITE of floatcoerce.go, where float()'s narrower 25-point set
// is correct and pythonparity.Strip would be wrong. Two adjacent functions in
// the same package needing two different whitespace classes is not an accident
// of this codebase -- Python genuinely has three (str.isspace, the numeric
// parsers' set, and str.splitlines' boundaries) -- so the class is named at
// every call site rather than inferred from what the neighbouring function does.
func ProviderBucket(provider string) string {
	return Bounded(pythonLower(pythonparity.Strip(provider)), Providers, "other")
}

// ModelBucket ports model_bucket.
//
// # THE PREFIX CHAIN IS ORDERED, AND THE ORDER IS THE LOGIC
//
// "gpt-5-nano" also starts with "gpt-5". Checking the shorter prefix first would
// collapse nano and mini into "openai-reasoning-other". Measured against the
// reference: "gpt-5-turbo" -> openai-reasoning-other while "gpt-5-nano-2025" ->
// gpt-5-nano, which only holds in this order. Sorting these or turning them into
// a map would destroy the behaviour while looking like a tidy-up.
//
// Note "local" alone does NOT match: the tuple is ("llama", "local-"), with the
// hyphen, so a bare "local" falls through to "other". That near-miss is pinned
// in the corpus because it reads like an oversight and is not.
//
// Empty (after strip) returns "unknown", not "other" -- a different label from
// the unrecognised case, so a missing model is distinguishable in the metrics
// from an unknown one.
func ModelBucket(model string) string {
	normalized := pythonLower(pythonparity.Strip(model))
	if normalized == "" {
		return "unknown"
	}
	switch {
	case strings.HasPrefix(normalized, "gpt-5-nano"):
		return "gpt-5-nano"
	case strings.HasPrefix(normalized, "gpt-5-mini"):
		return "gpt-5-mini"
	case strings.HasPrefix(normalized, "gpt-5"),
		strings.HasPrefix(normalized, "gpt-6"),
		strings.HasPrefix(normalized, "openai/gpt-oss"):
		return "openai-reasoning-other"
	case strings.HasPrefix(normalized, "gpt-4"):
		return "gpt-4"
	case strings.HasPrefix(normalized, "claude"):
		return "claude"
	case strings.HasPrefix(normalized, "gemini"):
		return "gemini"
	case strings.HasPrefix(normalized, "qwen"):
		return "qwen"
	case strings.HasPrefix(normalized, "llama"),
		strings.HasPrefix(normalized, "local-"):
		return "local"
	default:
		return "other"
	}
}

// ValidationErrorFamily ports validation_error_family:
// `bounded(raw_error.split(":", 1)[0].strip(), VALIDATION_ERROR_FAMILIES)`.
//
// split(":", 1)[0] is everything before the FIRST colon, and the whole string
// when there is no colon -- SplitN(s, ":", 2)[0] has exactly that behaviour
// including the no-colon case. There is no lower() here, unlike the two buckets
// above, so an upper-case family name falls through to "other"; pinned, because
// adding a lower() would look like a consistency fix and would change which
// bucket a mis-cased error lands in.
func ValidationErrorFamily(rawError string) string {
	prefix := pythonparity.Strip(strings.SplitN(rawError, ":", 2)[0])
	return Bounded(prefix, ValidationErrorFamilies, "other")
}

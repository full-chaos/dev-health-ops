package categorize

// categorize.go ports work_graph/investment/categorize.py -- the call/repair
// loop that turns one units.TextBundle into a CategorizationOutcome, plus the
// two failure classifiers materialize.py drives its concurrency and its
// stop-on-fatal decision from (materialize.py:110-141).
//
// The pieces this composes (BuildPrompt, BuildRepairPrompt, ParseLLMJSON,
// ValidateLLMPayload, Provider) all landed in #2178. This file is the
// orchestration between them, which did not.

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

// Outcome statuses, from categorize.py's four return sites.
const (
	StatusOK                = "ok"
	StatusRepaired          = "repaired"
	StatusInvalidLLMOutput  = "invalid_llm_output"
	StatusInsufficientChars = "insufficient_evidence"
	StatusNoTextSources     = "no_text_sources"
	StatusLLMTaskFailed     = "llm_task_failed"
)

// fallbackUncertainty is categorize.py:84's verbatim sentence, written into
// every fallback outcome. It reaches no durable column today (the uncertainty
// field is not one of work_unit_investments' columns), but it is part of the
// outcome contract the mix-explainer reads, so it is pinned rather than
// paraphrased.
const fallbackUncertainty = "Insufficient validated evidence to assign a confident subcategory mix."

// fallbackPrior is categorize.py:44-50 FALLBACK_PRIOR -- five subcategories at
// 0.2 each. Passed through EnsureFullSubcategoryVector, which fills in every
// other canonical key at 0.0 and renormalises, so the written vector is the
// full-width one, not these five.
func fallbackPrior() map[string]float64 {
	return map[string]float64{
		"feature_delivery.roadmap": 0.2,
		"operational.on_call":      0.2,
		"maintenance.debt":         0.2,
		"quality.bugfix":           0.2,
		"risk.security":            0.2,
	}
}

func fallbackDistribution() map[string]float64 {
	return EnsureFullSubcategoryVector(fallbackPrior())
}

// CategorizationOutcome is categorize.py:53-64's frozen dataclass.
//
// LLMModel is the model the provider REPORTED (CompletionResult.Model), not
// the one requested -- materialize.py writes the requested one into
// categorization_model_version and this one only into the token-usage row, and
// the two genuinely differ when a provider resolves an alias.
type CategorizationOutcome struct {
	Subcategories  map[string]float64
	EvidenceQuotes []EvidenceQuote
	Uncertainty    string
	Status         string
	Errors         []string
	Warnings       []string
	LLMCalls       int
	InputTokens    int
	OutputTokens   int
	LLMModel       string
}

// FallbackOutcome ports categorize.py:75-87 fallback_outcome.
//
// The reason lands in BOTH status and errors[0], which reads like a
// duplication and is not: status is written to
// work_unit_investments.categorization_status, errors is concatenated with
// warnings into categorization_errors_json (materialize.py:1691), and a
// consumer reading only one of the two columns still learns why.
//
// The `reason or "insufficient_evidence"` default is the reference's, kept
// because an empty status would violate the column's own vocabulary.
func FallbackOutcome(reason string) CategorizationOutcome {
	status := reason
	if status == "" {
		status = StatusInsufficientChars
	}
	return CategorizationOutcome{
		Subcategories:  fallbackDistribution(),
		EvidenceQuotes: []EvidenceQuote{},
		Uncertainty:    fallbackUncertainty,
		Status:         status,
		Errors:         []string{reason},
	}
}

// CategorizeOptions carries what categorize_text_bundle takes as keyword-only
// arguments. Provider is REQUIRED -- the reference falls back to
// `get_provider(provider_name, model=model)` when none is passed
// (categorize.py:104), constructing a provider per call from ambient
// configuration. That implicit construction is exactly the shape CHAOS-2476
// filed as a bug (a missing key silently produced mock categorizations), so
// the port requires the caller to supply the provider it already resolved
// once, and refuses rather than inventing one.
type CategorizeOptions struct {
	Provider Provider
	// ProviderName and Model are carried for the outcome/telemetry labels
	// only; the Provider itself is already bound to them.
	ProviderName string
	Model        string
}

// ErrNoProvider is returned when CategorizeTextBundle is called without a
// Provider. See CategorizeOptions for why this is an error rather than a
// silent construction.
var ErrNoProvider = errors.New("categorize: no LLM provider supplied")

// CategorizeTextBundle ports categorize.py:115-141.
func CategorizeTextBundle(ctx context.Context, bundle units.TextBundle, opts CategorizeOptions) (CategorizationOutcome, error) {
	if opts.Provider == nil {
		return CategorizationOutcome{}, ErrNoProvider
	}
	completion, err := opts.Provider.Complete(ctx, CategorizationRequest(BuildPrompt(bundle.SourceBlock)))
	if err != nil {
		return CategorizationOutcome{}, err
	}
	return categorizeCompletion(ctx, bundle, completion.Text, opts, completionTally{
		InputTokens:   tokenCount(completion.InputTokens),
		OutputTokens:  tokenCount(completion.OutputTokens),
		LLMCalls:      1,
		ResolvedModel: completion.Model,
	})
}

// completionTally is the running (tokens, calls, resolved model) triple
// categorize_text_bundle_completion threads through both stages. It exists so
// the repair stage ACCUMULATES onto the initial call's counts rather than
// replacing them -- categorize.py:207-210 uses `+=`, and a port that assigned
// instead would under-report every repaired unit's cost, which is the number
// the llm_token_usage row is for.
type completionTally struct {
	InputTokens   int
	OutputTokens  int
	LLMCalls      int
	ResolvedModel string
}

func tokenCount(value *int) int {
	if value == nil {
		return 0
	}
	return *value
}

// categorizeCompletion ports categorize.py:144-274
// categorize_text_bundle_completion: validate, and on failure make exactly ONE
// repair attempt before giving up. The repair budget is one call, not a loop.
func categorizeCompletion(
	ctx context.Context, bundle units.TextBundle, completionText string,
	opts CategorizeOptions, tally completionTally,
) (CategorizationOutcome, error) {
	validation := validateCompletionText(completionText, bundle)
	if validation.OK {
		return successOutcome(validation, StatusOK, tally), nil
	}

	repairPrompt := BuildRepairPrompt(validation.Errors, bundle.SourceBlock, completionText)
	repaired, err := opts.Provider.Complete(ctx, CategorizationRequest(repairPrompt))
	if err != nil {
		return CategorizationOutcome{}, err
	}
	tally.InputTokens += tokenCount(repaired.InputTokens)
	tally.OutputTokens += tokenCount(repaired.OutputTokens)
	tally.LLMCalls++
	// `repaired_completion.model or resolved_model` (categorize.py:210): the
	// repair's reported model wins ONLY when non-empty, so a provider that
	// omits it on the second call does not blank what the first call reported.
	if repaired.Model != "" {
		tally.ResolvedModel = repaired.Model
	}

	validation = validateCompletionText(repaired.Text, bundle)
	if validation.OK {
		return successOutcome(validation, StatusRepaired, tally), nil
	}

	// Both attempts failed validation. This is NOT an error return: the
	// reference produces a real outcome with the fallback distribution and
	// status invalid_llm_output, and materialize.py then CLAMPS that unit's
	// evidence_quality to 0.3 (materialize.py:1686-1687). Returning an error
	// here instead would drop the unit entirely and lose that signal.
	return CategorizationOutcome{
		Subcategories:  fallbackDistribution(),
		EvidenceQuotes: []EvidenceQuote{},
		Uncertainty:    fallbackUncertainty,
		Status:         StatusInvalidLLMOutput,
		Errors:         validation.Errors,
		Warnings:       validation.Warnings,
		LLMCalls:       tally.LLMCalls,
		InputTokens:    tally.InputTokens,
		OutputTokens:   tally.OutputTokens,
		LLMModel:       tally.ResolvedModel,
	}, nil
}

// validateCompletionText is categorize.py's repeated parse-then-validate
// block (:156-168 and :211-223, byte-identical in the reference). A parse
// failure short-circuits to a not-ok result carrying the parse errors, WITHOUT
// calling validate_llm_payload -- the payload would be nil and every field
// check would add a second, redundant error.
func validateCompletionText(text string, bundle units.TextBundle) LLMValidationResult {
	payload, parseErrors := ParseLLMJSON(text)
	if len(parseErrors) > 0 {
		return LLMValidationResult{
			OK:             false,
			Errors:         parseErrors,
			Subcategories:  map[string]float64{},
			EvidenceQuotes: []EvidenceQuote{},
			Uncertainty:    "",
		}
	}
	if payload == nil {
		payload = map[string]any{}
	}
	return ValidateLLMPayload(payload, bundle.SourceTexts, bundle.HandleMap)
}

// successOutcome builds the ok/repaired outcome. Errors is an EMPTY slice, not
// the validation's (which is empty anyway on the ok path) and not nil --
// materialize.py concatenates errors+warnings into a JSON array and a nil
// would marshal as `null` where Python writes `[]`.
func successOutcome(validation LLMValidationResult, status string, tally completionTally) CategorizationOutcome {
	return CategorizationOutcome{
		Subcategories:  validation.Subcategories,
		EvidenceQuotes: validation.EvidenceQuotes,
		Uncertainty:    validation.Uncertainty,
		Status:         status,
		Errors:         []string{},
		Warnings:       validation.Warnings,
		LLMCalls:       tally.LLMCalls,
		InputTokens:    tally.InputTokens,
		OutputTokens:   tally.OutputTokens,
		LLMModel:       tally.ResolvedModel,
	}
}

// FailureClass ports materialize.py:110-130 _llm_failure_class.
//
// # WHY THIS DOES NOT MATCH ON err.Error()
//
// The reference matches substrings against `str(exc).lower()`. The naive port
// would match against err.Error() -- but this package's llmError.Error()
// SANITIZES its message (errors.go's secretPatterns), so a credential-shaped
// substring is replaced before any classifier could see it, and the classes
// that key on api-key wording ("missing_key", "invalid_api_key") would silently
// stop firing. Redaction is not optional, so the classifier reads the RAW
// message field instead, which same-package access makes available. This is the
// "never derive durable behaviour from a sanitised error string" rule applied
// deliberately rather than discovered later.
//
// The type-based branches come from the Kind discriminator rather than
// isinstance, which is the same test by a different mechanism.
func FailureClass(err error) string {
	if err == nil {
		return ""
	}

	var typed *llmError
	raw := err.Error()
	if errors.As(err, &typed) {
		raw = typed.message
	}
	text := strings.ToLower(raw)

	// Ordering is the reference's and is load-bearing: an auth error whose
	// message says "insufficient_quota" classifies as quota_exceeded, not
	// auth, because quota exhaustion is not fixed by new credentials.
	switch {
	case strings.Contains(text, "insufficient_quota") || strings.Contains(text, "quota exhausted"):
		return "quota_exceeded"
	case strings.Contains(text, "model_not_found") || strings.Contains(text, "model not found"):
		return "model_not_found"
	case strings.Contains(text, "missing") && strings.Contains(text, "api key"):
		return "missing_key"
	case strings.Contains(text, "invalid") && strings.Contains(text, "api key"):
		return "invalid_api_key"
	}

	if typed == nil {
		return "llm_error"
	}
	switch typed.kind {
	case llmErrorAuth:
		return "auth"
	case llmErrorRateLimit:
		return "rate_limit"
	case llmErrorServer:
		return "server_error"
	case llmErrorContextLength:
		return "context_length"
	case llmErrorOutput:
		return "output_error"
	default:
		return "llm_error"
	}
}

// IsDeterministicFailure ports materialize.py:133-141
// _is_deterministic_llm_failure.
//
// A deterministic failure is one that WILL recur on retry with the same
// configuration -- bad or absent credentials, an unknown model, an exhausted
// quota. materialize.py cancels every in-flight task and aborts the whole run
// on one of these, rather than letting several hundred components each spend a
// call discovering the same thing.
func IsDeterministicFailure(err error) bool {
	if err == nil {
		return false
	}
	var typed *llmError
	if errors.As(err, &typed) && typed.kind == llmErrorAuth {
		return true
	}
	switch FailureClass(err) {
	case "invalid_api_key", "missing_key", "model_not_found", "quota_exceeded":
		return true
	default:
		return false
	}
}

// FormatFailureSummary ports materialize.py:144-151 _format_llm_summary --
// the one-line "llm: 12 ok, 3 rate_limit" verdict, with failure classes in
// sorted order and zero counts omitted.
func FormatFailureSummary(ok int, failureCounts map[string]int) string {
	parts := make([]string, 0, len(failureCounts)+1)
	parts = append(parts, strconv.Itoa(ok)+" ok")

	classes := make([]string, 0, len(failureCounts))
	for class := range failureCounts {
		classes = append(classes, class)
	}
	sort.Strings(classes)
	for _, class := range classes {
		if failureCounts[class] == 0 {
			continue
		}
		parts = append(parts, strconv.Itoa(failureCounts[class])+" "+class)
	}
	return "llm: " + strings.Join(parts, ", ")
}

// EffectiveModelVersion ports materialize.py:154-163 _effective_model_version.
//
// This string is written to work_unit_investments.categorization_model_version
// AND is part of the skip-existing lookup key, so a change to its shape
// invalidates every cached categorization in the corpus. It is assembled here
// verbatim rather than composed from parts at each call site.
//
// resolvedModel is the caller's `resolve_model_name(...) or model or provider`
// chain already collapsed -- the caller has the org-scoped settings this port
// deliberately does not reach for (CHAOS-5006).
func EffectiveModelVersion(provider, resolvedModel string) string {
	return "provider=" + provider + ";model=" + resolvedModel +
		";taxonomy=" + TaxonomyVersion + ";prompt=" + PromptVersion
}

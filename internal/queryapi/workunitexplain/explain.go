package workunitexplain

import (
	"context"
	"log"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/investmentexplain"
)

// CompleteFunc is CompleteWorkUnitExplanationForOrg's own signature,
// extracted as a type for the same reason investmentexplain's CompleteFunc
// is: it lets a test drive this orchestration with a canned completion and
// no provider construction at all, while the route passes a closure over
// its own org id and settings resolver.
type CompleteFunc func(ctx context.Context, requestedProvider, requestedModel, fullPrompt string) (
	result categorize.CompletionResult, resolvedProvider string, resolvedModel string, err error)

// Options carries explain_work_unit's own parameters
// (work_unit_explain.py:49-57) that are not the investment itself.
type Options struct {
	OrgID string
	// LLMProvider is the RAW requested provider string, "auto" when the
	// caller named none -- kept raw because the token-usage gate below
	// compares it to "mock" literally, before any resolution.
	LLMProvider string
	// LLMModel is the caller's model override, empty when absent.
	LLMModel string
	// ResolvedProviderKind is what LLMProvider resolves to for this org,
	// resolved by the CALLER. The endpoint resolves it already, before any
	// read, because get_provider has to run first for a missing credential
	// to answer a 4xx instead of a 200 with a broken body -- and
	// _should_return_non_ai_explanation then only asks whether the
	// already-constructed provider is the NoneProvider. Taking the answer
	// rather than re-deriving it keeps that same division and avoids a
	// second org-settings read per request.
	ResolvedProviderKind categorize.ProviderKind
	// Now is the computed_at stamp for a token-usage row.
	Now time.Time
}

// ExplainWorkUnit ports explain_work_unit (api/services/work_unit_explain.py:
// 49-161): decide whether an explanation is possible at all, extract the
// allowed inputs, build the canonical prompt, complete it, account the
// tokens, then parse the completion into an Explanation.
//
// writer may be nil, in which case no token-usage row is attempted --
// matching the reference's own `if db_url` guard on a caller that has no
// analytics URL to write through.
func ExplainWorkUnit(
	ctx context.Context,
	writer *investmentexplain.CacheWriter,
	complete CompleteFunc,
	unit WorkUnit,
	opts Options,
) (Explanation, error) {
	// _should_return_non_ai_explanation (work_unit_explain.py:164-178). The
	// endpoint always hands explain_work_unit an already-constructed
	// provider, so only that function's FIRST branch is live on this path:
	// `provider.__class__.__name__ == "NoneProvider"`, which is true for
	// exactly the resolved provider name "none". Its is_llm_available
	// fallbacks are reachable only from a caller that passes provider=None,
	// and this route is not one.
	if opts.ResolvedProviderKind == categorize.ProviderKindNone {
		return nonAIExplanation(unit.WorkUnitID), nil
	}

	inputs, err := extractAllowedInputs(unit)
	if err != nil {
		return Explanation{}, err
	}
	prompt, err := buildExplanationPrompt(inputs)
	if err != nil {
		return Explanation{}, err
	}

	completion, resolvedProvider, _, err := complete(ctx, opts.LLMProvider, opts.LLMModel, prompt)
	if err != nil {
		return Explanation{}, err
	}

	// `if db_url and llm_provider != "mock"` (work_unit_explain.py:126) --
	// the RAW requested string, so a request that named "auto" and resolved
	// to mock still writes a row. Reproduced rather than tightened.
	if writer != nil && opts.LLMProvider != "mock" {
		// `model=completion.model or llm_model` -- the completion's own model
		// first, the caller's override second. An empty result is left nil so
		// BuildLLMTokenUsageRecord applies its own "unknown", exactly as
		// Python's `model or "unknown"` does for None.
		model := completion.Model
		if model == "" {
			model = opts.LLMModel
		}
		var modelPtr *string
		if model != "" {
			modelPtr = &model
		}
		if record, ok := investmentexplain.BuildLLMTokenUsageRecord(investmentexplain.TokenUsageInput{
			OrgID:        opts.OrgID,
			Source:       "work_unit_explain",
			Provider:     resolvedProvider,
			Model:        modelPtr,
			InputTokens:  completion.InputTokens,
			OutputTokens: completion.OutputTokens,
		}, opts.Now); ok {
			// Best-effort, matching the reference's own
			// `except Exception: logger.debug(...)` around the whole sink
			// block: a failed accounting row never fails the explanation.
			if writeErr := writer.WriteLLMTokenUsage(ctx, record); writeErr != nil {
				log.Printf("query-api: work_unit_explain: token usage write failed: org_id=%s err=%v",
					opts.OrgID, writeErr)
			}
		}
	}

	// validate_explanation_language's result is informational on both
	// planes -- logged, never allowed to reject a response.
	if violations := validateExplanationLanguage(completion.Text); len(violations) > 0 {
		log.Printf("query-api: work_unit_explain: language violations: work_unit_id=%s violations=%v",
			unit.WorkUnitID, violations)
	}

	return parseLLMResponse(completion.Text, unit)
}

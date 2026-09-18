package categorize

import "context"

// CompletionResult is llm/providers/base.py's CompletionResult.
type CompletionResult struct {
	Text              string
	InputTokens       *int
	OutputTokens      *int
	Model             string
	CachedInputTokens *int
}

// CompletionRequest is a prompt plus everything a provider needs to know
// about the RESPONSE it should produce -- deliberately separate from
// Provider itself so one Provider implementation serves every caller.
//
// Python's providers instead re-derive this per call by SNIFFING the
// prompt text (openai.py's is_json_schema_prompt/
// is_investment_mix_explanation_prompt, keyed on prompts.go's
// RESPONSE_FORMAT_MARKER line): a Provider there has exactly one caller
// shape baked in implicitly. This port makes that choice an explicit,
// caller-supplied field instead -- the caller (investment.categorize
// today; a future investment-mix-explanation caller, the reason this type
// exists) already knows which schema it wants and states it directly,
// rather than a Provider parsing prompt text to guess.
type CompletionRequest struct {
	Prompt string
	// SystemMessage is the instructions/system-role text -- openai.py's
	// system_message(prompt), now a value the caller supplies instead of
	// text the provider derives.
	SystemMessage string
	// ResponseFormatName is the structured-output schema's name on the
	// wire (OpenAI's `text.format.name` / the Chat Completions
	// `json_schema.name`) -- also doubles as the discriminator a caller
	// like MockProvider uses to pick which canned response shape to
	// return. Use the CategorizationRequest/InvestmentMixExplanationRequest
	// constructors rather than setting this by hand.
	ResponseFormatName string
	// JSONSchema is the strict JSON Schema sent for structured-output
	// mode. nil means "no schema requested" -- a provider falls back to
	// a plain json_object/text response format, matching Python's own
	// non-schema-prompt branch.
	JSONSchema map[string]any
	// MaxOutputTokens is a per-format FLOOR, combined with a provider's
	// own configured floor via max() -- openai.py's OpenAIGPT5Provider
	// floors at 2048 for categorization but 4096 for investment-mix
	// explanation ("Explanation payloads are large; start higher than
	// 4096" is openai.py's own comment). Zero means "no per-request
	// floor," leaving the provider's own configured minimum as-is.
	MaxOutputTokens int
}

// CategorizationRequest builds the CompletionRequest investment
// categorization has always sent -- the ONLY request shape this package
// built before CompletionRequest existed, so wrapping BuildPrompt's output
// in this preserves that behavior byte-for-byte, unchanged.
func CategorizationRequest(prompt string) CompletionRequest {
	return CompletionRequest{
		Prompt:             prompt,
		SystemMessage:      categorizationSystemMessage,
		ResponseFormatName: categorizationResponseFormatName,
		JSONSchema:         categorizationJSONSchema(),
		MaxOutputTokens:    categorizationMaxOutputTokensFloor,
	}
}

// InvestmentMixExplanationRequest builds the CompletionRequest for the
// OTHER response format Python's providers support (openai.py's
// INVESTMENT_MIX_RESPONSE_FORMAT / investment_mix_explanation_json_schema)
// -- the aggregate investment-mix narrative CHAOS-4977's future API-side
// caller needs. No Go caller builds this prompt yet; this constructor
// exists so a Provider implementation has something concrete to be tested
// against today, ahead of that caller landing.
func InvestmentMixExplanationRequest(prompt string) CompletionRequest {
	return CompletionRequest{
		Prompt:             prompt,
		SystemMessage:      explanationSystemMessage,
		ResponseFormatName: investmentMixExplanationResponseFormatName,
		JSONSchema:         investmentMixExplanationJSONSchema(),
		MaxOutputTokens:    explanationMaxOutputTokensFloor,
	}
}

// WorkUnitExplanationRequest builds the CompletionRequest for the
// per-work-unit explanation prompt (llm/explainers/work_unit_explainer.py's
// build_explanation_prompt). It is the SCHEMA-FREE explanation request:
// JSONSchema is nil because that prompt carries no
// DEV_HEALTH_RESPONSE_FORMAT marker (openai.py's _response_format_kind),
// so Python asks for a bare `{"type": "json_object"}` response and names no
// schema. Both OpenAIProvider and LocalProvider take their own
// schema-free branch for a nil JSONSchema and never read
// ResponseFormatName, so the name below reaches no provider API; it exists
// to select MockProvider's explanation branch. SystemMessage and the token
// floor are Python's own defaults for a non-categorization prompt, which
// is why both are the shared explanation values rather than new ones.
func WorkUnitExplanationRequest(prompt string) CompletionRequest {
	return CompletionRequest{
		Prompt:             prompt,
		SystemMessage:      explanationSystemMessage,
		ResponseFormatName: workUnitExplanationResponseFormatName,
		JSONSchema:         nil,
		MaxOutputTokens:    explanationMaxOutputTokensFloor,
	}
}

// Provider is the Go equivalent of llm/providers/base.py's LLMProvider
// Protocol -- the narrow capability any LLM-backed caller in this package
// needs from a backend, deliberately small enough that any of Python's six
// providers (openai/anthropic/gemini/qwen/local/mock) could implement it.
// Day-one implementations: MockProvider (dev/test), OpenAIProvider
// (openaiprovider.go, GPT-5 family via the Responses API), and
// LocalProvider (localprovider.go, an OpenAI-compatible endpoint such as
// LM Studio/Ollama). Anthropic/gemini/qwen have no Go port yet -- add one
// by implementing this same interface, not by widening it.
//
// Model is the one exception to that "don't widen it" rule: it is the
// SINGLE source of truth for the model this Provider was constructed to
// use, read back by any caller that must report the model it will
// actually send before, or regardless of, calling Complete -- whose own
// CompletionResult.Model reflects the identical value only on the return
// paths that set it, and is empty on an early error return. A caller that
// computes its own, second, independent notion of "the model" (env-var
// resolution duplicated outside this package, a hardcoded default) can
// drift from what Complete actually sends; reading it back from the
// constructed Provider cannot.
type Provider interface {
	Complete(ctx context.Context, request CompletionRequest) (CompletionResult, error)
	Close() error
	Model() string
}

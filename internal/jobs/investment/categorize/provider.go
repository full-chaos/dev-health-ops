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

// Provider is the Go equivalent of llm/providers/base.py's LLMProvider
// Protocol -- the narrow capability materialize's categorization step
// needs from any LLM backend, deliberately small enough that any of
// Python's six providers (openai/anthropic/gemini/qwen/local/mock) could
// implement it. Day-one implementations: MockProvider (dev/test),
// OpenAIProvider (openaiprovider.go, GPT-5 family via the Responses API), and
// LocalProvider (localprovider.go, an OpenAI-compatible endpoint such as
// Ollama). Anthropic/gemini/qwen have no Go port yet -- add one by
// implementing this same interface, not by widening it.
type Provider interface {
	Complete(ctx context.Context, prompt string) (CompletionResult, error)
	Close() error
}

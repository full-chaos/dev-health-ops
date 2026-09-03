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
// needs from any LLM backend. No real provider client lands in this PR
// (plan.md 5a-adjacent scoping, chris's Q1 follow-up: day-one providers
// are prod-configured + mock, prod provider still pending); MockProvider
// below is the only implementation so far.
type Provider interface {
	Complete(ctx context.Context, prompt string) (CompletionResult, error)
	Close() error
}

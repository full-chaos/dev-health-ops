package categorize

import "context"

// NoneProvider is llm/providers/none.py's NoneProvider: the operator
// kill-switch (LLM_PROVIDER=none) that always succeeds with an empty
// completion rather than erroring -- distinct from unimplementedProvider,
// which refuses. "none" means deliberately disabled, not "not built yet."
type NoneProvider struct{}

func (NoneProvider) Complete(_ context.Context, _ string) (CompletionResult, error) {
	return CompletionResult{Model: "none"}, nil
}

func (NoneProvider) Close() error { return nil }

var _ Provider = NoneProvider{}

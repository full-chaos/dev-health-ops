package categorize

import (
	"context"
	"log/slog"
)

// LogCompletion writes the one Info line of an LLM call. It never carries
// prompt or output text. The cache counts are left off the line when the
// provider did not report them.
func LogCompletion(ctx context.Context, result CompletionResult) {
	attrs := []any{
		"model", result.Model,
		"input_tokens", tokenCount(result.InputTokens),
		"output_tokens", tokenCount(result.OutputTokens),
		"format_path", result.FormatPath,
		"format_reason", result.FormatReason,
	}
	if result.CacheCreationInputTokens != nil {
		attrs = append(attrs, "cache_creation_input_tokens", *result.CacheCreationInputTokens)
	}
	if result.CachedInputTokens != nil {
		attrs = append(attrs, "cached_input_tokens", *result.CachedInputTokens)
	}
	slog.InfoContext(ctx, "llm.call", attrs...)
}

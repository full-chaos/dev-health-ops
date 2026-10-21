package categorize

import (
	"context"
	"testing"
)

func TestLogCompletionCarriesCacheCountsOnlyWhenReported(t *testing.T) {
	in, out, created, read, zero := 120, 7, 100, 0, 0
	for _, c := range []struct {
		name   string
		result CompletionResult
		want   map[string]any
		absent []string
	}{
		{"write", CompletionResult{Model: "m", InputTokens: &in, OutputTokens: &out, CacheCreationInputTokens: &created, CachedInputTokens: &read},
			map[string]any{"cache_creation_input_tokens": float64(100), "cached_input_tokens": float64(0), "input_tokens": float64(120)}, nil},
		{"zero is reported", CompletionResult{Model: "m", InputTokens: &in, OutputTokens: &out, CacheCreationInputTokens: &zero},
			map[string]any{"cache_creation_input_tokens": float64(0)}, []string{"cached_input_tokens"}},
		{"unreported", CompletionResult{Model: "m", InputTokens: &in, OutputTokens: &out},
			map[string]any{"input_tokens": float64(120)}, []string{"cache_creation_input_tokens", "cached_input_tokens"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			buf := captureLLMCallLines(t)
			LogCompletion(context.Background(), c.result)
			records := llmCallRecords(t, buf)
			if len(records) != 1 {
				t.Fatalf("lines = %d: %s", len(records), buf.String())
			}
			requireLLMCallRecord(t, records[0], c.want)
			for _, key := range c.absent {
				if _, present := records[0][key]; present {
					t.Errorf("%s is on the line though the provider did not report it: %v", key, records[0])
				}
			}
		})
	}
}

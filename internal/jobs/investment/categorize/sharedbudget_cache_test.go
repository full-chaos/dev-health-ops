package categorize

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/llm"

	"github.com/full-chaos/dev-health-ops/internal/llmbudget"
)

func TestSharedBudgetPassesTheCacheWriteMarkerAndCreationCount(t *testing.T) {
	store := &recordingStore{}
	guard := llmbudget.Guard{Store: store, OrgID: "org", Provider: "anthropic", Model: "claude-haiku-5-5",
		Now: func() time.Time { return time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC) }}
	one, thousand, zero := 1, 1000, 0
	err := sharedBudget{guard: guard}.Run(context.Background(),
		llm.Attempt{IdempotencyKey: "k", Provider: llm.KindAnthropic, Model: "claude-haiku-5-5", MaxInputBytes: 1000, MaxOutputTokens: 1, CacheWrite: true},
		func(context.Context) (llm.Usage, error) {
			return llm.Usage{InputTokens: &thousand, OutputTokens: &one, CachedInputTokens: &zero, CacheCreationInputTokens: &thousand}, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	// 1000 input tokens at the 625000 write rate + 1 output token at 2500000.
	if got := store.admits[0].MaxCost; got == nil || *got != 628 {
		t.Errorf("reserved = %v, want 628 (whole input at the write rate)", got)
	}
	settle := store.settles[0]
	if settle.Cost == nil || *settle.Cost != 628 || settle.Usage.CacheCreationInputTokens == nil || *settle.Usage.CacheCreationInputTokens != 1000 {
		t.Errorf("settle = %+v cost %v", settle.Usage, settle.Cost)
	}
}

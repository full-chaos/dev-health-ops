package investmentexplain

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
)

// TestInvestmentMixExplainLiveSmoke explains the fixture investment mix for
// real through the route's own path (ExplainInvestmentMix with the platform
// provider builder): the provider must accept the explanation request and
// the answer must pass the route's own parser and validation (status
// "valid"), not only answer HTTP 200. Opt-in: LLM_LIVE_SMOKE=1 plus
// LLM_LIVE_SMOKE_KINDS; a listed kind without its key FAILS. Logs status,
// model, token counts and latency only.
func TestInvestmentMixExplainLiveSmoke(t *testing.T) {
	if os.Getenv("LLM_LIVE_SMOKE") != "1" {
		t.Skip("live smoke: set LLM_LIVE_SMOKE=1 and LLM_LIVE_SMOKE_KINDS")
	}
	kinds := strings.FieldsFunc(os.Getenv("LLM_LIVE_SMOKE_KINDS"), func(r rune) bool { return r == ' ' || r == ',' })
	if len(kinds) == 0 {
		t.Fatal("LLM_LIVE_SMOKE=1 with no LLM_LIVE_SMOKE_KINDS: nothing would be measured")
	}
	reader, err := NewReader(explainFixtureClient{})
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range kinds {
		t.Run(kind, func(t *testing.T) {
			var completion categorize.CompletionResult
			complete := func(ctx context.Context, provider, model, prompt string) (categorize.CompletionResult, string, string, error) {
				result, resolvedProvider, resolvedModel, err := CompleteInvestmentMixExplanation(ctx, provider, model, prompt)
				completion = result
				return result, resolvedProvider, resolvedModel, err
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			started := time.Now()
			got, err := reader.ExplainInvestmentMix(ctx, nil, availabilityFromIsLLMAvailable, complete, ExplainInvestmentMixOptions{
				OrgID:        "org-live-smoke",
				StartTS:      time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC),
				EndTS:        time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
				LLMProvider:  kind,
				ForceRefresh: true,
				Now:          explainFixtureNow,
			})
			elapsed := time.Since(started).Round(time.Millisecond)
			if err != nil {
				t.Fatalf("live smoke %s investment_mix: class=%s after %s: %v", kind, categorize.FailureClass(err), elapsed, err)
			}
			status := "<nil>"
			if got.Status != nil {
				status = *got.Status
			}
			t.Logf("live smoke %s investment_mix: status=%s model=%s input_tokens=%v output_tokens=%v latency=%s",
				kind, status, completion.Model, deref(completion.InputTokens), deref(completion.OutputTokens), elapsed)
			if status != "valid" {
				t.Fatalf("live smoke %s investment_mix: status %s, want valid", kind, status)
			}
		})
	}
}

func deref(value *int) any {
	if value == nil {
		return nil
	}
	return *value
}

package workunitexplain

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/investmentexplain"
)

// TestWorkUnitExplainLiveSmoke explains the first golden work unit for real
// through the route's own path (ExplainWorkUnit with the platform provider
// builder): the answer must pass the route's own parser (an AI explanation
// with a summary), not only answer HTTP 200. Opt-in: LLM_LIVE_SMOKE=1 plus
// LLM_LIVE_SMOKE_KINDS; a listed kind without its key FAILS. Logs status,
// model, token counts and latency only.
func TestWorkUnitExplainLiveSmoke(t *testing.T) {
	if os.Getenv("LLM_LIVE_SMOKE") != "1" {
		t.Skip("live smoke: set LLM_LIVE_SMOKE=1 and LLM_LIVE_SMOKE_KINDS")
	}
	kinds := strings.FieldsFunc(os.Getenv("LLM_LIVE_SMOKE_KINDS"), func(r rune) bool { return r == ' ' || r == ',' })
	if len(kinds) == 0 {
		t.Fatal("LLM_LIVE_SMOKE=1 with no LLM_LIVE_SMOKE_KINDS: nothing would be measured")
	}
	cases := loadExplainGolden(t)
	if len(cases) == 0 {
		t.Fatal("no golden work unit to explain")
	}
	unit := cases[0].Investment.toWorkUnit(t)
	for _, kind := range kinds {
		t.Run(kind, func(t *testing.T) {
			var completion categorize.CompletionResult
			complete := func(ctx context.Context, provider, model, prompt string) (categorize.CompletionResult, string, string, error) {
				result, resolvedProvider, resolvedModel, err := investmentexplain.CompleteWorkUnitExplanationForOrg(ctx, provider, model, "org-live-smoke", nil, nil, prompt)
				completion = result
				return result, resolvedProvider, resolvedModel, err
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			started := time.Now()
			explanation, err := ExplainWorkUnit(ctx, nil, complete, unit, Options{
				OrgID: "org-live-smoke", LLMProvider: kind, ResolvedProviderKind: categorize.ProviderKind(kind), Now: time.Now().UTC(),
			})
			elapsed := time.Since(started).Round(time.Millisecond)
			if err != nil {
				t.Fatalf("live smoke %s work_unit: class=%s after %s", kind, categorize.FailureClass(err), elapsed)
			}
			t.Logf("live smoke %s work_unit: ai_generated=%v summary_chars=%d model=%s input_tokens=%v output_tokens=%v latency=%s",
				kind, explanation.AIGenerated, len(explanation.Summary), completion.Model, tokens(completion.InputTokens), tokens(completion.OutputTokens), elapsed)
			if !explanation.AIGenerated || strings.TrimSpace(explanation.Summary) == "" {
				t.Fatalf("live smoke %s work_unit: ai_generated=%v summary empty=%v, want an AI explanation with a summary", kind, explanation.AIGenerated, explanation.Summary == "")
			}
		})
	}
}

func tokens(value *int) any {
	if value == nil {
		return nil
	}
	return *value
}

package categorize

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

// TestHostedProvidersLiveSmoke categorizes one work unit for real through the
// production builder (platform env) of each hosted kind: the provider must
// accept the categorization request (strict JSON schema included) and the
// answer must pass the categorization validation, not fall back. Opt-in:
// LLM_LIVE_SMOKE=1 plus LLM_LIVE_SMOKE_KINDS (e.g. "anthropic gemini"); a
// listed kind without its key FAILS. It never runs in default CI. It logs
// status, model, token counts and latency only.
func TestHostedProvidersLiveSmoke(t *testing.T) {
	if os.Getenv("LLM_LIVE_SMOKE") != "1" {
		t.Skip("live smoke: set LLM_LIVE_SMOKE=1 and LLM_LIVE_SMOKE_KINDS")
	}
	kinds := liveSmokeKinds(os.Getenv("LLM_LIVE_SMOKE_KINDS"))
	if len(kinds) == 0 {
		t.Fatal("LLM_LIVE_SMOKE=1 with no LLM_LIVE_SMOKE_KINDS: nothing would be measured")
	}
	bundle, err := units.BuildTextBundle(units.BuildTextBundleInput{
		WorkUnitID: "live-smoke",
		IssueIDs:   []string{"ENG-1"},
		WorkItemMap: map[string]map[string]any{
			"ENG-1": {"title": "Export job times out for large workspaces", "type": "Bug",
				"description": "The nightly export worker times out for workspaces over 10k items and needs a streamed writer with retries."},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range kinds {
		t.Run(string(kind), func(t *testing.T) {
			provider, err := NewProviderFromEnv(kind)
			if err != nil {
				t.Fatalf("build %s from the platform env: %v", kind, err)
			}
			defer func() { _ = provider.Close() }()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			started := time.Now()
			outcome, err := CategorizeTextBundle(ctx, bundle, CategorizeOptions{Provider: provider, ProviderName: string(kind), Model: provider.Model()})
			elapsed := time.Since(started)
			if err != nil {
				t.Fatalf("live smoke %s: class=%s after %s", kind, FailureClass(err), elapsed.Round(time.Millisecond))
			}
			t.Logf("live smoke %s: status=%s model=%s calls=%d input_tokens=%d output_tokens=%d latency=%s",
				kind, outcome.Status, outcome.LLMModel, outcome.LLMCalls, outcome.InputTokens, outcome.OutputTokens, elapsed.Round(time.Millisecond))
			if outcome.Status != StatusOK && outcome.Status != StatusRepaired {
				t.Fatalf("live smoke %s: status %s, want ok or repaired", kind, outcome.Status)
			}
		})
	}
}

func liveSmokeKinds(raw string) []ProviderKind {
	var kinds []ProviderKind
	for _, field := range strings.FieldsFunc(raw, func(r rune) bool { return r == ' ' || r == ',' }) {
		kinds = append(kinds, ProviderKind(field))
	}
	return kinds
}

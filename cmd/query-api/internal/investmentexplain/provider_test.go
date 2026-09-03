package investmentexplain

import (
	"context"
	"encoding/json"
	"testing"
)

// TestCompleteInvestmentMixExplanationMockProviderEndToEnd proves the
// wiring actually reaches categorize.MockProvider's investment-mix-
// explanation branch (mockInvestmentMixExplanation) and returns its
// canned, schema-shaped-but-NOT-parser-shaped JSON -- confirmed against
// investment_mix_validation.py's TOP_LEVEL_KEYS: the mock's real Python
// response ({"summary", "dominant_themes", "key_drivers",
// "operational_signals", "confidence_note"}) does NOT match what
// parse_investment_mix_response requires ({"summary", "top_findings",
// "confidence", "what_to_check_next", "anti_claims"}), which is why every
// llm_provider="mock" investment/explain call is expected to land on the
// invalid_llm_output fallback path in the real system, not "valid" --
// this is Python's actual, preserved behavior, not a Go-port defect.
func TestCompleteInvestmentMixExplanationMockProviderEndToEnd(t *testing.T) {
	result, resolvedProvider, resolvedModel, err := CompleteInvestmentMixExplanation(
		context.Background(), "mock", "", "some prompt text\nEvidence Quality: (high)\n  - velocity.feature: 62.50%\n",
	)
	if err != nil {
		t.Fatalf("CompleteInvestmentMixExplanation: %v", err)
	}
	if resolvedProvider != "mock" {
		t.Fatalf("resolvedProvider = %q, want %q", resolvedProvider, "mock")
	}
	if resolvedModel != "mock" {
		t.Fatalf("resolvedModel = %q, want %q", resolvedModel, "mock")
	}
	if result.Model != "mock" {
		t.Fatalf("result.Model = %q, want %q", result.Model, "mock")
	}

	var decoded map[string]any
	if err := json.Unmarshal([]byte(result.Text), &decoded); err != nil {
		t.Fatalf("mock response is not valid JSON: %v\ntext: %s", err, result.Text)
	}
	wantKeys := []string{"summary", "dominant_themes", "key_drivers", "operational_signals", "confidence_note"}
	for _, key := range wantKeys {
		if _, ok := decoded[key]; !ok {
			t.Fatalf("mock response missing expected key %q: %s", key, result.Text)
		}
	}
	if _, ok := decoded["top_findings"]; ok {
		t.Fatalf("mock response unexpectedly has a top_findings key -- the parser-shaped and mock-shaped responses should never collide: %s", result.Text)
	}

	parseStatus := ParseInvestmentMixResponse(result.Text, ParseOptions{FallbackLevel: "unknown"}).Status
	if parseStatus != ParseStatusInvalidLLMOutput {
		t.Fatalf("expected the mock provider's response to fail strict parsing with status %q, got %q -- if this now passes, the mock provider's shape changed and this test's own documentation is stale", ParseStatusInvalidLLMOutput, parseStatus)
	}
}

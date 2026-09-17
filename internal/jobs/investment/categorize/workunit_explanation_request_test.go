package categorize

import (
	"context"
	"strings"
	"testing"
)

// workUnitExplanationPrompt is the shape build_explanation_prompt
// (llm/explainers/work_unit_explainer.py) produces, reduced to the two
// lines the mock's explanation branch actually parses.
const workUnitExplanationPrompt = `You are explaining a precomputed investment view.

Investment Vector:
  - feature_delivery: 48.00%
  - quality: 32.00%

Evidence Quality: 0.91 (high)
`

// TestWorkUnitExplanationRequestAsksForNoSchema pins the property that
// keeps this request identical to Python's on the wire: the per-work-unit
// prompt carries no DEV_HEALTH_RESPONSE_FORMAT marker, so openai.py asks
// for a bare `{"type": "json_object"}` and names no schema. A nil
// JSONSchema is what puts OpenAIProvider and LocalProvider on that same
// branch; a non-nil one here would send a structured-output schema Python
// never sends, and would start putting ResponseFormatName on the wire.
func TestWorkUnitExplanationRequestAsksForNoSchema(t *testing.T) {
	request := WorkUnitExplanationRequest(workUnitExplanationPrompt)

	if request.JSONSchema != nil {
		t.Errorf("JSONSchema = %v, want nil (the reference sends no schema for this prompt)", request.JSONSchema)
	}
	if request.Prompt != workUnitExplanationPrompt {
		t.Error("Prompt was altered")
	}
	// system_message's non-categorization branch (openai.py:90-95), which
	// is the SAME text the investment-mix request carries -- the reference
	// has one such branch, not one per explanation format.
	if request.SystemMessage != explanationSystemMessage {
		t.Errorf("SystemMessage = %q, want the shared explanation system message", request.SystemMessage)
	}
	// `max(self.cfg.max_output_tokens, 4096 if not is_schema_prompt else 2048)`
	// -- this prompt is not a schema prompt, so the floor is the 4096 one.
	if request.MaxOutputTokens != explanationMaxOutputTokensFloor {
		t.Errorf("MaxOutputTokens = %d, want %d", request.MaxOutputTokens, explanationMaxOutputTokensFloor)
	}
}

// TestMockProviderServesTheExplanationBranchForAWorkUnitRequest is the
// red/green for routing the new response-format name: before it existed,
// a work-unit request fell through to mockCategorization and answered a
// subcategories/evidence_quotes payload, where Python's own prompt-content
// sniff sends this prompt to the explanation branch.
func TestMockProviderServesTheExplanationBranchForAWorkUnitRequest(t *testing.T) {
	result, err := MockProvider{}.Complete(context.Background(), WorkUnitExplanationRequest(workUnitExplanationPrompt))
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if result.Model != "mock" {
		t.Errorf("Model = %q, want %q", result.Model, "mock")
	}
	for _, key := range []string{"summary", "dominant_themes", "key_drivers", "operational_signals", "confidence_note"} {
		if !strings.Contains(result.Text, `"`+key+`"`) {
			t.Errorf("response is missing the explanation key %q\ntext=%s", key, result.Text)
		}
	}
	for _, key := range []string{"subcategories", "evidence_quotes"} {
		if strings.Contains(result.Text, `"`+key+`"`) {
			t.Errorf("response carries the CATEGORIZATION key %q -- the request fell through to the wrong branch\ntext=%s", key, result.Text)
		}
	}
	// The prompt's "(high)" marker and its top-scoring category must both
	// have reached the canned text, which is what proves the branch parsed
	// this prompt rather than ignoring it.
	if !strings.Contains(result.Text, "indicate high uncertainty") {
		t.Errorf("evidence-quality band did not reach the response\ntext=%s", result.Text)
	}
	if !strings.Contains(result.Text, "lean toward feature_delivery work") {
		t.Errorf("top category did not reach the response\ntext=%s", result.Text)
	}
}

// TestMockProviderExplanationBranchIsShapedLikePythonJSONDumps pins the
// encoding, not just the values. The per-work-unit explanation parser
// finds no markdown headers in this JSON and returns the WHOLE text as its
// `summary` field, so these bytes ARE a wire value: member order is
// mock.py's own literal order, and the separators are json.dumps'
// defaults, `", "` and `": "`. Go's encoding/json would sort the members
// and drop both spaces.
func TestMockProviderExplanationBranchIsShapedLikePythonJSONDumps(t *testing.T) {
	result, err := MockProvider{}.Complete(context.Background(), WorkUnitExplanationRequest(workUnitExplanationPrompt))
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}

	if !strings.HasPrefix(result.Text, `{"summary": "`) {
		t.Errorf("response does not start with the `summary` member and json.dumps' `\": \"` separator\ntext=%s", result.Text)
	}
	if strings.Contains(result.Text, `","`) {
		t.Errorf("response carries a space-free member separator; json.dumps writes `\", \"`\ntext=%s", result.Text)
	}

	previous := -1
	for _, key := range []string{"summary", "dominant_themes", "key_drivers", "operational_signals", "confidence_note"} {
		index := strings.Index(result.Text, `"`+key+`":`)
		if index < 0 {
			t.Fatalf("member %q is missing\ntext=%s", key, result.Text)
		}
		if index <= previous {
			t.Errorf("member %q appears out of mock.py's own literal order\ntext=%s", key, result.Text)
		}
		previous = index
	}
}

// TestMockProviderExplanationBranchIsShared pins that both explanation
// response formats reach ONE branch producing ONE text, the way Python's
// single `else` does -- not two canned payloads that could drift apart.
func TestMockProviderExplanationBranchIsShared(t *testing.T) {
	workUnit, err := MockProvider{}.Complete(context.Background(), WorkUnitExplanationRequest(workUnitExplanationPrompt))
	if err != nil {
		t.Fatalf("Complete (work unit): %v", err)
	}
	mix, err := MockProvider{}.Complete(context.Background(), InvestmentMixExplanationRequest(workUnitExplanationPrompt))
	if err != nil {
		t.Fatalf("Complete (investment mix): %v", err)
	}
	if workUnit.Text != mix.Text {
		t.Errorf("the two explanation formats produced different text for the same prompt\nwork unit=%s\nmix=%s",
			workUnit.Text, mix.Text)
	}
}

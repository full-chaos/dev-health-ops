package categorize

import "github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"

// Response format names -- openai.py's CATEGORIZATION_RESPONSE_FORMAT /
// INVESTMENT_MIX_RESPONSE_FORMAT, doubling as the JSON-schema "name" field
// on the wire (see CompletionRequest.ResponseFormatName's own doc comment).
const (
	categorizationResponseFormatName           = "categorization"
	investmentMixExplanationResponseFormatName = "investment_mix_explanation"
)

// categorizationSystemMessage and investmentMixExplanationSystemMessage
// port openai.py's system_message: the JSON-schema branch (categorization)
// gets a strict "generate only JSON" instruction; the other branch
// (investment-mix explanation, an aggregate narrative over PRECOMPUTED
// analytics -- no Go caller builds this prompt yet, see
// CompletionRequest.InvestmentMixExplanationRequest) gets a softer
// "explain what's already computed, hedge, don't editorialize"
// instruction. Both are still requests for JSON; only the framing differs.
const categorizationSystemMessage = "You are a specialized JSON generator.\n" +
	"Return ONLY valid JSON.\n" +
	"No markdown. No commentary.\n" +
	"Output must start with { and end with }."

const investmentMixExplanationSystemMessage = "You are an assistant that explains PRECOMPUTED work analytics.\n" +
	"Use probabilistic language (appears, suggests, leans).\n" +
	"Do NOT introduce new conclusions or recommendations.\n" +
	"Return ONLY valid JSON."

// categorizationJSONSchema ports openai.py:246-294 categorization_json_schema
// -- the strict JSON Schema sent to a provider's structured-output mode, one
// property per canonical subcategory. Built from units.SortedSubcategories
// rather than a second hardcoded key list, so this schema and
// ValidateLLMPayload's own allowed-subcategory check (schema.go) can never
// drift apart.
func categorizationJSONSchema() map[string]any {
	keys := units.SortedSubcategories[:]

	subcategoryProperties := make(map[string]any, len(keys))
	for _, key := range keys {
		subcategoryProperties[key] = map[string]any{"type": "number", "minimum": 0}
	}

	return map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"required":             []string{"subcategories", "evidence_quotes", "uncertainty"},
		"properties": map[string]any{
			"subcategories": map[string]any{
				"type":                 "object",
				"additionalProperties": false,
				"required":             append([]string{}, keys...),
				"properties":           subcategoryProperties,
			},
			"evidence_quotes": map[string]any{
				"type":     "array",
				"minItems": 1,
				"maxItems": 10,
				"items": map[string]any{
					"type":                 "object",
					"required":             []string{"quote", "source", "id"},
					"additionalProperties": false,
					"properties": map[string]any{
						"quote":  map[string]any{"type": "string", "minLength": 1, "maxLength": 280},
						"source": map[string]any{"type": "string", "enum": []string{"issue", "pr", "commit"}},
						"id":     map[string]any{"type": "string", "minLength": 1},
					},
				},
			},
			"uncertainty": map[string]any{"type": "string", "minLength": 1, "maxLength": 280},
		},
	}
}

// investmentMixExplanationJSONSchema ports openai.py:297-424
// investment_mix_explanation_json_schema -- the strict JSON Schema for the
// aggregate investment-mix narrative CHAOS-4977's future caller sends. No
// canonical-subcategory coupling here (unlike categorizationJSONSchema):
// this schema's shape is fixed prose structure (findings/confidence/action
// items), not a per-taxonomy-key object.
func investmentMixExplanationJSONSchema() map[string]any {
	bandKeys := []string{"high", "moderate", "low", "very_low", "unknown"}
	evidenceQualityBandEnum := []any{"high", "moderate", "low", "very_low", "unknown", nil}

	findingEvidenceSchema := map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"required": []string{
			"theme", "subcategory", "share_pct", "delta_pct_points",
			"evidence_quality_mean", "evidence_quality_band",
		},
		"properties": map[string]any{
			"theme":            map[string]any{"type": "string", "minLength": 1},
			"subcategory":      map[string]any{"type": []string{"string", "null"}},
			"share_pct":        map[string]any{"type": "number", "minimum": 0, "maximum": 100},
			"delta_pct_points": map[string]any{"type": []string{"number", "null"}, "minimum": -100, "maximum": 100},
			"evidence_quality_mean": map[string]any{
				"type": []string{"number", "null"}, "minimum": 0, "maximum": 1,
			},
			"evidence_quality_band": map[string]any{
				"type": []string{"string", "null"},
				// Python's [*band_keys, None] -- the enum's LAST member is
				// JSON null, not the string "null" or "". []any (not
				// []string) is required to carry that literal nil through
				// to json.Marshal as the JSON `null` token.
				"enum": evidenceQualityBandEnum,
			},
		},
	}

	findingSchema := map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"required":             []string{"finding", "evidence"},
		"properties": map[string]any{
			"finding":  map[string]any{"type": "string", "minLength": 1, "maxLength": 500},
			"evidence": findingEvidenceSchema,
		},
	}

	bandMixProperties := make(map[string]any, len(bandKeys))
	for _, key := range bandKeys {
		bandMixProperties[key] = map[string]any{"type": "integer", "minimum": 0}
	}
	bandMixSchema := map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"required":             append([]string{}, bandKeys...),
		"properties":           bandMixProperties,
	}

	confidenceSchema := map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"required":             []string{"level", "quality_mean", "quality_stddev", "band_mix", "drivers"},
		"properties": map[string]any{
			"level":          map[string]any{"type": "string", "enum": []string{"high", "moderate", "low", "unknown"}},
			"quality_mean":   map[string]any{"type": []string{"number", "null"}, "minimum": 0, "maximum": 1},
			"quality_stddev": map[string]any{"type": []string{"number", "null"}, "minimum": 0, "maximum": 1},
			"band_mix":       bandMixSchema,
			"drivers": map[string]any{
				"type": "array", "maxItems": 10,
				"items": map[string]any{"type": "string", "minLength": 1, "maxLength": 120},
			},
		},
	}

	actionItemSchema := map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"required":             []string{"action", "why", "where"},
		"properties": map[string]any{
			"action": map[string]any{"type": "string", "minLength": 1, "maxLength": 200},
			"why":    map[string]any{"type": "string", "minLength": 1, "maxLength": 300},
			"where":  map[string]any{"type": "string", "minLength": 1, "maxLength": 200},
		},
	}

	return map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"required":             []string{"summary", "top_findings", "confidence", "what_to_check_next", "anti_claims"},
		"properties": map[string]any{
			"summary":      map[string]any{"type": "string", "minLength": 1, "maxLength": 1000},
			"top_findings": map[string]any{"type": "array", "maxItems": 10, "items": findingSchema},
			"confidence":   confidenceSchema,
			"what_to_check_next": map[string]any{
				"type": "array", "maxItems": 10, "items": actionItemSchema,
			},
			"anti_claims": map[string]any{
				"type": "array", "maxItems": 10,
				"items": map[string]any{"type": "string", "minLength": 1, "maxLength": 300},
			},
		},
	}
}

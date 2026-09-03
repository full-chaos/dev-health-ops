package categorize

import "github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"

// categorizationSystemMessage ports openai.py's system_message, narrowed to
// the categorization branch -- the only branch this package's providers
// ever need, since every prompt this package builds (prompts.go's
// BuildPrompt/BuildRepairPrompt) is a categorization prompt. The
// investment-mix-explanation branch is a different LLM use case with no Go
// port (see mockprovider.go's own comment on the same narrowing).
const categorizationSystemMessage = "You are a specialized JSON generator.\n" +
	"Return ONLY valid JSON.\n" +
	"No markdown. No commentary.\n" +
	"Output must start with { and end with }."

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

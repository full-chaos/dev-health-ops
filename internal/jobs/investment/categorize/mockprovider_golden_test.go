package categorize

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

type mockProviderGoldenQuote struct {
	Quote  string `json:"quote"`
	Source string `json:"source"`
	ID     string `json:"id"`
}

type mockProviderGoldenCase struct {
	Label            string                    `json:"label"`
	SourceBlock      string                    `json:"source_block"`
	TopCategory      string                    `json:"top_category"`
	TopWeight        float64                   `json:"top_weight"`
	EvidenceQuotes   []mockProviderGoldenQuote `json:"evidence_quotes"`
	Uncertainty      string                    `json:"uncertainty"`
	SubcategoryCount int                       `json:"subcategory_count"`
}

type mockProviderGoldenDocument struct {
	Cases []mockProviderGoldenCase `json:"cases"`
}

// mockProviderPrompt mirrors generate_mock_provider_python_golden.py's
// _prompt(): the canonical marker text that routes MockProvider.complete
// into the categorization branch, followed by the varying source block.
func mockProviderPrompt(sourceBlock string) string {
	if sourceBlock == "(EMPTY)" {
		sourceBlock = ""
	}
	const marker = "Output schema:\n{\n  \"subcategories\": {...}, \"evidence_quotes\": [...]}\n"
	return marker + "\nSource text (quotes must be exact substrings):\n" + sourceBlock
}

func loadMockProviderGolden(t *testing.T) mockProviderGoldenDocument {
	t.Helper()
	path := filepath.Join(categorizeRepositoryRoot(t), "tests", "fixtures", "mock_provider_python_golden.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v", path, err)
	}
	var doc mockProviderGoldenDocument
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("unmarshal golden: %v", err)
	}
	return doc
}

// TestMockProviderCompleteMatchesPythonGolden proves the Go MockProvider
// picks the same top category and evidence quote as mock.py's
// MockProvider for every case in the frozen golden -- STRUCTURAL parity,
// not byte-exact (see the generator's own docstring: MockProvider is
// dev/test-only, never a real provider or a hash input).
func TestMockProviderCompleteMatchesPythonGolden(t *testing.T) {
	doc := loadMockProviderGolden(t)
	if len(doc.Cases) == 0 {
		t.Fatal("golden carries zero cases -- a vacuous corpus proves nothing")
	}

	var provider MockProvider
	for _, testCase := range doc.Cases {
		t.Run(testCase.Label, func(t *testing.T) {
			result, err := provider.Complete(context.Background(), mockProviderPrompt(testCase.SourceBlock))
			if err != nil {
				t.Fatalf("Complete: %v", err)
			}

			var payload struct {
				Subcategories  map[string]float64        `json:"subcategories"`
				EvidenceQuotes []mockProviderGoldenQuote `json:"evidence_quotes"`
				Uncertainty    string                    `json:"uncertainty"`
			}
			if err := json.Unmarshal([]byte(result.Text), &payload); err != nil {
				t.Fatalf("Complete returned invalid JSON: %v\ntext=%s", err, result.Text)
			}

			if len(payload.Subcategories) != testCase.SubcategoryCount {
				t.Fatalf("len(subcategories) = %d, want %d", len(payload.Subcategories), testCase.SubcategoryCount)
			}

			topCategory := ""
			topWeight := -1.0
			for category, weight := range payload.Subcategories {
				if weight > topWeight {
					topCategory = category
					topWeight = weight
				}
			}
			if topCategory != testCase.TopCategory {
				t.Fatalf("top category = %q, want %q", topCategory, testCase.TopCategory)
			}
			if diff := topWeight - testCase.TopWeight; diff > 1e-9 || diff < -1e-9 {
				t.Fatalf("top weight = %v, want %v", topWeight, testCase.TopWeight)
			}

			if len(payload.EvidenceQuotes) != len(testCase.EvidenceQuotes) {
				t.Fatalf("len(evidence_quotes) = %d, want %d", len(payload.EvidenceQuotes), len(testCase.EvidenceQuotes))
			}
			for i, want := range testCase.EvidenceQuotes {
				got := payload.EvidenceQuotes[i]
				if got.Quote != want.Quote || got.Source != want.Source || got.ID != want.ID {
					t.Fatalf("evidence_quotes[%d] = %+v, want %+v", i, got, want)
				}
			}

			if payload.Uncertainty != testCase.Uncertainty {
				t.Fatalf("uncertainty = %q, want %q", payload.Uncertainty, testCase.Uncertainty)
			}
		})
	}
}

// TestMockProviderResponsePassesOwnValidator is an end-to-end sanity check
// distinct from the golden test above: every response MockProvider ever
// emits must pass THIS package's own ValidateLLMPayload with zero errors --
// proving the mock's output isn't just structurally similar to Python's,
// but actually well-formed by the port's own downstream schema, against
// the SAME handle_map/source_texts context the schema golden uses.
func TestMockProviderResponsePassesOwnValidator(t *testing.T) {
	var provider MockProvider
	sourceTexts := schemaSourceTexts()
	handleMap := schemaHandleMap()

	// Only a prompt with a real source block is exercised here -- the
	// no-source-block fallback deliberately emits a synthetic "unknown"
	// id (see mockCategorization), which is not a handle any real
	// handle_map would ever contain: BuildCategorizationPrompt never
	// hands the provider a prompt with zero source blocks in production,
	// so validating that synthetic id against this fixed handle_map
	// would test an unreachable combination, not a real one.
	prompts := []string{
		mockProviderPrompt("[issue] E1\nPlease Fix the login bug   before release. Thanks.\n"),
	}

	for _, prompt := range prompts {
		result, err := provider.Complete(context.Background(), prompt)
		if err != nil {
			t.Fatalf("Complete: %v", err)
		}

		payload, parseErrors := ParseLLMJSON(result.Text)
		if len(parseErrors) > 0 {
			t.Fatalf("ParseLLMJSON errors: %v", parseErrors)
		}

		got := ValidateLLMPayload(payload, sourceTexts, handleMap)
		if !got.OK {
			t.Fatalf("ValidateLLMPayload rejected mock response: errors=%v text=%s", got.Errors, result.Text)
		}
	}
}

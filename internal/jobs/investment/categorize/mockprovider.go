package categorize

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// mockSourceHeaders is the fixed set of source-block header tokens the
// canonical prompt's [issue]/[pr]/[commit] handles use, from
// evidence.go's sourceTypeOrder / mock.py's own {"issue", "pr", "commit"}.
var mockSourceHeaders = map[string]struct{}{"issue": {}, "pr": {}, "commit": {}}

// MockProvider is llm/providers/mock.py's MockProvider, narrowed to the
// _mock_categorization path -- the ONLY path investment.categorize's
// canonical prompt ever takes (mock.py's OTHER branch answers a different
// LLM use case, the investment-view explanation, out of scope here). A
// deterministic, dev/test-only stand-in: never a real provider, never a
// byte-exact parity target the way the prompt/schema ports are, so this
// file uses Go's plain strings.ToLower for keyword matching rather than a
// Python-parity fold -- the worst case is a different mock top_category
// pick for exotic non-ASCII input, which affects no real product path.
type MockProvider struct{}

// Complete ports mock.py's MockProvider.complete, restricted to the
// categorization branch (the canonical prompt always contains "Output
// schema", '"subcategories"' and '"evidence_quotes"' -- see prompts.go's
// canonicalPromptBody).
func (MockProvider) Complete(_ context.Context, prompt string) (CompletionResult, error) {
	text, err := mockCategorization(prompt)
	if err != nil {
		return CompletionResult{}, err
	}
	return CompletionResult{Text: text, Model: "mock"}, nil
}

// Close is a no-op, matching mock.py's MockProvider.aclose.
func (MockProvider) Close() error { return nil }

// mockCategorization ports mock.py's _mock_categorization.
func mockCategorization(prompt string) (string, error) {
	sourceType, sourceID, sourceText := firstMockSourceBlock(prompt)

	phrase := sourceText
	if phrase == "" {
		phrase = "incremental improvement"
	}

	topCategory := mockTopCategory(phrase)

	base := make(map[string]float64, len(units.SortedSubcategories))
	for _, category := range units.SortedSubcategories {
		base[category] = 1.0 / 15.0
	}
	base[topCategory] = 0.5
	remaining := 0.5 / 14.0
	for _, category := range units.SortedSubcategories {
		if category != topCategory {
			base[category] = remaining
		}
	}

	quote := pythonparity.Strip(pythonparity.TruncateRunes(phrase, 80))
	if quote == "" {
		quote = "incremental improvement"
	}

	quoteID := sourceID
	if quoteID == "" {
		quoteID = "unknown"
	}

	response := map[string]any{
		"subcategories": base,
		"evidence_quotes": []map[string]any{
			{"quote": quote, "source": sourceType, "id": quoteID},
		},
		"uncertainty": "Text evidence is limited; categorization suggests an initial interpretation.",
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

// firstMockSourceBlock ports mock.py's source-block scan: the first
// "[type] id" header line (type in issue/pr/commit, id non-empty) is
// found, then the first non-blank line AFTER it becomes the source text --
// UNLESS another header line is hit first, in which case source text stays
// empty. Only the FIRST such block in the whole prompt is used.
func firstMockSourceBlock(prompt string) (sourceType, sourceID, sourceText string) {
	sourceType = "issue"
	lines := pythonparity.SplitLines(prompt)

	for idx, rawLine := range lines {
		line := pythonparity.Strip(rawLine)
		if !strings.HasPrefix(line, "[") || !strings.Contains(line, "]") {
			continue
		}
		header, rest, found := strings.Cut(line, "]")
		if !found {
			continue
		}
		header = pythonparity.Strip(strings.Trim(header, "[]"))
		rest = pythonparity.Strip(rest)
		if _, ok := mockSourceHeaders[header]; !ok || rest == "" {
			continue
		}
		sourceType = header
		sourceID = rest
		// The real canonical prompt (evidence.go's sourceBlockLines) never
		// indents a "[type] handle" header line, so operating on the
		// STRIPPED line here (unlike mock.py, which checks the stripped
		// line's prefix but then splits the RAW, possibly-indented line --
		// an inconsistency that leaves a stray "[" in potential_header for
		// an indented header, a real but unreachable quirk given how
		// prompts are actually built) makes no observable difference on
		// any prompt this provider is ever handed.
		for _, nextLine := range lines[idx+1:] {
			stripped := pythonparity.Strip(nextLine)
			if strings.HasPrefix(stripped, "[") && strings.Contains(stripped, "]") {
				potentialHeader, _, found := strings.Cut(stripped, "]")
				if found {
					potentialHeader = pythonparity.Strip(strings.Trim(potentialHeader, "["))
					if _, ok := mockSourceHeaders[potentialHeader]; ok {
						break
					}
				}
			}
			if stripped == "" {
				continue
			}
			sourceText = stripped
			break
		}
		break
	}
	return sourceType, sourceID, sourceText
}

// mockTopCategory ports the keyword-priority chain in mock.py's
// _mock_categorization -- checked in this exact order, first match wins.
func mockTopCategory(phrase string) string {
	lowered := strings.ToLower(phrase)
	switch {
	case containsAny(lowered, "incident", "outage", "on-call", "hotfix"):
		return "operational.incident_response"
	case containsAny(lowered, "refactor", "cleanup", "chore", "upgrade"):
		return "maintenance.refactor"
	case containsAny(lowered, "bug", "fix", "test", "reliability"):
		return "quality.bugfix"
	case containsAny(lowered, "security", "vulnerability", "compliance"):
		return "risk.security"
	default:
		return "feature_delivery.customer"
	}
}

func containsAny(haystack string, tokens ...string) bool {
	for _, token := range tokens {
		if strings.Contains(haystack, token) {
			return true
		}
	}
	return false
}

var _ Provider = MockProvider{}

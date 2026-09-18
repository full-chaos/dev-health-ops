package categorize

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// mockSourceHeaders is the fixed set of source-block header tokens the
// canonical prompt's [issue]/[pr]/[commit] handles use, from
// evidence.go's sourceTypeOrder / mock.py's own {"issue", "pr", "commit"}.
var mockSourceHeaders = map[string]struct{}{"issue": {}, "pr": {}, "commit": {}}

// MockProvider is llm/providers/mock.py's MockProvider. Python's
// MockProvider.complete decides its response shape by SNIFFING the prompt
// text (does it contain "Output schema"+'"subcategories"'+
// '"evidence_quotes"', or "matching the schema"?); this port instead
// branches on CompletionRequest.ResponseFormatName, the explicit
// discriminator CompletionRequest exists to provide -- the caller already
// knows which format it asked for, so there is no need to re-derive it by
// parsing prompt text. A deterministic, dev/test-only stand-in, never a
// real provider.
//
// The EXPLANATION branch's text is byte-exact against Python's, because a
// caller puts it on the wire verbatim: the per-work-unit explanation
// parser (query-api's workunitexplain.parseLLMResponse) finds no markdown
// section headers in this JSON, falls through to its
// first-paragraph default, and so returns the whole response text as the
// explanation's own `summary` field. mockExplanation therefore encodes
// through pythonparity.MarshalPythonJSONInsertionOrder rather than
// encoding/json -- key order and `", "`/`": "` separators are observable.
// The CATEGORIZATION branch has no such caller (its consumer decodes the
// JSON), which is why this file still uses Go's plain strings.ToLower for
// that branch's keyword matching rather than a Python-parity fold -- the
// worst case there is a different mock top-category pick for exotic
// non-ASCII input, which affects no real product path.
type MockProvider struct{}

// Complete ports mock.py's MockProvider.complete's two branches:
// categorization, and the explanation shape Python returns for every
// prompt its categorization sniff does not match. Both explanation
// response formats -- the aggregate investment mix and the per-work-unit
// narrative -- select that single explanation branch, exactly as Python's
// own sniff sends both of those prompts down one `else`. Anything else
// requested falls back to the categorization shape, matching Python's own
// default for a prompt that DOES match the categorization sniff.
func (MockProvider) Complete(_ context.Context, request CompletionRequest) (CompletionResult, error) {
	if request.ResponseFormatName == investmentMixExplanationResponseFormatName ||
		request.ResponseFormatName == workUnitExplanationResponseFormatName {
		text, err := mockExplanation(request.Prompt)
		if err != nil {
			return CompletionResult{}, err
		}
		return CompletionResult{Text: text, Model: "mock"}, nil
	}
	text, err := mockCategorization(request.Prompt)
	if err != nil {
		return CompletionResult{}, err
	}
	return CompletionResult{Text: text, Model: "mock"}, nil
}

// Close is a no-op, matching mock.py's MockProvider.aclose.
func (MockProvider) Close() error { return nil }

// Model is always "mock" -- MockProvider takes no configured model (it
// never reads env or an explicit override), matching every Complete
// branch's own hardcoded CompletionResult.Model above.
func (MockProvider) Model() string { return "mock" }

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

// mockExplanation ports mock.py's MockProvider.complete's
// non-categorization branch: parse the prompt for an "Evidence Quality:
// (band)" marker and "  - category: NN.NN%" lines, keep whichever category
// scores highest (defaulting to feature_delivery.customer at 0.25 if none
// beats that), and build a canned narrative using only approved
// probabilistic language ("appears", "leans", "suggests" -- never "is",
// "was", "detected", "determined").
//
// Both the aggregate investment-mix prompt and the per-work-unit
// explanation prompt carry the "Evidence Quality: N.NN (band)" line and
// the "  - category: NN.NN%" investment-vector lines this parses, so one
// branch serves both -- the same single branch Python's sniff sends them
// both to.
func mockExplanation(prompt string) (string, error) {
	// Python: prompt.split("\n") -- a plain literal-separator split, NOT
	// splitlines() (the categorization branch below uses splitlines()
	// instead, via pythonparity.SplitLines -- the two Python methods
	// differ on \r\n/trailing-newline handling, so strings.Split(prompt,
	// "\n") is the exact match for THIS branch specifically).
	lines := strings.Split(prompt, "\n")
	evidenceQualityBand := "moderate"
	topCategory := "feature_delivery.customer"
	topScore := 0.25

	for _, rawLine := range lines {
		if strings.Contains(rawLine, "Evidence Quality:") {
			switch {
			case strings.Contains(rawLine, "(high)"):
				evidenceQualityBand = "high"
			case strings.Contains(rawLine, "(moderate)"):
				evidenceQualityBand = "moderate"
			case strings.Contains(rawLine, "(low)"):
				evidenceQualityBand = "low"
			case strings.Contains(rawLine, "(very_low)"):
				evidenceQualityBand = "very_low"
			}
		}
		if strings.Contains(rawLine, "  - ") && strings.Contains(rawLine, ":") && strings.Contains(rawLine, "%") {
			// Python: line.strip().lstrip("- ").split(":") -- lstrip("- ")
			// strips any run of '-'/' ' chars (a character SET, not a
			// literal prefix), then split(":") on every colon (not just
			// the first), silently ignoring a 3rd+ segment the same way
			// Python's parts[0]/parts[1] indexing does.
			trimmed := strings.TrimLeft(pythonparity.Strip(rawLine), "- ")
			parts := strings.Split(trimmed, ":")
			if len(parts) < 2 {
				continue
			}
			category := pythonparity.Strip(parts[0])
			scoreText := strings.TrimRight(pythonparity.Strip(parts[1]), "%")
			score, err := strconv.ParseFloat(scoreText, 64)
			if err != nil {
				// Python: except (ValueError, IndexError): pass -- a
				// malformed score line is silently ignored, keeping
				// whatever top_category/top_score was already found.
				continue
			}
			score /= 100
			if score > topScore {
				topScore = score
				topCategory = category
			}
		}
	}

	dominantTheme := topCategory
	if idx := strings.Index(topCategory, "."); idx >= 0 {
		dominantTheme = topCategory[:idx]
	}

	// Member order is mock.py's response_data literal order, and the
	// encoder is the json.dumps(value) equivalent -- see MockProvider's own
	// doc comment for the caller that puts these bytes on the wire verbatim.
	response := pythonparity.OrderedObject{
		{Key: "summary", Value: fmt.Sprintf(
			"Based on the precomputed investment view, this work unit appears to lean toward %s work.",
			topCategory,
		)},
		{Key: "dominant_themes", Value: []any{dominantTheme}},
		{Key: "key_drivers", Value: []any{
			"Structural evidence appears to contribute most significantly to the categorization.",
			"Textual phrases appear to align with the investment interpretation.",
		}},
		{Key: "operational_signals", Value: []any{
			fmt.Sprintf("Evidence quality bands indicate %s uncertainty.", evidenceQualityBand),
			"Lower-weight categories may still represent meaningful aspects of the work.",
		}},
		{Key: "confidence_note", Value: fmt.Sprintf(
			"This analysis reflects %s evidence quality. The categorization leans toward %s but may not fully capture the nuanced nature of the work.",
			evidenceQualityBand, topCategory,
		)},
	}
	encoded, err := pythonparity.MarshalPythonJSONInsertionOrder(response)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

var _ Provider = MockProvider{}

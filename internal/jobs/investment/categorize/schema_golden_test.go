package categorize

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

type parseGoldenCase struct {
	Label           string   `json:"label"`
	RawText         string   `json:"raw_text"`
	PayloadIsObject bool     `json:"payload_is_object"`
	Errors          []string `json:"errors"`
}

type validateGoldenQuote struct {
	Quote      string `json:"quote"`
	SourceType string `json:"source_type"`
	SourceID   string `json:"source_id"`
}

type validateGoldenCase struct {
	Label          string                `json:"label"`
	Payload        map[string]any        `json:"payload"`
	OK             bool                  `json:"ok"`
	Errors         []string              `json:"errors"`
	Subcategories  map[string]float64    `json:"subcategories"`
	EvidenceQuotes []validateGoldenQuote `json:"evidence_quotes"`
	Uncertainty    string                `json:"uncertainty"`
	Warnings       []string              `json:"warnings"`
}

type schemaGoldenDocument struct {
	ParseCases    []parseGoldenCase    `json:"parse_cases"`
	ValidateCases []validateGoldenCase `json:"validate_cases"`
}

func loadSchemaGolden(t *testing.T) schemaGoldenDocument {
	t.Helper()
	path := filepath.Join(categorizeRepositoryRoot(t), "tests", "fixtures", "llm_schema_python_golden.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v", path, err)
	}
	var doc schemaGoldenDocument
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("unmarshal golden: %v", err)
	}
	return doc
}

// schemaSourceTexts/schemaHandleMap mirror generate_llm_schema_python_golden.py's
// SOURCE_TEXTS/HANDLE_MAP exactly -- every validate_case in the golden was
// produced against this fixed context.
func schemaSourceTexts() map[string]map[string]string {
	return map[string]map[string]string{
		"issue":  {"i1": "Please Fix the login bug   before release. Thanks."},
		"pr":     {},
		"commit": {},
	}
}

func schemaHandleMap() map[string]units.SourceRef {
	return map[string]units.SourceRef{
		"E1": {SourceType: "issue", SourceID: "i1"},
	}
}

// TestParseLLMJSONMatchesPythonGolden is the exhaustive test for
// ParseLLMJSON.
func TestParseLLMJSONMatchesPythonGolden(t *testing.T) {
	doc := loadSchemaGolden(t)
	if len(doc.ParseCases) == 0 {
		t.Fatal("golden carries zero parse_cases -- a vacuous corpus proves nothing")
	}
	for _, testCase := range doc.ParseCases {
		t.Run(testCase.Label, func(t *testing.T) {
			payload, errors := ParseLLMJSON(testCase.RawText)
			gotIsObject := payload != nil
			if gotIsObject != testCase.PayloadIsObject {
				t.Fatalf("payload-is-object = %v, want %v", gotIsObject, testCase.PayloadIsObject)
			}
			wantHasErrors := len(testCase.Errors) > 0
			gotHasErrors := len(errors) > 0
			if gotHasErrors != wantHasErrors {
				t.Fatalf("has-errors = %v (%v), want %v (%v)", gotHasErrors, errors, wantHasErrors, testCase.Errors)
			}
		})
	}
}

// TestValidateLLMPayloadMatchesPythonGolden is the exhaustive test for
// ValidateLLMPayload: every case in the frozen golden, not a hand-picked
// subset. Errors/warnings are compared as SORTED sets (both sides sorted),
// not raw list order -- schema.go's own doc comment on the subcategories
// loop explains why: Python iterates dict insertion (JSON-source) order,
// Go iterates a deterministic sorted order, and no downstream consumer in
// this port depends on error POSITION, only presence.
func TestValidateLLMPayloadMatchesPythonGolden(t *testing.T) {
	doc := loadSchemaGolden(t)
	if len(doc.ValidateCases) == 0 {
		t.Fatal("golden carries zero validate_cases -- a vacuous corpus proves nothing")
	}
	sourceTexts := schemaSourceTexts()
	handleMap := schemaHandleMap()

	for _, testCase := range doc.ValidateCases {
		t.Run(testCase.Label, func(t *testing.T) {
			got := ValidateLLMPayload(testCase.Payload, sourceTexts, handleMap)

			if got.OK != testCase.OK {
				t.Fatalf("OK = %v, want %v (errors=%v)", got.OK, testCase.OK, got.Errors)
			}

			gotErrors := sortedCopyOf(got.Errors)
			wantErrors := sortedCopyOf(testCase.Errors)
			if !stringSlicesEqual(gotErrors, wantErrors) {
				t.Fatalf("errors = %v, want %v", gotErrors, wantErrors)
			}

			gotWarnings := sortedCopyOf(got.Warnings)
			wantWarnings := sortedCopyOf(testCase.Warnings)
			if !stringSlicesEqual(gotWarnings, wantWarnings) {
				t.Fatalf("warnings = %v, want %v", gotWarnings, wantWarnings)
			}

			if got.Uncertainty != testCase.Uncertainty {
				t.Fatalf("uncertainty = %q, want %q", got.Uncertainty, testCase.Uncertainty)
			}

			if testCase.OK {
				if len(got.Subcategories) != len(testCase.Subcategories) {
					t.Fatalf("len(subcategories) = %d, want %d", len(got.Subcategories), len(testCase.Subcategories))
				}
				for key, want := range testCase.Subcategories {
					got, ok := got.Subcategories[key]
					if !ok {
						t.Fatalf("subcategories missing key %q", key)
					}
					if diff := got - want; diff > 1e-9 || diff < -1e-9 {
						t.Fatalf("subcategories[%q] = %v, want %v", key, got, want)
					}
				}

				if len(got.EvidenceQuotes) != len(testCase.EvidenceQuotes) {
					t.Fatalf("len(evidence_quotes) = %d, want %d", len(got.EvidenceQuotes), len(testCase.EvidenceQuotes))
				}
				for i, want := range testCase.EvidenceQuotes {
					gotQuote := got.EvidenceQuotes[i]
					if gotQuote.Quote != want.Quote || gotQuote.SourceType != want.SourceType || gotQuote.SourceID != want.SourceID {
						t.Fatalf("evidence_quotes[%d] = %+v, want %+v", i, gotQuote, want)
					}
				}
			}
		})
	}
}

func sortedCopyOf(values []string) []string {
	out := make([]string, len(values))
	copy(out, values)
	sort.Strings(out)
	return out
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

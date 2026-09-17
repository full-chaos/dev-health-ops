package workunitexplain

import (
	"bytes"
	"encoding/json"
	"os"
	"reflect"
	"testing"
	"time"
)

// promptGoldenDocument is testdata/build_prompt_python_golden.json: the
// REAL extract_allowed_inputs and build_explanation_prompt, called
// directly and recorded.
//
// This is where the port's float formatting and timestamp spelling are
// pinned. The prompt interpolates three different fixed-precision specs
// (".2%" for each category share, ".2f" for the evidence-quality value,
// density and score, ".1f" for the span) plus datetime.isoformat() for
// both ends of the window -- a ".1f" where the reference writes ".2f", or
// Go's "Z" offset where the reference writes "+00:00", changes the text a
// model is asked to explain while still looking like a correct prompt.
//
// Every case carries at most ONE distinct structural evidence type on
// purpose. _summarize_evidence builds that list from a SET comprehension,
// whose iteration order CPython randomizes per process, so a case with two
// distinct types has no stable reference text to capture -- see
// summarizeEvidence's own doc comment.
type promptGoldenDocument struct {
	Cases []struct {
		Name                string             `json:"name"`
		WorkUnitID          string             `json:"work_unit_id"`
		TimeRangeStart      string             `json:"time_range_start"`
		TimeRangeEnd        string             `json:"time_range_end"`
		Categories          map[string]float64 `json:"categories"`
		EvidenceQualityVal  float64            `json:"evidence_quality_value"`
		EvidenceQualityBand string             `json:"evidence_quality_band"`
		Evidence            struct {
			Structural []map[string]any `json:"structural"`
			Contextual []map[string]any `json:"contextual"`
			Textual    []map[string]any `json:"textual"`
		} `json:"evidence"`
		// EvidenceSummary is recorded for the reader's benefit; the assertion
		// below is on the prompt text, which is what a provider receives.
		EvidenceSummary map[string]any `json:"evidence_summary"`
		Prompt          string         `json:"prompt"`
	} `json:"cases"`
}

func loadPromptGolden(t *testing.T) promptGoldenDocument {
	t.Helper()
	data, err := os.ReadFile("testdata/build_prompt_python_golden.json")
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var document promptGoldenDocument
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&document); err != nil {
		t.Fatalf("decode golden: %v", err)
	}
	if len(document.Cases) == 0 {
		t.Fatal("golden carries zero cases")
	}
	return document
}

// TestBuildExplanationPromptMatchesPythonGolden compares the prompt BYTE
// FOR BYTE, because that is what the reference sends to a provider.
func TestBuildExplanationPromptMatchesPythonGolden(t *testing.T) {
	for _, testCase := range loadPromptGolden(t).Cases {
		t.Run(testCase.Name, func(t *testing.T) {
			start, err := time.Parse(time.RFC3339Nano, testCase.TimeRangeStart)
			if err != nil {
				t.Fatalf("parse start: %v", err)
			}
			end, err := time.Parse(time.RFC3339Nano, testCase.TimeRangeEnd)
			if err != nil {
				t.Fatalf("parse end: %v", err)
			}

			// The captured case describes extract_allowed_inputs' arguments,
			// where the band and value are already normalised by
			// explain_work_unit. A band of "unknown" here is that
			// normalisation's own output, so it is passed through as a real
			// band value rather than as a null the port would re-normalise.
			band := testCase.EvidenceQualityBand
			value := testCase.EvidenceQualityVal
			themes := make([]ThemeWeight, 0, len(testCase.Categories))
			for key, weight := range testCase.Categories {
				themes = append(themes, ThemeWeight{Key: key, Value: weight})
			}

			unit := WorkUnit{
				WorkUnitID:           testCase.WorkUnitID,
				TimeRangeStart:       start,
				TimeRangeEnd:         end,
				Themes:               themes,
				EvidenceQualityValue: &value,
				EvidenceQualityBand:  &band,
				Evidence: Evidence{
					Structural: testCase.Evidence.Structural,
					Contextual: testCase.Evidence.Contextual,
					Textual:    testCase.Evidence.Textual,
				},
			}

			inputs, err := extractAllowedInputs(unit)
			if err != nil {
				t.Fatalf("extractAllowedInputs: %v", err)
			}
			got, err := buildExplanationPrompt(inputs)
			if err != nil {
				t.Fatalf("buildExplanationPrompt: %v", err)
			}
			if got != testCase.Prompt {
				t.Errorf("prompt mismatch\n got=%q\nwant=%q", got, testCase.Prompt)
			}
		})
	}
}

// TestSummarizeEvidenceTextualChainMatchesPythonGolden pins the one part
// of _summarize_evidence a reader is most likely to get wrong. Its phrase
// lookup is `item.get("phrase") or item.get("keyword") or item.get("quote")`
// and the match counts an entry when that result `is not None` -- so an
// entry whose only phrase-ish key is an EMPTY quote string still counts,
// because an `or` chain returns its LAST operand when all of them are
// falsy. The typical case's golden carries exactly that entry.
func TestSummarizeEvidenceTextualChainMatchesPythonGolden(t *testing.T) {
	for _, testCase := range loadPromptGolden(t).Cases {
		t.Run(testCase.Name, func(t *testing.T) {
			summary, err := summarizeEvidence(Evidence{
				Structural: testCase.Evidence.Structural,
				Contextual: testCase.Evidence.Contextual,
				Textual:    testCase.Evidence.Textual,
			})
			if err != nil {
				t.Fatalf("summarizeEvidence: %v", err)
			}

			reference, present := testCase.EvidenceSummary["textual"].(map[string]any)
			if !present {
				if summary.hasTextual {
					t.Fatalf("hasTextual = true, but the reference summary has no textual entry")
				}
				return
			}
			if !summary.hasTextual {
				t.Fatalf("hasTextual = false, want the reference's textual entry %v", reference)
			}
			if wantCount, ok := reference["match_count"].(float64); ok && summary.textualMatchCount != int(wantCount) {
				t.Errorf("textualMatchCount = %d, want %d", summary.textualMatchCount, int(wantCount))
			}
			var wantCategories []string
			for _, raw := range reference["categories_with_matches"].([]any) {
				wantCategories = append(wantCategories, raw.(string))
			}
			if !reflect.DeepEqual(summary.textualCategories, wantCategories) {
				t.Errorf("textualCategories = %v, want %v (dict insertion order)", summary.textualCategories, wantCategories)
			}
		})
	}
}

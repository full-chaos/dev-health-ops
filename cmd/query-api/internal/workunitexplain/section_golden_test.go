package workunitexplain

import (
	"bytes"
	"encoding/json"
	"os"
	"reflect"
	"sort"
	"testing"
)

// sectionGoldenDocument is testdata/section_extraction_python_golden.json:
// the REAL private helpers of api/services/work_unit_explain.py and
// llm/explainers/work_unit_explainer.py, called directly and recorded.
//
// These functions carry the port's highest-risk hand translation.
// _extract_section's reference pattern ends in a LOOKAHEAD
// (`(?=\n\n|\*\*|$)`) that RE2 cannot express, so extractSectionByHeader
// reimplements the bound rather than transcribing the regexp -- and the
// cases below are the ones where a plausible reimplementation goes wrong:
// a header at the very end of the text (empty result, NOT the default), a
// greedy `\s*` that runs past a blank line, `\r\n\r\n` which is not a
// `\n\n` terminator at all, and a `[:\*]*` run that has to give back its
// last two stars.
type sectionGoldenDocument struct {
	HeaderSections []struct {
		Name    string `json:"name"`
		Text    string `json:"text"`
		Header  string `json:"header"`
		Default string `json:"default"`
		Result  string `json:"result"`
	} `json:"header_sections"`
	FirstParagraphs []struct {
		Name    string `json:"name"`
		Text    string `json:"text"`
		Default string `json:"default"`
		Result  string `json:"result"`
	} `json:"first_paragraphs"`
	CategoryRationale []struct {
		Name       string             `json:"name"`
		Text       string             `json:"text"`
		Categories map[string]float64 `json:"categories"`
		Result     map[string]string  `json:"result"`
	} `json:"category_rationale"`
	EvidenceHighlights []struct {
		Name   string   `json:"name"`
		Text   string   `json:"text"`
		Result []string `json:"result"`
	} `json:"evidence_highlights"`
	Uncertainty []struct {
		Name   string `json:"name"`
		Text   string `json:"text"`
		Band   string `json:"band"`
		Result string `json:"result"`
	} `json:"uncertainty"`
	EvidenceQualityLimits []struct {
		Name   string   `json:"name"`
		Text   string   `json:"text"`
		Value  *float64 `json:"value"`
		Band   *string  `json:"band"`
		Result string   `json:"result"`
		Raises string   `json:"raises"`
	} `json:"evidence_quality_limits"`
	LanguageValidation []struct {
		Name   string   `json:"name"`
		Text   string   `json:"text"`
		Result []string `json:"result"`
	} `json:"language_validation"`
}

func loadSectionGolden(t *testing.T) sectionGoldenDocument {
	t.Helper()
	data, err := os.ReadFile("testdata/section_extraction_python_golden.json")
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var document sectionGoldenDocument
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&document); err != nil {
		t.Fatalf("decode golden: %v", err)
	}
	return document
}

func TestExtractSectionByHeaderMatchesPythonGolden(t *testing.T) {
	document := loadSectionGolden(t)
	if len(document.HeaderSections) == 0 {
		t.Fatal("golden carries zero header-section cases")
	}
	for _, testCase := range document.HeaderSections {
		t.Run(testCase.Name, func(t *testing.T) {
			if got := extractSectionByHeader(testCase.Text, testCase.Header, testCase.Default); got != testCase.Result {
				t.Errorf("= %q, want %q", got, testCase.Result)
			}
		})
	}
}

func TestExtractFirstParagraphMatchesPythonGolden(t *testing.T) {
	for _, testCase := range loadSectionGolden(t).FirstParagraphs {
		t.Run(testCase.Name, func(t *testing.T) {
			if got := extractFirstParagraph(testCase.Text, testCase.Default); got != testCase.Result {
				t.Errorf("= %q, want %q", got, testCase.Result)
			}
		})
	}
}

func TestCategoryRationaleMatchesPythonGolden(t *testing.T) {
	for _, testCase := range loadSectionGolden(t).CategoryRationale {
		t.Run(testCase.Name, func(t *testing.T) {
			keys := make([]string, 0, len(testCase.Categories))
			for key := range testCase.Categories {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			themes := make([]ThemeWeight, 0, len(keys))
			for _, key := range keys {
				themes = append(themes, ThemeWeight{Key: key, Value: testCase.Categories[key]})
			}
			value := 0.5
			band := "moderate"
			unit := WorkUnit{
				WorkUnitID:           "wu-ABC-123",
				Themes:               themes,
				EvidenceQualityValue: &value,
				EvidenceQualityBand:  &band,
			}
			explanation, err := parseLLMResponse(testCase.Text, unit)
			if err != nil {
				t.Fatalf("parseLLMResponse: %v", err)
			}
			if !reflect.DeepEqual(explanation.CategoryRationale, testCase.Result) {
				t.Errorf("= %v, want %v", explanation.CategoryRationale, testCase.Result)
			}
		})
	}
}

func TestEvidenceHighlightsMatchPythonGolden(t *testing.T) {
	for _, testCase := range loadSectionGolden(t).EvidenceHighlights {
		t.Run(testCase.Name, func(t *testing.T) {
			if got := extractEvidenceHighlights(testCase.Text); !reflect.DeepEqual(got, testCase.Result) {
				t.Errorf("= %v, want %v", got, testCase.Result)
			}
		})
	}
}

func TestExtractUncertaintyMatchesPythonGolden(t *testing.T) {
	for _, testCase := range loadSectionGolden(t).Uncertainty {
		t.Run(testCase.Name, func(t *testing.T) {
			if got := extractUncertainty(testCase.Text, testCase.Band); got != testCase.Result {
				t.Errorf("= %q, want %q", got, testCase.Result)
			}
		})
	}
}

func TestExtractEvidenceQualityLimitsMatchesPythonGolden(t *testing.T) {
	for _, testCase := range loadSectionGolden(t).EvidenceQualityLimits {
		t.Run(testCase.Name, func(t *testing.T) {
			unit := WorkUnit{EvidenceQualityValue: testCase.Value, EvidenceQualityBand: testCase.Band}
			got, err := extractEvidenceQualityLimits(testCase.Text, unit)
			if testCase.Raises != "" {
				if err == nil {
					t.Fatalf("want an error (reference raised %s), got %q", testCase.Raises, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("extractEvidenceQualityLimits: %v", err)
			}
			if got != testCase.Result {
				t.Errorf("= %q, want %q", got, testCase.Result)
			}
		})
	}
}

// TestValidateExplanationLanguageMatchesPythonGolden compares the SET of
// violations, not the order: the reference iterates a frozenset, whose
// order CPython randomizes per process, so there is no order to reproduce.
// The result is logged and never returned to a client, which is why an
// arbitrary-but-stable order is acceptable here at all.
func TestValidateExplanationLanguageMatchesPythonGolden(t *testing.T) {
	for _, testCase := range loadSectionGolden(t).LanguageValidation {
		t.Run(testCase.Name, func(t *testing.T) {
			got := append([]string(nil), validateExplanationLanguage(testCase.Text)...)
			want := append([]string(nil), testCase.Result...)
			sort.Strings(got)
			sort.Strings(want)
			if len(got) == 0 && len(want) == 0 {
				return
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("= %v, want %v", got, want)
			}
		})
	}
}

package workunitexplain

import (
	"os"
	"reflect"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/recordedpaths"
)

// CHAOS-7473: every scalar path of the recorded section golden is declared. Claims are the paths
// whose change makes a test of this package fail (backed by the mutation run in the pull request
// that added this file); the others are named with their reason.

var sectionGoldenClaims = []string{
	".category_rationale[].result.feature_delivery",
	".category_rationale[].result.feature_delivery.customer",
	".category_rationale[].result.quality",
	".category_rationale[].result.risk",
	".evidence_highlights[].result[]",
	".evidence_highlights[].text",
	".evidence_quality_limits[].band",
	".evidence_quality_limits[].result",
	".evidence_quality_limits[].text",
	".evidence_quality_limits[].value",
	".first_paragraphs[].default",
	".first_paragraphs[].result",
	".first_paragraphs[].text",
	".header_sections[].default",
	".header_sections[].header",
	".header_sections[].result",
	".header_sections[].text",
	".language_validation[].result[]",
	".language_validation[].text",
	".uncertainty[].band",
	".uncertainty[].result",
	".uncertainty[].text",
}

var sectionGoldenNotClaims = map[string]string{
	".uncertainty[].name":                                        "a case label (subtest name only)",
	".language_validation[].name":                                "a case label (subtest name only)",
	".header_sections[].name":                                    "a case label (subtest name only)",
	".first_paragraphs[].name":                                   "a case label (subtest name only)",
	".evidence_quality_limits[].name":                            "a case label (subtest name only)",
	".category_rationale[].name":                                 "a case label (subtest name only)",
	".evidence_highlights[].name":                                "a case label (subtest name only)",
	".category_rationale[].categories.feature_delivery":          "consumed only through its keys: work_unit_explain.py:253 iterates the themes dict (for category in categories) and never reads a weight; TestCategoryWeightsAreNotRead sets every weight to 0 and requires the recorded answer",
	".category_rationale[].categories.feature_delivery.customer": "consumed only through its keys: work_unit_explain.py:253 iterates the themes dict (for category in categories) and never reads a weight; TestCategoryWeightsAreNotRead sets every weight to 0 and requires the recorded answer",
	".category_rationale[].categories.quality":                   "consumed only through its keys: work_unit_explain.py:253 iterates the themes dict (for category in categories) and never reads a weight; TestCategoryWeightsAreNotRead sets every weight to 0 and requires the recorded answer",
	".category_rationale[].categories.risk":                      "consumed only through its keys: work_unit_explain.py:253 iterates the themes dict (for category in categories) and never reads a weight; TestCategoryWeightsAreNotRead sets every weight to 0 and requires the recorded answer",
	".category_rationale[].text":                                 "consumed input, not compared: an appended suffix leaves the extracted sentence unchanged; TestCategoryRationaleTextIsRead blanks it and requires the answer to change",
}

func TestEveryRecordedPathIsDeclared(t *testing.T) {
	raw, err := os.ReadFile("testdata/section_extraction_python_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	recordedpaths.Check(t, raw, sectionGoldenClaims, sectionGoldenNotClaims)
}

func categoryRationaleOf(t *testing.T, text string, themes []ThemeWeight) map[string]string {
	t.Helper()
	value := 0.5
	band := "moderate"
	explanation, err := parseLLMResponse(text, WorkUnit{
		WorkUnitID:           "wu-ABC-123",
		Themes:               themes,
		EvidenceQualityValue: &value,
		EvidenceQualityBand:  &band,
	})
	if err != nil {
		t.Fatalf("parseLLMResponse: %v", err)
	}
	out := map[string]string{}
	for key, value := range explanation.CategoryRationale.All() {
		out[key] = value
	}
	return out
}

// TestCategoryRationaleTextIsRead backs the not-a-claim `text`: the response text is the input
// the extraction reads. For every case whose recorded answer holds a sentence taken from the
// text (not one of the two fixed defaults), an empty text must change the answer.
func TestCategoryRationaleTextIsRead(t *testing.T) {
	defaults := map[string]bool{
		"Category appears in overall analysis.":          true,
		"Category leaning based on structural evidence.": true,
	}
	probed := 0
	for _, testCase := range loadSectionGolden(t).CategoryRationale {
		fromText := false
		for _, sentence := range testCase.Result.All() {
			if !defaults[sentence] {
				fromText = true
			}
		}
		if !fromText {
			continue
		}
		probed++
		var themes []ThemeWeight
		for key, value := range testCase.Categories.All() {
			themes = append(themes, ThemeWeight{Key: key, Value: value})
		}
		recorded := map[string]string{}
		for key, sentence := range testCase.Result.All() {
			recorded[key] = sentence
		}
		if reflect.DeepEqual(categoryRationaleOf(t, "", themes), recorded) {
			t.Errorf("%s: the Go answer is the recorded one with an empty response text: the text is not read", testCase.Name)
		}
	}
	if probed == 0 {
		t.Fatal("no case with a text-derived sentence: the probe proved nothing")
	}
}

// TestCategoryWeightsAreNotRead backs the not-a-claim weights: the reference iterates the theme
// keys only (work_unit_explain.py:253), so every weight set to zero must give the recorded answer.
func TestCategoryWeightsAreNotRead(t *testing.T) {
	for _, testCase := range loadSectionGolden(t).CategoryRationale {
		var themes []ThemeWeight
		for key := range testCase.Categories.All() {
			themes = append(themes, ThemeWeight{Key: key, Value: 0})
		}
		recorded := map[string]string{}
		for key, sentence := range testCase.Result.All() {
			recorded[key] = sentence
		}
		if got := categoryRationaleOf(t, testCase.Text, themes); !reflect.DeepEqual(got, recorded) {
			t.Errorf("%s: with every weight 0 the answer is %v, want the recorded %v: a weight is read", testCase.Name, got, recorded)
		}
	}
}

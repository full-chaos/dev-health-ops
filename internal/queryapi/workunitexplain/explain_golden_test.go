package workunitexplain

import (
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"

	"bytes"
	"context"
	"encoding/json"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
)

// goldenCase is one entry of testdata/explain_work_unit_python_golden.json.
// `investment` is a verbatim WorkUnitInvestment.model_dump_json() from the
// REAL Python builder, and `explanation` is what the REAL Python
// explain_work_unit returned for it; a case that instead carries `raises`
// records that the reference RAISED, which the endpoint turns into a 503.
type goldenCase struct {
	Name          string          `json:"name"`
	LLMProvider   string          `json:"llm_provider"`
	LLMModel      *string         `json:"llm_model"`
	Investment    goldenWorkUnit  `json:"investment"`
	Explanation   json.RawMessage `json:"explanation"`
	Raises        string          `json:"raises"`
	RaisesMessage string          `json:"raises_message"`
}

// goldenWorkUnit decodes a dumped WorkUnitInvestment. The members WorkUnit
// deliberately drops -- effort, work_unit_type, work_unit_name and the
// subcategory vector -- are DECLARED here and left unread rather than
// omitted, so DisallowUnknownFields stays strict: a member either plane
// starts or stops emitting fails this test instead of passing unnoticed.
// That they are unread is the allowed-input property WorkUnit's own doc
// comment states.
type goldenWorkUnit struct {
	WorkUnitID   string  `json:"work_unit_id"`
	WorkUnitType *string `json:"work_unit_type"`
	WorkUnitName *string `json:"work_unit_name"`
	TimeRange    struct {
		Start string `json:"start"`
		End   string `json:"end"`
	} `json:"time_range"`
	Effort struct {
		Metric string  `json:"metric"`
		Value  float64 `json:"value"`
	} `json:"effort"`
	Investment struct {
		Themes        pyjson.OrderedMap[float64] `json:"themes"`
		Subcategories map[string]float64         `json:"subcategories"`
	} `json:"investment"`
	EvidenceQuality struct {
		Value *float64 `json:"value"`
		Band  *string  `json:"band"`
	} `json:"evidence_quality"`
	Evidence struct {
		Textual    []map[string]any `json:"textual"`
		Structural []map[string]any `json:"structural"`
		Contextual []map[string]any `json:"contextual"`
	} `json:"evidence"`
}

// toWorkUnit sorts the theme vector by key. The reference carries it in
// dict insertion order, which a JSON object cannot preserve -- and no
// output of this package depends on it: buildExplanationPrompt sorts the
// vector itself, and parseLLMResponse writes one map entry per key whose
// value depends on the key alone.
func (g goldenWorkUnit) toWorkUnit(t *testing.T) WorkUnit {
	t.Helper()
	start, err := time.Parse(time.RFC3339Nano, g.TimeRange.Start)
	if err != nil {
		t.Fatalf("parse time_range.start %q: %v", g.TimeRange.Start, err)
	}
	end, err := time.Parse(time.RFC3339Nano, g.TimeRange.End)
	if err != nil {
		t.Fatalf("parse time_range.end %q: %v", g.TimeRange.End, err)
	}

	// The work unit's themes in the golden's document order: Python keeps
	// that order, and category_rationale follows it.
	themes := make([]ThemeWeight, 0, g.Investment.Themes.Len())
	for key, value := range g.Investment.Themes.All() {
		themes = append(themes, ThemeWeight{Key: key, Value: value})
	}

	return WorkUnit{
		WorkUnitID:           g.WorkUnitID,
		TimeRangeStart:       start,
		TimeRangeEnd:         end,
		Themes:               themes,
		EvidenceQualityValue: g.EvidenceQuality.Value,
		EvidenceQualityBand:  g.EvidenceQuality.Band,
		Evidence: Evidence{
			Textual:    g.Evidence.Textual,
			Structural: g.Evidence.Structural,
			Contextual: g.Evidence.Contextual,
		},
	}
}

func loadExplainGolden(t *testing.T) []goldenCase {
	t.Helper()
	data, err := os.ReadFile("testdata/explain_work_unit_python_golden.json")
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var document struct {
		Cases []goldenCase `json:"cases"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&document); err != nil {
		t.Fatalf("decode golden: %v", err)
	}
	if len(document.Cases) == 0 {
		t.Fatal("golden carries zero cases -- a vacuous corpus proves nothing")
	}
	return document.Cases
}

func decodeExplanation(t *testing.T, label string, data []byte) Explanation {
	t.Helper()
	var explanation Explanation
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&explanation); err != nil {
		t.Fatalf("%s: decode explanation: %v\nbody=%s", label, err, data)
	}
	return explanation
}

// TestExplainWorkUnitMatchesPythonGolden replays every captured case
// through ExplainWorkUnit driven by the REAL MockProvider and compares the
// encoded response against what the real Python explain_work_unit
// returned. Both sides are decoded with DisallowUnknownFields, so a field
// either plane stopped emitting fails the comparison rather than being
// quietly ignored.
//
// The `summary` field is where this test earns its keep. The mock's
// response carries no markdown section headers, so _parse_llm_response
// finds no SUMMARY section, falls through to its first-paragraph default
// and returns the WHOLE completion text -- which means the mock's own JSON
// bytes, separators and key order included, are part of this route's wire
// contract. Nothing else in either plane puts a provider's raw text on the
// wire like this.
func TestExplainWorkUnitMatchesPythonGolden(t *testing.T) {
	for _, testCase := range loadExplainGolden(t) {
		t.Run(testCase.Name, func(t *testing.T) {
			unit := testCase.Investment.toWorkUnit(t)
			model := ""
			if testCase.LLMModel != nil {
				model = *testCase.LLMModel
			}

			kind, err := categorize.ResolveProviderKind(testCase.LLMProvider)
			if err != nil {
				t.Fatalf("ResolveProviderKind(%q): %v", testCase.LLMProvider, err)
			}
			explanation, err := ExplainWorkUnit(
				context.Background(), nil, mockProviderComplete, unit,
				Options{
					OrgID: "org-ABC-123", LLMProvider: testCase.LLMProvider, LLMModel: model,
					ResolvedProviderKind: kind, Now: time.Unix(0, 0).UTC(),
				},
			)

			if testCase.Raises != "" {
				if err == nil {
					t.Fatalf("want an error (reference raised %s: %s), got explanation %+v",
						testCase.Raises, testCase.RaisesMessage, explanation)
				}
				return
			}
			if err != nil {
				t.Fatalf("ExplainWorkUnit: %v", err)
			}

			var encoded bytes.Buffer
			if err := WriteJSON(&encoded, explanation); err != nil {
				t.Fatalf("WriteJSON: %v", err)
			}
			got := decodeExplanation(t, "candidate", encoded.Bytes())
			want := decodeExplanation(t, "reference", testCase.Explanation)
			if !reflect.DeepEqual(got, want) {
				t.Errorf("explanation mismatch\n got=%+v\nwant=%+v", got, want)
			}
		})
	}
}

// TestNullEvidenceQualityValueIsRefusedNotSubstituted pins the ONE case
// above whose reference behaviour is a raise rather than a body, so that a
// future change substituting a number for the null is a test failure
// rather than a silent divergence.
//
// A work unit whose evidence_quality is NULL, explained by a completion
// that carries no "Evidence Quality Limits" section, reaches
// _extract_evidence_quality_limits' default branch, where format(None,
// ".0%") raises TypeError -- the endpoint's own outer handler answers 503
// "Explanation unavailable". This port refuses the same case rather than
// inventing a percentage the reference never emits. This is a
// reference-plane defect, not an intended contract.
func TestNullEvidenceQualityValueIsRefusedNotSubstituted(t *testing.T) {
	band := "moderate"
	unit := WorkUnit{
		WorkUnitID:          "wu-ABC-123",
		TimeRangeStart:      time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
		TimeRangeEnd:        time.Date(2026, 8, 15, 0, 0, 0, 0, time.UTC),
		Themes:              []ThemeWeight{{Key: "quality", Value: 1}},
		EvidenceQualityBand: &band,
	}
	if _, err := ExplainWorkUnit(
		context.Background(), nil, mockProviderComplete, unit,
		Options{
			OrgID: "org-ABC-123", LLMProvider: "mock",
			ResolvedProviderKind: categorize.ProviderKindMock, Now: time.Unix(0, 0).UTC(),
		},
	); err == nil {
		t.Fatal("want an error for a null evidence_quality.value reaching the default limits branch")
	}
}

// mockProviderComplete is the test's CompleteFunc: the REAL MockProvider
// reached through the REAL WorkUnitExplanationRequest constructor, so the
// response-format discriminator this route depends on is exercised rather
// than bypassed.
func mockProviderComplete(ctx context.Context, requestedProvider, requestedModel, fullPrompt string) (
	categorize.CompletionResult, string, string, error,
) {
	kind, err := categorize.ResolveProviderKind(requestedProvider)
	if err != nil {
		return categorize.CompletionResult{}, "", "", err
	}
	result, err := categorize.MockProvider{}.Complete(ctx, categorize.WorkUnitExplanationRequest(fullPrompt))
	if err != nil {
		return categorize.CompletionResult{}, string(kind), requestedModel, err
	}
	return result, string(kind), requestedModel, nil
}

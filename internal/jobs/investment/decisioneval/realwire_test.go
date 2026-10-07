package decisioneval

import (
	"os"
	"reflect"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

// The first live smoke (2026-10-07) returned these four bodies for one real
// development fixture (4 spans: E1_1, E1_2, E2_1, E2_2). They are stored
// verbatim (no source text is in them). Every answer must parse as valid under
// the adapter, and the interpretation must give the levels the live run saw.
func TestRealWireResponsesParseAndInterpret(t *testing.T) {
	spans := []Span{{ID: "E1_1", Handle: "E1", Text: "a"}, {ID: "E1_2", Handle: "E1", Text: "b"}, {ID: "E2_1", Handle: "E2", Text: "c"}, {ID: "E2_2", Handle: "E2", Text: "d"}}
	bundle := units.TextBundle{HandleMap: map[string]units.SourceRef{"E1": {SourceType: "issue", SourceID: "i"}, "E2": {SourceType: "pr", SourceID: "p"}},
		SourceTexts: map[string]map[string]string{"issue": {"i": "a b"}, "pr": {"p": "c d"}, "commit": {}}}
	cases := []struct {
		file     string
		rubric   *Rubric
		parse    func([]byte, []ExpectedQuestion) (Typed, error)
		model    string
		input    int64
		wantLvl  map[string]int
		evidence string
	}{
		{"real-jev-v1-response.json", testRubric(t), ParseJevResponse, "jev-1.13.0", 7268, map[string]int{"maintenance.upgrade": 3, "quality.bugfix": 3, "quality.reliability": 1}, "evidence__quality"},
		{"real-jev-v1c-response.json", testRubricCompact(t), ParseJevResponse, "jev-1.13.0", 2383, map[string]int{"maintenance.debt": 1, "maintenance.upgrade": 3, "quality.bugfix": 3, "quality.reliability": 1}, "evidence"},
		{"real-decisions-v1-response.json", testRubric(t), ParseDecisionsResponse, "gpt-6-luna", 8380, map[string]int{"quality.bugfix": 3, "quality.reliability": 3}, "evidence__quality"},
		{"real-decisions-v1c-response.json", testRubricCompact(t), ParseDecisionsResponse, "gpt-6-luna", 3197, map[string]int{"maintenance.upgrade": 1, "quality.bugfix": 3, "quality.reliability": 1}, "evidence"},
	}
	for _, c := range cases {
		t.Run(c.file, func(t *testing.T) {
			body, err := os.ReadFile("testdata/" + c.file)
			if err != nil {
				t.Fatal(err)
			}
			typed, err := c.parse(body, ExpectedQuestions(c.rubric, spans))
			if err != nil {
				t.Fatal(err)
			}
			if typed.ReturnedModel != c.model || !typed.Usage.Reported || typed.Usage.InputTokens != c.input || len(typed.ResponseErrors) != 0 {
				t.Fatalf("model=%q usage=%+v errors=%v", typed.ReturnedModel, typed.Usage, typed.ResponseErrors)
			}
			for id, qa := range typed.Answers {
				if qa.Status != QAOK || qa.Degraded != "" {
					t.Fatalf("%s: %+v (the real wire must parse as valid)", id, qa)
				}
			}
			in := Interpret(c.rubric, c.rubric.Weights, c.rubric.SelectedRule, c.model, bundle, spans, typed)
			if in.State != StateOK {
				t.Fatalf("state = %s %v", in.State, in.Details)
			}
			got := map[string]int{}
			for k, l := range in.Levels {
				if l > 0 {
					got[k] = l
				}
			}
			if !reflect.DeepEqual(got, c.wantLvl) {
				t.Fatalf("levels = %v, want %v", got, c.wantLvl)
			}
			if _, ok := in.EvidenceChoices[c.evidence]; !ok {
				t.Fatalf("evidence choices = %v", in.EvidenceChoices)
			}
			// score answers carry level probabilities on both providers
			if len(in.LevelProbs) != 15 || len(in.LevelProbs["quality.bugfix"]) != 4 {
				t.Fatalf("level probabilities = %d keys", len(in.LevelProbs))
			}
		})
	}
}

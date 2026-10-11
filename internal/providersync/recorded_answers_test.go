package providersync

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// recordedAnswer is one file of testdata/provider_answers: a real answer of a
// provider, recorded on 2026-10-10 by tools/record-provider-answers (see the
// README of that directory for how and what it holds).
type recordedAnswer struct {
	Case         string          `json:"case"`
	Method       string          `json:"method"`
	Path         string          `json:"path"`
	Status       int             `json:"status"`
	ContentType  string          `json:"content_type"`
	BodyShape    json.RawMessage `json:"body_shape"`
	BodyVerbatim string          `json:"body_verbatim"`
}

func loadRecordedAnswer(t testing.TB, provider, name string) recordedAnswer {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "provider_answers", provider, name+".json"))
	if err != nil {
		t.Fatalf("the recorded answer %s/%s is missing: %v", provider, name, err)
	}
	var answer recordedAnswer
	if err := json.Unmarshal(raw, &answer); err != nil {
		t.Fatalf("the recorded answer %s/%s is not JSON: %v", provider, name, err)
	}
	return answer
}

// verbatim is the body of a NOT FOUND answer exactly as the provider sent it.
func (answer recordedAnswer) verbatim(t testing.TB) string {
	t.Helper()
	if answer.BodyVerbatim == "" {
		t.Fatalf("the recorded answer %s holds no verbatim body (only a not-found probe does)", answer.Case)
	}
	return answer.BodyVerbatim
}

// foundBody builds the body of a FOUND answer from its recorded SHAPE: every
// key of the shape is kept, a string becomes "x", a number 1, a bool false;
// fill overrides a key (at any depth) with the value a test needs. It is a body
// CONSTRUCTED FROM a recorded shape (the recording holds no value of the
// organization), never a recorded body.
func (answer recordedAnswer) foundBody(t testing.TB, fill map[string]any) string {
	t.Helper()
	var shape any
	if err := json.Unmarshal(answer.BodyShape, &shape); err != nil {
		t.Fatal(err)
	}
	out, err := json.Marshal(fillFromShape(shape, fill))
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

func fillFromShape(shape any, fill map[string]any) any {
	switch typed := shape.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, value := range typed {
			if override, ok := fill[key]; ok {
				out[key] = override
				continue
			}
			out[key] = fillFromShape(value, fill)
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for index, value := range typed {
			out[index] = fillFromShape(value, fill)
		}
		return out
	case string:
		switch typed {
		case "<number>":
			return 1
		case "<bool>":
			return false
		}
		return "x"
	}
	return shape
}

// The recorded answers of Linear for an id that does not exist, verbatim: the
// default answer of a Linear double to a question of the absence prover.
var (
	linearRecordedTeamsNotFound   = mustRecordedVerbatim("linear", "teams_filter_key_not_found")
	linearRecordedProjectNotFound = mustRecordedVerbatim("linear", "project_by_id_not_found")
)

func mustRecordedVerbatim(provider, name string) string {
	raw, err := os.ReadFile(filepath.Join("testdata", "provider_answers", provider, name+".json"))
	if err != nil {
		panic(err)
	}
	var answer recordedAnswer
	if err := json.Unmarshal(raw, &answer); err != nil || answer.BodyVerbatim == "" {
		panic("the recorded answer " + provider + "/" + name + " holds no verbatim body")
	}
	return answer.BodyVerbatim
}

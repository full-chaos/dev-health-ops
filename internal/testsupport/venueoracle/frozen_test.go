package venueoracle

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func frozenRequests() []Request {
	return []Request{{Name: "list", Method: "GET", Path: "/a"}, {Name: "create", Method: "POST", Path: "/b"}}
}

func TestFrozenRecordsThenReplaysWhatItRecorded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "golden.json")
	t.Setenv(FrozenRecordEnv, "1")
	t.Setenv(FrozenShaEnv, "abc123")
	t.Run("record", func(t *testing.T) {
		frozen := OpenFrozen(t, path)
		if !frozen.Recording() {
			t.Fatal("not recording")
		}
		answers := frozen.Responses(frozenRequests(), func(body string) string { return strings.ReplaceAll(body, "RUN-ID", "<id>") }, func() []Response {
			return []Response{{Status: 200, Headers: map[string]string{"h": "1"}, Body: "x RUN-ID"}, {Status: 201, Body: "y"}}
		})
		if answers[0].Body != "x <id>" {
			t.Fatalf("the recording holds the un-normalized body %q", answers[0].Body)
		}
		if text := frozen.Text("calls", func() string { return "GET /v1/x" }); text != "GET /v1/x" {
			t.Fatalf("text = %q", text)
		}
	})
	t.Setenv(FrozenRecordEnv, "")
	golden, err := loadFrozen(path)
	if err != nil {
		t.Fatal(err)
	}
	if golden.RecordedAt != "abc123" || !strings.Contains(golden.Recipe, "TestFrozenRecordsThenReplaysWhatItRecorded") || golden.Texts["calls"] != "GET /v1/x" {
		t.Fatalf("golden = %+v", golden)
	}
	t.Run("replay", func(t *testing.T) {
		frozen := OpenFrozen(t, path)
		if frozen.Recording() {
			t.Fatal("recording in replay")
		}
		answers := frozen.Responses(frozenRequests(), nil, func() []Response { t.Fatal("live python used in replay"); return nil })
		if len(answers) != 2 || answers[0].Status != 200 || answers[0].Body != "x <id>" || answers[1].Status != 201 {
			t.Fatalf("replayed %+v", answers)
		}
		// a copy: mutating the replay does not touch the golden
		answers[0].Body = "changed"
		if again := frozen.Responses(frozenRequests(), nil, nil); again[0].Body != "x <id>" {
			t.Fatalf("replay is not a copy: %q", again[0].Body)
		}
		if text := frozen.Text("calls", nil); text != "GET /v1/x" {
			t.Fatalf("text = %q", text)
		}
	})
}

func TestFrozenReplayRefusesADriftedRequestSet(t *testing.T) {
	golden := FrozenGolden{RecordedAt: "sha", Requests: []FrozenRequestKey{{Name: "a", Method: "GET"}, {Name: "b", Method: "POST"}},
		Responses: []Response{{Status: 200}, {Status: 200}}}
	for name, keys := range map[string][]FrozenRequestKey{
		"fewer":     {{Name: "a", Method: "GET"}},
		"more":      {{Name: "a", Method: "GET"}, {Name: "b", Method: "POST"}, {Name: "c", Method: "GET"}},
		"renamed":   {{Name: "a", Method: "GET"}, {Name: "z", Method: "POST"}},
		"method":    {{Name: "a", Method: "GET"}, {Name: "b", Method: "PUT"}},
		"reordered": {{Name: "b", Method: "POST"}, {Name: "a", Method: "GET"}},
	} {
		if _, err := golden.replay("g.json", keys); err == nil || !strings.Contains(err.Error(), "re-record") {
			t.Errorf("%s: err = %v, want a re-record refusal", name, err)
		}
	}
	if out, err := golden.replay("g.json", golden.Requests); err != nil || len(out) != 2 {
		t.Fatalf("the recorded set: %v %v", out, err)
	}
}

func TestLoadFrozenRefusesAMissingOrHollowGolden(t *testing.T) {
	dir := t.TempDir()
	if _, err := loadFrozen(filepath.Join(dir, "absent.json")); err == nil || !strings.Contains(err.Error(), "unreadable") {
		t.Errorf("missing: %v", err)
	}
	for name, content := range map[string]string{
		"not json":       "{",
		"no sha":         `{"recorded_at_sha":"","requests":[{"name":"a","method":"GET"}],"responses":[{"status":200}]}`,
		"no responses":   `{"recorded_at_sha":"s","requests":[],"responses":[]}`,
		"count mismatch": `{"recorded_at_sha":"s","requests":[{"name":"a","method":"GET"}],"responses":[{"status":200},{"status":200}]}`,
	} {
		path := filepath.Join(dir, strings.ReplaceAll(name, " ", "_")+".json")
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := loadFrozen(path); err == nil {
			t.Errorf("%s: loaded", name)
		}
	}
}

func TestFrozenTextRefusesAnUnrecordedName(t *testing.T) {
	golden := FrozenGolden{Texts: map[string]string{"calls": "", "rows": "x"}}
	if _, err := golden.text("g.json", "absent"); err == nil || !strings.Contains(err.Error(), "re-record") {
		t.Fatalf("absent name: %v", err)
	}
	// an empty recorded text is a recording, not an absence
	if text, err := golden.text("g.json", "calls"); err != nil || text != "" {
		t.Fatalf("empty text: %q %v", text, err)
	}
	if text, err := golden.text("g.json", "rows"); err != nil || text != "x" {
		t.Fatalf("rows: %q %v", text, err)
	}
}

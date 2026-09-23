package pyidna

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
)

// behaviourGoldenPath holds a slice of the live idna package's answers,
// cut from TestBehaviourMatchesLivePython (which checks it is still
// exactly what the package says), so the ordinary test run pins the port
// without Python.
const behaviourGoldenPath = "testdata/behaviour_golden.jsonl"

type behaviourGolden struct {
	Call behaviourCall   `json:"call"`
	Want behaviourResult `json:"want"`
}

// messageClass is the message up to its first code point, quoted or
// numeric detail.
func messageClass(message string) string {
	end := len(message)
	if index := strings.IndexAny(message, "'\"0123456789"); index >= 0 && index < end {
		end = index
	}
	if index := strings.Index(message, "U+"); index >= 0 && index < end {
		end = index
	}
	return message[:end]
}

func TestBehaviourMatchesGolden(t *testing.T) {
	raw, err := os.ReadFile(behaviourGoldenPath)
	if err != nil {
		t.Fatal(err)
	}
	failures, total := 0, 0
	for _, line := range bytes.Split(bytes.TrimSpace(raw), []byte("\n")) {
		var c behaviourGolden
		if err := json.Unmarshal(line, &c); err != nil {
			t.Fatal(err)
		}
		total++
		expected := c.Want
		if expected.OK == nil && expected.Kind == "" {
			expected.OK = []rune{}
		}
		if got := goBehaviour(c.Call); fmt.Sprint(got) != fmt.Sprint(expected) {
			failures++
			if failures <= 10 {
				t.Errorf("%s(%U): go %+v, python %+v", c.Call.Fn, c.Call.Text, got, expected)
			}
		}
	}
	if total < 500 {
		t.Fatalf("golden holds %d cases; it was cut short", total)
	}
	if failures > 0 {
		t.Fatalf("%d of %d golden cases differ", failures, total)
	}
}

func checkGoldenLines(t *testing.T, values []behaviourGolden, regenerate bool) {
	t.Helper()
	var rendered []byte
	for _, value := range values {
		line, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		rendered = append(append(rendered, line...), '\n')
	}
	if regenerate {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(behaviourGoldenPath, rendered, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	committed, err := os.ReadFile(behaviourGoldenPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(committed, rendered) {
		t.Fatalf("%s differs from the live answers; regenerate with DEV_HEALTH_REGENERATE_TABLES=1", behaviourGoldenPath)
	}
}

package sessionscenario

import (
	"strings"
	"testing"
)

func TestQuirkTable(t *testing.T) {
	if problems := CheckQuirks(Quirks, GoldenAnswers(t)); len(problems) > 0 {
		t.Fatalf("%d quirk table problems:\n%s", len(problems), strings.Join(problems, "\n"))
	}
}

func TestCheckQuirksRefusals(t *testing.T) {
	answers := []GoldenAnswer{
		{"a: boom", 500, `{"detail":"Internal Server Error"}`},
		{"b: echo", 422, `{"detail":[{"type":"missing","loc":["body","x"],"msg":"Field required","input":{}}]}`},
		{"c: fine", 200, `{}`},
	}
	good := []Quirk{
		{ID: "q1", Class: Accidental, Reason: "r", Steps: []Step{{"a: boom", 500}}},
		{ID: "q2", Class: Unsafe, Reason: "r", Detector: "validation-echo"},
	}
	if p := CheckQuirks(good, answers); len(p) != 0 {
		t.Fatalf("good table refused: %v", p)
	}
	for name, table := range map[string][]Quirk{
		"unclassed 500":  {good[1]},
		"unclassed echo": {good[0]},
		"bad class":      {{ID: "q1", Class: "odd", Reason: "r", Steps: []Step{{"a: boom", 500}}}, good[1]},
		"no reason":      {{ID: "q1", Class: Accidental, Steps: []Step{{"a: boom", 500}}}, good[1]},
		"stale name":     {good[0], good[1], {ID: "q3", Class: Contractual, Reason: "r", Steps: []Step{{"zzz", 200}}}},
		"repeated id":    {good[0], good[1], good[0]},
		"blanket 5xx":    {{ID: "q1", Class: Accidental, Reason: "r", Detector: "server-error"}, good[1]},
		"covers nothing": {good[0], good[1], {ID: "q4", Class: Accidental, Reason: "r"}},
	} {
		if len(CheckQuirks(table, answers)) == 0 {
			t.Errorf("%s: not refused", name)
		}
	}
}

func TestCheckQuirksNamesAreExact(t *testing.T) {
	answers := []GoldenAnswer{{"a: boom", 500, `{}`}, {"a: boom, new", 500, `{}`}}
	table := []Quirk{{ID: "q1", Class: Accidental, Reason: "r", Steps: []Step{{"a: boom", 500}}}}
	if len(CheckQuirks(table, answers)) == 0 {
		t.Error("a new 5xx step whose name extends a classed step's name was silently accepted")
	}
}

func TestValidationEchoWithoutLocIsDetected(t *testing.T) {
	answers := []GoldenAnswer{{"a: echo", 422, `{"detail":[{"type":"value_error","msg":"bad","input":"secret"}]}`}}
	if len(CheckQuirks(nil, answers)) == 0 {
		t.Error("a validation echo with no loc member was silently accepted")
	}
	ctx := []GoldenAnswer{{"a: ctx", 422, `{"detail":[{"type":"json_invalid","ctx":{"error":"x"}}]}`}}
	if len(CheckQuirks(nil, ctx)) == 0 {
		t.Error("a validation ctx echo with no loc member was silently accepted")
	}
}

func TestCheckQuirksStatusIsPinned(t *testing.T) {
	table := []Quirk{{ID: "q1", Class: Contractual, Reason: "r", Steps: []Step{{"a: ok", 200}}}}
	if p := CheckQuirks(table, []GoldenAnswer{{"a: ok", 200, `{}`}}); len(p) != 0 {
		t.Fatalf("good table refused: %v", p)
	}
	if len(CheckQuirks(table, []GoldenAnswer{{"a: ok", 500, `{}`}})) == 0 {
		t.Error("a classed step that changed from 200 to 500 was silently accepted")
	}
}

func TestCheckQuirksDetectorEntryNeedsAHit(t *testing.T) {
	table := []Quirk{{ID: "q1", Class: Unsafe, Reason: "r", Detector: "validation-echo"}}
	if len(CheckQuirks(table, []GoldenAnswer{{"a: ok", 200, `{}`}})) == 0 {
		t.Error("a detector entry with no matching golden answer was silently accepted")
	}
}

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
		{ID: "q1", Class: Accidental, Reason: "r", Names: []string{"a:"}},
		{ID: "q2", Class: Unsafe, Reason: "r", Detector: "validation-echo"},
	}
	if p := CheckQuirks(good, answers); len(p) != 0 {
		t.Fatalf("good table refused: %v", p)
	}
	for name, table := range map[string][]Quirk{
		"unclassed 500":  {good[1]},
		"unclassed echo": {good[0]},
		"bad class":      {{ID: "q1", Class: "odd", Reason: "r", Names: []string{"a:"}}, good[1]},
		"no reason":      {{ID: "q1", Class: Accidental, Names: []string{"a:"}}, good[1]},
		"stale name":     {good[0], good[1], {ID: "q3", Class: Contractual, Reason: "r", Names: []string{"zzz"}}},
		"repeated id":    {good[0], good[1], good[0]},
		"blanket 5xx":    {{ID: "q1", Class: Accidental, Reason: "r", Detector: "server-error"}, good[1]},
		"covers nothing": {good[0], good[1], {ID: "q4", Class: Accidental, Reason: "r"}},
	} {
		if len(CheckQuirks(table, answers)) == 0 {
			t.Errorf("%s: not refused", name)
		}
	}
}

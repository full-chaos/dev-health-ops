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

// wantClasses repeats the class of every table entry on purpose: reclassing a quirk changes the contract the
// reimplementation is held to, so it takes an edit here as well as in the table.
var wantClasses = map[string]QuirkClass{
	"validation-echo":                     Unsafe,
	"lone-surrogate-500":                  Accidental,
	"origin-urlparse-500":                 Accidental,
	"non-ascii-signature-500":             Accidental,
	"forged-claim-type-500":               Accidental,
	"unknown-org-500":                     Accidental,
	"password-over-72-bytes-register-500": Accidental,
	"provider-payload-shape-500":          Accidental,
	"provider-not-configured-503":         Contractual,
	"social-link-by-email":                Unsafe,
	"refresh-grace-window":                Contractual,
	"display-name-email-login":            Accidental,
}

func TestQuirkClassesArePinned(t *testing.T) {
	seen := map[string]bool{}
	for _, q := range Quirks {
		seen[q.ID] = true
		want, ok := wantClasses[q.ID]
		if !ok {
			t.Errorf("quirk %s has no pinned class: add it to wantClasses", q.ID)
		} else if q.Class != want {
			t.Errorf("quirk %s is %s, pinned %s", q.ID, q.Class, want)
		}
	}
	for id := range wantClasses {
		if !seen[id] {
			t.Errorf("pinned quirk %s is not in the table", id)
		}
	}
}

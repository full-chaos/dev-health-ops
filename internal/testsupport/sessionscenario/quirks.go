package sessionscenario

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// QuirkClass says what a recorded behaviour that differs from a clean contract is.
type QuirkClass string

const (
	// Contractual behaviour clients depend on: a reimplementation keeps it.
	Contractual QuirkClass = "contractual"
	// Accidental behaviour no client can depend on: a reimplementation may answer cleanly instead.
	Accidental QuirkClass = "accidental"
	// Unsafe behaviour a reimplementation must not keep.
	Unsafe QuirkClass = "unsafe"
)

// Step is one golden step a quirk covers, by exact name and recorded status: a step whose status changes is a
// different behaviour and needs its own entry.
type Step struct {
	Name   string
	Status int
}

// Quirk is one classed behaviour.
// Detector names a mechanical detector whose every hit the quirk covers (see Detectors).
type Quirk struct {
	ID       string
	Class    QuirkClass
	Reason   string
	Steps    []Step
	Detector string
}

// GoldenAnswer is one frozen answer of either corpus golden.
type GoldenAnswer struct {
	Name   string
	Status int
	Body   string
}

// Detectors find recorded behaviour that is a quirk by its shape. A hit must be covered by a Quirk that
// names the step; only a detector in blanketDetectors may be covered as a whole.
var Detectors = map[string]func(GoldenAnswer) bool{
	"server-error":    func(a GoldenAnswer) bool { return a.Status >= 500 },
	"validation-echo": echoesInput,
}

var blanketDetectors = map[string]bool{"validation-echo": true}

// CheckQuirks returns every way table fails to classify answers: an unknown class, an empty reason, a
// repeated id, an entry that covers no golden step, and a detector hit no entry covers.
func CheckQuirks(table []Quirk, answers []GoldenAnswer) []string {
	var problems []string
	seen := map[string]bool{}
	for _, q := range table {
		switch {
		case q.ID == "":
			problems = append(problems, "a quirk has no id")
			continue
		case seen[q.ID]:
			problems = append(problems, q.ID+": id repeated")
		}
		seen[q.ID] = true
		if q.Class != Contractual && q.Class != Accidental && q.Class != Unsafe {
			problems = append(problems, fmt.Sprintf("%s: class %q is not contractual, accidental or unsafe", q.ID, q.Class))
		}
		if strings.TrimSpace(q.Reason) == "" {
			problems = append(problems, q.ID+": no reason")
		}
		if q.Detector != "" && (Detectors[q.Detector] == nil || !blanketDetectors[q.Detector]) {
			problems = append(problems, fmt.Sprintf("%s: detector %q is unknown or cannot be covered as a whole", q.ID, q.Detector))
		}
		if len(q.Steps) == 0 && q.Detector == "" {
			problems = append(problems, q.ID+": covers nothing")
		}
		for _, step := range q.Steps {
			if !anyStep(answers, step) {
				problems = append(problems, fmt.Sprintf("%s: %q with status %d is no golden step", q.ID, step.Name, step.Status))
			}
		}
		if q.Detector != "" && Detectors[q.Detector] != nil && !anyHit(answers, Detectors[q.Detector]) {
			problems = append(problems, fmt.Sprintf("%s: detector %q hits no golden step", q.ID, q.Detector))
		}
	}
	detectors := make([]string, 0, len(Detectors))
	for name := range Detectors {
		detectors = append(detectors, name)
	}
	sort.Strings(detectors)
	for _, answer := range answers {
		for _, name := range detectors {
			if Detectors[name](answer) && !covered(table, name, answer) {
				problems = append(problems, fmt.Sprintf("unclassed %s: %q", name, answer.Name))
			}
		}
	}
	return problems
}

func anyStep(answers []GoldenAnswer, step Step) bool {
	for _, a := range answers {
		if a.Name == step.Name && a.Status == step.Status {
			return true
		}
	}
	return false
}

func anyHit(answers []GoldenAnswer, hit func(GoldenAnswer) bool) bool {
	for _, a := range answers {
		if hit(a) {
			return true
		}
	}
	return false
}

func covered(table []Quirk, detector string, answer GoldenAnswer) bool {
	for _, q := range table {
		if q.Detector == detector {
			return true
		}
		for _, step := range q.Steps {
			if step == (Step{answer.Name, answer.Status}) {
				return true
			}
		}
	}
	return false
}

// echoesInput is true when the body holds a validation error that echoes submitted input or ctx: an object
// with a "type" member and an "input" or "ctx" member, at any depth.
func echoesInput(a GoldenAnswer) bool {
	var value any
	if json.Unmarshal([]byte(a.Body), &value) != nil {
		return false
	}
	return hasEcho(value)
}

func hasEcho(value any) bool {
	switch v := value.(type) {
	case map[string]any:
		_, typed := v["type"]
		_, input := v["input"]
		_, ctx := v["ctx"]
		if typed && (input || ctx) {
			return true
		}
		for _, member := range v {
			if hasEcho(member) {
				return true
			}
		}
	case []any:
		for _, member := range v {
			if hasEcho(member) {
				return true
			}
		}
	}
	return false
}

// authFlowGoldenPath is the auth-flow venue golden, frozen beside its oracle.
func authFlowGoldenPath() string {
	_, file, _, _ := moduleroot.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..", "apiservice", "authflowvenue", "testdata", "golden", "auth-flow-venue-oracle.json")
}

// GoldenAnswers are the answers of both corpus goldens: the session scenario's and the auth-flow venue's.
func GoldenAnswers(t *testing.T) []GoldenAnswer {
	t.Helper()
	var out []GoldenAnswer
	for _, a := range LoadGolden(t).Answers {
		out = append(out, GoldenAnswer{a.Name, a.Status, a.Body})
	}
	raw, err := os.ReadFile(authFlowGoldenPath())
	if err != nil {
		t.Fatal(err)
	}
	var flow struct {
		Requests []struct {
			Name   string `json:"name"`
			Status int    `json:"status"`
			Body   string `json:"body"`
		} `json:"requests"`
	}
	if err := json.Unmarshal(raw, &flow); err != nil {
		t.Fatal(err)
	}
	if len(flow.Requests) < 100 {
		t.Fatalf("auth-flow golden holds %d answers; it was cut short", len(flow.Requests))
	}
	for _, r := range flow.Requests {
		out = append(out, GoldenAnswer{r.Name, r.Status, r.Body})
	}
	return out
}

package corpusmatrix

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/sessionscenario"
)

// Class is the path class of one recorded step.
type Class string

const (
	Success           Class = "success"
	Denied            Class = "denied"
	Malformed         Class = "malformed"
	Expired           Class = "expired"
	Replayed          Class = "replayed"
	Concurrent        Class = "concurrent"
	DependencyFailure Class = "dependency failure"
)

// Classes is every path class a journey is checked against.
var Classes = []Class{Success, Denied, Malformed, Expired, Replayed, Concurrent, DependencyFailure}

// Step is one frozen golden step.
type Step struct {
	Corpus string
	Name   string
	Status int
}

// Kind says why a cell with no recorded step is accepted.
type Kind string

const (
	// NotApplicable: the journey has no such path (Reason says why).
	NotApplicable Kind = "not-applicable"
	// Gap: the path exists and is not recorded; Leaf is the issue that records it.
	Gap Kind = "gap"
)

// Cell names one journey and one path class.
type Cell struct {
	Journey string
	Class   Class
}

// Classification accepts an empty cell.
type Classification struct {
	Kind   Kind
	Reason string
	Leaf   string
}

var leafPattern = regexp.MustCompile(`^CHAOS-[0-9]+$`)

// journeyPrefixes maps the part of a step name before the colon to its journey.
var journeyPrefixes = map[string]string{
	"login": "login", "lockout": "login",
	"validate": "validate", "me": "me", "me organizations": "me",
	"switch-org": "switch-org", "logout": "logout", "refresh": "refresh", "social": "social",
	"register": "register", "verify": "verify", "resend": "resend",
	"forgot": "recovery", "reset": "recovery", "accept": "invite",
	"onboard": "onboarding", "skip": "onboarding", "state": "onboarding",
	"rate limit": "limits", "burst": "limits", "limit": "limits",
}

// Journeys is every journey of the corpus.
var Journeys = []string{"login", "validate", "me", "switch-org", "logout", "refresh", "social",
	"register", "verify", "resend", "recovery", "invite", "onboarding", "limits"}

func prefixOf(name string) string {
	prefix, _, _ := strings.Cut(name, ":")
	return prefix
}

// ClassOf is the path class of a step, from its name and recorded status. The first rule that matches wins.
func ClassOf(s Step) Class {
	name := strings.ToLower(s.Name)
	has := func(words ...string) bool {
		for _, w := range words {
			if strings.Contains(name, w) {
				return true
			}
		}
		return false
	}
	switch {
	case s.Status == 503 || has("unavailable", "transient", "outage", "dependency"):
		return DependencyFailure
	case has("reuse", "replay", "again", "original", "already accepted", "already used", "used twice"):
		return Replayed
	case has("race", "concurrent"):
		return Concurrent
	case has("expired", "stale", "aged", "lock expired"):
		return Expired
	case s.Status == 400 || s.Status == 405 || s.Status == 422 || s.Status >= 500 ||
		has("malformed", "garbage", "not json", "invalid", "missing", "empty", "forged", "surrogate", "not a uuid", "not an object", "no body", "body is a list"):
		return Malformed
	case s.Status >= 200 && s.Status < 300:
		return Success
	default:
		return Denied
	}
}

// JourneyOf is the journey of a step, or "" when its prefix maps to none.
func JourneyOf(s Step) string { return journeyPrefixes[prefixOf(s.Name)] }

// Check returns every way the table fails to account for steps.
func Check(journeys []string, prefixes map[string]string, classified map[Cell]Classification, steps []Step) []string {
	var problems []string
	known := map[string]bool{}
	for _, j := range journeys {
		known[j] = true
	}
	recorded := map[Cell]int{}
	for _, s := range steps {
		journey := prefixes[prefixOf(s.Name)]
		if journey == "" {
			problems = append(problems, fmt.Sprintf("%s step %q has no journey", s.Corpus, s.Name))
			continue
		}
		recorded[Cell{journey, ClassOf(s)}]++
	}
	validClass := map[Class]bool{}
	for _, c := range Classes {
		validClass[c] = true
	}
	for cell, c := range classified {
		id := fmt.Sprintf("%s/%s", cell.Journey, cell.Class)
		switch {
		case !known[cell.Journey]:
			problems = append(problems, id+": unknown journey")
			continue
		case !validClass[cell.Class]:
			problems = append(problems, id+": unknown class")
			continue
		}
		switch c.Kind {
		case NotApplicable:
			if c.Reason == "" {
				problems = append(problems, id+": not-applicable needs a reason")
			}
		case Gap:
			if !leafPattern.MatchString(c.Leaf) {
				problems = append(problems, id+": gap needs a leaf issue id (CHAOS-<n>)")
			}
			if c.Reason == "" {
				problems = append(problems, id+": gap needs a reason")
			}
		default:
			problems = append(problems, fmt.Sprintf("%s: kind %q is neither not-applicable nor gap", id, c.Kind))
		}
		if recorded[cell] > 0 {
			problems = append(problems, fmt.Sprintf("%s: classified but recorded by %d step(s); drop the classification", id, recorded[cell]))
		}
	}
	for _, j := range journeys {
		for _, c := range Classes {
			cell := Cell{j, c}
			if _, ok := classified[cell]; !ok && recorded[cell] == 0 {
				problems = append(problems, fmt.Sprintf("%s/%s: no recorded step and no classification", j, c))
			}
		}
	}
	sort.Strings(problems)
	return problems
}

// Steps reads both goldens.
func Steps(t *testing.T) []Step {
	t.Helper()
	var steps []Step
	for _, a := range sessionscenario.LoadGolden(t).Answers {
		steps = append(steps, Step{"session", a.Name, a.Status})
	}
	_, file, _, _ := moduleroot.Caller(0)
	raw, err := os.ReadFile(filepath.Join(filepath.Dir(file), "..", "..", "apiservice", "authflowvenue", "testdata", "golden", "auth-flow-venue-oracle.json"))
	if err != nil {
		t.Fatal(err)
	}
	var venue struct {
		Requests []struct {
			Name   string `json:"name"`
			Status int    `json:"status"`
		} `json:"requests"`
	}
	if err := json.Unmarshal(raw, &venue); err != nil {
		t.Fatal(err)
	}
	for _, r := range venue.Requests {
		steps = append(steps, Step{"venue", r.Name, r.Status})
	}
	return steps
}

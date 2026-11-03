package corpusmatrix

import (
	"fmt"
	"sort"
)

// Pin names the golden row that proves a not-applicable Denied cell: the route answers 200 (or 400, which the malformed
// class records) to a credential or address another route would refuse with 401 or 403, so the cell has no refusal to record.
type Pin struct {
	Corpus string
	Name   string
	Status int
}

// PinnedDenied proves, per journey, why Classified accepts the Denied cell as not applicable.
var PinnedDenied = map[Cell]Pin{
	{"validate", Denied}: {"session", "validate: garbage", 200},
	{"logout", Denied}:   {"session", "logout: anonymous garbage", 200},
	{"recovery", Denied}: {"venue", "forgot: unknown address", 200},
	{"resend", Denied}:   {"venue", "resend: unknown address", 200},
	{"verify", Denied}:   {"venue", "verify: signed, no such token", 400},
}

// CheckPins returns every way the pins fail to prove their cells: a pin on a cell that is not classified not-applicable,
// a not-applicable Denied cell with no pin, or a pinned row missing from the goldens or recorded with another status.
func CheckPins(pins map[Cell]Pin, classified map[Cell]Classification, steps []Step) []string {
	var problems []string
	rows := map[Pin]bool{}
	for _, s := range steps {
		rows[Pin{s.Corpus, s.Name, s.Status}] = true
	}
	for cell, pin := range pins {
		id := fmt.Sprintf("%s/%s", cell.Journey, cell.Class)
		if c, ok := classified[cell]; !ok || c.Kind != NotApplicable {
			problems = append(problems, id+": pinned but not classified not-applicable")
		}
		if !rows[pin] {
			problems = append(problems, fmt.Sprintf("%s: pinned row %s %q is not in the golden with status %d", id, pin.Corpus, pin.Name, pin.Status))
		}
	}
	for cell, c := range classified {
		if _, ok := pins[cell]; cell.Class == Denied && c.Kind == NotApplicable && !ok {
			problems = append(problems, fmt.Sprintf("%s/%s: not-applicable Denied needs a pinned golden row", cell.Journey, cell.Class))
		}
	}
	sort.Strings(problems)
	return problems
}

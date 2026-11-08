package shadowvenue

import (
	"errors"
	"fmt"
	"strings"
)

const throwawayHeader = "kind\tlabel\tuuid"

// ThrowawayIDs are the ids of the throwaway pool, frozen before the run: its orgs and their principals. A destructive
// slot may send no other id.
type ThrowawayIDs struct{ ids map[string]bool }

// ParseThrowawayIDs reads the frozen throwaway ids file: a header, then one "org" or "user" line per id.
func ParseThrowawayIDs(raw []byte) (ThrowawayIDs, error) {
	lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	if len(lines) == 0 || lines[0] != throwawayHeader {
		return ThrowawayIDs{}, fmt.Errorf("throwaway ids: the first line is not %q", throwawayHeader)
	}
	out := ThrowawayIDs{ids: map[string]bool{}}
	orgs := 0
	for n, line := range lines[1:] {
		f := strings.Split(line, "\t")
		if len(f) != 3 || (f[0] != "org" && f[0] != "user") || f[1] == "" || !isUUID(f[2]) {
			return ThrowawayIDs{}, fmt.Errorf("throwaway ids: line %d: want org|user, a label and a uuid", n+2)
		}
		id := strings.ToLower(f[2])
		if out.ids[id] || id == MissingID {
			return ThrowawayIDs{}, fmt.Errorf("throwaway ids: line %d: id %s twice or the missing id", n+2, id)
		}
		out.ids[id] = true
		if f[0] == "org" {
			orgs++
		}
	}
	if orgs == 0 {
		return ThrowawayIDs{}, errors.New("throwaway ids: no org")
	}
	return out, nil
}

// Holds refuses a path or body that holds a UUID which is neither a frozen throwaway id nor MissingID.
func (t ThrowawayIDs) Holds(texts ...string) error {
	if len(t.ids) == 0 {
		return errors.New("no frozen throwaway ids")
	}
	for _, text := range texts {
		for _, id := range anyUUID.FindAllString(text, -1) {
			if id = strings.ToLower(id); id != MissingID && !t.ids[id] {
				return fmt.Errorf("id %s is not a frozen throwaway id", id)
			}
		}
	}
	return nil
}

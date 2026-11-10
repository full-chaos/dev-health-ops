package shadowvenue

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

const statesHeader = "row\tcaller\tk\tstate"

// statePlaceholder is the body placeholder of a minted single-use state: one per slot, from the states file.
const statePlaceholder = "{state}"

// States are the signed single-use states the venue minted for one run, one per slot of a state row (a row whose body
// holds {state}). The values are secrets: no error, plan line or slots file carries one.
type States struct{ byCell map[string]string }

func stateCell(row, caller string, k int) string { return row + "/" + caller + "/" + strconv.Itoa(k) }

// ParseStates reads a states file: a header, then one line per slot. A cell twice, or one state value for two cells
// (a reused state, which the real path refuses), refuses the file.
func ParseStates(raw []byte) (States, error) {
	lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	if len(lines) == 0 || lines[0] != statesHeader {
		return States{}, fmt.Errorf("states: the first line is not %q", statesHeader)
	}
	out := States{byCell: map[string]string{}}
	used := map[string]bool{}
	for n, line := range lines[1:] {
		f := strings.Split(line, "\t")
		if len(f) != 4 || f[0] == "" || !slices.Contains(cellCallers, f[1]) || f[3] == "" {
			return States{}, fmt.Errorf("states: line %d: want row, caller, k and a state", n+2)
		}
		k, err := strconv.Atoi(f[2])
		if err != nil || k < 1 {
			return States{}, fmt.Errorf("states: line %d: k is not a positive number", n+2)
		}
		cell := stateCell(f[0], f[1], k)
		switch {
		case out.byCell[cell] != "":
			return States{}, fmt.Errorf("states: line %d: cell %s twice", n+2, cell)
		case used[f[3]]:
			return States{}, fmt.Errorf("states: line %d: the state of cell %s is used by another cell: a state is single-use", n+2, cell)
		}
		used[f[3]] = true
		out.byCell[cell] = f[3]
	}
	return out, nil
}

// fill puts the state of cell (row, caller, k) into body, JSON-escaped.
func (s States) fill(body, row, caller string, k int) (string, error) {
	if !strings.Contains(body, statePlaceholder) {
		return body, nil
	}
	state := s.byCell[stateCell(row, caller, k)]
	if state == "" {
		return "", fmt.Errorf("plan: row %s %s k=%d needs a minted state and the states file has none (-states)", row, caller, k)
	}
	quoted, _ := json.Marshal(state)
	out := strings.ReplaceAll(body, `"`+statePlaceholder+`"`, string(quoted))
	if strings.Contains(out, statePlaceholder) {
		return "", fmt.Errorf("plan: row %s: {state} must be a whole JSON string value of the body", row)
	}
	return out, nil
}

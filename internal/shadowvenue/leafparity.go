package shadowvenue

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"unicode"
)

// FrozenVolatile is the frozen set of leaf paths, per row, that differ between two legacy runs of the same cell. It is read
// from the file bodydiff -paths wrote, verbatim.
type FrozenVolatile struct {
	SHA12 string
	ByRow map[string]map[string]bool
}

// ParseFrozenVolatile reads the frozen file: lines "row<TAB>path", strictly ascending, nothing else (no comment, no blank
// line, no third column). An empty file is the empty set.
func ParseFrozenVolatile(raw []byte) (FrozenVolatile, error) {
	out := FrozenVolatile{ByRow: map[string]map[string]bool{}}
	text := string(raw)
	if text == "" {
		return out, nil
	}
	if !strings.HasSuffix(text, "\n") {
		return FrozenVolatile{}, errors.New("frozen volatile: the last line has no newline")
	}
	prev := ""
	for n, line := range strings.Split(strings.TrimSuffix(text, "\n"), "\n") {
		f := strings.Split(line, "\t")
		if len(f) != 2 || f[0] == "" || strings.ContainsAny(f[0], " #") || !strings.HasPrefix(f[1], "$") || strings.ContainsFunc(f[1], unicode.IsControl) {
			return FrozenVolatile{}, fmt.Errorf("frozen volatile: line %d is not row<TAB>path", n+1)
		}
		if n > 0 && line <= prev {
			return FrozenVolatile{}, fmt.Errorf("frozen volatile: line %d is not after line %d (a frozen file is sorted and unique)", n+1, n)
		}
		prev = line
		if out.ByRow[f[0]] == nil {
			out.ByRow[f[0]] = map[string]bool{}
		}
		out.ByRow[f[0]][f[1]] = true
	}
	return out, nil
}

// PathsPerRow counts the volatile paths per row, sorted by row, for the record line.
func (v FrozenVolatile) PathsPerRow() string {
	rows := make([]string, 0, len(v.ByRow))
	for row := range v.ByRow {
		rows = append(rows, row)
	}
	slices.Sort(rows)
	parts := make([]string, len(rows))
	for i, row := range rows {
		parts[i] = fmt.Sprintf("row %s: %d", row, len(v.ByRow[row]))
	}
	return strings.Join(parts, ", ")
}

// LeafParity is the per-leaf body comparison of an enforced run against its yardstick: the leaves of every sent slot of
// both runs by slot seq, and the frozen volatile set.
type LeafParity struct {
	Volatile       FrozenVolatile
	Yard, Enforced map[int][]Leaf
}

// differs compares the two answers of one cell after dropping the leaves whose path is volatile for the cell's row. missing
// is set when a side file has no line for one of the two slots.
func (p *LeafParity) differs(slot Slot, y YardSlot) (differs, missing bool) {
	a, okA := p.Yard[y.Seq]
	b, okB := p.Enforced[slot.Seq]
	if !okA || !okB {
		return false, true
	}
	drop := p.Volatile.ByRow[slot.Row]
	keep := func(leaves []Leaf) []Leaf {
		var out []Leaf
		for _, l := range leaves {
			if !drop[l.Path] {
				out = append(out, l)
			}
		}
		return out
	}
	return len(KeyDiff(keep(a), keep(b))) > 0, false
}

// DiffPathLines lists, as "row<TAB>path" lines sorted and unique, the leaf paths that differ between the two runs of a
// cell (the cells are joined by row, caller and k; the leaves by each run's own slot seq).
func DiffPathLines(a, b []Slot, leavesA, leavesB map[int][]Leaf) []string {
	type cell struct {
		row, caller string
		k           int
	}
	other := map[cell]Slot{}
	for _, s := range b {
		other[cell{s.Row, s.Caller, s.K}] = s
	}
	set := map[string]bool{}
	for _, sa := range a {
		sb, ok := other[cell{sa.Row, sa.Caller, sa.K}]
		if !ok || sa.Gap() {
			continue
		}
		for _, path := range KeyDiff(leavesA[sa.Seq], leavesB[sb.Seq]) {
			set[sa.Row+"\t"+path] = true
		}
	}
	out := make([]string, 0, len(set))
	for line := range set {
		out = append(out, line)
	}
	slices.Sort(out)
	return out
}

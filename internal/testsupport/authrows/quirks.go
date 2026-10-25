package authrows

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// Kind says why a row on the ACP target differs from the golden's.
type Kind string

const (
	// Deliberate is a difference auth-service is built to keep.
	Deliberate Kind = "deliberate"
	// Gap is a difference until the ticket in Tracked lands; the quirk is then dead and the run fails until it is removed.
	Gap Kind = "gap"
)

// rowSeparator joins the rows of venueoracle.TableRows.
const rowSeparator = " | "

// Quirk is one named, exact difference between the golden's rows and the ACP target's. Exactly one of Rewrite and
// Extra is set. It is live only when every aggregate in Needs is forwarded; a live quirk that covers no row
// fails the run.
type Quirk struct {
	ID      string
	Kind    Kind
	Reason  string
	Tracked string
	Table   string
	Needs   []Aggregate
	// Rewrite maps a line of the golden's rows to the line the ACP target holds in its place; ok is false for a
	// line the quirk does not cover.
	Rewrite func(line string) (rewritten string, ok bool)
	// Extra is true for a line only the ACP target holds.
	Extra func(line string) bool
	// Missing is true for a golden line the ACP target holds no row for: the row is counted, never silent.
	Missing func(line string) bool
	// Max is how many rows an Extra or Missing quirk may cover in one run; a row beyond it is unclassified.
	Max int
	// Exact makes the run cover exactly Max rows: fewer is a failure too.
	Exact bool
}

// CheckTable is every way table is malformed, in the order of the table.
func CheckTable(table []Quirk) []string {
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
		if q.Kind != Deliberate && q.Kind != Gap {
			problems = append(problems, fmt.Sprintf("%s: kind %q is not deliberate or gap", q.ID, q.Kind))
		}
		if strings.TrimSpace(q.Reason) == "" {
			problems = append(problems, q.ID+": no reason")
		}
		if q.Kind == Gap && strings.TrimSpace(q.Tracked) == "" {
			problems = append(problems, q.ID+": a gap names the ticket that closes it")
		}
		if !projected[q.Table] {
			problems = append(problems, fmt.Sprintf("%s: table %q has no projection", q.ID, q.Table))
		}
		if len(q.Needs) == 0 {
			problems = append(problems, q.ID+": needs no aggregate, so it would always be live")
		}
		for _, a := range q.Needs {
			if !isAggregate(a) {
				problems = append(problems, fmt.Sprintf("%s: %q is not an aggregate", q.ID, a))
			}
		}
		if q.Extra != nil && q.Max < 1 {
			problems = append(problems, q.ID+": an extra-row quirk sets Max, the rows it may cover")
		}
		if q.Missing != nil && q.Max < 1 {
			problems = append(problems, q.ID+": a missing-row quirk sets Max, the rows it may cover")
		}
		if q.Extra == nil && q.Missing == nil && q.Max != 0 {
			problems = append(problems, q.ID+": only an extra-row or missing-row quirk sets Max")
		}
		if q.Extra == nil && q.Missing == nil && q.Exact {
			problems = append(problems, q.ID+": only an extra-row or missing-row quirk sets Exact")
		}
		set := 0
		for _, present := range []bool{q.Rewrite != nil, q.Extra != nil, q.Missing != nil} {
			if present {
				set++
			}
		}
		if set != 1 {
			problems = append(problems, q.ID+": exactly one of Rewrite, Extra and Missing is set")
		}
	}
	return problems
}

func isAggregate(a Aggregate) bool {
	for _, known := range Aggregates {
		if a == known {
			return true
		}
	}
	return false
}

func (q Quirk) live(f Forwarded) bool {
	for _, a := range q.Needs {
		if !f.Has(a) {
			return false
		}
	}
	return true
}

// Ledger compares rows against the golden's under a quirk table and counts what each quirk covered.
type Ledger struct {
	forwarded Forwarded
	table     []Quirk
	covered   map[string]int
	compared  int
}

// NewLedger is a Ledger over Quirks for a run that forwarded f. A malformed table is an error.
func NewLedger(f Forwarded) (*Ledger, error) { return newLedger(f, Quirks) }

func newLedger(f Forwarded, table []Quirk) (*Ledger, error) {
	if problems := CheckTable(table); len(problems) > 0 {
		return nil, fmt.Errorf("authrows: the quirk table is malformed: %s", strings.Join(problems, "; "))
	}
	return &Ledger{forwarded: f, table: table, covered: map[string]int{}}, nil
}

// Compare returns every way the ACP target's rows differ from the golden's for table, beyond the live quirks'. A
// difference is a line held by one side more often than by the other. Empty rows on either side are a problem.
func (l *Ledger) Compare(table, golden, acp string) []string {
	l.compared++
	if golden == "" || acp == "" {
		return []string{fmt.Sprintf("%s: rows are empty (golden %d bytes, target %d bytes)", table, len(golden), len(acp))}
	}
	goldenOnly, acpOnly := difference(strings.Split(golden, rowSeparator), strings.Split(acp, rowSeparator))
	var problems []string
	for _, line := range sortedKeys(goldenOnly) {
		for goldenOnly[line] > 0 {
			covered := false
			if final, used, ok := l.rewriteChain(table, line, acpOnly); ok {
				goldenOnly[line]--
				acpOnly[final]--
				for _, id := range used {
					l.covered[id]++
				}
				covered = true
			}
			if !covered {
				for _, q := range l.table {
					if q.Table == table && q.live(l.forwarded) && q.Missing != nil && l.covered[q.ID] < q.Max && q.Missing(line) {
						goldenOnly[line]--
						l.covered[q.ID]++
						covered = true
						break
					}
				}
			}
			if !covered {
				problems = append(problems, fmt.Sprintf("%s: unclassified difference, golden only: %s", table, line))
				goldenOnly[line]--
			}
		}
	}
	for _, line := range sortedKeys(acpOnly) {
		for acpOnly[line] > 0 {
			covered := false
			for _, q := range l.table {
				if q.Table == table && q.live(l.forwarded) && q.Extra != nil && l.covered[q.ID] < q.Max && q.Extra(line) {
					acpOnly[line]--
					l.covered[q.ID]++
					covered = true
					break
				}
			}
			if !covered {
				problems = append(problems, fmt.Sprintf("%s: unclassified difference, target only: %s", table, line))
				acpOnly[line]--
			}
		}
	}
	return problems
}

// maxRewriteChain is how many rewrites may follow one another on a golden line: a row can differ for more than one
// named reason (its organization, then its revoked count), each reason its own quirk.
const maxRewriteChain = 3

// rewriteChain finds the quirks, each used at most once and in table order of discovery, whose rewrites take a line of
// the golden to a line only the ACP target holds. Every step is a live quirk of table.
func (l *Ledger) rewriteChain(table, line string, acpOnly map[string]int) (final string, used []string, ok bool) {
	var walk func(current string, taken []string) (string, []string, bool)
	walk = func(current string, taken []string) (string, []string, bool) {
		if len(taken) == maxRewriteChain {
			return "", nil, false
		}
	next:
		for _, q := range l.table {
			if q.Table != table || !q.live(l.forwarded) || q.Rewrite == nil {
				continue
			}
			for _, id := range taken {
				if id == q.ID {
					continue next
				}
			}
			rewritten, applies := q.Rewrite(current)
			if !applies {
				continue
			}
			chain := append(append([]string(nil), taken...), q.ID)
			if acpOnly[rewritten] > 0 {
				return rewritten, chain, true
			}
			if final, ids, found := walk(rewritten, chain); found {
				return final, ids, true
			}
		}
		return "", nil, false
	}
	return walk(line, nil)
}

// Dead is every live quirk that covered no row, and an empty ledger.
func (l *Ledger) Dead() []string {
	var problems []string
	if l.compared == 0 {
		problems = append(problems, "no row comparison went through the ledger")
	}
	for _, q := range l.table {
		switch {
		case !q.live(l.forwarded):
		case l.covered[q.ID] == 0:
			problems = append(problems, fmt.Sprintf("%s: a live quirk covered no row (%s)", q.ID, q.Reason))
		case q.Exact && l.covered[q.ID] != q.Max:
			problems = append(problems, fmt.Sprintf("%s: covered %d rows, the scenario holds exactly %d (%s)", q.ID, l.covered[q.ID], q.Max, q.Reason))
		}
	}
	return problems
}

// Covered is the rows each quirk covered, by id, one "id=n" per live quirk in table order.
func (l *Ledger) Covered() string {
	var parts []string
	for _, q := range l.table {
		if q.live(l.forwarded) {
			parts = append(parts, fmt.Sprintf("%s=%d", q.ID, l.covered[q.ID]))
		}
	}
	return strings.Join(parts, " ")
}

// difference is the lines each side holds more often than the other, with how many more.
func difference(golden, acp []string) (goldenOnly, acpOnly map[string]int) {
	count := map[string]int{}
	for _, line := range golden {
		count[line]++
	}
	for _, line := range acp {
		count[line]--
	}
	goldenOnly, acpOnly = map[string]int{}, map[string]int{}
	for line, n := range count {
		switch {
		case n > 0:
			goldenOnly[line] = n
		case n < 0:
			acpOnly[line] = -n
		}
	}
	return goldenOnly, acpOnly
}

func sortedKeys(m map[string]int) []string {
	out := make([]string, 0, len(m))
	for key := range m {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

// replaceOnce returns line with the one match of pattern replaced, ok false when pattern does not match.
func replaceOnce(pattern *regexp.Regexp, line, replacement string) (string, bool) {
	if !pattern.MatchString(line) {
		return "", false
	}
	return pattern.ReplaceAllString(line, replacement), true
}

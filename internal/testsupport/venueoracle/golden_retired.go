package venueoracle

import (
	"fmt"
	"sort"
	"strings"
	"testing"
)

// Retired declares answers whose Python reference a ruling retired: the
// Python plane's answer no longer states the intended behavior of the
// route, so the test pins Go's own answer (GoPin) and the rule tests own the
// contract. reason names the ruling and is written into the proof. Like
// Consumed, each answer is used once; nothing it is given is compared.
func (g *Golden) Retired(t *testing.T, reason string, answers ...Response) {
	t.Helper()
	if err := retiredReasonErr(reason); err != nil {
		t.Fatal(err)
	}
	if err := g.consume(answers); err != nil {
		t.Fatal(err)
	}
	g.noteRetired(reason, len(answers), 0)
}

// retireBound turns an answer Diff bound for comparison into a retired one.
func (g *Golden) retireBound(answer Response, reason string) {
	bound := &g.slots[answer.slot-1]
	bound.compared, bound.consumed = false, true
	g.noteRetired(reason, 1, 0)
}

// RetireRows declares a frozen row comparison retired by a ruling, as
// Retired does for answers: the snapshot counts as used and is not compared.
// A recording cannot retire a comparison, since it would write a snapshot no
// run reads.
func (g *Golden) RetireRows(t *testing.T, name, reason string) {
	t.Helper()
	if err := retiredReasonErr(reason); err != nil {
		t.Fatal(err)
	}
	g.step(t, "RetireRows", statePython, stateDiffed)
	if g.recording {
		t.Fatalf("golden %s: row comparison %q is retired (%s); a recording cannot write it", g.spec.Path, name, reason)
	}
	if _, err := g.frozenRows(name); err != nil {
		t.Fatal(err)
	}
	g.rowsUsed[name] = true
	g.noteRetired(reason, 0, 1)
}

func retiredReasonErr(reason string) error {
	if strings.TrimSpace(reason) == "" || strings.ContainsAny(reason, "\n\r") {
		return fmt.Errorf("a retired comparison needs a one-line reason naming the ruling")
	}
	return nil
}

type retiredCount struct{ answers, rows int }

func (g *Golden) noteRetired(reason string, answers, rows int) {
	if g.retired == nil {
		g.retired = map[string]retiredCount{}
	}
	count := g.retired[reason]
	count.answers += answers
	count.rows += rows
	g.retired[reason] = count
}

// retiredText is the proof's note of what was retired, empty when nothing was.
func (g *Golden) retiredText() string {
	if len(g.retired) == 0 {
		return ""
	}
	reasons := make([]string, 0, len(g.retired))
	for reason := range g.retired {
		reasons = append(reasons, reason)
	}
	sort.Strings(reasons)
	parts := make([]string, 0, len(reasons))
	for _, reason := range reasons {
		count := g.retired[reason]
		parts = append(parts, fmt.Sprintf("%d answers and %d row comparisons retired by %s", count.answers, count.rows, reason))
	}
	return "; " + strings.Join(parts, "; ")
}

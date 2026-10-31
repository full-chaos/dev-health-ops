package corpusmatrix

import (
	"strings"
	"testing"
)

func hasProblem(problems []string, want string) bool {
	for _, p := range problems {
		if strings.Contains(p, want) {
			return true
		}
	}
	return false
}

func TestMatrixEveryCellClassified(t *testing.T) {
	if problems := Check(Journeys, journeyPrefixes, Classified, Steps(t)); len(problems) > 0 {
		t.Fatalf("the coverage matrix has gaps:\n%s", strings.Join(problems, "\n"))
	}
}

func TestMatrixEveryGoldenStepHasJourney(t *testing.T) {
	for _, s := range Steps(t) {
		if JourneyOf(s) == "" {
			t.Errorf("%s step %q has no journey", s.Corpus, s.Name)
		}
	}
}

func TestMatrixGoldenStepsAreCounted(t *testing.T) {
	steps := Steps(t)
	if len(steps) < 450 {
		t.Fatalf("%d steps read from the two goldens; the readers were cut short", len(steps))
	}
}

func TestMatrixCheckNamesEveryDefect(t *testing.T) {
	steps := []Step{{"session", "login: a", 200}, {"session", "mystery: b", 200}}
	prefixes := map[string]string{"login": "login"}
	bad := map[Cell]Classification{
		{"login", Denied}:    {Kind: NotApplicable},
		{"login", Malformed}: {Kind: Gap, Reason: "r", Leaf: "not-an-id"},
		{"login", Expired}:   {Kind: "other", Reason: "r"},
		{"login", Success}:   {Kind: NotApplicable, Reason: "r"},
		{"nowhere", Success}: {Kind: NotApplicable, Reason: "r"},
	}
	problems := Check([]string{"login"}, prefixes, bad, steps)
	for _, want := range []string{
		"login/denied: not-applicable needs a reason",
		"login/malformed: gap needs a leaf",
		"login/expired: kind",
		"login/success: classified but recorded",
		"nowhere/success: unknown journey",
		"login/replayed: no recorded step and no classification",
		"mystery: b",
	} {
		if !hasProblem(problems, want) {
			t.Errorf("Check missed %q in:\n%s", want, strings.Join(problems, "\n"))
		}
	}
}

func TestMatrixClassOfReadsNameAndStatus(t *testing.T) {
	cases := []struct {
		name   string
		status int
		want   Class
	}{
		{"login: alice", 200, Success},
		{"login: alice wrong password", 401, Denied},
		{"login: malformed json", 422, Malformed},
		{"validate: expired", 401, Expired},
		{"refresh: original outside the grace window (reuse)", 401, Replayed},
		{"refresh: rotation race, first rotation", 200, Concurrent},
		{"login: store unavailable", 503, DependencyFailure},
	}
	for _, c := range cases {
		if got := ClassOf(Step{"session", c.name, c.status}); got != c.want {
			t.Errorf("%q (%d): class %q, want %q", c.name, c.status, got, c.want)
		}
	}
}

func TestMatrixDeniedNotApplicableCellsArePinned(t *testing.T) {
	if problems := CheckPins(PinnedDenied, Classified, Steps(t)); len(problems) > 0 {
		t.Fatalf("a not-applicable Denied cell is asserted, not proven:\n%s", strings.Join(problems, "\n"))
	}
}

func TestMatrixGateFailsOnTheRealTable(t *testing.T) {
	steps := Steps(t)
	copyOf := func() map[Cell]Classification {
		c := make(map[Cell]Classification, len(Classified))
		for k, v := range Classified {
			c[k] = v
		}
		return c
	}
	t.Run("a missing cell fails", func(t *testing.T) {
		table := copyOf()
		delete(table, Cell{"login", DependencyFailure})
		if p := Check(Journeys, journeyPrefixes, table, steps); !hasProblem(p, "login/dependency failure: no recorded step and no classification") {
			t.Fatalf("deleting a gap row did not fail the gate: %v", p)
		}
	})
	t.Run("a recorded cell cannot stay classified", func(t *testing.T) {
		table := copyOf()
		table[Cell{"login", Success}] = Classification{Kind: NotApplicable, Reason: "r"}
		if p := Check(Journeys, journeyPrefixes, table, steps); !hasProblem(p, "login/success: classified but recorded") {
			t.Fatalf("classifying a recorded cell did not fail the gate: %v", p)
		}
	})
	t.Run("a vanished pinned row fails", func(t *testing.T) {
		pins := map[Cell]Pin{}
		for k, v := range PinnedDenied {
			pins[k] = v
		}
		pins[Cell{"logout", Denied}] = Pin{"session", "logout: no such row", 200}
		if p := CheckPins(pins, Classified, steps); !hasProblem(p, "logout/denied: pinned row") {
			t.Fatalf("a pin on a missing row did not fail: %v", p)
		}
	})
	t.Run("an unpinned not-applicable Denied cell fails", func(t *testing.T) {
		pins := map[Cell]Pin{}
		for k, v := range PinnedDenied {
			pins[k] = v
		}
		delete(pins, Cell{"resend", Denied})
		if p := CheckPins(pins, Classified, steps); !hasProblem(p, "resend/denied: not-applicable Denied needs a pinned golden row") {
			t.Fatalf("a missing pin did not fail: %v", p)
		}
	})
}

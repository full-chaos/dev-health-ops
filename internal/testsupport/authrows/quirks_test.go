package authrows

import (
	"strings"
	"testing"
)

func TestQuirkTableIsWellFormed(t *testing.T) {
	if problems := CheckTable(Quirks); len(problems) > 0 {
		t.Fatalf("the quirk table is malformed:\n%s", strings.Join(problems, "\n"))
	}
	bad := []Quirk{
		{ID: "", Kind: Deliberate},
		{ID: "a", Kind: "other", Reason: "r", Table: TableUsers, Needs: []Aggregate{Sessions}, Extra: func(string) bool { return true }},
		{ID: "b", Kind: Deliberate, Table: TableUsers, Needs: []Aggregate{Sessions}, Extra: func(string) bool { return true }},
		{ID: "c", Kind: Gap, Reason: "r", Table: TableUsers, Needs: []Aggregate{Sessions}, Extra: func(string) bool { return true }},
		{ID: "d", Kind: Deliberate, Reason: "r", Table: "organizations", Needs: []Aggregate{Sessions}, Extra: func(string) bool { return true }},
		{ID: "e", Kind: Deliberate, Reason: "r", Table: TableUsers, Extra: func(string) bool { return true }},
		{ID: "f", Kind: Deliberate, Reason: "r", Table: TableUsers, Needs: []Aggregate{"nope"}, Extra: func(string) bool { return true }},
		{ID: "g", Kind: Deliberate, Reason: "r", Table: TableUsers, Needs: []Aggregate{Sessions}},
		{ID: "h", Kind: Deliberate, Reason: "r", Table: TableUsers, Needs: []Aggregate{Sessions}, Extra: func(string) bool { return true }},
		{ID: "g", Kind: Deliberate, Reason: "r", Table: TableUsers, Needs: []Aggregate{Sessions},
			Extra: func(string) bool { return true }, Rewrite: func(l string) (string, bool) { return l, true }},
	}
	problems := CheckTable(bad)
	for _, want := range []string{"no id", "a: kind", "b: no reason", "c: a gap names", "d: table", "e: needs no aggregate", "f: \"nope\"", "g: exactly one", "g: id repeated", "h: an extra-row quirk sets Max"} {
		found := false
		for _, p := range problems {
			found = found || strings.Contains(p, want)
		}
		if !found {
			t.Errorf("no problem containing %q in:\n%s", want, strings.Join(problems, "\n"))
		}
	}
}

func testTable() []Quirk {
	return []Quirk{
		{ID: "swap", Kind: Deliberate, Reason: "r", Table: TableUsers, Needs: []Aggregate{LocalLogin},
			Rewrite: func(line string) (string, bool) {
				return strings.Replace(line, "old", "new", 1), strings.Contains(line, "old")
			}},
		{ID: "more", Kind: Deliberate, Reason: "r", Table: TableUsers, Needs: []Aggregate{LocalLogin}, Max: 1,
			Extra: func(line string) bool { return strings.HasPrefix(line, "extra") }},
	}
}

func ledger(t *testing.T, f string) *Ledger {
	t.Helper()
	l, err := newLedger(forwarded(t, f), testTable())
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func TestQuirkTableFailsADeadEntry(t *testing.T) {
	l := ledger(t, "local_login")
	if problems := l.Compare(TableUsers, "a | b old", "a | b old"); len(problems) != 0 {
		t.Fatalf("identical rows: %v", problems)
	}
	dead := l.Dead()
	if len(dead) != 2 || !strings.Contains(dead[0], "swap") || !strings.Contains(dead[1], "more") {
		t.Fatalf("both live quirks covered nothing, Dead = %v", dead)
	}
	if problems := l.Compare(TableUsers, "a | b old", "a | b new | extra row"); len(problems) != 0 {
		t.Fatalf("both quirks apply: %v", problems)
	}
	if left := l.Dead(); len(left) != 0 {
		t.Fatalf("both quirks covered a row, Dead = %v", left)
	}
	if got := l.Covered(); got != "swap=1 more=1" {
		t.Fatalf("Covered = %q", got)
	}
}

func TestQuirkOfAnAggregateThatIsNotForwardedIsNotLive(t *testing.T) {
	l := ledger(t, "sessions")
	if problems := l.Compare(TableUsers, "a | b old", "a | b new"); len(problems) != 2 {
		t.Fatalf("a quirk that needs local_login covered a row of a run without it: %v", problems)
	}
	if dead := l.Dead(); len(dead) != 0 {
		t.Fatalf("a quirk that is not live is not dead: %v", dead)
	}
}

func TestQuirkTableFailsAnUnclassifiedDifference(t *testing.T) {
	l := ledger(t, "local_login")
	cases := []struct{ name, golden, acp string }{
		{"a changed line", "a | b old", "a | c new"},
		{"a missing line", "a | b | c", "a | b"},
		{"an extra line", "a | b", "a | b | other"},
		{"a repeated line the target holds once", "a | a", "a"},
		{"another table's quirk", "a | b old", "a | b new"},
	}
	for _, c := range cases {
		table := TableUsers
		if c.name == "another table's quirk" {
			table = TableAuditLogs
		}
		if problems := l.Compare(table, c.golden, c.acp); len(problems) == 0 {
			t.Errorf("%s: no problem", c.name)
		}
	}
}

func TestQuirkRewriteMustLandOnALineTheTargetHolds(t *testing.T) {
	l := ledger(t, "local_login")
	if problems := l.Compare(TableUsers, "b old", "something else"); len(problems) != 2 {
		t.Fatalf("a rewrite whose result the target does not hold covered a row: %v", problems)
	}
}

func TestEmptyRowsAndAnUnusedLedgerFail(t *testing.T) {
	l := ledger(t, "local_login")
	if problems := l.Compare(TableUsers, "", "a"); len(problems) == 0 {
		t.Error("empty golden rows passed")
	}
	if problems := l.Compare(TableUsers, "a", ""); len(problems) == 0 {
		t.Error("empty target rows passed")
	}
	if dead := ledger(t, "local_login").Dead(); len(dead) == 0 || !strings.Contains(dead[0], "no row comparison") {
		t.Errorf("a ledger no comparison went through passed: %v", dead)
	}
}

func TestNoRowSkipRemains(t *testing.T) {
	l := ledger(t, "local_login")
	for _, table := range []string{TableUsers, TableRefreshTokens, TableAuditLogs} {
		if problems := l.Compare(table, "a", "b"); len(problems) == 0 {
			t.Errorf("%s: a difference was not reported; the ledger has a way to pass without comparing", table)
		}
	}
}

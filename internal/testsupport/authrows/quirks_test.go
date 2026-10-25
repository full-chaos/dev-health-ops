package authrows

import (
	"fmt"
	"strconv"
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
	l, err := newLedger(forwarded(t, f), false, testTable())
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

func TestMissingQuirkCountsAGoldenOnlyRowAndFailsOverAndUnderMax(t *testing.T) {
	table := []Quirk{{ID: "gone", Kind: Deliberate, Reason: "r", Table: TableAuditLogs, Needs: []Aggregate{Credentials}, Max: 2, Exact: true,
		Missing: func(line string) bool { return strings.HasPrefix(line, "gone") }}}
	if problems := CheckTable(table); len(problems) > 0 {
		t.Fatalf("a Missing quirk is well formed: %v", problems)
	}
	for _, c := range []struct {
		golden string
		want   bool
	}{{"keep | gone1 | gone2", true}, {"keep | gone1", false}, {"keep | gone1 | gone2 | gone3", false}} {
		l, err := newLedger(forwarded(t, "credentials"), false, table)
		if err != nil {
			t.Fatal(err)
		}
		problems := append(l.Compare(TableAuditLogs, c.golden, "keep"), l.Dead()...)
		if (len(problems) == 0) != c.want {
			t.Errorf("golden %q: problems %v, want clean=%v", c.golden, problems, c.want)
		}
	}
	bad := []Quirk{
		{ID: "m1", Kind: Deliberate, Reason: "r", Table: TableAuditLogs, Needs: []Aggregate{Credentials}, Missing: func(string) bool { return true }},
		{ID: "m2", Kind: Deliberate, Reason: "r", Table: TableAuditLogs, Needs: []Aggregate{Credentials}, Max: 1, Missing: func(string) bool { return true },
			Extra: func(string) bool { return true }},
		{ID: "m3", Kind: Deliberate, Reason: "r", Table: TableAuditLogs, Needs: []Aggregate{Credentials}, Exact: true,
			Rewrite: func(l string) (string, bool) { return l, true }},
	}
	got := strings.Join(CheckTable(bad), "\n")
	for _, want := range []string{"m1: a missing-row quirk sets Max", "m2: exactly one", "m3: an exact quirk sets Max"} {
		if !strings.Contains(got, want) {
			t.Errorf("no problem containing %q in:\n%s", want, got)
		}
	}
}

func rewriteGone(max int, exact bool) []Quirk {
	return []Quirk{{ID: "swap", Kind: Deliberate, Reason: "r", Table: TableUsers, Needs: []Aggregate{LocalLogin}, Max: max, Exact: exact,
		Rewrite: func(line string) (string, bool) {
			return strings.Replace(line, "old", "new", 1), strings.Contains(line, "old")
		}}}
}

func TestRewriteQuirkHonoursMaxAndExact(t *testing.T) {
	if problems := CheckTable(rewriteGone(2, true)); len(problems) > 0 {
		t.Fatalf("a bounded Rewrite quirk is well formed: %v", problems)
	}
	for _, c := range []struct {
		n    int
		want bool
	}{{2, true}, {1, false}, {3, false}} {
		l, err := newLedger(forwarded(t, "local_login"), false, rewriteGone(2, true))
		if err != nil {
			t.Fatal(err)
		}
		var golden, acp []string
		for i := 0; i < c.n; i++ {
			golden = append(golden, fmt.Sprintf("r%d old", i))
			acp = append(acp, fmt.Sprintf("r%d new", i))
		}
		problems := append(l.Compare(TableUsers, strings.Join(golden, rowSeparator), strings.Join(acp, rowSeparator)), l.Dead()...)
		if (len(problems) == 0) != c.want {
			t.Errorf("%d rewritten rows: problems %v, want clean=%v", c.n, problems, c.want)
		}
	}
	l, _ := newLedger(forwarded(t, "local_login"), false, rewriteGone(0, false))
	if problems := l.Compare(TableUsers, "a old | b old | c old", "a new | b new | c new"); len(problems) != 0 {
		t.Fatalf("an unbounded Rewrite quirk covers every row: %v", problems)
	}
}

func TestOwnershipDecidesWhichLegsAQuirkIsLiveOn(t *testing.T) {
	for _, c := range []struct {
		owned Ownership
		leg   bool
		live  bool
	}{{"", false, true}, {"", true, true}, {NotOwned, false, true}, {NotOwned, true, false}, {OwnedOnly, false, false}, {OwnedOnly, true, true}} {
		table := rewriteGone(0, false)
		table[0].Owned = c.owned
		l, err := newLedger(forwarded(t, "local_login"), c.leg, table)
		if err != nil {
			t.Fatal(err)
		}
		problems := l.Compare(TableUsers, "a old", "a new")
		if got := len(problems) == 0; got != c.live || (strings.Contains(l.Covered(), "swap=1") != c.live) {
			t.Errorf("owned %q on owned leg %v: live %v, want %v (covered %q)", c.owned, c.leg, got, c.live, l.Covered())
		}
	}
	bad := rewriteGone(0, false)
	bad[0].Owned = "sometimes"
	if problems := CheckTable(bad); len(problems) != 1 || !strings.Contains(problems[0], "owned") {
		t.Errorf("an unknown ownership is one problem: %v", problems)
	}
}

func TestReportNamesEachQuirkFormDistinctly(t *testing.T) {
	table := []Quirk{
		{ID: "r", Kind: Deliberate, Reason: "r", Table: TableUsers, Needs: []Aggregate{LocalLogin},
			Rewrite: func(l string) (string, bool) { return strings.Replace(l, "old", "new", 1), strings.Contains(l, "old") }},
		{ID: "e", Kind: Deliberate, Reason: "r", Table: TableUsers, Needs: []Aggregate{LocalLogin}, Max: 1,
			Extra: func(l string) bool { return strings.HasPrefix(l, "extra") }},
		{ID: "m", Kind: Deliberate, Reason: "r", Table: TableUsers, Needs: []Aggregate{LocalLogin}, Max: 1,
			Missing: func(l string) bool { return strings.HasPrefix(l, "gone") }},
	}
	l, err := newLedger(forwarded(t, "local_login"), false, table)
	if err != nil {
		t.Fatal(err)
	}
	if problems := l.Compare(TableUsers, "a old | gone", "a new | extra"); len(problems) != 0 {
		t.Fatal(problems)
	}
	if got, want := l.Report(), "r=1 rewrite, e=1 extra, m=1 missing"; got != want {
		t.Fatalf("Report = %q, want %q", got, want)
	}
}

func TestAQuirkCannotBeBothMissingAndExtra(t *testing.T) {
	both := []Quirk{{ID: "x", Kind: Deliberate, Reason: "r", Table: TableUsers, Needs: []Aggregate{LocalLogin}, Max: 1,
		Missing: func(string) bool { return true }, Extra: func(string) bool { return true }}}
	if problems := CheckTable(both); len(problems) == 0 || !strings.Contains(strings.Join(problems, ";"), "exactly one") {
		t.Fatalf("a quirk setting Missing and Extra was accepted: %v", problems)
	}
}

func TestMissingQuirkWithNoGoldenRowToCoverIsDead(t *testing.T) {
	table := []Quirk{{ID: "m", Kind: Deliberate, Reason: "r", Table: TableUsers, Needs: []Aggregate{LocalLogin}, Max: 1,
		Missing: func(l string) bool { return strings.HasPrefix(l, "gone") }}}
	l, err := newLedger(forwarded(t, "local_login"), false, table)
	if err != nil {
		t.Fatal(err)
	}
	if problems := l.Compare(TableUsers, "a", "a"); len(problems) != 0 {
		t.Fatal(problems)
	}
	dead := l.Dead()
	if len(dead) != 1 || !strings.Contains(dead[0], "m: a live missing-row quirk covered no row") {
		t.Fatalf("Dead = %v, want the missing-row quirk reported", dead)
	}
}

func ownedTable() []Quirk {
	return []Quirk{{ID: "stamp", Kind: Gap, Tracked: "CHAOS-10630", Reason: "r", Table: TableUsers, Needs: []Aggregate{LocalLogin}, Owned: NotOwned,
		Rewrite: func(line string) (string, bool) {
			if line == "login true" {
				return "login false", true
			}
			return "", false
		}}}
}

func TestOwnedLegFailsALastLoginFalseRow(t *testing.T) {
	l, err := newLedger(forwarded(t, "local_login"), true, ownedTable())
	if err != nil {
		t.Fatal(err)
	}
	problems := l.Compare(TableUsers, "login true", "login false")
	if len(problems) != 2 || !strings.Contains(problems[0], "users: unclassified difference") {
		t.Fatalf("a never-stamped row on the owned leg must fail as unclassified: %v", problems)
	}
}

func TestRealTableLastLoginQuirkIsNotOwnedOnly(t *testing.T) {
	for _, q := range Quirks {
		if q.ID == "users-last-login-not-stamped" && q.Owned != NotOwned {
			t.Fatalf("users-last-login-not-stamped must be NotOwned, is %q", q.Owned)
		}
	}
}

func linkMailLines(format string, subjects ...string) string {
	lines := make([]string, 0, len(subjects))
	for _, subject := range subjects {
		lines = append(lines, LinkMailRow(subject, format))
	}
	return strings.Join(lines, rowSeparator)
}

var linkMailSubjects = []string{"Verify your email address", "Verify your email address", "Reset your password"}

// On the owned leg auth-service's link mails carry the opaque token where the golden's carry go-api's signed one: the
// named quirk covers each such mail, once.
func TestLinkTokenFormatQuirkCoversTheOwnedLegsMails(t *testing.T) {
	l, err := NewLedger(forwarded(t, "sessions,local_login,credentials"), true)
	if err != nil {
		t.Fatal(err)
	}
	golden, acp := linkMailLines(LinkTokenGoAPI, linkMailSubjects...), linkMailLines(LinkTokenOpaque, linkMailSubjects...)
	if problems := l.Compare(TableLinkMails, golden, acp); len(problems) != 0 {
		t.Fatalf("the owned leg's opaque tokens: %v", problems)
	}
	if got := l.Covered(); !strings.Contains(got, "link-token-format-auth-service=3") {
		t.Fatalf("Covered = %q, want the quirk covering 3 mails", got)
	}
}

// Off the owned leg the quirk is not live: an opaque token there is unclassified, and so is a mail whose format is neither.
func TestLinkTokenFormatQuirkIsOwnedLegOnly(t *testing.T) {
	l, err := NewLedger(forwarded(t, "sessions,local_login,credentials"), false)
	if err != nil {
		t.Fatal(err)
	}
	golden := linkMailLines(LinkTokenGoAPI, linkMailSubjects...)
	if problems := l.Compare(TableLinkMails, golden, linkMailLines(LinkTokenOpaque, linkMailSubjects...)); len(problems) == 0 {
		t.Fatal("an opaque token off the owned leg must fail")
	}
	l2, _ := NewLedger(forwarded(t, "sessions,local_login,credentials"), false)
	if problems := l2.Compare(TableLinkMails, golden, golden); len(problems) != 0 {
		t.Fatalf("equal formats off the owned leg: %v", problems)
	}
	if strings.Contains(l2.Covered(), "link-token-format-auth-service") {
		t.Fatalf("the quirk is live off the owned leg: %q", l2.Covered())
	}
}

// The quirk covers at most the golden's link mails: one more opaque mail than the golden holds is unclassified, an
// unknown format never is, and on the owned leg a go-api format token is no difference (the quirk then covers nothing and dies).
func TestLinkTokenFormatQuirkIsBounded(t *testing.T) {
	l, _ := NewLedger(forwarded(t, "sessions,local_login,credentials"), true)
	golden := linkMailLines(LinkTokenGoAPI, linkMailSubjects...)
	if problems := l.Compare(TableLinkMails, golden, linkMailLines(LinkTokenOpaque, append(linkMailSubjects, "Reset your password")...)); len(problems) == 0 {
		t.Fatal("a ninth-style extra mail must be unclassified")
	}
	l2, _ := NewLedger(forwarded(t, "sessions,local_login,credentials"), true)
	if problems := l2.Compare(TableLinkMails, golden, linkMailLines("other", linkMailSubjects...)); len(problems) == 0 {
		t.Fatal("an unknown token format must be unclassified")
	}
	l3, _ := NewLedger(forwarded(t, "sessions,local_login,credentials"), true)
	if problems := l3.Compare(TableLinkMails, golden, golden); len(problems) != 0 {
		t.Fatalf("equal formats: %v", problems)
	}
	if dead := strings.Join(l3.Dead(), "\n"); !strings.Contains(dead, "link-token-format-auth-service") {
		t.Fatalf("on the owned leg a go-api format token leaves the quirk covering nothing: Dead() must name it, got:\n%s", dead)
	}
}

// The control of the dead check: opaque mails on the owned leg are covered, so the quirk is not listed dead.
func TestLinkTokenFormatQuirkIsNotDeadWhenTheOwnedLegMailsOpaqueTokens(t *testing.T) {
	l, _ := NewLedger(forwarded(t, "sessions,local_login,credentials"), true)
	if problems := l.Compare(TableLinkMails, linkMailLines(LinkTokenGoAPI, linkMailSubjects...), linkMailLines(LinkTokenOpaque, linkMailSubjects...)); len(problems) != 0 {
		t.Fatal(problems)
	}
	if dead := strings.Join(l.Dead(), "\n"); strings.Contains(dead, "link-token-format-auth-service") {
		t.Fatalf("the quirk covered mails and is listed dead:\n%s", dead)
	}
}

// The quirk covers exactly the golden's link mails: all of them pass, one more is unclassified.
func TestLinkTokenFormatQuirkCoversExactlyTheGoldensLinkMails(t *testing.T) {
	n := len(lines(goldenRows(t)[TableLinkMails]))
	subjects := make([]string, 0, n+1)
	for i := 0; i < n+1; i++ {
		subjects = append(subjects, "Verify your email address")
	}
	l, _ := NewLedger(forwarded(t, "sessions,local_login,credentials"), true)
	if problems := l.Compare(TableLinkMails, linkMailLines(LinkTokenGoAPI, subjects[:n]...), linkMailLines(LinkTokenOpaque, subjects[:n]...)); len(problems) != 0 {
		t.Fatalf("%d golden mails and %d opaque mails must pass: %v", n, n, problems)
	}
	if got := l.Covered(); !strings.Contains(got, "link-token-format-auth-service="+strconv.Itoa(n)) {
		t.Fatalf("Covered = %q, want the quirk covering %d", got, n)
	}
	l2, _ := NewLedger(forwarded(t, "sessions,local_login,credentials"), true)
	if problems := l2.Compare(TableLinkMails, linkMailLines(LinkTokenGoAPI, subjects...), linkMailLines(LinkTokenOpaque, subjects...)); len(problems) == 0 {
		t.Fatalf("mail %d is beyond the quirk's Max and must be unclassified", n+1)
	}
}

// Max is the number of link mails the golden's Python plane sent, counted from the frozen golden.
func TestLinkTokenFormatQuirkMaxIsTheGoldensLinkMails(t *testing.T) {
	golden := lines(goldenRows(t)[TableLinkMails])
	for _, q := range Quirks {
		if q.ID == "link-token-format-auth-service" {
			if q.Owned != OwnedOnly || q.Table != TableLinkMails || q.Max != len(golden) {
				t.Fatalf("the quirk = owned %q table %q max %d, want owned-only, link_mails, %d (the golden's link mails)", q.Owned, q.Table, q.Max, len(golden))
			}
			return
		}
	}
	t.Fatal("no quirk link-token-format-auth-service")
}

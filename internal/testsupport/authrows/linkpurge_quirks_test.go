package authrows

import (
	"strings"
	"testing"
)

const (
	umaLegacyVerification = "uma@example.comtrue false 64 1440"
	judyLegacyReset       = "judy@example.comtrue false 64 60"
	legacyResetPurgedID   = "legacy-reset-row-purged-at-switch"
)

// No leg classifies a never-stamped last login: auth-service stamps public.users.last_login_at on a sign-in while it owns
// users, and the legs where it did not are retired.
func TestNoLegCoversANeverStampedLastLogin(t *testing.T) {
	for _, owned := range []bool{true, false} {
		l := ownedUsersLedger(t, owned)
		if problems := l.Compare(TableUsers, aliceGolden, aliceTarget); len(problems) == 0 {
			t.Errorf("owned=%v: a never-stamped last login was covered (%q)", owned, l.Covered())
		}
	}
}

// The users-owned switch purges go-api's legacy link-token rows (dho auth legacy-link-purge), so uma's seeded expired
// verification row is gone: on no leg is it an extra row a quirk covers.
func TestNoLegCoversUmasLegacyVerificationRow(t *testing.T) {
	golden := goldenRows(t)[TableEmailVerificationTokens]
	for _, owned := range []bool{true, false} {
		l := ownedUsersLedger(t, owned)
		if problems := l.Compare(TableEmailVerificationTokens, golden, golden+rowSeparator+umaLegacyVerification); len(problems) == 0 {
			t.Errorf("owned=%v: uma's legacy verification row was covered (%q)", owned, l.Covered())
		}
	}
}

// The same purge removes judy's seeded expired reset row, which the golden keeps and nothing else writes: the owned leg
// covers that one missing row, exactly once, and the table is empty after it.
func TestOwnedLegCoversJudysPurgedLegacyResetRowOnly(t *testing.T) {
	golden := goldenRows(t)[TablePasswordResetTokens]
	if golden != judyLegacyReset {
		t.Fatalf("the golden's reset rows have changed: %q", golden)
	}
	l := ownedUsersLedger(t, true)
	if problems := append(l.Compare(TablePasswordResetTokens, golden, ""), deadOf(l, legacyResetPurgedID)...); len(problems) != 0 {
		t.Fatalf("judy's purged reset row on the owned leg: %v", problems)
	}
	if got := l.Covered(); !strings.Contains(got, legacyResetPurgedID+"=1") {
		t.Fatalf("Covered = %q", got)
	}
	for name, c := range map[string]struct {
		owned          bool
		table, golden  string
		target         string
		deadIsAProblem bool
	}{
		"not owned":                 {false, TablePasswordResetTokens, golden, "", false},
		"judy's row kept":           {true, TablePasswordResetTokens, golden, golden, true},
		"another user's row gone":   {true, TablePasswordResetTokens, golden + rowSeparator + "zed@example.comtrue false 64 60", golden, false},
		"a live row of judy's gone": {true, TablePasswordResetTokens, "judy@example.comtrue true 64 60", "", false},
		"the verification table":    {true, TableEmailVerificationTokens, goldenRows(t)[TableEmailVerificationTokens] + rowSeparator + judyLegacyReset, goldenRows(t)[TableEmailVerificationTokens], false},
	} {
		l := ownedUsersLedger(t, c.owned)
		problems := l.Compare(c.table, c.golden, c.target)
		if c.deadIsAProblem {
			problems = append(problems, deadOf(l, legacyResetPurgedID)...)
		}
		if len(problems) == 0 {
			t.Errorf("%s: passed (covered %q)", name, l.Covered())
		}
	}
}

// An empty target with an expected row that no live missing-row quirk covers fails: the empty-target rule does not hide
// a lost row.
func TestAnEmptyTargetWithAnUncoveredExpectedRowFails(t *testing.T) {
	for name, golden := range map[string]string{
		"a users row":                    aliceGolden,
		"a second purged row past Max 1": judyLegacyReset + rowSeparator + judyLegacyReset,
		"another user's reset row":       "zed@example.comtrue false 64 60",
	} {
		table := TablePasswordResetTokens
		if name == "a users row" {
			table = TableUsers
		}
		l := ownedUsersLedger(t, true)
		if problems := l.Compare(table, golden, ""); len(problems) == 0 {
			t.Errorf("%s: an empty target passed (covered %q)", name, l.Covered())
		}
	}
}

// An empty target with an empty expected set fails as empty on every leg: it is never a pass.
func TestAnEmptyTargetWithAnEmptyExpectedSetFails(t *testing.T) {
	for _, owned := range []bool{true, false} {
		l := ownedUsersLedger(t, owned)
		problems := l.Compare(TablePasswordResetTokens, "", "")
		if len(problems) != 1 || !strings.Contains(problems[0], "rows are empty") {
			t.Errorf("owned=%v: problems %v, want the one rows-are-empty problem", owned, problems)
		}
	}
}

// No quirk names a ticket that is Done: the two gaps of the pre-owned users path are gone from the table.
func TestRetiredUsersPathQuirksAreGone(t *testing.T) {
	for _, q := range Quirks {
		switch q.ID {
		case "users-last-login-not-stamped", "email-verification-legacy-row-not-purged-owned":
			t.Errorf("quirk %s is still in the table", q.ID)
		}
	}
}

// Dead's "covered no row" case is the only dead report of a quirk that is not Exact: the table's own login-organization
// rewrite quirk, live on a sessions leg, covers no row when the target's refresh rows equal the golden's, and is reported.
func TestARealNonExactQuirkThatCoversNoRowIsDead(t *testing.T) {
	const id = "login-organization-chosen-without-activity"
	for _, q := range Quirks {
		if q.ID == id && (q.Exact || q.Max != 0) {
			t.Fatalf("%s is Exact or bounded (exact %v, max %d): the cell needs a quirk with no Max", id, q.Exact, q.Max)
		}
	}
	golden := goldenRows(t)[TableRefreshTokens]
	l, err := NewLedger(forwarded(t, "sessions,local_login"), true)
	if err != nil {
		t.Fatal(err)
	}
	if problems := l.Compare(TableRefreshTokens, golden, golden); len(problems) != 0 {
		t.Fatalf("refresh rows equal to the golden's: %v", problems)
	}
	if dead := deadOf(l, id); len(dead) != 1 || !strings.Contains(dead[0], "covered no row") {
		t.Fatalf("Dead for %s = %v, want one covered-no-row report", id, dead)
	}
}

package ttlclock

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const siteSource = `//go:build integration

package p

import "time"

func seed() {
	at := time.Date(2026, time.August, 10, 12, 0, 0, 0, time.UTC)
	_ = at
	_ = "INSERT INTO events_ttl (ts) VALUES (?)"
}
`

const siteNever = "//ttlclock:allow never reaches a TTL table: planted tables=events_ttl"

func siteProblems(t *testing.T, source string) []string {
	t.Helper()
	return checkSites("p/p_test.go", []byte(source), plantedTables, plantedToday)
}

func withMarker(source, marker string) string {
	return strings.Replace(source, "\tat := time.Date", "\t"+marker+"\n\tat := time.Date", 1)
}

func wantOne(t *testing.T, name string, problems []string, want string) {
	t.Helper()
	if len(problems) == 0 || !strings.Contains(strings.Join(problems, "\n"), want) {
		t.Errorf("%s: problems = %q, want one containing %q", name, problems, want)
	}
}

func wantNone(t *testing.T, name string, problems []string) {
	t.Helper()
	if len(problems) != 0 {
		t.Errorf("%s: problems = %q, want none", name, problems)
	}
}

func TestSiteLiteralWithoutMarkerIsRed(t *testing.T) {
	wantOne(t, "no marker", siteProblems(t, siteSource), "p/p_test.go:8: date literal 2026-08-10")
}

func TestSiteNeverMarkerIsGreen(t *testing.T) {
	wantNone(t, "marker above", siteProblems(t, withMarker(siteSource, siteNever)))
	trailing := strings.Replace(siteSource, "time.UTC)\n\t_ = at", "time.UTC) "+siteNever+"\n\t_ = at", 1)
	wantNone(t, "marker on the literal's line", siteProblems(t, trailing))
	oneLiteral := strings.Replace(siteSource, `_ = "INSERT INTO events_ttl (ts) VALUES (?)"`, "//ttlclock:allow never reaches a TTL table: planted tables=events_ttl\n\t_ = `INSERT INTO events_ttl (ts) VALUES ('2026-01-01', '2026-01-02')`", 1)
	wantNone(t, "two dates in one string literal are one literal", siteProblems(t, withMarker(oneLiteral, siteNever)))
}

func TestSiteMovedLiteralStillPasses(t *testing.T) {
	marked := withMarker(siteSource, siteNever)
	wantNone(t, "marked", siteProblems(t, marked))
	moved := strings.Replace(marked, "func seed() {\n", "func seed() {\n\t_ = 0\n\t_ = 1\n", 1)
	wantNone(t, "lines inserted above the marker", siteProblems(t, moved))
	renamed := strings.Replace(marked, "func seed() {", "func reseed() {", 1)
	wantNone(t, "other function", siteProblems(t, renamed))
}

func TestSiteOrphanMarkerIsRed(t *testing.T) {
	wantOne(t, "no date literal near it", siteProblems(t, strings.Replace(withMarker(siteSource, siteNever), "time.Date(2026, time.August, 10, 12, 0, 0, 0, time.UTC)", "time.Now()", 1)), "p/p_test.go:8: no date literal")
	apart := strings.Replace(withMarker(siteSource, siteNever), siteNever+"\n", siteNever+"\n\n", 1)
	wantOne(t, "a blank line between marker and literal", siteProblems(t, apart), "no date literal")
	wantOne(t, "...and the literal is unmarked", siteProblems(t, apart), "date literal 2026-08-10")
	twoUp := strings.Replace(withMarker(siteSource, siteNever), siteNever+"\n", siteNever+"\n\t_ = 0\n", 1)
	wantOne(t, "marker two lines up", siteProblems(t, twoUp), "no date literal")
	plain := strings.Replace(withMarker(siteSource, siteNever), "//go:build integration\n\n", "", 1)
	wantOne(t, "file is no integration test", siteProblems(t, plain), "no date literal")
	farYear := strings.Replace(withMarker(siteSource, siteNever), "time.Date(2026, time.August, 10", "time.Date(2099, time.August, 10", 1)
	wantOne(t, "literal is not a finding (2099)", siteProblems(t, farYear), "no date literal")
}

func TestSiteEmptyReasonIsRed(t *testing.T) {
	for _, marker := range []string{"//ttlclock:allow", "//ttlclock:allow   ", "//ttlclock:allow\t"} {
		wantOne(t, "empty reason "+marker, siteProblems(t, withMarker(siteSource, marker)), "needs a reason")
	}
}

func TestSiteNotAMarker(t *testing.T) {
	for _, marker := range []string{"//ttlclock:allowed never reaches a TTL table: x tables=events_ttl", "/* ttlclock:allow never reaches a TTL table: x tables=events_ttl */", "// ttlclock:allow never reaches a TTL table: x tables=events_ttl"} {
		wantOne(t, marker, siteProblems(t, withMarker(siteSource, marker)), "p/p_test.go:9: date literal 2026-08-10")
	}
}

func TestSiteMarkerCoversOnlyOneLiteral(t *testing.T) {
	twoOnALine := strings.Replace(siteSource, "\tat := time.Date(2026, time.August, 10, 12, 0, 0, 0, time.UTC)\n",
		"\tat, bt := time.Date(2026, time.August, 10, 12, 0, 0, 0, time.UTC), time.Date(2026, time.September, 1, 12, 0, 0, 0, time.UTC)\n\t_ = bt\n", 1)
	wantOne(t, "two literals on a commented line", siteProblems(t, withMarkerBt(twoOnALine, siteNever)), "second literal")

	nextLine := strings.Replace(siteSource, "\t_ = at\n", "\tbt := time.Date(2026, time.September, 1, 12, 0, 0, 0, time.UTC)\n\t_, _ = at, bt\n", 1)
	trailing := strings.Replace(nextLine, "time.UTC)\n\tbt", "time.UTC) "+siteNever+"\n\tbt", 1)
	wantOne(t, "trailing marker of line N also claimed by line N+1", siteProblems(t, trailing), "second literal")

	farApart := strings.Replace(siteSource, "\t_ = at\n", "\t_ = at\n\t_ = 0\n\tbt := time.Date(2026, time.September, 1, 12, 0, 0, 0, time.UTC)\n\t_ = bt\n", 1)
	wantOne(t, "one marker, a second literal two lines later", siteProblems(t, withMarker(farApart, siteNever)), "p/p_test.go:12: date literal 2026-09-01")
}

func withMarkerBt(source, marker string) string {
	return strings.Replace(source, "\tat, bt := time.Date", "\t"+marker+"\n\tat, bt := time.Date", 1)
}

func TestSiteTTLFormNamesTheComputedExpiry(t *testing.T) {
	far := strings.Replace(siteSource, "time.Date(2026, time.August, 10", "time.Date(1899, 12, 31", 1)
	wantNone(t, "wrapped expiry named", siteProblems(t, withMarker(far, "//ttlclock:allow driver edge, the row lives until 2036-05-06")))
	wantOne(t, "no date in the reason", siteProblems(t, withMarker(far, "//ttlclock:allow driver edge")), "must name the expiry 2036-05-06")
	wantOne(t, "wrong date in the reason", siteProblems(t, withMarker(far, "//ttlclock:allow driver edge, the row lives until 2099-01-01")), "must name the expiry 2036-05-06")
	// 2026-08-10 + 90 d = 2026-11-08, inside 180 days of 2026-10-27.
	wantOne(t, "inside 180 days", siteProblems(t, withMarker(siteSource, "//ttlclock:allow planted, expires 2026-11-08")), "within 180 days of 2026-10-27")
	// (ii) a table the file starts to name moves the earliest expiry: daily_ttl 365 d, no wrap, 1899-12-31 -> 1900-12-31.
	more := strings.Replace(far, "\t_ = at\n", "\t_ = at\n\t_ = \"SELECT 1 FROM daily_ttl\"\n", 1)
	wantOne(t, "new table moves the earliest expiry", siteProblems(t, withMarker(more, "//ttlclock:allow driver edge, the row lives until 2036-05-06")), "must name the expiry 1900-12-31")
}

func TestSiteNeverFormPinsTheTTLTables(t *testing.T) {
	wantOne(t, "no tables= field", siteProblems(t, withMarker(siteSource, "//ttlclock:allow never reaches a TTL table: planted")), "tables=")
	wantOne(t, "wrong list", siteProblems(t, withMarker(siteSource, "//ttlclock:allow never reaches a TTL table: planted tables=daily_ttl")), "STALE")
	// (i) the file gains a TTL table after the marker was written.
	more := strings.Replace(siteSource, "\t_ = at\n", "\t_ = at\n\t_ = \"SELECT 1 FROM daily_ttl\"\n", 1)
	wantOne(t, "file gains a TTL table", siteProblems(t, withMarker(more, siteNever)), "STALE")
	wantNone(t, "list follows the file", siteProblems(t, withMarker(more, "//ttlclock:allow never reaches a TTL table: planted tables=daily_ttl,events_ttl")))
}

func TestSitesAreClockFreeLiteralsStayGreen(t *testing.T) {
	clock := "//go:build integration\n\npackage p\n\nimport \"time\"\n\nfunc seed() {\n\tat := time.Now().UTC().AddDate(0, 0, -3)\n\t_ = at.Format(\"2006-01-02\")\n\t_ = \"INSERT INTO events_ttl (ts) VALUES (?)\"\n\t_ = time.Date(2099, 6, 15, 12, 0, 0, 0, time.UTC)\n}\n"
	wantNone(t, "clock-relative", siteProblems(t, clock))
}

func TestAllowTSVIsGone(t *testing.T) {
	if _, err := os.Stat("allow.tsv"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("allow.tsv must not exist (one mechanism: //ttlclock:allow site comments), stat err = %v", err)
	}
}

func TestWalkChecksEveryFileHoldingTheMarker(t *testing.T) {
	root := t.TempDir()
	write := func(name, body string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(root, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("plain_test.go", "package p\n\n//ttlclock:allow never reaches a TTL table: stale tables=events_ttl\nvar x = 1\n")
	write("integ_test.go", withMarker(siteSource, siteNever))
	problems, scannedFiles, err := scanTree(root, plantedTables, plantedToday)
	if err != nil {
		t.Fatal(err)
	}
	if scannedFiles != 1 {
		t.Errorf("scanned table-naming integration tests = %d, want 1", scannedFiles)
	}
	if len(problems) != 1 || !strings.Contains(problems[0], "plain_test.go:3: no date literal") {
		t.Errorf("problems = %q, want exactly the orphan marker in plain_test.go (a non-integration file holding the marker)", problems)
	}
}

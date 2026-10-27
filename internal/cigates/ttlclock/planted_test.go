package ttlclock

import (
	"strings"
	"testing"
	"time"
)

var plantedTables = map[string]ttlTable{
	"events_ttl": {days: 90, wrap: true},
	"daily_ttl":  {days: 365, wrap: false},
}

var plantedToday = time.Date(2026, time.October, 27, 0, 0, 0, 0, time.UTC)

func plantedFindings(t *testing.T, source string) []finding {
	t.Helper()
	found, err := scanFile("p/p_test.go", []byte(source), plantedTables)
	if err != nil {
		t.Fatal(err)
	}
	return found
}

func plantedCheck(t *testing.T, source, allow string) []string {
	t.Helper()
	rows, problems := parseAllow(allow)
	return append(problems, check(plantedFindings(t, source), rows, plantedTables, plantedToday, func(rel string) bool { return rel == "p/p_test.go" })...)
}

const plantedLiteral = `//go:build integration

package p

import "time"

func seed() {
	at := time.Date(2026, time.August, 10, 12, 0, 0, 0, time.UTC)
	_ = at
	_ = "INSERT INTO events_ttl (ts) VALUES (?)"
}
`

const plantedClock = `//go:build integration

package p

import "time"

func seed() {
	at := time.Now().UTC().AddDate(0, 0, -3)
	_ = at.Format("2006-01-02")
	_ = "INSERT INTO events_ttl (ts) VALUES (?)"
	_ = time.Date(2099, 6, 15, 12, 0, 0, 0, time.UTC)
}
`

func TestPlantedLiteralDateIsRed(t *testing.T) {
	problems := plantedCheck(t, plantedLiteral, "")
	if len(problems) != 1 || !strings.Contains(problems[0], "p/p_test.go at 8:seed: date literal 2026-08-10") || !strings.Contains(problems[0], "names TTL table(s) events_ttl;") {
		t.Fatalf("problems = %q, want the one literal finding", problems)
	}
	sql := strings.Replace(plantedLiteral, `"INSERT INTO events_ttl (ts) VALUES (?)"`, `"INSERT INTO daily_ttl VALUES (toDate('2026-01-05'))"`, 1)
	if problems := plantedCheck(t, sql, ""); len(problems) != 2 || !strings.Contains(strings.Join(problems, "\n"), "date literal 2026-01-05") {
		t.Fatalf("a date inside SQL text: problems = %q", problems)
	}
}

func TestPlantedClockRelativeIsGreen(t *testing.T) {
	if problems := plantedCheck(t, plantedClock, ""); len(problems) != 0 {
		t.Fatalf("clock-relative dates, the Go layout and a 2099 date: problems = %q, want none", problems)
	}
	if found := plantedFindings(t, strings.Replace(plantedLiteral, "//go:build integration", "//go:build !race", 1)); len(found) != 0 {
		t.Fatalf("a file without the integration tag: %v", found)
	}
	if found := plantedFindings(t, strings.Replace(plantedLiteral, "events_ttl", "events_plain", 1)); len(found) != 0 {
		t.Fatalf("a file that names no TTL table: %v", found)
	}
	if found := plantedFindings(t, strings.Replace(plantedLiteral, `_ = "INSERT INTO events_ttl (ts) VALUES (?)"`, `// INSERT INTO events_ttl`, 1)); len(found) != 0 {
		t.Fatalf("a table named only in a comment: %v", found)
	}
}

// row is one allow.tsv line for the planted file: names events_ttl, the literal at line 8 in seed.
func row(file, date, table, expiry, reason string) string {
	return strings.Join([]string{file, date, table, expiry, "events_ttl", "8:seed", reason}, "\t") + "\n"
}

func TestPlantedAllowRowWithoutReasonIsRed(t *testing.T) {
	if problems := plantedCheck(t, plantedLiteral, row("p/p_test.go", "2026-08-10", "-", "never", "never written")); len(problems) != 0 {
		t.Fatalf("a never row with a reason: problems = %q, want none", problems)
	}
	problems := plantedCheck(t, plantedLiteral, row("p/p_test.go", "2026-08-10", "-", "never", " "))
	if len(problems) != 1 || !strings.Contains(problems[0], "row without a reason") {
		t.Fatalf("problems = %q, want the missing reason", problems)
	}
}

func TestPlantedExpiringAllowRowIsRed(t *testing.T) {
	// 2026-08-10 + 90 d = 2026-11-08, inside 180 days of 2026-10-27.
	problems := plantedCheck(t, plantedLiteral, row("p/p_test.go", "2026-08-10", "events_ttl", "2026-11-08", "planted"))
	if len(problems) != 1 || !strings.Contains(problems[0], "expires 2026-11-08, within 180 days of 2026-10-27") {
		t.Fatalf("problems = %q, want the expiring row", problems)
	}
	far := strings.Replace(plantedLiteral, "time.Date(2026, time.August, 10", "time.Date(1899, 12, 31", 1)
	if problems := plantedCheck(t, far, row("p/p_test.go", "1899-12-31", "events_ttl", "2036-05-06", "the UInt32 wrap of toDateTime")); len(problems) != 0 {
		t.Fatalf("a wrapped expiry in 2036: problems = %q, want none", problems)
	}
}

func TestPlantedNeverRowGoesStaleWhenItsFileChanges(t *testing.T) {
	never := row("p/p_test.go", "2026-08-10", "-", "never", "never written")
	cases := map[string]struct{ source, want string }{
		"the file names one more TTL table": {
			strings.Replace(plantedLiteral, `_ = "INSERT INTO events_ttl (ts) VALUES (?)"`, `_ = "INSERT INTO events_ttl (ts) VALUES (?)"
	_ = "SELECT 1 FROM daily_ttl"`, 1),
			"STALE, the file now names TTL table(s) daily_ttl,events_ttl (row: events_ttl)",
		},
		"the file names another TTL table": {
			strings.Replace(plantedLiteral, "INSERT INTO events_ttl", "INSERT INTO daily_ttl", 1),
			"STALE, the file now names TTL table(s) daily_ttl (row: events_ttl)",
		},
		"the literal moves to another line": {
			strings.Replace(plantedLiteral, "func seed() {\n", "func seed() {\n\t_ = 0\n", 1),
			"STALE, the literal is now at 9:seed (row: 8:seed)",
		},
		"the literal moves to another function": {
			strings.Replace(plantedLiteral, "func seed() {", "func reseed() {", 1),
			"STALE, the literal is now at 8:reseed (row: 8:seed)",
		},
		"a second occurrence": {
			strings.Replace(plantedLiteral, "\t_ = at\n", "\t_ = at\n\t_ = \"2026-08-10\"\n", 1),
			"STALE, the literal is now at 8:seed,10:seed (row: 8:seed)",
		},
	}
	for name, c := range cases {
		problems := plantedCheck(t, c.source, never)
		if len(problems) != 1 || !strings.Contains(problems[0], c.want) {
			t.Errorf("%s: problems = %q, want %q", name, problems, c.want)
		}
	}
}

func TestPlantedMalformedAndStaleAllowRowsAreRed(t *testing.T) {
	ok := row("p/p_test.go", "2026-08-10", "-", "never", "r")
	cases := map[string]struct{ allow, want string }{
		"wrong field count":  {"p/p_test.go\t2026-08-10\t-\tnever\tr\n", "want 7 tab-separated fields"},
		"wrong expiry":       {row("p/p_test.go", "2026-08-10", "daily_ttl", "2099-01-01", "r"), "expiry 2099-01-01, computed 2027-08-10"},
		"never with a table": {row("p/p_test.go", "2026-08-10", "daily_ttl", "never", "r"), "expiry never, computed"},
		"dash with a date":   {row("p/p_test.go", "2026-08-10", "-", "2099-01-01", "r"), `needs expiry "never"`},
		"table without TTL":  {row("p/p_test.go", "2026-08-10", "plain", "never", "r"), "plain has no TTL at the migration head"},
		"bad date":           {row("p/p_test.go", "2026-8-10", "-", "never", "r"), "date is not YYYY-MM-DD"},
		"duplicate":          {ok + ok, "duplicate row"},
		"literal gone":       {ok + row("p/p_test.go", "2026-08-11", "-", "never", "r"), "2026-08-11): the literal is no longer a finding"},
		"file gone":          {ok + row("p/gone_test.go", "2026-08-10", "-", "never", "r"), "the file no longer exists"},
	}
	for name, c := range cases {
		problems := plantedCheck(t, plantedLiteral, c.allow)
		if !strings.Contains(strings.Join(problems, "\n"), c.want) {
			t.Errorf("%s: problems = %q, want %q", name, problems, c.want)
		}
	}
}

func TestPlantedMigrationsAreRead(t *testing.T) {
	head := []byte(`{"objects":[
		{"name":"a","create":"CREATE TABLE a (` + "`ts`" + ` DateTime64(3)) ENGINE = MergeTree ORDER BY ts TTL toDateTime(ts) + toIntervalDay(90) SETTINGS index_granularity = 8192"},
		{"name":"b","create":"CREATE TABLE b (day Date) ENGINE = MergeTree ORDER BY day TTL day + toIntervalDay(365) SETTINGS index_granularity = 8192"},
		{"name":"c","create":"CREATE TABLE c (day Date) ENGINE = MergeTree ORDER BY day"},
		{"name":"gone","create":"CREATE TABLE gone (day Date) ENGINE = MergeTree ORDER BY day TTL day + toIntervalDay(30)"}]}`)
	tables, problems := ttlTables(head, map[string][]byte{
		"101_d.sql": []byte("-- TTL in a comment is not a clause\nCREATE TABLE IF NOT EXISTS d (at DateTime64(3)) ENGINE = ReplacingMergeTree(at) ORDER BY at\nTTL toDateTime(at) + INTERVAL 400 DAY;\nDROP TABLE IF EXISTS gone;"),
		"102_e.sql": []byte("CREATE TABLE e (at DateTime) ENGINE = MergeTree ORDER BY at TTL at + INTERVAL 7 DAY DELETE;"),
	})
	if len(problems) != 0 {
		t.Fatalf("problems = %q", problems)
	}
	want := map[string]ttlTable{"a": {90, true}, "b": {365, false}, "d": {400, true}, "e": {7, false}}
	if len(tables) != len(want) {
		t.Fatalf("tables = %v, want %v", tables, want)
	}
	for name, ttl := range want {
		if tables[name] != ttl {
			t.Fatalf("%s = %v, want %v", name, tables[name], ttl)
		}
	}
	for name, sql := range map[string]string{
		"modify":     "ALTER TABLE a MODIFY TTL toDateTime(ts) + INTERVAL 1 DAY;",
		"where":      "CREATE TABLE f (at DateTime) ENGINE = MergeTree ORDER BY at TTL at + INTERVAL 7 DAY DELETE WHERE x = 1;",
		"to disk":    "CREATE TABLE f (at DateTime) ENGINE = MergeTree ORDER BY at TTL at + INTERVAL 7 DAY TO DISK 'cold';",
		"column ttl": "CREATE TABLE f (at DateTime, x String TTL at + INTERVAL 1 DAY) ENGINE = MergeTree ORDER BY at;",
		"two":        "CREATE TABLE f (at DateTime) ENGINE = MergeTree ORDER BY at TTL at + INTERVAL 7 DAY, at + INTERVAL 9 DAY TO VOLUME 'v';",
		"month":      "CREATE TABLE f (at DateTime) ENGINE = MergeTree ORDER BY at TTL at + INTERVAL 1 MONTH;",
	} {
		if _, problems := ttlTables(head, map[string][]byte{"103_f.sql": []byte(sql)}); len(problems) != 1 {
			t.Errorf("%s: problems = %q, want one unmodelled TTL", name, problems)
		}
	}
}

func TestExpiryFollowsTheToDateTimeWrap(t *testing.T) {
	day := func(text string) time.Time {
		parsed, err := time.Parse(dateLayout, text)
		if err != nil {
			t.Fatal(err)
		}
		return parsed
	}
	for _, c := range []struct {
		date string
		ttl  ttlTable
		want string
	}{
		{"2026-08-10", ttlTable{90, true}, "2026-11-08"},
		{"2026-01-04", ttlTable{365, false}, "2027-01-04"},
		{"1899-12-31", ttlTable{180, true}, "2036-08-04"},
		{"1900-01-01", ttlTable{180, true}, "2036-08-05"},
	} {
		if got := expiry(day(c.date), c.ttl).Format(dateLayout); got != c.want {
			t.Errorf("expiry(%s, %v) = %s, want %s", c.date, c.ttl, got, c.want)
		}
	}
}

package ttlclock

import (
	"testing"
	"time"
)

var plantedTables = map[string]ttlTable{
	"events_ttl": {days: 90, wrap: true},
	"daily_ttl":  {days: 365, wrap: false},
}

var plantedToday = time.Date(2026, time.October, 27, 0, 0, 0, 0, time.UTC)

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

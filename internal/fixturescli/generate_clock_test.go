package fixturescli

import (
	"reflect"
	"testing"
	"time"
)

// producedRows are the rows LoadWorld would insert for the world at now: the non-derived tables with
// their server-stamped columns dropped, moved by the world's own shift.
func producedRows(t *testing.T, world FrozenWorld, now time.Time) map[string]WorldTable {
	t.Helper()
	days, err := world.WholeDays(now)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]WorldTable{}
	for _, table := range world.Tables {
		if table.Derived {
			continue
		}
		table.FrozenTable = table.FrozenTable.WithoutServerStamped()
		rows, err := table.Transform(days, world.OrgID, world.OrgID)
		if err != nil {
			t.Fatal(err)
		}
		table.Rows = rows
		out[table.Name] = table
	}
	return out
}

// pastValuesAfterTheShift checks every date and timestamp the frozen world holds at or before its
// frozen_at: after the shift it must not be later than now, and it must have moved by exactly the
// whole days WholeDays names. It returns how many values it checked, and whether repos.created_at
// was among them.
func pastValuesAfterTheShift(t *testing.T, world FrozenWorld, now time.Time) (checked int, sawRepoCreated bool) {
	t.Helper()
	frozen := mustInstant(t, world.FrozenAt)
	days, err := world.WholeDays(now)
	if err != nil {
		t.Fatal(err)
	}
	for _, table := range world.Tables {
		if table.Derived {
			continue
		}
		table.FrozenTable = table.FrozenTable.WithoutServerStamped()
		moved, err := table.Transform(days, world.OrgID, world.OrgID)
		if err != nil {
			t.Fatal(err)
		}
		for index, column := range table.Columns {
			if _, ok, err := shiftLayout(column.Type); err != nil || !ok {
				continue
			}
			for rowIndex, row := range table.Rows {
				if row[index] == nil {
					continue
				}
				before, err := parseWorldTime(row[index].(string))
				if err != nil {
					t.Fatalf("%s.%s: %v", table.Name, column.Name, err)
				}
				if before.After(frozen) {
					continue
				}
				after, err := parseWorldTime(moved[rowIndex][index].(string))
				if err != nil {
					t.Fatalf("%s.%s: %v", table.Name, column.Name, err)
				}
				checked++
				if table.Name == "repos" && column.Name == "created_at" {
					sawRepoCreated = true
				}
				if want := before.AddDate(0, 0, days); !after.Equal(want) {
					t.Errorf("%s.%s: %s moved to %s, want %s", table.Name, column.Name, before, after, want)
				}
				if after.After(now) {
					t.Errorf("clock %s: %s.%s holds %s after the shift, later than the clock", now.Format(time.RFC3339Nano), table.Name, column.Name, after.Format(time.RFC3339Nano))
				}
			}
		}
	}
	return checked, sawRepoCreated
}

func eachFrozenWorld(t *testing.T, body func(t *testing.T, world FrozenWorld)) {
	t.Helper()
	count := 0
	for path := range frozenWorldDigests {
		count++
		t.Run(path, func(t *testing.T) {
			raw, err := worldFiles.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			world, err := decodeWorld(raw)
			if err != nil {
				t.Fatal(err)
			}
			body(t, world)
		})
	}
	if count == 0 {
		t.Fatal("no frozen world was checked")
	}
}

func mustInstant(t *testing.T, text string) time.Time {
	t.Helper()
	at, err := time.Parse(time.RFC3339Nano, text)
	if err != nil {
		t.Fatal(err)
	}
	return at
}

// No produced value is later than the generation clock, at any time of day (CHAOS-8915).
func TestGeneratedRowsAreNeverLaterThanTheClock(t *testing.T) {
	eachFrozenWorld(t, func(t *testing.T, world FrozenWorld) {
		frozen := mustInstant(t, world.FrozenAt)
		for _, offset := range []time.Duration{0, time.Second, 90 * time.Minute, 3*time.Hour + 10*time.Minute, 23*time.Hour + 59*time.Minute} {
			for _, day := range []int{0, 1, 11} {
				midnight := time.Date(frozen.Year(), frozen.Month(), frozen.Day()+day, 0, 0, 0, 0, time.UTC)
				now := midnight.Add(offset)
				checked, sawRepo := pastValuesAfterTheShift(t, world, now)
				if checked == 0 || !sawRepo {
					t.Fatalf("checked %d value(s), repos.created_at seen: %v", checked, sawRepo)
				}
			}
		}
	})
}

// A clock before the frozen time of day (the early-UTC case) puts the last day on yesterday; at the
// frozen instant, and after it, the rows are those of the whole-day shift.
func TestGeneratedRowsKeepTheWholeDayShiftOnceTheClockPassesTheWorld(t *testing.T) {
	eachFrozenWorld(t, func(t *testing.T, world FrozenWorld) {
		latest := mustInstant(t, world.FrozenAt)
		from := time.Date(latest.Year(), latest.Month(), latest.Day(), 0, 0, 0, 0, time.UTC)
		for _, tc := range []struct {
			name string
			now  time.Time
			want int
		}{
			{"the day boundary", from.AddDate(0, 0, 10), 9},
			{"one nanosecond before the latest instant", from.AddDate(0, 0, 10).Add(latest.Sub(from) - 1), 9},
			{"exactly the latest instant", from.AddDate(0, 0, 10).Add(latest.Sub(from)), 10},
			{"after the latest instant", from.AddDate(0, 0, 10).Add(latest.Sub(from) + time.Nanosecond), 10},
			{"the end of the day", from.AddDate(0, 0, 10).Add(24*time.Hour - time.Nanosecond), 10},
			{"the frozen day, before the instant", from.Add(latest.Sub(from) - time.Second), -1},
			{"the frozen day, at the instant", from.Add(latest.Sub(from)), 0},
		} {
			got, err := world.WholeDays(tc.now)
			if err != nil || got != tc.want {
				t.Errorf("%s: WholeDays(%s) = %d, %v; want %d", tc.name, tc.now.Format(time.RFC3339Nano), got, err, tc.want)
			}
		}
	})
}

// The same seed and the same clock give identical rows.
func TestGeneratedRowsAreDeterministicForOneClock(t *testing.T) {
	eachFrozenWorld(t, func(t *testing.T, world FrozenWorld) {
		now := mustInstant(t, world.FrozenAt).AddDate(0, 0, 5).Add(-6 * time.Hour)
		run := func() map[string][][]any {
			days, err := world.WholeDays(now)
			if err != nil {
				t.Fatal(err)
			}
			out := map[string][][]any{}
			for _, table := range world.Tables {
				rows, err := table.Transform(days, world.OrgID, world.OrgID)
				if err != nil {
					t.Fatal(err)
				}
				out[table.Name] = rows
			}
			return out
		}
		if !reflect.DeepEqual(run(), run()) {
			t.Fatal("two runs with one clock produced different rows")
		}
	})
}

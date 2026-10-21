package timewindow

import (
	"errors"
	"testing"
	"time"
)

// TestNamedEndIsInclusiveAndNeverTodayOrLater pins D6101 for every reader that takes an end date: the named end counts
// (inclusive), and a day that is not over is in no window, even when named. Today is 2026-10-18.
func TestNamedEndIsInclusiveAndNeverTodayOrLater(t *testing.T) {
	today := date(t, "2026-10-18")
	start := date(t, "2026-10-01")
	cases := []struct {
		name         string
		end          string // "" = no end date
		wantEndExcl  string
		wantStartDay string
	}{
		{"no end", "", "2026-10-18", "2026-10-04"},
		{"yesterday", "2026-10-17", "2026-10-18", "2026-10-04"},
		{"today is the window of no end", "2026-10-18", "2026-10-18", "2026-10-04"},
		{"tomorrow is clipped too", "2026-10-19", "2026-10-18", "2026-10-04"},
		{"a year ahead is clipped", "2027-10-18", "2026-10-18", "2026-10-04"},
		{"a past end counts its own day", "2026-10-10", "2026-10-11", "2026-09-27"},
	}
	for _, c := range cases {
		var end *time.Time
		if c.end != "" {
			d := date(t, c.end)
			end = &d
		}
		window, err := Compute(14, 14, nil, end, today)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if got := window.EndDay.Format(time.DateOnly); got != c.wantEndExcl {
			t.Errorf("%s: EndDay = %s, want %s (exclusive)", c.name, got, c.wantEndExcl)
		}
		if got := window.StartDay.Format(time.DateOnly); got != c.wantStartDay {
			t.Errorf("%s: StartDay = %s, want %s", c.name, got, c.wantStartDay)
		}
		if c.end != "" {
			last := date(t, c.end)
			exclusive, err := ExclusiveEnd(last, today)
			if err != nil || !exclusive.Equal(window.EndDay) {
				t.Errorf("%s: ExclusiveEnd = %v (%v), Compute's EndDay = %v: two readers of one end must agree", c.name, exclusive, err, window.EndDay)
			}
		}
	}
	// With an explicit start the window is [start, end+1), the end clipped the same way.
	end := date(t, "2026-10-18")
	window, err := Compute(14, 14, &start, &end, today)
	if err != nil || window.StartDay != start || window.EndDay.Format(time.DateOnly) != "2026-10-18" {
		t.Errorf("start 10-01 end today: %+v, %v; want [2026-10-01, 2026-10-18)", window, err)
	}
	// A start on the day that is not over, end of today: nothing is left to read, the window is empty (see
	// TestStartOnTodayWithEndOfTodayIsAnEmptyWindowNotYesterday).
	late := date(t, "2026-10-18")
	window, err = Compute(14, 14, &late, &end, today)
	if err != nil || !window.StartDay.Equal(window.EndDay) {
		t.Errorf("start today: %+v, %v; want an empty window", window, err)
	}
}

func TestExclusiveEndAndLastFinalDayAtAnyHour(t *testing.T) {
	for _, hour := range []time.Duration{0, 5 * time.Hour, 23*time.Hour + 59*time.Minute} {
		today := date(t, "2026-10-18").Add(hour)
		final, err := LastFinalDay(today)
		if err != nil || final.Format(time.DateOnly) != "2026-10-17" {
			t.Errorf("LastFinalDay at +%v = %v, %v; want 2026-10-17", hour, final, err)
		}
		clipped, err := ClipLastDay(date(t, "2026-10-18").Add(7*time.Hour), today)
		if err != nil || clipped.Format(time.DateOnly) != "2026-10-17" {
			t.Errorf("ClipLastDay at +%v = %v, %v; want 2026-10-17", hour, clipped, err)
		}
	}
}

// A window Python's date arithmetic cannot hold is refused even though the end is then clipped to yesterday: the
// port never accepts what the reference refused.
func TestExplicitEndThatThePythonReferenceRefusesIsStillRefused(t *testing.T) {
	today := date(t, "2026-10-18")
	end := date(t, "9999-12-31")
	if _, err := Compute(14, 14, nil, &end, today); !errors.Is(err, ErrOverflow) {
		t.Fatalf("end 9999-12-31: err = %v, want ErrOverflow", err)
	}
}

// A start on the day that is not over, with that day named as the end (or no end): the clip leaves nothing to read, and the
// window is empty, as the GraphQL dateRange reads it. It is not the reference's repair (start moved to yesterday), which
// stays for a start that is after the end the caller named.
func TestStartOnTodayWithEndOfTodayIsAnEmptyWindowNotYesterday(t *testing.T) {
	today := date(t, "2026-10-18")
	start := date(t, "2026-10-18")
	for _, end := range []*time.Time{nil, &today} {
		window, err := Compute(14, 14, &start, end, today)
		if err != nil {
			t.Fatal(err)
		}
		if !window.StartDay.Equal(today) || !window.EndDay.Equal(today) {
			t.Errorf("start today, end %v: window [%s, %s), want the empty [2026-10-18, 2026-10-18)", end, window.StartDay.Format(time.DateOnly), window.EndDay.Format(time.DateOnly))
		}
		if !window.CompareEnd.Equal(window.StartDay) {
			t.Errorf("compare window ends %s, want it to end where the window starts", window.CompareEnd.Format(time.DateOnly))
		}
	}
	// A start after the end the caller named is the reference's repair: the one day before the end.
	pastEnd := date(t, "2026-10-10")
	late := date(t, "2026-10-14")
	window, err := Compute(14, 14, &late, &pastEnd, today)
	if err != nil || window.StartDay.Format(time.DateOnly) != "2026-10-10" || window.EndDay.Format(time.DateOnly) != "2026-10-11" {
		t.Errorf("start after the named end: %+v, %v; want the reference repair [2026-10-10, 2026-10-11)", window, err)
	}
}

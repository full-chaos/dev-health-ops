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
	// A start on or after the clipped end moves to the day before it, as the reference does.
	late := date(t, "2026-10-18")
	window, err = Compute(14, 14, &late, &end, today)
	if err != nil || window.StartDay.Format(time.DateOnly) != "2026-10-17" {
		t.Errorf("start today: %+v, %v; want the start moved to the last final day", window, err)
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

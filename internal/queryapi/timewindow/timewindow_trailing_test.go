package timewindow

import (
	"errors"
	"testing"
	"time"
)

func date(t *testing.T, text string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.DateOnly, text)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

// TestComputeTrailingWindowEndsYesterday pins the declared divergence from the Python reference: a window with no
// end date is [today-N, today), its last day is yesterday, and the comparison window is N days before its start.
func TestComputeTrailingWindowEndsYesterday(t *testing.T) {
	today := date(t, "2026-10-18")
	window, err := Compute(30, 30, nil, nil, today)
	if err != nil {
		t.Fatal(err)
	}
	want := Window{
		StartDay: date(t, "2026-09-18"), EndDay: date(t, "2026-10-18"),
		CompareStart: date(t, "2026-08-19"), CompareEnd: date(t, "2026-09-18"),
	}
	if window != want {
		t.Fatalf("window = %+v, want %+v", window, want)
	}
	// The same instant at any hour gives the same window: no sample of the clock moves it within a day.
	late, err := Compute(30, 30, nil, nil, today.Add(23*time.Hour+59*time.Minute))
	if err != nil || late != want {
		t.Fatalf("late in the day: %+v, %v; want %+v", late, err, want)
	}
}

// TestComputeTrailingWindowIsTheReferenceWindowOfYesterday pins that the only change is the default end: for every
// range and compare count the window with no end date is the reference window of an end date of yesterday, and an
// explicit end date keeps the reference's inclusive reading.
func TestComputeTrailingWindowIsTheReferenceWindowOfYesterday(t *testing.T) {
	today := date(t, "2026-10-18")
	yesterday := date(t, "2026-10-17")
	start := date(t, "2026-10-01")
	explicitEnd := date(t, "2026-10-14")
	for _, rangeDays := range []int{-3, 0, 1, 14, 90, 365} {
		for _, compareDays := range []int{-1, 1, 14, 90} {
			for _, startDate := range []*time.Time{nil, &start} {
				got, gotErr := Compute(rangeDays, compareDays, startDate, nil, today)
				want, wantErr := ComputeReference(rangeDays, compareDays, startDate, &yesterday, today)
				if got != want || (gotErr == nil) != (wantErr == nil) {
					t.Errorf("range %d compare %d start %v: %+v/%v, reference of yesterday %+v/%v", rangeDays, compareDays, startDate, got, gotErr, want, wantErr)
				}
				explicit, err := Compute(rangeDays, compareDays, startDate, &explicitEnd, today)
				reference, refErr := ComputeReference(rangeDays, compareDays, startDate, &explicitEnd, today)
				if explicit != reference || (err == nil) != (refErr == nil) {
					t.Errorf("explicit end changed: %+v/%v vs reference %+v/%v", explicit, err, reference, refErr)
				}
			}
		}
	}
}

func TestComputeTrailingWindowOverflowsOnlyBeforeYearOne(t *testing.T) {
	if _, err := Compute(14, 14, nil, nil, date(t, "0001-01-01")); !errors.Is(err, ErrOverflow) {
		t.Fatalf("err = %v, want ErrOverflow: there is no yesterday before 0001-01-01", err)
	}
	if _, err := Compute(14, 14, nil, nil, date(t, "9999-12-31")); err != nil {
		t.Fatalf("the last day of the range has a trailing window ending yesterday: %v", err)
	}
}

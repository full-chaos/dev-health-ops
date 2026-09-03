package remaining

import (
	"testing"
	"time"
)

func day(t *testing.T, text string) time.Time {
	t.Helper()
	parsed, err := time.ParseInLocation("2006-01-02", text, time.UTC)
	if err != nil {
		t.Fatalf("parse %q: %v", text, err)
	}
	return parsed
}

// TestEvaluationInstantIncludesTheFinalizedDay is the assertion the whole
// anchoring exists for, stated as a window question rather than a date-maths one.
//
// The engine derives window_end from now's date and the loader treats it as
// EXCLUSIVE (`day < {end:Date}`). So anchoring `now` to as_of_day itself would
// make window_end == as_of_day and exclude the very partition that just
// finalized -- every run reading one day short, silently (CHAOS-2373 r2).
//
// The test therefore asks the question that matters: given as_of_day, is a row
// ON that day inside the window, and is a row on the day AFTER outside it?
func TestEvaluationInstantIncludesTheFinalizedDay(t *testing.T) {
	asOf := day(t, "2026-08-31")
	now, asOfDay := EvaluationInstant(&asOf, func() time.Time {
		t.Fatal("the wall clock must not be consulted when as_of is supplied")
		return time.Time{}
	})

	if !asOfDay.Equal(asOf) {
		t.Errorf("as_of_day %s, want %s", asOfDay.Format("2006-01-02"), asOf.Format("2006-01-02"))
	}

	// The window the executor would build, derived the same way it derives it.
	windowEnd := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	inWindow := func(d time.Time) bool { return !d.Before(windowEnd.AddDate(0, 0, -30)) && d.Before(windowEnd) }

	if !inWindow(asOf) {
		t.Errorf("a row ON the finalized day %s is OUTSIDE the window [.., %s) -- the "+
			"just-finalized partition is being skipped, which is exactly the CHAOS-2373 "+
			"round-2 defect the +1 anchoring exists to prevent",
			asOf.Format("2006-01-02"), windowEnd.Format("2006-01-02"))
	}
	if after := asOf.AddDate(0, 0, 1); inWindow(after) {
		t.Errorf("a row on %s (the day AFTER the finalized day) is INSIDE the window; "+
			"the anchoring has over-shot and the run reads a day that has not finalized",
			after.Format("2006-01-02"))
	}
}

// TestEvaluationInstantAnchorsAtMidnight pins the time component, because it is
// persisted.
//
// `now` becomes computed_at, which is the argMax key the readers order by. A
// wall-clock instant on the as_of path would produce the correct WINDOW while
// writing a different computed_at -- rows that look right and order differently.
func TestEvaluationInstantAnchorsAtMidnight(t *testing.T) {
	asOf := day(t, "2026-08-31")
	now, _ := EvaluationInstant(&asOf, func() time.Time { return time.Time{} })

	want := day(t, "2026-09-01")
	if !now.Equal(want) {
		t.Errorf("now = %s, want %s (midnight UTC on as_of_day + 1)",
			now.Format(time.RFC3339), want.Format(time.RFC3339))
	}
	if now.Location() != time.UTC {
		t.Errorf("now is in %v, want UTC", now.Location())
	}
}

// TestEvaluationInstantFallsBackToTheWallClock covers the other branch: with no
// as_of, the reference uses wall-clock now and derives as_of_day from it.
//
// Both branches are exercised because the executor cannot tell which one it is
// on -- it receives the resolved instant -- so a bug confined to one branch
// would be invisible from the executor's own tests.
func TestEvaluationInstantFallsBackToTheWallClock(t *testing.T) {
	fixed := time.Date(2026, 9, 2, 13, 45, 7, 0, time.UTC)
	now, asOfDay := EvaluationInstant(nil, func() time.Time { return fixed })

	if !now.Equal(fixed) {
		t.Errorf("now = %s, want the wall clock %s", now.Format(time.RFC3339), fixed.Format(time.RFC3339))
	}
	if want := day(t, "2026-09-02"); !asOfDay.Equal(want) {
		t.Errorf("as_of_day = %s, want %s (the wall clock's date)",
			asOfDay.Format("2006-01-02"), want.Format("2006-01-02"))
	}
	// On this branch the window ENDS today, so today is excluded -- which is
	// correct here and wrong on the as_of path. The two branches genuinely
	// differ, and that difference is the reason the anchoring exists.
	if now.Truncate(24 * time.Hour).Equal(asOfDay.AddDate(0, 0, 1)) {
		t.Error("the wall-clock branch must NOT apply the +1; only the as_of path does")
	}
}

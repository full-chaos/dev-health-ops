package remaining

import (
	"testing"
	"time"
)

func overlapDay(s string) time.Time {
	parsed, err := time.Parse("2006-01-02", s)
	if err != nil {
		panic(err)
	}
	return parsed
}

func TestWindowIntersectionIsTheDaysTwoWindowsShare(t *testing.T) {
	cases := []struct {
		name             string
		requestAnchor    string
		requestDays      int
		runAnchor        string
		runDays          int
		wantFrom, wantTo string
		wantDays         int
	}{
		{"a recompute day inside a wider window anchored later", "2026-10-12", 2, "2026-10-11", 1, "2026-10-11", "2026-10-11", 1},
		{"identical single days", "2026-10-11", 1, "2026-10-11", 1, "2026-10-11", "2026-10-11", 1},
		{"disjoint, run before the window", "2026-10-12", 2, "2026-10-09", 1, "", "", 0},
		{"disjoint, run after the window", "2026-10-12", 2, "2026-10-13", 1, "", "", 0},
		{"a wide manual window reaches into the request", "2026-10-12", 1, "2026-10-20", 30, "2026-10-12", "2026-10-12", 1},
		{"two wide windows share a stretch", "2026-10-12", 10, "2026-10-15", 10, "2026-10-06", "2026-10-12", 7},
		{"the request holds the run window", "2026-10-30", 30, "2026-10-20", 3, "2026-10-18", "2026-10-20", 3},
	}
	for _, c := range cases {
		from, to, days := windowIntersection(overlapDay(c.requestAnchor), c.requestDays, overlapDay(c.runAnchor), c.runDays)
		if days != c.wantDays {
			t.Errorf("%s: days = %d, want %d", c.name, days, c.wantDays)
			continue
		}
		if days == 0 {
			continue
		}
		if from.Format("2006-01-02") != c.wantFrom || to.Format("2006-01-02") != c.wantTo {
			t.Errorf("%s: window = %s..%s, want %s..%s", c.name, from.Format("2006-01-02"), to.Format("2006-01-02"), c.wantFrom, c.wantTo)
		}
	}
}

func TestWindowContainsDayMatchesTheBlockerRule(t *testing.T) {
	anchor := overlapDay("2026-10-12")
	for _, c := range []struct {
		backfillDays int
		day          string
		want         bool
	}{
		{1, "2026-10-12", true}, {1, "2026-10-11", false}, {1, "2026-10-13", false},
		{3, "2026-10-10", true}, {3, "2026-10-09", false}, {3, "2026-10-12", true},
	} {
		if got := windowContainsDay(anchor, c.backfillDays, overlapDay(c.day)); got != c.want {
			t.Errorf("window of %d days ending 2026-10-12 holds %s = %v, want %v", c.backfillDays, c.day, got, c.want)
		}
	}
}

func TestManualRunKindNamesOnlyTheManualGenerations(t *testing.T) {
	for generation, want := range map[string]string{
		"manual-backfill:dora:00000000-0000-4000-8000-000000009674:2026-10-01..2026-10-05": manualRunKindBackfill,
		"manual-recompute:cfr-125:dora:00000000-0000-4000-8000-000000009674":               manualRunKindRecompute,
		"manual-recompute:cfr-125:dora:00000000-0000-4000-8000-000000009674:retry-1":       manualRunKindRecompute,
		"post-sync:abc":               "",
		"fixed-schedule:dora_daily:1": "",
		"":                            "",
		"manual:backfill":             "",
	} {
		if got := manualRunKind(generation); got != want {
			t.Errorf("manualRunKind(%q) = %q, want %q", generation, got, want)
		}
	}
}

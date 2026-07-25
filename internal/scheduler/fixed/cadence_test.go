package fixed

import (
	"testing"
	"time"
)

func mustTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		t.Fatalf("parse %s: %v", value, err)
	}
	return parsed.UTC()
}

func TestIntervalCadenceIsEpochAlignedAndReplicaIndependent(t *testing.T) {
	cadence := EveryInterval(300 * time.Second)
	// Two replicas observing different instants inside the same bucket must
	// derive the same canonical due time. That property, not a shared marker,
	// is what makes duplicate insertion converge on one occurrence.
	first, ok := cadence.Previous(mustTime(t, "2026-07-24T10:02:31Z"), time.UTC)
	if !ok {
		t.Fatal("Previous() returned no occurrence")
	}
	second, ok := cadence.Previous(mustTime(t, "2026-07-24T10:04:59Z"), time.UTC)
	if !ok {
		t.Fatal("Previous() returned no occurrence")
	}
	if !first.Equal(second) {
		t.Fatalf("bucket drift: %s vs %s", first, second)
	}
	if want := mustTime(t, "2026-07-24T10:00:00Z"); !first.Equal(want) {
		t.Fatalf("Previous() = %s, want %s", first, want)
	}
	next, ok := cadence.Next(mustTime(t, "2026-07-24T10:02:31Z"), time.UTC)
	if !ok || !next.Equal(mustTime(t, "2026-07-24T10:05:00Z")) {
		t.Fatalf("Next() = %s ok=%t", next, ok)
	}
}

func TestIntervalCadenceBoundaryIsInclusive(t *testing.T) {
	cadence := EveryInterval(60 * time.Second)
	previous, ok := cadence.Previous(mustTime(t, "2026-07-24T10:00:00Z"), time.UTC)
	if !ok || !previous.Equal(mustTime(t, "2026-07-24T10:00:00Z")) {
		t.Fatalf("Previous() = %s ok=%t; the boundary instant is itself due", previous, ok)
	}
}

func TestDailyCadenceMatchesTheLegacyCrontab(t *testing.T) {
	cadence := DailyAt(1, 0)
	previous, ok := cadence.Previous(mustTime(t, "2026-07-24T00:30:00Z"), time.UTC)
	if !ok || !previous.Equal(mustTime(t, "2026-07-23T01:00:00Z")) {
		t.Fatalf("Previous() = %s ok=%t; before today's fire time the last run is yesterday", previous, ok)
	}
	previous, ok = cadence.Previous(mustTime(t, "2026-07-24T01:00:00Z"), time.UTC)
	if !ok || !previous.Equal(mustTime(t, "2026-07-24T01:00:00Z")) {
		t.Fatalf("Previous() = %s ok=%t", previous, ok)
	}
	next, ok := cadence.Next(mustTime(t, "2026-07-24T01:00:00Z"), time.UTC)
	if !ok || !next.Equal(mustTime(t, "2026-07-25T01:00:00Z")) {
		t.Fatalf("Next() = %s ok=%t", next, ok)
	}
}

func TestDailyCadenceCrossesMonthAndYearBoundaries(t *testing.T) {
	cadence := DailyAt(5, 15)
	previous, ok := cadence.Previous(mustTime(t, "2026-01-01T02:00:00Z"), time.UTC)
	if !ok || !previous.Equal(mustTime(t, "2025-12-31T05:15:00Z")) {
		t.Fatalf("Previous() = %s ok=%t", previous, ok)
	}
	previous, ok = cadence.Previous(mustTime(t, "2026-03-01T02:00:00Z"), time.UTC)
	if !ok || !previous.Equal(mustTime(t, "2026-02-28T05:15:00Z")) {
		t.Fatalf("Previous() = %s ok=%t", previous, ok)
	}
}

func TestWeeklyCadenceMatchesTheLegacyMondayCrontab(t *testing.T) {
	cadence := WeeklyAt(time.Monday, 4, 0)
	// 2026-07-24 is a Friday; the previous Monday 04:00 is 2026-07-20.
	previous, ok := cadence.Previous(mustTime(t, "2026-07-24T09:00:00Z"), time.UTC)
	if !ok || !previous.Equal(mustTime(t, "2026-07-20T04:00:00Z")) {
		t.Fatalf("Previous() = %s ok=%t", previous, ok)
	}
	// Monday before the fire time must fall back a full week.
	previous, ok = cadence.Previous(mustTime(t, "2026-07-20T03:59:00Z"), time.UTC)
	if !ok || !previous.Equal(mustTime(t, "2026-07-13T04:00:00Z")) {
		t.Fatalf("Previous() = %s ok=%t", previous, ok)
	}
	next, ok := cadence.Next(mustTime(t, "2026-07-20T04:00:00Z"), time.UTC)
	if !ok || !next.Equal(mustTime(t, "2026-07-27T04:00:00Z")) {
		t.Fatalf("Next() = %s ok=%t", next, ok)
	}
}

func TestBetweenEnumeratesMissedOccurrencesAscending(t *testing.T) {
	cadence := DailyAt(1, 0)
	occurrences, truncated := cadence.Between(
		mustTime(t, "2026-07-20T00:00:00Z"),
		mustTime(t, "2026-07-24T02:00:00Z"),
		time.UTC,
		10,
	)
	if truncated {
		t.Fatal("Between() truncated a window that fits")
	}
	want := []string{
		"2026-07-20T01:00:00Z", "2026-07-21T01:00:00Z", "2026-07-22T01:00:00Z",
		"2026-07-23T01:00:00Z", "2026-07-24T01:00:00Z",
	}
	if len(occurrences) != len(want) {
		t.Fatalf("Between() returned %d occurrences, want %d", len(occurrences), len(want))
	}
	for index, expected := range want {
		if !occurrences[index].Equal(mustTime(t, expected)) {
			t.Fatalf("occurrence %d = %s, want %s", index, occurrences[index], expected)
		}
	}
}

func TestBetweenReportsTruncationAndKeepsTheNewestWork(t *testing.T) {
	cadence := DailyAt(1, 0)
	occurrences, truncated := cadence.Between(
		mustTime(t, "2026-07-01T00:00:00Z"),
		mustTime(t, "2026-07-24T02:00:00Z"),
		time.UTC,
		2,
	)
	if !truncated {
		t.Fatal("Between() did not report truncation")
	}
	if len(occurrences) != 2 {
		t.Fatalf("Between() returned %d occurrences, want 2", len(occurrences))
	}
	// Truncation must keep the most recent occurrences: stalling on the oldest
	// missed day would never let a long outage catch up.
	if !occurrences[1].Equal(mustTime(t, "2026-07-24T01:00:00Z")) {
		t.Fatalf("truncated window dropped the newest occurrence: %s", occurrences[1])
	}
}

func TestCadenceValidationRejectsMalformedDeclarations(t *testing.T) {
	for name, cadence := range map[string]Cadence{
		"interval too short":        EveryInterval(time.Second),
		"interval too long":         EveryInterval(48 * time.Hour),
		"sub-second interval":       {Kind: CadenceInterval, Interval: 1500 * time.Millisecond},
		"daily hour out of range":   DailyAt(24, 0),
		"daily minute out of range": DailyAt(1, 60),
		"daily with weekday":        {Kind: CadenceDaily, Hour: 1, Weekday: time.Monday},
		"weekly with interval":      {Kind: CadenceWeekly, Interval: time.Minute, Weekday: time.Monday},
		"unknown kind":              {Kind: "hourly"},
	} {
		if err := cadence.Validate(); err == nil {
			t.Errorf("%s: Validate() accepted an invalid cadence", name)
		}
	}
}

func TestCadenceFingerprintsAreStableAndDistinct(t *testing.T) {
	seen := map[string]string{}
	for name, cadence := range map[string]Cadence{
		"interval 300":  EveryInterval(300 * time.Second),
		"interval 60":   EveryInterval(60 * time.Second),
		"daily 01:00":   DailyAt(1, 0),
		"daily 00:45":   DailyAt(0, 45),
		"weekly Mon 04": WeeklyAt(time.Monday, 4, 0),
	} {
		fingerprint := cadence.Fingerprint()
		if previous, duplicate := seen[fingerprint]; duplicate {
			t.Fatalf("%s and %s share fingerprint %s", name, previous, fingerprint)
		}
		seen[fingerprint] = name
	}
	if got := EveryInterval(300 * time.Second).Fingerprint(); got != "interval:300s" {
		t.Fatalf("interval fingerprint = %s", got)
	}
	if got := DailyAt(1, 0).Fingerprint(); got != "cron:0 1 * * *" {
		t.Fatalf("daily fingerprint = %s", got)
	}
	if got := WeeklyAt(time.Monday, 4, 0).Fingerprint(); got != "cron:0 4 * * 1" {
		t.Fatalf("weekly fingerprint = %s", got)
	}
}

func TestWallClockCadenceUsesTheDeclaredZone(t *testing.T) {
	location, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("zoneinfo unavailable: %v", err)
	}
	cadence := DailyAt(1, 0)
	previous, ok := cadence.Previous(mustTime(t, "2026-07-24T04:30:00Z"), location)
	if !ok {
		t.Fatal("Previous() returned no occurrence")
	}
	// 01:00 New York on 2026-07-24 is 05:00Z, which is after the observation,
	// so the previous occurrence is the day before at 05:00Z.
	if !previous.Equal(mustTime(t, "2026-07-23T05:00:00Z")) {
		t.Fatalf("Previous() = %s", previous)
	}
}

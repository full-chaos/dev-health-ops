package venueoracle

import (
	"strings"
	"testing"
	"time"
)

func TestScrubRunValuesBlanksGeneratedIDsAndRunTimesOnly(t *testing.T) {
	floor := time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC)
	ceiling := time.Date(2030, 12, 1, 0, 0, 0, 0, time.UTC)
	scrub := ScrubRunValues(floor, ceiling)
	seeded := StableUUID("seed-1")
	random := "3f2a9c10-7b1e-4c55-9d02-1a2b3c4d5e6f"
	in := strings.Join([]string{
		"id=" + seeded, "id=" + random,
		"run=2026-10-01T12:00:00.123456Z", "run=2026-10-01 12:00:00.5+00", "run=2026-10-01 12:00:00+00:00", "run=2026-10-01T12:00:00",
		"seeded=2026-08-01T00:00:00Z", "request=2031-01-01T00:00:00.5+05:30", "notatime=2026-13-45T99:99:99",
	}, "\n")
	want := strings.Join([]string{
		"id=" + seeded, "id=<id>",
		"run=<now>", "run=<now>", "run=<now>", "run=<now>",
		"seeded=2026-08-01T00:00:00Z", "request=2031-01-01T00:00:00.5+05:30", "notatime=2026-13-45T99:99:99",
	}, "\n")
	got := scrub(in)
	if got != want {
		t.Fatalf("scrub =\n%s\nwant\n%s", got, want)
	}
	if again := scrub(got); again != got {
		t.Fatalf("scrub is not idempotent:\n%s", again)
	}
	if !strings.Contains(seeded, "-5") {
		t.Fatalf("StableUUID %s is not a version 5 id; the scrub's premise (seeded ids are version 5) is broken", seeded)
	}
}

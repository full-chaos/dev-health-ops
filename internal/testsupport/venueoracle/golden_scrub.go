package venueoracle

import (
	"regexp"
	"strings"
	"testing"
	"time"
)

// generatedUUID is a random (version 4) UUID. The ids a test seeds are version
// 5 (StableUUID), so a version 4 id is one a plane generated during the run.
var generatedUUID = regexp.MustCompile(`(?i)[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}`)

// isoTimestamp is an ISO date and time, with a T or a space between them, an
// optional fraction and an optional zone.
var isoTimestamp = regexp.MustCompile(`\d{4}-\d\d-\d\d[T ]\d\d:\d\d:\d\d(?:\.\d+)?(?:Z|[+-]\d\d(?::?\d\d)?)?`)

var timestampLayouts = []string{
	time.RFC3339Nano,
	"2006-01-02T15:04:05.999999999",
	"2006-01-02 15:04:05.999999999Z07:00",
	"2006-01-02 15:04:05.999999999Z07",
	"2006-01-02 15:04:05.999999999-0700",
	"2006-01-02 15:04:05.999999999",
}

// ScrubRunValues is a GoldenSpec.Scrub for what a plane makes during the run
// and a test does not seed: a random (version 4) UUID becomes "<id>", and a
// timestamp in [floor, ceiling) becomes "<now>". Pick floor after the
// goldens' seeded times and before the recording, and ceiling before the
// earliest time a request supplies: a time outside the window stays as
// written, so a seeded or requested time is still compared by value. A
// timestamp that does not parse stays as written. The scrub is idempotent
// and runs on both planes.
//
// keep lists ids that have the shape of a random UUID and are deterministic
// (a test that seeds "10000000-0000-4000-8000-000000000001"): they are never
// blanked, so they stay compared by value. A test names its seeded ids here;
// StableUUID ids need no entry.
func ScrubRunValues(floor, ceiling time.Time, keep ...string) func(string) string {
	kept := map[string]bool{}
	for _, id := range keep {
		kept[strings.ToLower(id)] = true
	}
	return func(text string) string {
		text = generatedUUID.ReplaceAllStringFunc(text, func(match string) string {
			if kept[strings.ToLower(match)] {
				return match
			}
			return "<id>"
		})
		return isoTimestamp.ReplaceAllStringFunc(text, func(match string) string {
			for _, layout := range timestampLayouts {
				if at, err := time.Parse(layout, match); err == nil {
					if !at.Before(floor) && at.Before(ceiling) {
						return "<now>"
					}
					return match
				}
			}
			return match
		})
	}
}

// Project is response as the golden compares it: tokens projected to their
// claims, the spec's Scrub applied, Volatile header values replaced. Diff does
// this to the Go plane's response itself; a test that compares with Compare
// instead of Diff (its two planes send different requests) calls Project on the
// Go response before it compares, because the golden's answers are stored
// projected. A response that cannot be projected fails the test.
func (g *Golden) Project(t *testing.T, response Response) Response {
	t.Helper()
	projected, err := g.projectResponse(response)
	if err != nil {
		t.Fatalf("golden %s: %v", g.spec.Path, err)
	}
	return projected
}

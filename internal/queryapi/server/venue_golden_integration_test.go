//go:build integration

package server

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// venuePythonBuild is the build whose Python api answered the frozen venue
// oracles of this package: the last one that still carried the Python
// resolvers.
const venuePythonBuild = "a4847c5e93607451a0c987b314d37e02fc43ce85"

// venueGolden is the spec of one frozen venue oracle of this package. pin is the
// literal "PIN:<name>" until the record verb writes the digest.
func venueGolden(file, test, pin string) venueoracle.GoldenSpec {
	return venueoracle.GoldenSpec{
		Path:        "testdata/venue/" + file + ".json",
		PythonBuild: venuePythonBuild,
		SHA256:      pin,
		Recipe: fmt.Sprintf("git worktree add --detach $DIR %s (with its .venv: uv sync --frozen --no-install-project); then from the repository root: "+
			"go run ./internal/testsupport/venueoracle/goldenrecord -pkg ./internal/queryapi/server/ -test '^%s$' -python-root $DIR "+
			"(records on the pinned build, replays the candidate in a fresh process, and only then promotes it and pins its digest)",
			venuePythonBuild, test),
		Scrub: runValueScrub,
	}
}

// stableVenueID is the id a golden-backed seed gives the row it names: the
// recording run and every frozen run seed the same ids, so a request or a
// response that carries one is the same bytes in both.
func stableVenueID(name string) uuid.UUID {
	return uuid.MustParse(venueoracle.StableUUID(name))
}

// The window of the readings a plane makes during a run: after every instant a
// test seeds (the latest is the 2026-01-02 seed) and far ahead. The frozen
// answers hold such a reading as a placeholder, and the Go answers of a later
// run are scrubbed with the same window, so a reading of the clock is never
// compared as a value.
var (
	runValueFloor   = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	runValueCeiling = time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
)

// runValueScrub numbers the ids a plane generated (version 4 or 7) during the run by first
// appearance in the text (an id a test seeds stays) and turns a timestamp inside
// the run-value window into a placeholder that keeps what a plane is held to:
// whether it has a fractional part and which offset form it is written in.
// It is deterministic and idempotent, as GoldenSpec.Scrub requires.
func runValueScrub(text string) string {
	numbered := map[string]int{}
	text = generatedID.ReplaceAllStringFunc(text, func(id string) string {
		if seededIDs[id] {
			return id
		}
		if _, ok := numbered[id]; !ok {
			numbered[id] = len(numbered) + 1
		}
		return fmt.Sprintf("<uuid#%d>", numbered[id])
	})
	// A Python datetime written as its repr (a database error message quotes its parameters).
	text = pythonDatetimeRepr.ReplaceAllStringFunc(text, func(repr string) string {
		parts := pythonDatetimeRepr.FindStringSubmatch(repr)
		var fields [7]int
		for index := range fields {
			if parts[index+1] != "" {
				fields[index], _ = strconv.Atoi(parts[index+1])
			}
		}
		at := time.Date(fields[0], time.Month(fields[1]), fields[2], fields[3], fields[4], fields[5], fields[6]*1000, time.UTC)
		if at.Before(runValueFloor) || !at.Before(runValueCeiling) {
			return repr
		}
		return "<datetime>"
	})
	return oracleTime.ReplaceAllStringFunc(text, func(stamp string) string {
		parsed, err := time.Parse(time.RFC3339Nano, stamp)
		if err != nil || parsed.Before(runValueFloor) || !parsed.Before(runValueCeiling) {
			return stamp
		}
		shape := "nofrac"
		if strings.Contains(stamp, ".") {
			shape = "frac"
		}
		offset := "Z"
		if strings.HasSuffix(stamp, "+00:00") {
			offset = "+00:00"
		} else if !strings.HasSuffix(stamp, "Z") {
			offset = "other"
		}
		return "<ts:" + shape + ":" + offset + ">"
	})
}

// pythonDatetimeRepr is datetime.datetime(year, month, day[, hour[, minute[, second[, microsecond]]]], tzinfo=datetime.timezone.utc).
var pythonDatetimeRepr = regexp.MustCompile(`datetime\.datetime\((\d+), (\d+), (\d+)(?:, (\d+))?(?:, (\d+))?(?:, (\d+))?(?:, (\d+))?, tzinfo=datetime\.timezone\.utc\)`)

// generatedID is a random (version 4) or time-ordered (version 7) id: the Python plane mints version 4 and
// the Go api version 7. An id derived from a name (version 5) is data and is compared as written.
var generatedID = regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[47][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}`)

var scrubbedTimestamp = regexp.MustCompile(`<ts:(frac|nofrac):([^>]*)>`)

//go:build integration

package admin_test

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// adminPythonBuild is a build whose Python api still answered the admin routes
// the goldens under testdata/admin freeze (users, organisations, invites,
// impersonation, org deletion, LLM settings status, the admin rate limits): main
// when they were recorded, before the Python admin routers were deleted.
const adminPythonBuild = "a4847c5e93607451a0c987b314d37e02fc43ce85"

// adminGolden is the GoldenSpec of one admin venue oracle: file is the golden's
// name under testdata/admin, test the oracle's function name, digest the SHA-256
// the test pins ("PIN:<file>" until its first recording).
func adminGolden(file, test, digest string) venueoracle.GoldenSpec {
	return venueoracle.GoldenSpec{
		Path:        "testdata/admin/" + file + ".json",
		PythonBuild: adminPythonBuild,
		SHA256:      digest,
		Recipe: fmt.Sprintf("git worktree add --detach $DIR %s (with its .venv: uv sync --frozen --no-install-project); then from the repository root: "+
			"go run ./internal/testsupport/venueoracle/goldenrecord -pkg ./internal/apiservice/admin/ -test '^%s$' -python-root $DIR "+
			"(records on the pinned build, replays the candidate in a fresh process, and only then promotes it and pins its digest)",
			adminPythonBuild, test),
	}
}

// The run window of the admin goldens: a time a plane makes while the oracle
// runs lies in it. The floor is after every time a seed writes down (seedClock
// starts on 2026-01-01) and before the day the goldens were recorded; the
// ceiling is after the farthest time a route computes from the clock (the
// impersonation TTL of about 292 years). No request of these oracles supplies
// a time, and a time outside the window stays compared by value.
var (
	adminRunFloor   = time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC)
	adminRunCeiling = time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC)
)

// adminRunValuesGolden is adminGolden for an oracle whose answers hold what a
// plane makes during the run: a random id of a created row becomes "<id>" and
// a time in the run window "<now>", on both planes, so the golden holds no
// value that is another one in every run. The golden's header lists what was
// blanked, and the record verb refuses a blanked leaf that is the same in two
// recordings.
//
// more are the scrubs of what else the oracle's routes make at random; each
// runs after the run values, on the same text.
func adminRunValuesGolden(file, test, digest string, more ...func(string) string) venueoracle.GoldenSpec {
	spec := adminGolden(file, test, digest)
	runValues := venueoracle.ScrubRunValues(adminRunFloor, adminRunCeiling)
	spec.Scrub = func(text string) string {
		text = runValues(text)
		for _, scrub := range more {
			text = scrub(text)
		}
		return text
	}
	return spec
}

var adminISOTime = regexp.MustCompile(`\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?(?:Z|[+-]\d\d:\d\d)?`)

// adminRunTimeShapesGolden is adminGolden for an oracle whose seed writes a
// time relative to the wall clock with a fixed precision: a time in the run
// window keeps its spelling (each digit becomes D), so the precision and the
// zone suffix of the two planes are still compared. The test checks the Go
// value itself against what the seed wrote.
func adminRunTimeShapesGolden(file, test, digest string) venueoracle.GoldenSpec {
	spec := adminGolden(file, test, digest)
	inWindow := venueoracle.ScrubRunValues(adminRunFloor, adminRunCeiling)
	spec.Scrub = func(text string) string {
		return adminISOTime.ReplaceAllStringFunc(text, func(match string) string {
			if inWindow(match) != "<now>" {
				return match
			}
			return strings.Map(func(r rune) rune {
				if r >= '0' && r <= '9' {
					return 'D'
				}
				return r
			}, match)
		})
	}
	return spec
}

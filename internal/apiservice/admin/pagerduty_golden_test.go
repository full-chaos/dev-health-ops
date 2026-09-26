//go:build integration

package admin_test

import (
	"fmt"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// pagerDutyPythonBuild is the last build whose Python api still answered the
// PagerDuty admin routes: the parent of the change that reduced their Python
// bodies to the Go-served refusal (CHAOS-6843). The goldens under
// testdata/pagerduty are the answers of that build's Python plane, EXECUTED
// (never authored) by the recipe each golden's spec prints, and frozen by the
// digest each test pins.
const pagerDutyPythonBuild = "9642a8da15bb47d7400cef13cc93547b46e5f67a"

// pagerDutyGolden is the GoldenSpec of one PagerDuty venue oracle: file is the
// golden's name under testdata/pagerduty, test the oracle's function name, and
// digest the SHA-256 the test pins (empty only while recording).
func pagerDutyGolden(file, test, digest string) venueoracle.GoldenSpec {
	return venueoracle.GoldenSpec{
		Path:        "testdata/pagerduty/" + file + ".json",
		PythonBuild: pagerDutyPythonBuild,
		SHA256:      digest,
		Recipe: fmt.Sprintf("git worktree add --detach $DIR %s; then from the repository root: "+
			"go run ./internal/testsupport/venueoracle/goldenrecord -pkg ./internal/apiservice/admin/ -test '^%s$' -python-root $DIR "+
			"(records on the pinned build, replays the candidate in a fresh process, and only then promotes it and pins its digest)",
			pagerDutyPythonBuild, test),
	}
}

//go:build !race

package workflows_test

import (
	"testing"
)

// These tests run real ci scripts and child processes; -race adds nothing to them and they sink the package under the race timeout. ci/race_excluded_tests.tsv runs each one without -race.

func TestGoWorkflowPathFilters(t *testing.T) {
	expectNoViolations(t, pathFilterViolations(t, repoRoot(t), uncoveredFixtureDirectories))
}

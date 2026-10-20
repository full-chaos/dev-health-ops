//go:build !race

package affectedreject_test

import (
	"testing"
)

// This test runs real ci scripts and child processes; -race adds nothing to it and it sinks the package under the race timeout. ci/race_excluded_tests.tsv runs it without -race.

func TestGoAffectedIntegrationPlannerRejectsMalformedSelections(t *testing.T) {
	t.Parallel()
	script := scriptPath(t, "ci/check_go.sh")
	scenarios := affectedPlannerRejectScenarios()
	assertAllPass(t, script, scenarios)
	assertPlantsCaught(t, script, scenarios, []plant{
		pc("unplanned package accepted", `|| die "affected selection names '${line}', which is not a planned integration package"`, `|| true`,
			"planner_rejects_an_unplanned_package", "accepted"),
		pc("count mismatch accepted", `|| die "affected selection declares ${declared} package(s) but lists ${count}"`, `|| true`,
			"planner_rejects_a_count_mismatch", "accepted"),
		pc("duplicate key accepted", `|| die "affected selection lists '${line}' more than once"`, `|| true`,
			"planner_rejects_a_duplicate_key", "accepted"),
		pc("unknown first line accepted", `*) die "INTEGRATION_AFFECTED_FILE ${file} has an unknown first line '${first}'" ;;`, `*) return 0 ;;`,
			"planner_rejects_an_unknown_first_line", "accepted"),
	})
}

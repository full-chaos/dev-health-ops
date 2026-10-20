//go:build !race

package checkgo_test

import (
	"testing"
)

// These tests run real ci scripts and child processes; -race adds nothing to them and they sink the package under the race timeout. ci/race_excluded_tests.tsv runs each one without -race.

func TestCheckGoMultiReplicaGateExecutes(t *testing.T) {
	t.Parallel()
	script := scriptPath(t, "ci/check_go.sh")
	scenarios := multiReplicaScenarios()
	assertAllPass(t, script, scenarios)
	assertPlantsCaught(t, script, scenarios, []plant{
		pc("the gate returns before running anything", "check_multi_replica_workers() {\n  local proof_dir proof_file measured result\n", "check_multi_replica_workers() {\n  return 0\n  local proof_dir proof_file measured result\n",
			"multi_replica_gate_runs_the_required_test", "want exit 0"),
		pc("zero measured jobs accepted", `if [ "${measured}" -le 0 ]; then`, `if false; then`,
			"multi_replica_gate_fails_on_zero_measured_jobs", "measured zero jobs"),
		pc("a missing proof accepted", `if [ ! -f "${proof_file}" ]; then`, `if false; then`,
			"multi_replica_gate_fails_without_a_proof_file", "no measured-job proof"),
		pc("the test run is warm", "-tags=integration -count=1 -timeout=5m", "-tags=integration -timeout=5m",
			"multi_replica_gate_runs_the_required_test", "-count=1"),
	})
}

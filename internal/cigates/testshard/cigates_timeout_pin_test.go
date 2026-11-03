package testshard_test

import (
	"strings"
	"testing"
)

// CHAOS-11787 (interim, removed by CHAOS-11768): the text of the timeout rule in check_test_shard and the race leg.

func TestTheTestLegTimeoutsComeFromTheCigatesHelper(t *testing.T) {
	script := readCheckGo(t)
	body := functionBody(t, script, "check_test_shard")
	for _, want := range []string{
		`rows_timeout="$(cigates_go_test_timeout ${rest_rows})"`,
		`go_test_leg "${log}" "${legjson}" -timeout "$(cigates_go_test_timeout ${rest})" ${rest}`,
		// The security call has no -count=1 of its own: go_test_leg adds it, once, under MERGE_GROUP_FRESH (rest_sec exists only then).
		`go_test_leg "${log}" "${legjson}" -timeout "$(cigates_go_test_timeout ${rest_sec})" ${rest_sec}`,
		`go_test_leg "${log}" "${legjson}" -timeout 10m "${SERIAL_HEAVY_TEST_PACKAGE}"`,
	} {
		if strings.Count(body, want) != 1 {
			t.Errorf("check_test_shard lacks exactly one of: %s", want)
		}
	}
	if strings.Contains(body, "CHECKGO_PACKAGE") || strings.Contains(script, "\nCHECKGO_PACKAGE=") {
		t.Error("the checkgo-only rule is still in ci/check_go.sh")
	}
}

func TestTheRaceLegKeepsItsOwnTimeoutVariable(t *testing.T) {
	script := readCheckGo(t)
	if n := strings.Count(script, `-timeout "${DEV_HEALTH_GO_RACE_TEST_TIMEOUT:-10m}"`); n != 4 {
		t.Errorf("the race leg names DEV_HEALTH_GO_RACE_TEST_TIMEOUT:-10m %d time(s), want 4 (the helper does not touch it)", n)
	}
	if strings.Contains(functionBody(t, script, "check_race_shard"), "cigates_go_test_timeout") {
		t.Error("the race leg uses the cigates helper")
	}
}

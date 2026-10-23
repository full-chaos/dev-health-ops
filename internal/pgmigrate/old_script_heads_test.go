package pgmigrate_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// statesPythonBuild is the build whose Python answered the upgrade, walk and derivation oracles of this package
// (the same build as the downgrade golden).
const statesPythonBuild = "de4bc686e35308fb20670ad6ce8333952e2b43e6"

// oldScriptHeadsSHA256 pins testdata/golden/old_script_heads.json .
const oldScriptHeadsSHA256 = "caf5ba12ace9b6d9c358ce4978e8377accf1912bd3cafb611da81e5b98d628aa"

// TestBuildHeadsAreTheOldScriptsHeads is the old-script oracle: the heads the roll pre-check derived from the
// Alembic scripts are the heads the preflight reports for this build. The derivation ran once over the scripts
// of statesPythonBuild and its answer is frozen in testdata/golden/old_script_heads.json (CHAOS-7797), so no
// Python starts here and the test outlives the scripts. An empty answer fails: the measurement did not happen.
func TestBuildHeadsAreTheOldScriptsHeads(t *testing.T) {
	golden := venueoracle.OpenGolden(t, venueoracle.GoldenSpec{
		Path:        "testdata/golden/old_script_heads.json",
		PythonBuild: statesPythonBuild,
		SHA256:      oldScriptHeadsSHA256,
	})
	request := venueoracle.ProgramRequest("old script derivation", nil)
	answers := golden.Produce(t, []venueoracle.Request{request})
	golden.Consumed(t, answers...)
	derived := strings.Fields(answers[0].Body)
	if len(derived) == 0 {
		t.Fatal("the old script's derivation found no revision: the measurement did not happen")
	}
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	chain = pgmigrate.ChainThrough(chain, pgmigrate.FrozenHead) // the derivation ran over the frozen scripts
	if got := pgmigrate.Heads(baseline, chain); !reflect.DeepEqual(got, derived) {
		t.Fatalf("the old script derived the heads %v, the preflight's build_heads are %v", derived, got)
	}
	golden.SkipDiff(t)
	golden.Finish(t)
}

package analytics

import (
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/ownershipwindow"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
	"github.com/full-chaos/dev-health-ops/internal/teamactive"
)

var ownershipRuleNames = ownershipwindow.Names{Org: teamscope.OwnerSharesBindingOrgID, AsOf: teamscope.OwnerSharesBindingAsOf}

// ruleDefects names what the investment-path team source lacks of the one ownership rule (D5987 option C): the window of
// package ownershipwindow (the instant clamped to the repository's first record) and its inactive-team exclusion. The
// source takes both from teamscope.OwnerShares, which renders them; a copy that rewrites either is a defect.
func ruleDefects(source string) []string {
	var defects []string
	if !strings.Contains(source, ownershipwindow.Window(ownershipRuleNames)) {
		defects = append(defects, "the shared validity window is not rendered")
	}
	if !strings.Contains(source, ownershipwindow.ActiveTeam(ownershipRuleNames)) {
		defects = append(defects, "the shared inactive-team exclusion is not rendered")
	}
	return defects
}

func TestOwnershipTeamSourceRendersTheOneOwnershipRule(t *testing.T) {
	asOf := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	for _, scoped := range [][]string{nil, {"team-a"}} {
		source, bindings := ownershipTeamSourceAt("org-1", scoped, asOf)
		if defects := ruleDefects(source); len(defects) != 0 {
			t.Errorf("scoped %v: %v\n%s", scoped, defects, source)
		}
		var sawAsOf bool
		for _, b := range bindings {
			if b.Name == teamscope.OwnerSharesBindingAsOf {
				sawAsOf = b.Value == asOf
			}
		}
		if !sawAsOf {
			t.Errorf("scoped %v: the source is not bound to the given instant %v: %+v", scoped, asOf, bindings)
		}
	}
	// The investment path asks at the present.
	now, bindings := ownershipTeamSource("org-1", nil)
	if defects := ruleDefects(now); len(defects) != 0 {
		t.Errorf("ownershipTeamSource: %v", defects)
	}
	bound := false
	for _, b := range bindings {
		if b.Name == teamscope.OwnerSharesBindingAsOf {
			bound = true
			if at, ok := b.Value.(time.Time); !ok || time.Since(at) > time.Minute || at.After(time.Now().Add(time.Minute)) {
				t.Errorf("ownershipTeamSource is not bound to the present: %v", b.Value)
			}
		}
	}
	if !bound {
		t.Errorf("ownershipTeamSource binds no instant: %+v", bindings)
	}
}

// plantedRuleSources are copies of the source with one defect of the rule each; the check above must refuse every one.
func plantedRuleSources(source string) map[string]string {
	clamp := "greatest({" + teamscope.OwnerSharesBindingAsOf + ":DateTime64(3, 'UTC')}, "
	return map[string]string{
		"clamp removed":           strings.ReplaceAll(source, clamp, "coalesce({"+teamscope.OwnerSharesBindingAsOf+":DateTime64(3, 'UTC')}, "),
		"clamp on every row":      strings.ReplaceAll(source, clamp, "coalesce(NULL, "),
		"inactive team counted":   strings.ReplaceAll(source, teamactive.NewestRowInactive, "1 = 0"),
		"inactive guard inverted": strings.ReplaceAll(source, "NOT IN (", "IN ("),
	}
}

func TestOwnershipRuleChecksFireOnPlantedDefects(t *testing.T) {
	source, _ := ownershipTeamSourceAt("org-1", nil, time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC))
	for name, planted := range plantedRuleSources(source) {
		if planted == source {
			t.Errorf("%s: the plant changed nothing", name)
			continue
		}
		if len(ruleDefects(planted)) == 0 {
			t.Errorf("%s: the check does not refuse the planted source", name)
		}
	}
}

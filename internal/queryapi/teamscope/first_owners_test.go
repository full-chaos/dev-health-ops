package teamscope

import (
	"fmt"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/sqlshape"
)

// D5987 option C (CHAOS-9404): the ownership window clamps the instant up to the repository's first recorded ownership,
// so the first recorded owners apply before it; one definition for RepoCondition and OwnerShares.
func TestOwnershipWindowClampsTheInstantToTheFirstRecord(t *testing.T) {
	condition, _ := RepoCondition("org-1", "repo_id", []string{"team-a"}, asOf)
	instant := fmt.Sprintf("greatest({%s:DateTime64(3, 'UTC')}, ", BindingAsOf)
	if strings.Count(condition, instant) != 2 {
		t.Errorf("both window bounds must test the clamped instant %q:\n%s", instant, condition)
	}
	if !strings.Contains(condition, "min(o.valid_from)") {
		t.Errorf("the first record of a repository is its earliest valid_from:\n%s", condition)
	}
	shares, _ := OwnerShares("org-1", nil, asOf)
	if !strings.Contains(shares, fmt.Sprintf("greatest({%s:DateTime64(3, 'UTC')}, ", OwnerSharesBindingAsOf)) {
		t.Errorf("OwnerShares does not clamp the instant to the first record:\n%s", shares)
	}
}

// An inactive team owns nothing (section 0.4g rule 1): its rows are not owners and do not set the first record.
func TestOwnershipExcludesInactiveTeams(t *testing.T) {
	for name, text := range map[string]string{
		"RepoCondition": func() string { c, _ := RepoCondition("org-1", "repo_id", []string{"team-a"}, asOf); return c }(),
		"OwnerShares":   func() string { s, _ := OwnerShares("org-1", nil, asOf); return s }(),
	} {
		if !strings.Contains(text, "argMax(is_active") {
			t.Errorf("%s reads no inactive-team rule:\n%s", name, text)
		}
		idx := strings.Index(text, "trimBoth(o.team_id) NOT IN")
		if idx < 0 {
			t.Errorf("%s does not drop the rows of an inactive team:\n%s", name, text)
			continue
		}
		depths := sqlshape.Depths(text)
		ownership := strings.Index(text, "FROM team_repo_ownership AS o FINAL")
		if depths[idx] != depths[ownership] {
			t.Errorf("%s: the inactive-team guard sits at depth %d, the ownership read at %d", name, depths[idx], depths[ownership])
		}
	}
}

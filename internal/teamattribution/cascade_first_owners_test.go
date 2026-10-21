package teamattribution

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/ownershipwindow"
)

func TestLoadReposTestsTheOwnershipWindowOfTheSharedRule(t *testing.T) {
	window := ownershipwindow.Window(ownershipwindow.Names{Org: "org_id", AsOf: "as_of"})
	query := loadReposQuery(loadReposOwnershipWindow)
	if !strings.Contains(query, "WHERE "+window+"\n") {
		t.Errorf("LoadRepos does not render the window of package ownershipwindow:\n%s", query)
	}
	if strings.Contains(query, "o.valid_from <= {as_of:DateTime64(3, 'UTC')} AND") {
		t.Errorf("LoadRepos still tests the plain window:\n%s", query)
	}
	for _, want := range []string{
		"FROM repos FINAL",
		"AND r.provider = o.provider",
		"AND lower(r.repo) = lower(o.repo_full_name)",
	} {
		if !strings.Contains(query, want) {
			t.Errorf("LoadRepos lacks the repos join part %q that the window's repository key reads:\n%s", want, query)
		}
	}
}

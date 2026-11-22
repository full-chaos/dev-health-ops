package teamownership

import (
	"os"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/ownershipwindow"
)

// Every ownership query of this package renders the one validity window of package ownershipwindow (D5987 option C,
// CHAOS-9404): the first recorded owners apply before the first record. A query with its own bound would give the
// readers two rules.
func TestEveryOwnershipQueryRendersTheOneWindow(t *testing.T) {
	if want := ownershipwindow.Window(ownershipwindow.Names{Org: "org_id", AsOf: "as_of"}); ownershipWindow != want {
		t.Fatalf("ownershipWindow is not the shared window:\n%s", ownershipWindow)
	}
	raw, err := os.ReadFile("teamownership.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)
	if got := strings.Count(source, "FROM team_repo_ownership AS o FINAL"); got != 4 {
		t.Fatalf("this test expects four ownership queries, found %d: render the window in the new one too", got)
	}
	if got := strings.Count(source, "`+ownershipWindow+`"); got != 3 {
		t.Errorf("%d of the three ownership queries with their own activity read render the shared window", got)
	}
	// OwnerCandidatesByRepo renders the same window over the caller's activity snapshot.
	if got := strings.Count(source, "`+ownershipWindowBoundInactive+`"); got != 1 {
		t.Errorf("%d ownership queries render the bound-snapshot window, want 1", got)
	}
	if want := ownershipwindow.WindowBoundInactive(ownershipwindow.Names{Org: "org_id", AsOf: "as_of"}, inactiveTeamIDsParam); ownershipWindowBoundInactive != want {
		t.Errorf("ownershipWindowBoundInactive is not the shared bound-snapshot window:\n%s", ownershipWindowBoundInactive)
	}
	for _, own := range []string{"o.valid_from <= {", "o.valid_to > {"} {
		if strings.Contains(source, own) {
			t.Errorf("a query bounds the validity window itself (%q): it must render ownershipWindow", own)
		}
	}
}

package apiservice

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/routescan"
)

// A line inserted above a registration or a validator moves every line below it and changes no code. The row gates
// must hold on such a tree: a row is tied to its registration and its validators by symbol, not by line number.

func shiftedRoot(t *testing.T, shifts map[string]string) (string, func(string) string) {
	t.Helper()
	root := routescan.ShiftedTree(t, "../..", []string{"internal/api", "internal/apiservice"}, nil, shifts)
	return root, func(scanned string) string {
		rel, err := filepath.Rel(root, scanned)
		if err != nil {
			t.Fatal(err)
		}
		return filepath.ToSlash(rel)
	}
}

func TestRowRegistrationsSurviveALineShift(t *testing.T) {
	root, rel := shiftedRoot(t, map[string]string{"internal/api/teamsidentity/teamsidentity.go": "func Routes("})
	regs := scanRegistrations(t, filepath.Join(root, "internal/api"), filepath.Join(root, "internal/apiservice"))
	problems, checked := rowRegistrationProblems(t, regs, loadGoAPIRows(t), mountedRoutes(t), rel)
	if len(problems) != 0 {
		t.Errorf("a line shift broke %d of %d rows:\n%s", len(problems), checked, strings.Join(problems, "\n"))
	}
}

func TestValidatorAnchorsSurviveALineShift(t *testing.T) {
	root, _ := shiftedRoot(t, map[string]string{
		"internal/api/policy/guard.go":                "func (g *Guard) Wrap(",
		"internal/api/externalingest/deps.go":         "func Routes(",
		"internal/api/teamsidentity/teamsidentity.go": "func Routes(",
	})
	problems, checked := anchorNoteProblems(t, root, loadGoAPIRows(t))
	if len(problems) != 0 {
		t.Errorf("a line shift broke %d of %d validator anchors:\n%s", len(problems), checked, strings.Join(problems, "\n"))
	}
}

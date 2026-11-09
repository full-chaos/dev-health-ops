package apiservice

import (
	"encoding/json"
	"os"
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
	problems, checked, _ := rowRegistrationProblems(t, regs, loadGoAPIRows(t), mountedRoutes(t), rel)
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
	problems, checked, _ := anchorNoteProblems(t, root, loadGoAPIRows(t))
	if len(problems) != 0 {
		t.Errorf("a line shift broke %d of %d validator anchors:\n%s", len(problems), checked, strings.Join(problems, "\n"))
	}
}

// doubleLine writes the line holding marker, and the line after it, twice into the file at path.
func doubleLine(t *testing.T, path, marker string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(string(raw), "\n")
	for i, line := range lines {
		if strings.Contains(line, marker) && i+1 < len(lines) {
			lines = append(lines[:i+2], append([]string{lines[i], lines[i+1]}, lines[i+2:]...)...)
			if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")), 0o644); err != nil {
				t.Fatal(err)
			}
			return
		}
	}
	t.Fatalf("%s: no line holds %q", path, marker)
}

// A registration written twice in its file is not one symbol: the row gate fails it as ambiguous.
func TestRowGateFailsOnADoubledRegistration(t *testing.T) {
	root, rel := shiftedRoot(t, nil)
	doubleLine(t, filepath.Join(root, "internal/api/teamsidentity/teamsidentity.go"), `{Method: http.MethodGet, Pattern: "/api/v1/admin/teams",`)
	regs := scanRegistrations(t, filepath.Join(root, "internal/api"), filepath.Join(root, "internal/apiservice"))
	problems, _, _ := rowRegistrationProblems(t, regs, loadGoAPIRows(t), mountedRoutes(t), rel)
	if len(problems) != 1 || !strings.Contains(problems[0], "GET /api/v1/admin/teams@api: source anchor ambiguous") || !strings.Contains(problems[0], "2 times") {
		t.Fatalf("a doubled registration gave %v", problems)
	}
}

// A validator anchor whose note names no place, or several places outside the row's own registration, fails.
func TestValidatorAnchorFailsWhenItsNoteDoesNotResolveToOnePlace(t *testing.T) {
	var base map[string]any
	for _, row := range loadGoAPIRows(t) {
		if row["id"] == "GET /api/v1/admin/identities [dev-health-ops-api @api]" {
			raw, _ := json.Marshal(row)
			_ = json.Unmarshal(raw, &base)
		}
	}
	if base == nil {
		t.Fatal("the identities read row is missing")
	}
	cases := []struct {
		name  string
		plant func(row map[string]any)
		want  string
	}{
		{"note on no line", func(row map[string]any) {
			row["primary_validator"].(map[string]any)["anchor"].(map[string]any)["note"] = "func (g *Guard) WrapNowhere"
		}, "anchor stale: internal/api/policy/guard.go holds no line with"},
		{"note on several lines of another file", func(row map[string]any) {
			row["primary_validator"].(map[string]any)["anchor"].(map[string]any)["note"] = "func (g *Guard)"
		}, "anchor ambiguous: \"func (g *Guard)\" is on"},
		{"weak note on the row's file, route not registered", func(row map[string]any) {
			row["route"] = "/api/v1/admin/identities-gone"
			anchor := row["primary_validator"].(map[string]any)["anchor"].(map[string]any)
			anchor["path"], anchor["note"] = "internal/api/teamsidentity/teamsidentity.go", "Pattern:"
		}, "source anchor stale: "},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw, _ := json.Marshal(base)
			var row map[string]any
			_ = json.Unmarshal(raw, &row)
			tc.plant(row)
			problems, _, _ := anchorNoteProblems(t, "../..", []map[string]any{row})
			if len(problems) != 1 || !strings.Contains(problems[0], tc.want) {
				t.Fatalf("got %v, want one problem with %q", problems, tc.want)
			}
		})
	}
}

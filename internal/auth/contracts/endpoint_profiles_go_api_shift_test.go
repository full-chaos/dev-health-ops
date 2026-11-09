package contracts

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/routescan"
)

// A line inserted above the teams registrations moves every line below it and changes no code. The admin-field
// checks must hold on such a tree: they find a row's registration by symbol, never by the line the row cites.
func TestAdminFieldsSurviveALineShift(t *testing.T) {
	rows := coveredRows(t)
	var cited []string
	for _, row := range rows {
		if source, ok := row["source"].(map[string]any); ok {
			if file, ok := source["file"].(string); ok {
				cited = append(cited, file)
			}
		}
		for _, field := range []string{"resource_resolver", "current_state_cache_behavior", "disclosure_behavior"} {
			text, _ := row[field].(string)
			for _, match := range anchorPattern.FindAllStringSubmatch(text, -1) {
				cited = append(cited, match[1])
			}
		}
	}
	root := routescan.ShiftedTree(t, testRoot(t), []string{"internal/api", "internal/apiservice"}, cited,
		map[string]string{"internal/api/teamsidentity/teamsidentity.go": "func Routes("})
	scopes := catalogScopes(t)
	var problems []string
	resolved := 0
	for _, row := range rows {
		problems = append(problems, adminFieldViolations(row, scopes, root)...)
		if _, _, err := registrationEntry(row, root); err == nil {
			resolved++
		}
	}
	if len(problems) != 0 {
		t.Errorf("a line shift broke %d admin-field checks:\n%s", len(problems), strings.Join(problems, "\n"))
	}
	if resolved < 208 {
		t.Fatalf("only %d of %d covered rows resolved their registration on the shifted tree: the measurement did not happen", resolved, len(rows))
	}
}

// A registration written twice in its file is not one symbol: the row that cites it fails as ambiguous, and so does a
// row whose method or file names no registration (the planted cells in TestAdminFieldChecksCatchPlantedDefects).
func TestAdminFieldsFailOnADoubledRegistration(t *testing.T) {
	const file = "internal/api/teamsidentity/teamsidentity.go"
	root := routescan.ShiftedTree(t, testRoot(t), []string{"internal/api/teamsidentity"}, nil, nil)
	path := filepath.Join(root, file)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(string(raw), "\n")
	at := -1
	for i, line := range lines {
		if strings.Contains(line, `{Method: http.MethodGet, Pattern: "/api/v1/admin/teams",`) {
			at = i
		}
	}
	if at < 0 || at+1 >= len(lines) {
		t.Fatal("the GET /api/v1/admin/teams registration is missing")
	}
	lines = append(lines[:at+2], append([]string{lines[at], lines[at+1]}, lines[at+2:]...)...)
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")), 0o644); err != nil {
		t.Fatal(err)
	}
	row := map[string]any{"id": "GET /api/v1/admin/teams", "method": "GET", "route": "/api/v1/admin/teams",
		"source": map[string]any{"file": file, "line": float64(at + 1)}}
	if _, _, err := registrationEntry(row, root); err == nil || !strings.Contains(err.Error(), "source anchor ambiguous") || !strings.Contains(err.Error(), "2 times") {
		t.Fatalf("a doubled registration resolved: %v", err)
	}
}

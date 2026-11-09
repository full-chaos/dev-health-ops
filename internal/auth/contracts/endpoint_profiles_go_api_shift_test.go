package contracts

import (
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
	for _, row := range rows {
		problems = append(problems, adminFieldViolations(row, scopes, root)...)
	}
	if len(problems) != 0 {
		t.Errorf("a line shift broke %d admin-field checks:\n%s", len(problems), strings.Join(problems, "\n"))
	}
}

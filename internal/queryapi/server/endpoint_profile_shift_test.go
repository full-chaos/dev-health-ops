package server

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/routescan"
)

// queryAnchorFiles are the files the query-api rows cite: each row's source and its validator anchors.
func queryAnchorFiles(rows []map[string]any) []string {
	var files []string
	var walk func(node any)
	walk = func(node any) {
		switch value := node.(type) {
		case map[string]any:
			if path, ok := value["path"].(string); ok && (value["repo"] == nil || value["repo"] == "ops") {
				files = append(files, path)
			}
			for _, child := range value {
				walk(child)
			}
		case []any:
			for _, child := range value {
				walk(child)
			}
		}
	}
	for _, row := range rows {
		files = append(files, row["source"].(map[string]any)["file"].(string))
		walk(row["primary_validator"])
	}
	return files
}

// A line inserted above the query and REST registrations moves every line below it and changes no code. The row
// checks must hold on such a tree: a row is tied to its registration by symbol, not by line number.
func TestQueryRowAnchorsSurviveALineShift(t *testing.T) {
	rows := loadQueryAPIRows(t)
	root := routescan.ShiftedTree(t, "../../..", []string{"internal/queryapi/server", "internal/platform/health"}, queryAnchorFiles(rows),
		map[string]string{
			"internal/queryapi/server/server.go":      "func mountQueryRoute(",
			"internal/queryapi/server/rest_routes.go": "func RESTRoutes(",
		})
	problems, checked := queryRowAnchorProblems(t, root, rows)
	if len(problems) != 0 {
		t.Errorf("a line shift broke %d of %d query-api anchors:\n%s", len(problems), checked, strings.Join(problems, "\n"))
	}
}

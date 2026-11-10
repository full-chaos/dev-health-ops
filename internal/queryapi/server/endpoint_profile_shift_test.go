package server

import (
	"encoding/json"
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
	problems, checked, _ := queryRowAnchorProblems(t, root, rows)
	if len(problems) != 0 {
		t.Errorf("a line shift broke %d of %d query-api anchors:\n%s", len(problems), checked, strings.Join(problems, "\n"))
	}
	if checked < 30 {
		t.Fatalf("only %d query-api anchors were checked on the shifted tree: the measurement did not happen", checked)
	}
}

// A row whose source names no registration of its method and route, or two that its listeners do not tell apart,
// fails; the line it cites never decides. A method-less mux registration serves every method.
func TestQueryRowFailsWhenItsSourceDoesNotResolveToOneRegistration(t *testing.T) {
	rows := map[string]map[string]any{}
	for _, row := range loadQueryAPIRows(t) {
		rows[row["id"].(string)] = row
	}
	const mcpQuery, summary = "POST /query [dev-health-query-api @mcp]", "GET /api/v1/people/{person_id}/summary [dev-health-query-api @query,internal]"
	cases := []struct {
		name, base string
		plant      func(row map[string]any)
		want       string
	}{
		{"route not registered", mcpQuery, func(row map[string]any) { row["route"] = "/query-gone" }, "source anchor stale: internal/queryapi/server/server.go holds no registration of POST /query-gone"},
		{"listeners no mux serves", mcpQuery, func(row map[string]any) { row["id"] = "POST /query [dev-health-query-api @query,mcp]" }, "source anchor stale: internal/queryapi/server/server.go holds no registration of POST /query"},
		{"method outside the Methods list", summary, func(row map[string]any) { row["method"] = "DELETE" }, "source anchor stale: internal/queryapi/server/rest_routes.go holds no registration of DELETE /api/v1/people/{person_id}/summary"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if rows[tc.base] == nil {
				t.Fatalf("row %s is missing", tc.base)
			}
			raw, _ := json.Marshal(rows[tc.base])
			var row map[string]any
			_ = json.Unmarshal(raw, &row)
			tc.plant(row)
			problems, _, _ := queryRowAnchorProblems(t, "../../..", []map[string]any{row})
			if len(problems) != 1 || !strings.Contains(problems[0], tc.want) {
				t.Fatalf("got %v, want one problem with %q", problems, tc.want)
			}
		})
	}
}

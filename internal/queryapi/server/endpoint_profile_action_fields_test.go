package server

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

var actionFieldKeys = []string{"action", "resource_resolver", "current_state_cache_behavior", "impersonation_policy", "disclosure_behavior"}

var impersonationPolicies = map[string]bool{
	"subject_to_impersonation_override": true,
	"exempt_no_org_context":             true,
	"not_applicable_non_main_app":       true,
	"unknown":                           true,
}

func rowGaps(row map[string]any) []string {
	var gaps []string
	list, _ := row["gaps"].([]any)
	for _, gap := range list {
		if text, ok := gap.(string); ok {
			gaps = append(gaps, text)
		}
	}
	return gaps
}

// checkActionFieldRows returns every defect of the query-api rows against the
// action fields: a public row carries none of the five (not applicable); a
// protected row carries all five; a null value is paired with a gap that
// begins "<field>:"; an action is a catalog action; the impersonation policy
// is one of the schema's values.
func checkActionFieldRows(rows []map[string]any, cat *policycatalog.Catalog) []string {
	var defects []string
	for _, row := range rows {
		id, _ := row["id"].(string)
		if row["classification"] == "public" {
			for _, key := range actionFieldKeys {
				if _, present := row[key]; present {
					defects = append(defects, fmt.Sprintf("%s: public row carries %s", id, key))
				}
			}
			continue
		}
		gaps := rowGaps(row)
		for _, key := range actionFieldKeys {
			value, present := row[key]
			if !present {
				defects = append(defects, fmt.Sprintf("%s: protected row has no %s", id, key))
				continue
			}
			if value == nil {
				named := false
				for _, gap := range gaps {
					if strings.HasPrefix(gap, key+":") {
						named = true
					}
				}
				if !named {
					defects = append(defects, fmt.Sprintf("%s: %s is null with no %q gap", id, key, key+":"))
				}
				continue
			}
			text, isString := value.(string)
			if !isString || text == "" {
				defects = append(defects, fmt.Sprintf("%s: %s is not a non-empty string", id, key))
				continue
			}
			if key == "action" {
				if _, err := cat.Action(text); err != nil {
					defects = append(defects, fmt.Sprintf("%s: action %q is not in the catalog", id, text))
				}
			}
			if key == "impersonation_policy" && !impersonationPolicies[text] {
				defects = append(defects, fmt.Sprintf("%s: impersonation_policy %q is not a schema value", id, text))
			}
		}
	}
	return defects
}

func TestQueryAPIRowsCarryTheActionFields(t *testing.T) {
	rows := loadQueryAPIRows(t)
	if len(rows) < 40 {
		t.Fatalf("only %d rows: the row load stopped seeing them", len(rows))
	}
	for _, defect := range checkActionFieldRows(rows, policycatalog.Default()) {
		t.Error(defect)
	}
}

func TestActionFieldRowChecksCatchPlantedDefects(t *testing.T) {
	cat := policycatalog.Default()
	protected := func() map[string]any {
		return map[string]any{
			"id": "GET /x", "classification": "protected",
			"action": "analytics.read", "resource_resolver": "r", "current_state_cache_behavior": "c",
			"impersonation_policy": "unknown", "disclosure_behavior": "d", "gaps": []any{},
		}
	}
	public := func() map[string]any {
		return map[string]any{"id": "GET /p", "classification": "public", "gaps": []any{}}
	}
	if defects := checkActionFieldRows([]map[string]any{protected(), public()}, cat); len(defects) != 0 {
		t.Fatalf("the clean fixture is refused: %v", defects)
	}
	cases := []struct {
		name string
		row  map[string]any
		want string
	}{
		{"public row with a field", func() map[string]any { r := public(); r["action"] = "analytics.read"; return r }(), "public row carries action"},
		{"protected row missing a field", func() map[string]any { r := protected(); delete(r, "resource_resolver"); return r }(), "has no resource_resolver"},
		{"null without a gap", func() map[string]any { r := protected(); r["action"] = nil; return r }(), "action is null with no"},
		{"unknown action", func() map[string]any { r := protected(); r["action"] = "nope.read"; return r }(), "not in the catalog"},
		{"bad impersonation value", func() map[string]any { r := protected(); r["impersonation_policy"] = "maybe"; return r }(), "not a schema value"},
		{"empty string", func() map[string]any { r := protected(); r["disclosure_behavior"] = ""; return r }(), "not a non-empty string"},
	}
	for _, c := range cases {
		joined := strings.Join(checkActionFieldRows([]map[string]any{c.row}, cat), "\n")
		if !strings.Contains(joined, c.want) {
			t.Errorf("%s: the check did not report %q, it reported %q", c.name, c.want, joined)
		}
	}
}

// routeFamily names the authentication call a route's handler makes and the
// file that makes it, read from the route table and the handler files.
func routeFamily(id, route string) (call, file string) {
	switch {
	case route == "/graphql":
		return "scope.Impersonation(", "graphql_edge_route.go"
	case strings.Contains(id, "@mcp"):
		return "mcpAuthenticate(", "mcp_route.go"
	case route == "/buildinfo":
		return "authenticateInternalRequest(", "buildinfo_route.go"
	case strings.HasPrefix(route, "/query"):
		return "authenticateInternalRequest(", "query_route.go"
	}
	for _, rest := range RESTRoutes() {
		if rest.Pattern == route {
			return "authenticateRESTRequest(", rest.File
		}
	}
	return "", ""
}

// checkActionFieldTruth holds the protected rows' statements to the handler
// source: the authentication function a row cites is the one its handler file
// calls (and not the other one), a GraphQL row says impersonation applies
// exactly when its handler chain applies the impersonation middleware, an
// internal-carrier route that reads an organization says impersonation applies
// and one that reads none says it has no org context, and a REST data route
// does not claim more than "unknown".
func checkActionFieldTruth(rows []map[string]any, read func(file string) string) []string {
	var defects []string
	for _, row := range rows {
		if row["classification"] == "public" {
			continue
		}
		id, _ := row["id"].(string)
		route, _ := row["route"].(string)
		call, file := routeFamily(id, route)
		if call == "" {
			defects = append(defects, id+": no handler family found for the route")
			continue
		}
		if !strings.Contains(read(file), call) {
			defects = append(defects, fmt.Sprintf("%s: %s does not call %s", id, file, call))
			continue
		}
		text := fmt.Sprint(row["resource_resolver"], " ", row["current_state_cache_behavior"], " ", row["disclosure_behavior"])
		policy, _ := row["impersonation_policy"].(string)
		cites := func(name string) bool { return strings.Contains(text, name) }
		switch call {
		case "authenticateRESTRequest(":
			if cites("authenticateInternalRequest") {
				defects = append(defects, id+": cites authenticateInternalRequest but its handler calls authenticateRESTRequest")
			}
			if policy != "unknown" {
				defects = append(defects, fmt.Sprintf("%s: impersonation_policy %q, a REST data route is not proven beyond unknown", id, policy))
			}
		case "authenticateInternalRequest(":
			if cites("authenticateRESTRequest") {
				defects = append(defects, id+": cites authenticateRESTRequest but its handler calls authenticateInternalRequest")
			}
			want := "subject_to_impersonation_override"
			if route == "/buildinfo" {
				want = "exempt_no_org_context"
				if !strings.HasPrefix(fmt.Sprint(row["resource_resolver"]), "none:") || cites("organization's rows") {
					defects = append(defects, id+": /buildinfo reads no organization, its resource_resolver must start none: and name no organization rows")
				}
			}
			if policy != want {
				defects = append(defects, fmt.Sprintf("%s: impersonation_policy %q, want %q", id, policy, want))
			}
		case "scope.Impersonation(":
			if policy != "subject_to_impersonation_override" {
				defects = append(defects, fmt.Sprintf("%s: the /graphql chain applies scope.Impersonation, impersonation_policy is %q", id, policy))
			}
		case "mcpAuthenticate(":
			if policy != "unknown" {
				defects = append(defects, fmt.Sprintf("%s: impersonation_policy %q, the MCP class refuses an impersonating identity: want unknown with a named gap", id, policy))
			}
		}
		if route == "/query/proof-write" && !strings.Contains(fmt.Sprint(row["disclosure_behavior"]), "allowlist") {
			defects = append(defects, id+": the route is org-allowlist gated, disclosure_behavior must say so")
		}
	}
	return defects
}

func TestQueryAPIRowStatementsMatchTheHandlerSource(t *testing.T) {
	read := func(file string) string {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	for _, defect := range checkActionFieldTruth(loadQueryAPIRows(t), read) {
		t.Error(defect)
	}
}

func TestActionFieldTruthChecksCatchPlantedDefects(t *testing.T) {
	src := map[string]string{
		"buildinfo_route.go":    "authenticateInternalRequest(",
		"query_route.go":        "authenticateInternalRequest(",
		"graphql_edge_route.go": "scope.Impersonation(",
		"mcp_route.go":          "mcpAuthenticate(",
		"heatmap_route.go":      "authenticateRESTRequest(",
		"rest_routes.go":        "",
	}
	read := func(file string) string { return src[file] }
	rest := func() map[string]any {
		return map[string]any{"id": "GET /api/v1/heatmap [x]", "route": "/api/v1/heatmap", "classification": "protected",
			"resource_resolver": "authenticateRESTRequest", "current_state_cache_behavior": "authenticateRESTRequest", "disclosure_behavior": "d", "impersonation_policy": "unknown"}
	}
	build := func() map[string]any {
		return map[string]any{"id": "GET /buildinfo [x]", "route": "/buildinfo", "classification": "protected",
			"resource_resolver": "none: build identity", "current_state_cache_behavior": "authenticateInternalRequest", "disclosure_behavior": "d", "impersonation_policy": "exempt_no_org_context"}
	}
	gql := func() map[string]any {
		return map[string]any{"id": "GET /graphql [x]", "route": "/graphql", "classification": "protected",
			"resource_resolver": "r", "current_state_cache_behavior": "c", "disclosure_behavior": "d", "impersonation_policy": "subject_to_impersonation_override"}
	}
	proof := func() map[string]any {
		return map[string]any{"id": "POST /query/proof-write [x]", "route": "/query/proof-write", "classification": "protected",
			"resource_resolver": "r", "current_state_cache_behavior": "authenticateInternalRequest", "disclosure_behavior": "org allowlist", "impersonation_policy": "subject_to_impersonation_override"}
	}
	for _, row := range []map[string]any{rest(), build(), gql(), proof()} {
		if defects := checkActionFieldTruth([]map[string]any{row}, read); len(defects) != 0 {
			t.Fatalf("the clean fixture %v is refused: %v", row["id"], defects)
		}
	}
	cases := []struct {
		name string
		row  map[string]any
		want string
	}{
		{"graphql claims not applicable", func() map[string]any { r := gql(); r["impersonation_policy"] = "not_applicable_non_main_app"; return r }(), "applies scope.Impersonation"},
		{"buildinfo claims organization rows", func() map[string]any {
			r := build()
			r["resource_resolver"] = "organization_from_credential: x"
			return r
		}(), "reads no organization"},
		{"proof-write drops the allowlist", func() map[string]any {
			r := proof()
			r["disclosure_behavior"] = "any caller reads its own organization's rows"
			return r
		}(), "org-allowlist gated"},
		{"rest row cites the internal function", func() map[string]any { r := rest(); r["resource_resolver"] = "authenticateInternalRequest"; return r }(), "calls authenticateRESTRequest"},
		{"internal row cites the rest function", func() map[string]any { r := proof(); r["resource_resolver"] = "authenticateRESTRequest"; return r }(), "calls authenticateInternalRequest"},
		{"rest row over-claims impersonation", func() map[string]any {
			r := rest()
			r["impersonation_policy"] = "not_applicable_non_main_app"
			return r
		}(), "not proven beyond unknown"},
	}
	for _, c := range cases {
		joined := strings.Join(checkActionFieldTruth([]map[string]any{c.row}, read), "\n")
		if !strings.Contains(joined, c.want) {
			t.Errorf("%s: the check did not report %q, it reported %q", c.name, c.want, joined)
		}
	}
}

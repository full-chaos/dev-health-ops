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
		disclosure := fmt.Sprint(row["disclosure_behavior"])
		validator := ""
		if pv, ok := row["primary_validator"].(map[string]any); ok {
			validator, _ = pv["description"].(string)
		}
		cites := func(name string) bool { return strings.Contains(text, name) }
		switch call {
		case "authenticateRESTRequest(":
			if cites("authenticateInternalRequest") {
				defects = append(defects, id+": cites authenticateInternalRequest but its handler calls authenticateRESTRequest")
			}
			if strings.Contains(read("rest_error_response.go"), "Organization context required") {
				if strings.Contains(validator, "every failure answers the same 401") {
					defects = append(defects, id+": primary_validator says every failure answers 401, authenticateRESTRequest also answers 403 Organization context required")
				}
				if row["disclosure_behavior"] == nil {
					defects = append(defects, id+": a protected REST route's disclosure_behavior is null, the shared authentication outcomes are known")
				} else {
					for _, need := range []string{"403", "superuser", "503"} {
						if !strings.Contains(disclosure, need) {
							defects = append(defects, id+": disclosure_behavior omits "+need+" (orgless edge token 403 unless superuser, failed users-row read 503)")
						}
					}
				}
				if !strings.Contains(validator, "superuser") {
					defects = append(defects, id+": primary_validator omits that an orgless superuser passes")
				}
			}
			if policy != "unknown" {
				defects = append(defects, fmt.Sprintf("%s: impersonation_policy %q, a REST data route is not proven beyond unknown", id, policy))
			}
		case "authenticateInternalRequest(":
			if cites("authenticateRESTRequest") {
				defects = append(defects, id+": cites authenticateRESTRequest but its handler calls authenticateInternalRequest")
			}
			want := "subject_to_impersonation_override"
			if (route == "/query" || route == "/query/proof") && row["action"] != nil {
				defects = append(defects, id+": the route serves registered GraphQL documents, its action is the served root field's (the per-field map): action must be null")
			}
			if (route == "/query" || route == "/query/proof") && !strings.Contains(disclosure, "404") {
				defects = append(defects, id+": an authenticated unregistered document answers 404 (the shared dispatcher), disclosure_behavior omits it")
			}
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
			if cites("authenticateRESTRequest") || !cites("authenticateGraphQLEdge") {
				defects = append(defects, id+": /graphql authenticates with authenticateGraphQLEdge, the row must cite it and not authenticateRESTRequest")
			}
			for _, need := range []string{"401", "403", "404", "X-Org-Id", "UNREGISTERED_DOCUMENT"} {
				if !strings.Contains(disclosure, need) {
					defects = append(defects, id+": disclosure_behavior omits "+need)
				}
			}
			if !strings.Contains(fmt.Sprint(row["resource_resolver"]), "X-Org-Id") {
				defects = append(defects, id+": resource_resolver omits the X-Org-Id selection (scope.OrgScope)")
			}
		case "mcpAuthenticate(":
			if strings.Contains(read("mcp_route.go"), "http.StatusForbidden") && !strings.Contains(disclosure, "403") {
				defects = append(defects, id+": mcpAuthenticate refuses an elevated identity with 403, disclosure_behavior omits it")
			}
			if row["action"] != nil {
				defects = append(defects, id+": the MCP route serves registered GraphQL documents, action must be null")
			}
			for _, need := range []string{"allowlist", "404"} {
				if !strings.Contains(disclosure, need) {
					defects = append(defects, id+": disclosure_behavior omits "+need+" (a root field outside the allowlist answers 403, a root field not enabled answers 404)")
				}
			}
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
		"buildinfo_route.go":     "authenticateInternalRequest(",
		"query_route.go":         "authenticateInternalRequest(",
		"graphql_edge_route.go":  "scope.Impersonation(",
		"mcp_route.go":           "mcpAuthenticate( http.StatusForbidden",
		"heatmap_route.go":       "authenticateRESTRequest(",
		"rest_error_response.go": "Organization context required",
		"rest_routes.go":         "",
	}
	read := func(file string) string { return src[file] }
	rest := func() map[string]any {
		return map[string]any{"id": "GET /api/v1/heatmap [x]", "route": "/api/v1/heatmap", "classification": "protected",
			"resource_resolver": "authenticateRESTRequest", "current_state_cache_behavior": "authenticateRESTRequest", "disclosure_behavior": "401; 403 Organization context required unless superuser; 503 users-row read", "impersonation_policy": "unknown",
			"primary_validator": map[string]any{"description": "authenticateRESTRequest: failures answer 401 or 403; a superuser passes without an organization"}}
	}
	build := func() map[string]any {
		return map[string]any{"id": "GET /buildinfo [x]", "route": "/buildinfo", "classification": "protected",
			"resource_resolver": "none: build identity", "current_state_cache_behavior": "authenticateInternalRequest", "disclosure_behavior": "d", "impersonation_policy": "exempt_no_org_context"}
	}
	gql := func() map[string]any {
		return map[string]any{"id": "GET /graphql [x]", "route": "/graphql", "classification": "protected",
			"resource_resolver": "X-Org-Id selection", "current_state_cache_behavior": "c", "disclosure_behavior": "authenticateGraphQLEdge 401; 403 for X-Org-Id; 404 browser GET; 404 UNREGISTERED_DOCUMENT", "impersonation_policy": "subject_to_impersonation_override"}
	}
	mcp := func() map[string]any {
		return map[string]any{"id": "POST /query [x @mcp]", "route": "/query", "classification": "protected", "action": nil,
			"resource_resolver": "r", "current_state_cache_behavior": "c", "disclosure_behavior": "401; elevated identity 403; allowlist 403; not enabled 404", "impersonation_policy": "unknown"}
	}
	query := func() map[string]any {
		return map[string]any{"id": "POST /query [x @query]", "route": "/query", "classification": "protected", "action": nil,
			"resource_resolver": "r", "current_state_cache_behavior": "authenticateInternalRequest", "disclosure_behavior": "unregistered document 404", "impersonation_policy": "subject_to_impersonation_override"}
	}
	proof := func() map[string]any {
		return map[string]any{"id": "POST /query/proof-write [x]", "route": "/query/proof-write", "classification": "protected",
			"resource_resolver": "r", "current_state_cache_behavior": "authenticateInternalRequest", "disclosure_behavior": "org allowlist", "impersonation_policy": "subject_to_impersonation_override"}
	}
	for _, row := range []map[string]any{rest(), build(), gql(), proof(), mcp(), query()} {
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
		{"rest validator says every failure is 401", func() map[string]any {
			r := rest()
			r["primary_validator"] = map[string]any{"description": "every failure answers the same 401"}
			return r
		}(), "also answers 403"},
		{"rest disclosure omits the superuser", func() map[string]any { r := rest(); r["disclosure_behavior"] = "401 403 503"; return r }(), "omits superuser"},
		{"rest disclosure omits the 503", func() map[string]any { r := rest(); r["disclosure_behavior"] = "401 403 superuser"; return r }(), "omits 503"},
		{"rest disclosure null", func() map[string]any { r := rest(); r["disclosure_behavior"] = nil; return r }(), "disclosure_behavior is null"},
		{"rest validator omits the superuser", func() map[string]any {
			r := rest()
			r["primary_validator"] = map[string]any{"description": "401 or 403"}
			return r
		}(), "orgless superuser passes"},
		{"query omits the 404", func() map[string]any { r := query(); r["disclosure_behavior"] = "401"; return r }(), "unregistered document answers 404"},
		{"graphql omits the unregistered 404", func() map[string]any {
			r := gql()
			r["disclosure_behavior"] = "authenticateGraphQLEdge 401 403 404 X-Org-Id"
			return r
		}(), "omits UNREGISTERED_DOCUMENT"},
		{"mcp omits the allowlist", func() map[string]any { r := mcp(); r["disclosure_behavior"] = "403 404"; return r }(), "omits allowlist"},
		{"rest disclosure omits the 403", func() map[string]any { r := rest(); r["disclosure_behavior"] = "401 only"; return r }(), "omits 403"},
		{"graphql cites the REST function", func() map[string]any {
			r := gql()
			r["disclosure_behavior"] = "authenticateRESTRequest 401 403 404 X-Org-Id"
			return r
		}(), "must cite it and not authenticateRESTRequest"},
		{"graphql omits the 404", func() map[string]any {
			r := gql()
			r["disclosure_behavior"] = "authenticateGraphQLEdge 401 403 X-Org-Id"
			return r
		}(), "omits 404"},
		{"graphql resolver omits X-Org-Id", func() map[string]any { r := gql(); r["resource_resolver"] = "credential only"; return r }(), "omits the X-Org-Id selection"},
		{"mcp action not null", func() map[string]any { r := mcp(); r["action"] = "analytics.read"; return r }(), "action must be null"},
		{"mcp omits the 403", func() map[string]any { r := mcp(); r["disclosure_behavior"] = "refused"; return r }(), "omits it"},
		{"query action not null", func() map[string]any { r := query(); r["action"] = "analytics.read"; return r }(), "action must be null"},
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

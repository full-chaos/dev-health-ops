package contracts

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/contracts/auth/v1/vocabulary"
)

// The go-api admin rows record, for each route, what the code does today: the action, how the resource is
// resolved, the cache the decision reads, how a missing or foreign id is answered, and what the impersonation
// middleware does to it. This file pins the shape of those fields and that their code anchors still resolve.
// It records behaviour; it does not set policy (which actions are denied under impersonation is not decided here).

const (
	byIDPrefix     = "by-id route, behaviour today:"
	noIDPrefix     = "no path resource id, behaviour today:"
	platformPrefix = "platform route, behaviour today:"
	cachePrefix    = "none: per-request read ("
)

var (
	anchorPattern = regexp.MustCompile(`((?:internal|contracts|cmd)/[A-Za-z0-9_./-]+\.(?:go|json|tsv|sql)):([0-9]+)`)
	denialPattern = regexp.MustCompile(`(?i)(denied|refused|rejected|blocked|forbidden)[^.;]{0,40}impersonat|impersonat[^.;]{0,40}(denied|refused|rejected|blocked|forbidden)`)
	pathParam     = regexp.MustCompile(`\{[^}]+\}`)
)

// The three impersonation routes read no tenant organization themselves; each states its own value.
var impersonationRoutes = map[string]string{
	"POST /api/v1/admin/impersonate [dev-health-ops-api @api]":       "exempt_no_org_context",
	"POST /api/v1/admin/impersonate/stop [dev-health-ops-api @api]":  "exempt_no_org_context",
	"GET /api/v1/admin/impersonate/status [dev-health-ops-api @api]": "exempt_no_org_context",
}

func isAdminRow(row map[string]any) bool {
	route, _ := row["route"].(string)
	return strings.HasPrefix(route, "/api/v1/admin")
}

func gapNames(row map[string]any, field string) bool {
	gaps, _ := row["gaps"].([]any)
	for _, gap := range gaps {
		if text, ok := gap.(string); ok && strings.HasPrefix(strings.TrimSpace(text), field+":") {
			return true
		}
	}
	return false
}

func catalogScopes(t *testing.T) map[string]string {
	t.Helper()
	var catalog struct {
		Actions []struct {
			Action string `json:"action"`
			Scope  string `json:"scope"`
		} `json:"actions"`
	}
	if err := json.Unmarshal(vocabulary.ActionsV1, &catalog); err != nil {
		t.Fatal(err)
	}
	scopes := map[string]string{}
	for _, a := range catalog.Actions {
		scopes[a.Action] = a.Scope
	}
	return scopes
}

// adminFieldViolations is every way a row breaks the shape of the five fields. root resolves the code anchors.
func adminFieldViolations(row map[string]any, scopes map[string]string, root string) []string {
	id, _ := row["id"].(string)
	var out []string
	bad := func(format string, args ...any) {
		out = append(out, id+": "+strings.TrimSpace(sprintf(format, args...)))
	}

	// action: a catalog action, or null with a gap naming it.
	switch action := row["action"].(type) {
	case string:
		scope, known := scopes[action]
		if !known {
			bad("action %q is not in the actions catalog", action)
		}
		tenancy, _ := row["tenant_requirement"].(string)
		if known && (scope == "platform") != (tenancy == "cross_org_superuser_only") {
			bad("action %q has scope %q but the row's tenant_requirement is %q", action, scope, tenancy)
		}
	case nil:
		if _, present := row["action"]; !present {
			bad("action is absent")
		} else if !gapNames(row, "action") {
			bad("action is null without a gaps entry naming it")
		}
	default:
		bad("action is not a string or null")
	}

	// resource resolver, cache text and impersonation enum are always recorded.
	resolver, _ := row["resource_resolver"].(string)
	if !strings.HasPrefix(resolver, "organization_from_session:") && !strings.HasPrefix(resolver, "organization_from_path:") && !strings.HasPrefix(resolver, "none:") {
		bad("resource_resolver does not start with a resolver kind: %q", resolver)
	}
	cache, _ := row["current_state_cache_behavior"].(string)
	if cache == "" || (!strings.HasPrefix(cache, cachePrefix) && !strings.HasPrefix(cache, "valkey:")) {
		bad("current_state_cache_behavior is missing or does not name its cache rule: %q", cache)
	}
	policy, _ := row["impersonation_policy"].(string)
	tenancy, _ := row["tenant_requirement"].(string)
	switch {
	case policy == "" || policy == "unknown":
		bad("impersonation_policy is missing or unknown")
	case impersonationRoutes[id] != "" && policy != impersonationRoutes[id]:
		bad("an impersonation route has impersonation_policy %q, want %q", policy, impersonationRoutes[id])
	case impersonationRoutes[id] != "":
	case tenancy == "org_scoped" && policy != "subject_to_impersonation_override":
		bad("an org-scoped route has impersonation_policy %q", policy)
	case tenancy == "cross_org_superuser_only" && policy == "subject_to_impersonation_override":
		bad("a cross-org superuser route has impersonation_policy %q", policy)
	}

	// disclosure: the rule class is named; a by-id route never claims the no-id rule; unproven = null + gap.
	route, _ := row["route"].(string)
	byID := pathParam.MatchString(route)
	disclosure := row["disclosure_behavior"]
	switch text := disclosure.(type) {
	case string:
		switch {
		case strings.HasPrefix(text, byIDPrefix):
			if !strings.Contains(text, "404") || len(anchorPattern.FindAllString(text, -1)) < 3 {
				bad("a by-id disclosure text names no 404 or fewer than three anchors (404 site, scoping lookup, 403 guard)")
			}
		case strings.HasPrefix(text, noIDPrefix):
			if byID && !strings.Contains(text, "fixed mode, not a resource id") {
				bad("a route with a path id claims the no-resource-id rule without saying the segment is a fixed mode")
			}
			if strings.Contains(text, "404") {
				bad("a no-resource-id disclosure text mentions 404")
			}
		case strings.HasPrefix(text, platformPrefix):
			if !strings.Contains(text, "403") || !strings.Contains(text, "superuser") || len(anchorPattern.FindAllString(text, -1)) < 1 {
				bad("a platform disclosure text names no superuser 403 or no anchor")
			}
			if tenancy != "cross_org_superuser_only" {
				bad("a tenant route claims the platform rule")
			}
		default:
			bad("disclosure_behavior names no rule class: %q", text)
		}
	case nil:
		if _, present := row["disclosure_behavior"]; !present {
			bad("disclosure_behavior is absent")
		} else if !gapNames(row, "disclosure_behavior") {
			bad("disclosure_behavior is null without a gaps entry naming it")
		}
	default:
		bad("disclosure_behavior is not a string or null")
	}

	// a registration that wraps the handler in bodyFirst always says so (a handler may decode first by other means and say so too).
	if text, ok := disclosure.(string); ok {
		claims := strings.Contains(text, bodyFirstClause)
		if registered, found := registeredBodyFirst(row, root); found && registered != claims {
			bad("the body-first clause (%v) disagrees with the route registration (%v)", claims, registered)
		}
	}

	// the text is a record, not a policy: no denial under impersonation is claimed, and every anchor resolves.
	for _, field := range []string{"resource_resolver", "current_state_cache_behavior", "disclosure_behavior"} {
		text, _ := row[field].(string)
		if denialPattern.MatchString(text) {
			bad("%s claims a denial under impersonation: %q", field, text)
		}
		for _, match := range anchorPattern.FindAllStringSubmatch(text, -1) {
			line, _ := strconv.Atoi(match[2])
			if problem := anchorProblem(root, match[1], line); problem != "" {
				bad("%s anchor %s:%d %s", field, match[1], line, problem)
			}
		}
	}
	return out
}

const bodyFirstClause = "once any body-first validation has run"

// bodyFirstPattern names the registration wrappers that read the body before the guard: Guard.BodyFirst and the
// handlers' bodyFirst, the customer-push adminBody helper (a BodyFirst closure) and the teams decodeFirst wrapper
// (decodes the body, then runs the guard). The integrations routes name their BodyFirst closure write.
var bodyFirstPattern = regexp.MustCompile(`(?i)(bodyFirst|adminBody\(|decodeFirst)`)

var bodyFirstClosures = map[string]string{"internal/api/integrationsadmin/routes.go": "write("}

// registeredBodyFirst reports whether the registration entry at the row's source line wraps its handler in bodyFirst.
func registeredBodyFirst(row map[string]any, root string) (registered, found bool) {
	source, _ := row["source"].(map[string]any)
	file, _ := source["file"].(string)
	line, _ := source["line"].(float64)
	raw, err := os.ReadFile(filepath.Join(root, file))
	if err != nil || int(line) < 1 {
		return false, false
	}
	lines := strings.Split(string(raw), "\n")
	if int(line) > len(lines) {
		return false, false
	}
	entry := lines[int(line)-1]
	for i := int(line); i < len(lines) && i < int(line)+3 && !strings.Contains(lines[i], "Method:"); i++ {
		entry += "\n" + lines[i]
	}
	closure, named := bodyFirstClosures[file]
	return bodyFirstPattern.MatchString(entry) || (named && strings.Contains(entry, closure)), true
}

func anchorProblem(root, rel string, line int) string {
	raw, err := os.ReadFile(filepath.Join(root, rel))
	if err != nil {
		return "names a file that does not exist"
	}
	lines := strings.Split(string(raw), "\n")
	if line < 1 || line > len(lines) {
		return "is past the end of the file"
	}
	if strings.TrimSpace(lines[line-1]) == "" {
		return "points at a blank line"
	}
	return ""
}

func sprintf(format string, args ...any) string {
	if len(args) == 0 {
		return format
	}
	return strings.TrimSpace(fmt.Sprintf(format, args...))
}

func coveredAdminRows(t *testing.T) []map[string]any {
	t.Helper()
	var rows []map[string]any
	for _, item := range loadGoAPIDocument(t)["rows"].([]any) {
		row := item.(map[string]any)
		if !isAdminRow(row) {
			continue
		}
		rows = append(rows, row)
	}
	return rows
}

func TestEveryCoveredAdminRowCarriesTheFields(t *testing.T) {
	rows := coveredAdminRows(t)
	if len(rows) < 140 {
		t.Fatalf("only %d covered admin rows: the set of files lost its rows", len(rows))
	}
	scopes := catalogScopes(t)
	root := testRoot(t)
	for _, row := range rows {
		for _, violation := range adminFieldViolations(row, scopes, root) {
			t.Error(violation)
		}
	}
}

func TestAdminFieldChecksCatchPlantedDefects(t *testing.T) {
	root := testRoot(t)
	scopes := catalogScopes(t)
	var base map[string]any
	for _, row := range coveredAdminRows(t) {
		if row["id"] == "GET /api/v1/admin/teams/{team_id} [dev-health-ops-api @api]" {
			raw, _ := json.Marshal(row)
			_ = json.Unmarshal(raw, &base)
		}
	}
	if base == nil {
		t.Fatal("the team read row is missing")
	}
	if got := adminFieldViolations(base, scopes, root); len(got) != 0 {
		t.Fatalf("the unmodified row has violations: %v", got)
	}
	mutations := map[string]func(map[string]any){
		"unknown action":                         func(r map[string]any) { r["action"] = "teams.destroy" },
		"action with the wrong scope":            func(r map[string]any) { r["action"] = "admin.orgs" },
		"null action without a gap":              func(r map[string]any) { r["action"] = nil },
		"absent resolver":                        func(r map[string]any) { delete(r, "resource_resolver") },
		"cache text without a rule":              func(r map[string]any) { r["current_state_cache_behavior"] = "unknown" },
		"impersonation unknown":                  func(r map[string]any) { r["impersonation_policy"] = "unknown" },
		"impersonation exempt on a tenant route": func(r map[string]any) { r["impersonation_policy"] = "exempt_no_org_context" },
		"by-id route claims the no-id rule":      func(r map[string]any) { r["disclosure_behavior"] = noIDPrefix + " the caller's own organization" },
		"disclosure names no class":              func(r map[string]any) { r["disclosure_behavior"] = "returns 404" },
		"disclosure null without a gap":          func(r map[string]any) { r["disclosure_behavior"] = nil },
		"anchor past the end of the file": func(r map[string]any) {
			r["disclosure_behavior"] = byIDPrefix + " 404 (internal/api/teamsidentity/teamsidentity.go:99999; internal/api/teamsidentity/store.go:125); 403 (internal/api/policy/guard.go:129)"
		},
		"anchor names a missing file": func(r map[string]any) {
			r["disclosure_behavior"] = byIDPrefix + " 404 (internal/api/nope/nope.go:5; internal/api/teamsidentity/store.go:125); 403 (internal/api/policy/guard.go:129)"
		},
		"tenant route claims the platform rule": func(r map[string]any) {
			r["disclosure_behavior"] = platformPrefix + " only a superuser passes, any other caller answers 403 (internal/api/policy/guard.go:137)"
		},
		"body-first clause on a route registered without it": func(r map[string]any) {
			r["disclosure_behavior"] = strings.Replace(r["disclosure_behavior"].(string), "403", bodyFirstClause+", 403", 1)
		},
		"claims a denial under impersonation": func(r map[string]any) {
			r["current_state_cache_behavior"] = cachePrefix + "internal/api/policy/request_reads.go:13); the route is denied under impersonation"
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			raw, _ := json.Marshal(base)
			var row map[string]any
			_ = json.Unmarshal(raw, &row)
			mutate(row)
			if got := adminFieldViolations(row, scopes, root); len(got) == 0 {
				t.Fatal("the planted defect passed every check")
			}
		})
	}
}

func TestBodyFirstRegistrationMustBeNamed(t *testing.T) {
	root := testRoot(t)
	scopes := catalogScopes(t)
	for _, row := range coveredAdminRows(t) {
		if row["id"] != "PUT /api/v1/admin/settings/{category}/{key} [dev-health-ops-api @api]" {
			continue
		}
		raw, _ := json.Marshal(row)
		var mutated map[string]any
		_ = json.Unmarshal(raw, &mutated)
		if got := adminFieldViolations(mutated, scopes, root); len(got) != 0 {
			t.Fatalf("the unmodified row has violations: %v", got)
		}
		mutated["disclosure_behavior"] = strings.ReplaceAll(mutated["disclosure_behavior"].(string), bodyFirstClause+", ", "")
		if got := adminFieldViolations(mutated, scopes, root); len(got) == 0 {
			t.Fatal("a bodyFirst-registered route without the clause passed every check")
		}
		return
	}
	t.Fatal("the settings write row is missing")
}

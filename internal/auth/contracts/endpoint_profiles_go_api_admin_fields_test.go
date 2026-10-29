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
	noReadPrefix   = "none: no authorization read ("
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

// coveredFamilies are the route prefixes whose protected rows carry the fields.
var coveredFamilies = []string{
	"/api/v1/admin", "/api/v1/auth", "/api/v1/billing", "/api/v1/licensing", "/api/v1/orgs", "/api/v1/internal",
	"/api/v1/external-ingest", "/api/v1/telemetry", "/api/v1/webhooks", "/buildinfo",
}

// uncoveredFamilies are protected route prefixes that do not carry the fields yet; a protected row in no listed
// family fails, so a new family cannot arrive without a decision.
var uncoveredFamilies = []string{}

// Resolver kinds for a route whose organization does not come from the session: the credential's own row (a push
// token, a webhook binding) or a body field. Such a route is not subject to the impersonation override.
const (
	credentialResolver = "organization_from_credential:"
	bodyResolver       = "organization_from_body:"
)

const (
	nonSessionGapMessage    = "is exempt_no_org_context without a gaps entry naming impersonation_policy"
	nonSessionPolicyMessage = "whose organization does not come from the session has impersonation_policy"
	bodyBindingMessage      = "takes its organization from the body without a credential_binding gap"
)

func nonSessionResolver(resolver string) bool {
	return strings.HasPrefix(resolver, credentialResolver) || strings.HasPrefix(resolver, bodyResolver)
}

func hasFamilyPrefix(route string, families []string) bool {
	for _, family := range families {
		if route == family || strings.HasPrefix(route, family+"/") {
			return true
		}
	}
	return false
}

func isCoveredRow(row map[string]any) bool {
	route, _ := row["route"].(string)
	class, _ := row["classification"].(string)
	return class == "protected" && hasFamilyPrefix(route, coveredFamilies)
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
	if !strings.HasPrefix(resolver, "organization_from_session:") && !strings.HasPrefix(resolver, "organization_from_path:") && !strings.HasPrefix(resolver, "none:") && !nonSessionResolver(resolver) {
		bad("resource_resolver does not start with a resolver kind: %q", resolver)
	}
	cache, _ := row["current_state_cache_behavior"].(string)
	if cache == "" || (!strings.HasPrefix(cache, cachePrefix) && !strings.HasPrefix(cache, noReadPrefix) && !strings.HasPrefix(cache, "valkey:")) {
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
	case tenancy == "org_scoped" && strings.HasSuffix(id, "@internal]") && policy == "subject_to_impersonation_override":
		bad("an internal-listener org-scoped route claims subject_to_impersonation_override (the internal listener runs no impersonation middleware)")
		if !gapNames(row, "credential_binding") {
			bad("an internal-listener org-scoped route has no credential_binding gap")
		}
	case tenancy == "org_scoped" && strings.HasSuffix(id, "@internal]") && policy == "not_applicable_non_main_app":
		if !gapNames(row, "impersonation_policy") {
			bad("an internal-listener org-scoped route is not_applicable_non_main_app without a gaps entry naming impersonation_policy")
		}
		if !gapNames(row, "credential_binding") {
			bad("an internal-listener org-scoped route has no credential_binding gap")
		}
	case tenancy == "org_scoped" && nonSessionResolver(resolver) && policy != "exempt_no_org_context":
		bad("an org-scoped route %s %q, want exempt_no_org_context", nonSessionPolicyMessage, policy)
	case tenancy == "org_scoped" && nonSessionResolver(resolver):
		if !gapNames(row, "impersonation_policy") {
			bad("an org-scoped route whose organization comes from the credential or the body %s", nonSessionGapMessage)
		}
	case tenancy == "org_scoped" && policy != "subject_to_impersonation_override":
		bad("an org-scoped route has impersonation_policy %q", policy)
	case tenancy == "cross_org_superuser_only" && policy == "subject_to_impersonation_override":
		bad("a cross-org superuser route has impersonation_policy %q", policy)
	}
	if strings.HasPrefix(resolver, bodyResolver) && !gapNames(row, "credential_binding") {
		bad("a route %s", bodyBindingMessage)
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

func coveredRows(t *testing.T) []map[string]any {
	t.Helper()
	var rows []map[string]any
	for _, item := range loadGoAPIDocument(t)["rows"].([]any) {
		row := item.(map[string]any)
		if !isCoveredRow(row) {
			continue
		}
		rows = append(rows, row)
	}
	return rows
}

func TestEveryCoveredAdminRowCarriesTheFields(t *testing.T) {
	rows := coveredRows(t)
	// CHAOS-11813 retired the 6 legacy ingest rows (214 and 71 before).
	if len(rows) < 208 {
		t.Fatalf("only %d covered rows: a covered family lost its rows", len(rows))
	}
	nonAdmin := 0
	for _, row := range rows {
		if route, _ := row["route"].(string); !strings.HasPrefix(route, "/api/v1/admin") {
			nonAdmin++
		}
	}
	if nonAdmin < 65 {
		t.Fatalf("only %d covered rows outside the admin family", nonAdmin)
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
	for _, row := range coveredRows(t) {
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
	for _, row := range coveredRows(t) {
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

func TestEveryProtectedRowIsInAListedFamily(t *testing.T) {
	for _, item := range loadGoAPIDocument(t)["rows"].([]any) {
		row := item.(map[string]any)
		if class, _ := row["classification"].(string); class != "protected" {
			continue
		}
		route, _ := row["route"].(string)
		if !hasFamilyPrefix(route, coveredFamilies) && !hasFamilyPrefix(route, uncoveredFamilies) {
			t.Errorf("%v is a protected row in a family that is neither covered nor listed as not yet covered", row["id"])
		}
	}
}

func TestCoveredFamilyPredicateSelectsByPrefixAndClass(t *testing.T) {
	cases := []struct {
		route, class string
		want         bool
	}{
		{"/api/v1/billing/plans", "protected", true},
		{"/api/v1/billing/plans", "public", false},
		{"/api/v1/billingx/plans", "protected", false},
		{"/api/v1/internal/acr/health", "protected", true},
		{"/api/v1/ingest/commits", "protected", false}, // the legacy ingest family is retired (CHAOS-11813)
		{"/api/v1/auth/me", "protected", true},
		{"/api/v1/external-ingest/batches", "protected", true},
		{"/api/v1/external-ingest/schemas", "public", false},
		{"/api/v1/telemetry/status", "protected", true},
		{"/api/v1/webhooks/github", "protected", true},
		{"/api/v1/webhooks/health", "public", false},
		{"/buildinfo", "protected", true},
		{"/buildinfox", "protected", false},
	}
	for _, c := range cases {
		if got := isCoveredRow(map[string]any{"route": c.route, "classification": c.class}); got != c.want {
			t.Errorf("%s %s: covered = %v, want %v", c.class, c.route, got, c.want)
		}
	}
}

// Each family outside admin has one row that is mutated in every field way the admin controls do, so a gap in one
// family's rows cannot hide behind the admin family's controls.
func TestFieldChecksCatchPlantedDefectsOnEveryNewFamily(t *testing.T) {
	root := testRoot(t)
	scopes := catalogScopes(t)
	ids := []string{
		"GET /api/v1/billing/invoices [dev-health-ops-api @api]",
		"POST /api/v1/billing/plans/pull-stripe [dev-health-ops-api @api]",
		"GET /api/v1/auth/me [dev-health-ops-api @api]",
		"GET /api/v1/orgs/me [dev-health-ops-api @api]",
		"GET /api/v1/licensing/entitlements/{org_id} [dev-health-ops-api @api]",
		"GET /api/v1/internal/acr/entitlements/{org_id} [dev-health-ops-api @internal]",
		"GET /api/v1/external-ingest/batches/{ingestion_id} [dev-health-ops-api @api]",
		"GET /api/v1/telemetry/status [dev-health-ops-api @api]",
		"POST /api/v1/telemetry/report [dev-health-ops-api @api]",
		"POST /api/v1/webhooks/github [dev-health-ops-api @api]",
		"POST /api/v1/webhooks/pagerduty/{binding_id} [dev-health-ops-api @api]",
		"GET /buildinfo [dev-health-ops-api @api]",
	}
	mutations := map[string]func(map[string]any){
		"unknown action":            func(r map[string]any) { r["action"] = "billing.nope" },
		"action absent":             func(r map[string]any) { delete(r, "action") },
		"null action without a gap": func(r map[string]any) { r["action"] = nil; r["gaps"] = []any{} },
		"absent resolver":           func(r map[string]any) { delete(r, "resource_resolver") },
		"cache text without a rule": func(r map[string]any) { r["current_state_cache_behavior"] = "unknown" },
		"absent cache text":         func(r map[string]any) { delete(r, "current_state_cache_behavior") },
		"impersonation unknown":     func(r map[string]any) { r["impersonation_policy"] = "unknown" },
		"impersonation absent":      func(r map[string]any) { delete(r, "impersonation_policy") },
		"disclosure absent":         func(r map[string]any) { delete(r, "disclosure_behavior") },
		"disclosure names no class": func(r map[string]any) { r["disclosure_behavior"] = "returns 404" },
		"anchor names a missing file": func(r map[string]any) {
			r["resource_resolver"] = "none: the route reads no organization (internal/api/nope/nope.go:5)"
		},
	}
	seen := 0
	for _, row := range coveredRows(t) {
		id, _ := row["id"].(string)
		wanted := false
		for _, want := range ids {
			wanted = wanted || want == id
		}
		if !wanted {
			continue
		}
		seen++
		raw, _ := json.Marshal(row)
		var base map[string]any
		_ = json.Unmarshal(raw, &base)
		if got := adminFieldViolations(base, scopes, root); len(got) != 0 {
			t.Fatalf("%s: the unmodified row has violations: %v", id, got)
		}
		for name, mutate := range mutations {
			var row map[string]any
			_ = json.Unmarshal(raw, &row)
			mutate(row)
			if got := adminFieldViolations(row, scopes, root); len(got) == 0 {
				t.Errorf("%s: planted defect %q passed every check", id, name)
			}
		}
	}
	if seen != len(ids) {
		t.Fatalf("found %d of the %d planted-control rows", seen, len(ids))
	}
}

func TestInternalListenerOrgScopedRowNeedsItsImpersonationGap(t *testing.T) {
	root := testRoot(t)
	scopes := catalogScopes(t)
	for _, row := range coveredRows(t) {
		if row["id"] != "GET /api/v1/internal/acr/entitlements/{org_id} [dev-health-ops-api @internal]" {
			continue
		}
		raw, _ := json.Marshal(row)
		var mutated map[string]any
		_ = json.Unmarshal(raw, &mutated)
		mutated["gaps"] = []any{"action: no catalog action covers the internal service entitlement and configuration reads"}
		if got := adminFieldViolations(mutated, scopes, root); len(got) == 0 {
			t.Fatal("a not-applicable impersonation value without its gap passed every check")
		}
		return
	}
	t.Fatal("the internal entitlement row is missing")
}

const (
	internalFlipMessage = "claims subject_to_impersonation_override"
	internalGapMessage  = "no credential_binding gap"
)

var internalOrgScopedIDs = []string{
	"GET /api/v1/internal/acr/entitlements/{org_id} [dev-health-ops-api @internal]",
	"GET /api/v1/internal/acr/orgs/{org_id}/llm-config [dev-health-ops-api @internal]",
}

var internalACRCacheIDs = append([]string{"GET /api/v1/internal/acr/health [dev-health-ops-api @internal]"}, internalOrgScopedIDs...)

func violationsContain(violations []string, want string) bool {
	for _, v := range violations {
		if strings.Contains(v, want) {
			return true
		}
	}
	return false
}

func cloneRow(t *testing.T, row map[string]any) map[string]any {
	t.Helper()
	raw, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func dropGap(row map[string]any, field string) {
	var kept []any
	gaps, _ := row["gaps"].([]any)
	for _, gap := range gaps {
		if text, ok := gap.(string); ok && strings.HasPrefix(strings.TrimSpace(text), field+":") {
			continue
		}
		kept = append(kept, gap)
	}
	row["gaps"] = kept
}

func rowsByID(t *testing.T, ids []string) map[string]map[string]any {
	t.Helper()
	found := map[string]map[string]any{}
	for _, row := range coveredRows(t) {
		id, _ := row["id"].(string)
		for _, want := range ids {
			if id == want {
				found[id] = row
			}
		}
	}
	for _, want := range ids {
		if found[want] == nil {
			t.Fatalf("the row %q is missing", want)
		}
	}
	return found
}

func TestInternalOrgScopedRowsPlantedImpersonationAndCredentialGap(t *testing.T) {
	root := testRoot(t)
	scopes := catalogScopes(t)
	for id, row := range rowsByID(t, internalOrgScopedIDs) {
		t.Run(id, func(t *testing.T) {
			if got := adminFieldViolations(cloneRow(t, row), scopes, root); len(got) != 0 {
				t.Fatalf("the unmodified row has violations: %v", got)
			}
			flipped := cloneRow(t, row)
			flipped["impersonation_policy"] = "subject_to_impersonation_override"
			got := adminFieldViolations(flipped, scopes, root)
			if !violationsContain(got, internalFlipMessage) {
				t.Fatalf("a flip to subject_to_impersonation_override was not rejected for that reason: %v", got)
			}
			ungapped := cloneRow(t, row)
			dropGap(ungapped, "credential_binding")
			got = adminFieldViolations(ungapped, scopes, root)
			if !violationsContain(got, internalGapMessage) {
				t.Fatalf("a dropped credential_binding gap was not rejected for that reason: %v", got)
			}
			both := cloneRow(t, flipped)
			dropGap(both, "credential_binding")
			got = adminFieldViolations(both, scopes, root)
			if !violationsContain(got, internalFlipMessage) || !violationsContain(got, internalGapMessage) {
				t.Fatalf("the flip with the dropped gap did not report both: %v", got)
			}
		})
	}
}

func TestInternalACRRowsNameBothCredentialPaths(t *testing.T) {
	for id, row := range rowsByID(t, internalACRCacheIDs) {
		cache, _ := row["current_state_cache_behavior"].(string)
		for _, want := range []string{
			cachePrefix,
			"svc_acr_ credential: store read every request, no cache",
			"workload token: signature verify only, no store read",
		} {
			if !strings.Contains(cache, want) {
				t.Errorf("%s: cache text lacks %q: %q", id, want, cache)
			}
		}
		if strings.HasPrefix(cache, noReadPrefix) {
			t.Errorf("%s: cache text still says no authorization read: %q", id, cache)
		}
	}
	llm := rowsByID(t, internalOrgScopedIDs[1:])[internalOrgScopedIDs[1]]
	validator, _ := llm["primary_validator"].(map[string]any)
	text, _ := validator["description"].(string)
	for _, banned := range []string{"routeAuth", "keyed", "limiter"} {
		if strings.Contains(text, banned) {
			t.Errorf("the llm-config validator text names %q: %q", banned, text)
		}
	}
	for _, want := range []string{"llmConfigHandler", "llm_config:read"} {
		if !strings.Contains(text, want) {
			t.Errorf("the llm-config validator text lacks %q: %q", want, text)
		}
	}
}

type organizationSource struct{ resolver, policy, tenancy string }

// Where each intake, telemetry and build row takes its organization from, and what that means for impersonation.
var intakeOrganizationSources = map[string]organizationSource{
	"GET /api/v1/external-ingest/availability [dev-health-ops-api @api]":           {credentialResolver, "exempt_no_org_context", "org_scoped"},
	"POST /api/v1/external-ingest/validate [dev-health-ops-api @api]":              {credentialResolver, "exempt_no_org_context", "org_scoped"},
	"POST /api/v1/external-ingest/batches [dev-health-ops-api @api]":               {credentialResolver, "exempt_no_org_context", "org_scoped"},
	"GET /api/v1/external-ingest/batches [dev-health-ops-api @api]":                {credentialResolver, "exempt_no_org_context", "org_scoped"},
	"GET /api/v1/external-ingest/batches/{ingestion_id} [dev-health-ops-api @api]": {credentialResolver, "exempt_no_org_context", "org_scoped"},
	"GET /api/v1/telemetry/status [dev-health-ops-api @api]":                       {"organization_from_session:", "subject_to_impersonation_override", "org_scoped"},
	"POST /api/v1/telemetry/opt-in [dev-health-ops-api @api]":                      {"organization_from_session:", "subject_to_impersonation_override", "org_scoped"},
	"POST /api/v1/telemetry/opt-out [dev-health-ops-api @api]":                     {"organization_from_session:", "subject_to_impersonation_override", "org_scoped"},
	"POST /api/v1/telemetry/report [dev-health-ops-api @api]":                      {"organization_from_session:", "subject_to_impersonation_override", "org_scoped"},
	"POST /api/v1/webhooks/github [dev-health-ops-api @api]":                       {"none:", "exempt_no_org_context", "none"},
	"POST /api/v1/webhooks/pagerduty/{binding_id} [dev-health-ops-api @api]":       {credentialResolver, "exempt_no_org_context", "org_scoped"},
	"GET /buildinfo [dev-health-ops-api @api]":                                     {"organization_from_session:", "subject_to_impersonation_override", "org_scoped"},
}

func TestIntakeRowsRecordTheirOrganizationSource(t *testing.T) {
	ids := make([]string, 0, len(intakeOrganizationSources))
	for id := range intakeOrganizationSources {
		ids = append(ids, id)
	}
	for id, row := range rowsByID(t, ids) {
		want := intakeOrganizationSources[id]
		resolver, _ := row["resource_resolver"].(string)
		policy, _ := row["impersonation_policy"].(string)
		tenancy, _ := row["tenant_requirement"].(string)
		if !strings.HasPrefix(resolver, want.resolver) || policy != want.policy || tenancy != want.tenancy {
			t.Errorf("%s: resolver %q, impersonation_policy %q, tenant_requirement %q; want resolver kind %q, %q, %q",
				id, resolver, policy, tenancy, want.resolver, want.policy, want.tenancy)
		}
	}
}

func TestNonSessionOrganizationRowsRecordTheirImpersonationGap(t *testing.T) {
	root := testRoot(t)
	scopes := catalogScopes(t)
	ids := []string{
		"POST /api/v1/external-ingest/batches [dev-health-ops-api @api]",
		"POST /api/v1/webhooks/pagerduty/{binding_id} [dev-health-ops-api @api]",
	}
	for id, row := range rowsByID(t, ids) {
		t.Run(id, func(t *testing.T) {
			if got := adminFieldViolations(cloneRow(t, row), scopes, root); len(got) != 0 {
				t.Fatalf("the unmodified row has violations: %v", got)
			}
			ungapped := cloneRow(t, row)
			dropGap(ungapped, "impersonation_policy")
			if got := adminFieldViolations(ungapped, scopes, root); !violationsContain(got, nonSessionGapMessage) {
				t.Errorf("a dropped impersonation_policy gap was not rejected for that reason: %v", got)
			}
			flipped := cloneRow(t, row)
			flipped["impersonation_policy"] = "subject_to_impersonation_override"
			if got := adminFieldViolations(flipped, scopes, root); !violationsContain(got, nonSessionPolicyMessage) {
				t.Errorf("a flip to subject_to_impersonation_override was not rejected for that reason: %v", got)
			}
			session := cloneRow(t, row)
			session["resource_resolver"] = "organization_from_session: the caller's organization (internal/api/policy/guard.go:115)"
			if got := adminFieldViolations(session, scopes, root); !violationsContain(got, `an org-scoped route has impersonation_policy "exempt_no_org_context"`) {
				t.Errorf("a session resolver with exempt_no_org_context was not rejected for that reason: %v", got)
			}
		})
	}
}

// No current row takes its organization from the request body: the legacy ingest rows, the only ones that did,
// are retired with their routes (CHAOS-11813). The check below still runs over every covered row, so a body-kind
// row added later must name its credential_binding gap; a body-kind row that appears here must be classified.
func TestNoRowTakesItsOrganizationFromTheBody(t *testing.T) {
	for _, row := range coveredRows(t) {
		if resolver, _ := row["resource_resolver"].(string); strings.HasPrefix(resolver, bodyResolver) {
			t.Errorf("%v takes its organization from the body: name its credential binding and classify it here", row["id"])
		}
	}
}

// The body-kind binding rule still bites with no body-kind row left: a credential-kind row re-planted as a
// body-kind row is rejected without a credential_binding gap and accepted with one.
func TestBodyOrganizationRowsNameTheirCredentialBinding(t *testing.T) {
	root := testRoot(t)
	scopes := catalogScopes(t)
	const id = "POST /api/v1/external-ingest/batches [dev-health-ops-api @api]"
	base := rowsByID(t, []string{id})[id]
	resolver, _ := base["resource_resolver"].(string)
	if !strings.HasPrefix(resolver, credentialResolver) {
		t.Fatalf("%s: resolver %q is not a credential kind; the planted body row needs one", id, resolver)
	}
	planted := cloneRow(t, base)
	planted["resource_resolver"] = bodyResolver + strings.TrimPrefix(resolver, credentialResolver)
	dropGap(planted, "credential_binding")
	if got := adminFieldViolations(planted, scopes, root); !violationsContain(got, bodyBindingMessage) {
		t.Errorf("a body-kind row without a credential_binding gap was not rejected for that reason: %v", got)
	}
	bound := cloneRow(t, planted)
	gaps, _ := bound["gaps"].([]any)
	bound["gaps"] = append(gaps, "credential_binding: planted: the body organization is not bound to the credential")
	if got := adminFieldViolations(bound, scopes, root); violationsContain(got, bodyBindingMessage) {
		t.Errorf("a body-kind row with a credential_binding gap was still rejected for it: %v", got)
	}
}

// The billing and licensing rows that today's code lets a catalog role set perform carry that action.
var billingActionRows = map[string]string{
	"GET /api/v1/billing/invoices [dev-health-ops-api @api]":                    "billing.read",
	"GET /api/v1/billing/invoices/{invoice_id} [dev-health-ops-api @api]":       "billing.read",
	"GET /api/v1/billing/subscriptions [dev-health-ops-api @api]":               "billing.read",
	"GET /api/v1/billing/subscriptions/list [dev-health-ops-api @api]":          "billing.read",
	"GET /api/v1/billing/subscriptions/history [dev-health-ops-api @api]":       "billing.read",
	"GET /api/v1/billing/entitlements/{org_id} [dev-health-ops-api @api]":       "license.read",
	"GET /api/v1/licensing/entitlements/{org_id} [dev-health-ops-api @api]":     "license.read",
	"POST /api/v1/billing/invoices/{invoice_id}/void [dev-health-ops-api @api]": "billing.manage",
	"POST /api/v1/billing/subscriptions/change-plan [dev-health-ops-api @api]":  "billing.manage",
	"POST /api/v1/billing/subscriptions/cancel [dev-health-ops-api @api]":       "billing.manage",
	"POST /api/v1/billing/subscriptions/reactivate [dev-health-ops-api @api]":   "billing.manage",
}

// The billing rows no catalog role set fits: superuser-only handlers, the anonymous plan reads, the platform
// pull and the routes every organization role may call. They keep action null with a gap that says why.
var billingNullRows = []string{
	"GET /api/v1/billing/plans [dev-health-ops-api @api]",
	"GET /api/v1/billing/plans/{plan_id} [dev-health-ops-api @api]",
	"POST /api/v1/billing/plans [dev-health-ops-api @api]",
	"PUT /api/v1/billing/plans/{plan_id} [dev-health-ops-api @api]",
	"DELETE /api/v1/billing/plans/{plan_id} [dev-health-ops-api @api]",
	"POST /api/v1/billing/plans/{plan_id}/sync-stripe [dev-health-ops-api @api]",
	"POST /api/v1/billing/plans/pull-stripe [dev-health-ops-api @api]",
	"GET /api/v1/billing/audit [dev-health-ops-api @api]",
	"GET /api/v1/billing/audit/{audit_id} [dev-health-ops-api @api]",
	"POST /api/v1/billing/audit/{audit_id}/resolve [dev-health-ops-api @api]",
	"GET /api/v1/billing/refunds [dev-health-ops-api @api]",
	"GET /api/v1/billing/refunds/{refund_id} [dev-health-ops-api @api]",
	"POST /api/v1/billing/refunds [dev-health-ops-api @api]",
	"POST /api/v1/billing/reconcile [dev-health-ops-api @api]",
	"POST /api/v1/billing/checkout [dev-health-ops-api @api]",
	"POST /api/v1/billing/portal [dev-health-ops-api @api]",
}

func TestBillingAndLicenseRowsRecordTheirAction(t *testing.T) {
	ids := append([]string(nil), billingNullRows...)
	for id := range billingActionRows {
		ids = append(ids, id)
	}
	for id, row := range rowsByID(t, ids) {
		want, set := billingActionRows[id]
		got, isString := row["action"].(string)
		switch {
		case set && (!isString || got != want):
			t.Errorf("%s: action %v, want %q", id, row["action"], want)
		case set && gapNames(row, "action"):
			t.Errorf("%s: carries action %q and still an action gap", id, got)
		case !set && row["action"] != nil:
			t.Errorf("%s: action %v, want null", id, row["action"])
		case !set:
			gaps, _ := row["gaps"].([]any)
			for _, gap := range gaps {
				if text, _ := gap.(string); strings.HasPrefix(text, "action:") && strings.Contains(text, "the action it needs is") {
					t.Errorf("%s: the action gap still names a needed action the catalog now holds: %q", id, text)
				}
			}
		}
	}
}

// adminRoutePattern matches a billing route registration whose guard level is exactly policy.Admin.
var adminRoutePattern = regexp.MustCompile(`Method: http\.Method([A-Za-z]+), Pattern: prefix \+ "([^"]+)".*policy\.Admin,`)

func adminRoutes(source string) map[string]bool {
	out := map[string]bool{}
	for _, match := range adminRoutePattern.FindAllStringSubmatch(source, -1) {
		out[strings.ToUpper(match[1])+" /api/v1/billing"+match[2]+" [dev-health-ops-api @api]"] = true
	}
	return out
}

// billing.manage is recorded on exactly the billing routes that the policy.Admin guard (an organization admin or
// owner) wraps; a route added to or taken from that guard, or a row given the action without it, fails here.
func TestBillingManageRowsAreExactlyThePolicyAdminRoutes(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(testRoot(t), "internal/api/billing/routes.go"))
	if err != nil {
		t.Fatal(err)
	}
	admin := adminRoutes(string(raw))
	if len(admin) < 4 {
		t.Fatalf("found %d policy.Admin billing routes; the route parse measured nothing", len(admin))
	}
	recorded := map[string]bool{}
	for _, item := range loadGoAPIDocument(t)["rows"].([]any) {
		row := item.(map[string]any)
		if row["action"] == "billing.manage" {
			recorded[row["id"].(string)] = true
		}
	}
	for id := range admin {
		if !recorded[id] {
			t.Errorf("%s is wrapped in policy.Admin but its row does not record billing.manage", id)
		}
	}
	for id := range recorded {
		if !admin[id] {
			t.Errorf("%s records billing.manage but is not wrapped in policy.Admin", id)
		}
	}
}

func TestAdminRouteParseSeesOnlyTheAdminGuard(t *testing.T) {
	source := `		{Method: http.MethodPost, Pattern: prefix + "/a", Allow: http.MethodPost, Handler: wrap(policy.Admin, h.a)},
		{Method: http.MethodGet, Pattern: prefix + "/b", Handler: wrap(policy.AdminOrg, h.b)},
		{Method: http.MethodPost, Pattern: prefix + "/c", Handler: body(policy.Authenticated, h.c)},
		{Method: http.MethodPut, Pattern: prefix + "/d/{id}", Handler: body(policy.Admin, h.d)},`
	got := adminRoutes(source)
	want := []string{"POST /api/v1/billing/a [dev-health-ops-api @api]", "PUT /api/v1/billing/d/{id} [dev-health-ops-api @api]"}
	if len(got) != len(want) || !got[want[0]] || !got[want[1]] {
		t.Fatalf("adminRoutes = %v, want exactly %v", got, want)
	}
}

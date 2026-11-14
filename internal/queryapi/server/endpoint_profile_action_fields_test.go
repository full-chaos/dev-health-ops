package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
)

// declaredGapTicket matches the open ticket a declared gap must name.
var declaredGapTicket = regexp.MustCompile(`\bCHAOS-[0-9]+\b`)

const (
	fixtureGapTicket = "CHAOS-1"
	actionMapTicket  = "CHAOS-12657"
	unknownValue     = "unknown"
)

var actionFieldKeys = []string{"action", "resource_resolver", "current_state_cache_behavior", "impersonation_policy", "disclosure_behavior"}

var impersonationPolicies = map[string]bool{
	"subject_to_impersonation_override": true,
	"exempt_no_org_context":             true,
	"not_applicable_non_main_app":       true,
	unknownValue:                        true,
}

// declText is the comment-free, gofmt-printed text of one declaration of a Go
// file: a function (a method is "Type.Name"), or a var or const spec by name.
// A proof needle is searched in this text only, so a mention in a comment or
// elsewhere in the file never proves a statement.
func declText(src, name string) (string, bool) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "", src, 0)
	if err != nil {
		return "", false
	}
	print := func(node any) string {
		var buf bytes.Buffer
		if err := format.Node(&buf, fset, node); err != nil {
			return ""
		}
		return buf.String()
	}
	for _, decl := range file.Decls {
		switch d := decl.(type) {
		case *ast.FuncDecl:
			got := d.Name.Name
			if d.Recv != nil && len(d.Recv.List) == 1 {
				recv := d.Recv.List[0].Type
				if star, ok := recv.(*ast.StarExpr); ok {
					recv = star.X
				}
				if id, ok := recv.(*ast.Ident); ok {
					got = id.Name + "." + got
				}
			}
			if got == name {
				return print(d), true
			}
		case *ast.GenDecl:
			for _, spec := range d.Specs {
				if v, ok := spec.(*ast.ValueSpec); ok {
					for _, id := range v.Names {
						if id.Name == name {
							return print(v), true
						}
					}
				}
			}
		}
	}
	return "", false
}

// proofDefects returns what is missing for a proof to hold: the declaration,
// a needle, or (when anyOf is set) every alternative.
func proofDefects(who string, proof claimProof, read func(file string) string, decl string) []string {
	text, found := declText(read(proof.file), decl)
	if !found {
		return []string{fmt.Sprintf("%s: %s holds no declaration %q to prove the statement", who, proof.file, decl)}
	}
	var defects []string
	for _, needle := range proof.needles {
		if !strings.Contains(text, needle) {
			defects = append(defects, fmt.Sprintf("%s: the body of %s in %s does not contain %q", who, decl, proof.file, needle))
		}
	}
	if len(proof.anyOf) > 0 {
		hit := false
		for _, needle := range proof.anyOf {
			hit = hit || strings.Contains(text, needle)
		}
		if !hit {
			defects = append(defects, fmt.Sprintf("%s: the body of %s in %s contains none of %q", who, decl, proof.file, proof.anyOf))
		}
	}
	return defects
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

func rowFamily(id, route string) string {
	switch {
	case route == "/graphql":
		return "gql"
	case strings.Contains(id, "@mcp"):
		return "mcp"
	case route == "/buildinfo":
		return "build"
	case route == "/query/proof-write":
		return "proofw"
	case strings.HasPrefix(route, "/query"):
		return "query"
	case strings.Contains(route, "{"):
		return "rest-id"
	}
	return "rest"
}

// checkRowFields returns every defect of the query-api rows against the rule
// that a row asserts only what a source proof in this change states: a public
// row carries none of the five action fields; a protected row carries all
// five, never null; "action" is the catalog action the route action table
// (route_actions.go) names for the row, never "unknown", with no "action:" gap
// and a platform-scope action only on a resource_resolver of "none"; each other
// key is "unknown" with a named gap that names a ticket other than CHAOS-11042, or the exact
// statement registered for the row's family and field, whose every proof needle
// is present in the named source or test file; an impersonation policy is a
// schema value; the REST validator description is the registered statement.
func checkRowFields(rows []map[string]any, cat *policycatalog.Catalog, actions map[string]string, claims map[string]map[string]rowClaim, read func(file string) string) []string {
	var defects []string
	protectedKeys := map[string]bool{}
	prove := func(id, what string, claim rowClaim) {
		for _, proof := range claim.proofs {
			defects = append(defects, proofDefects(id+" "+what, proof, read, proof.decl)...)
		}
	}
	for _, row := range rows {
		id, _ := row["id"].(string)
		route, _ := row["route"].(string)
		if row["classification"] == "public" {
			for _, key := range actionFieldKeys {
				if _, present := row[key]; present {
					defects = append(defects, fmt.Sprintf("%s: public row carries %s", id, key))
				}
			}
			continue
		}
		fam := rowFamily(id, route)
		gaps := rowGaps(row)
		protectedKeys[rowActionKey(id)] = true
		for _, key := range actionFieldKeys {
			value, present := row[key]
			if !present {
				defects = append(defects, fmt.Sprintf("%s: protected row has no %s", id, key))
				continue
			}
			if value == nil {
				defects = append(defects, fmt.Sprintf("%s: %s is null, a gap is the value %q with a named gap", id, key, unknownValue))
				continue
			}
			text, isString := value.(string)
			if !isString || text == "" {
				defects = append(defects, fmt.Sprintf("%s: %s is not a non-empty string", id, key))
				continue
			}
			if key == "action" {
				resolver, _ := row["resource_resolver"].(string)
				defects = append(defects, checkRowAction(id, text, resolver, gaps, cat, actions[rowActionKey(id)])...)
				continue
			}
			switch {
			case text == unknownValue:
				named := false
				for _, gap := range gaps {
					if strings.HasPrefix(gap, key+":") && declaredGapTicket.MatchString(gap) && !strings.Contains(gap, retiredGapTicket) {
						named = true
					}
				}
				if !named {
					defects = append(defects, fmt.Sprintf("%s: %s is unknown without a %q gap naming an open ticket", id, key, key+":"))
				}
			default:
				claim, registered := claims[fam][key]
				switch {
				case !registered:
					defects = append(defects, fmt.Sprintf("%s: no proof is registered for %s %q of family %s, state it as %q with a gap", id, key, text, fam, unknownValue))
				case claim.value != text:
					defects = append(defects, fmt.Sprintf("%s: %s is not the statement proven for family %s: %.80q", id, key, fam, text))
				default:
					prove(id, key, claim)
				}
			}
			if key == "impersonation_policy" && !impersonationPolicies[text] {
				defects = append(defects, fmt.Sprintf("%s: impersonation_policy %q is not a schema value", id, text))
			}
		}
		if fam == "rest" || fam == "rest-id" {
			if pv, ok := row["primary_validator"].(map[string]any); ok {
				description, _ := pv["description"].(string)
				if claim, registered := claims[fam]["primary_validator"]; registered {
					if description != claim.value {
						defects = append(defects, fmt.Sprintf("%s: primary_validator description is not the proven statement: %.80q", id, description))
					} else {
						prove(id, "primary_validator", claim)
					}
				}
			}
		}
	}
	for key := range protectedKeys {
		if _, listed := actions[key]; !listed {
			defects = append(defects, "protected row with no entry in the route action table: "+key)
		}
	}
	for key := range actions {
		if !protectedKeys[key] {
			defects = append(defects, "route action table entry for a route with no protected row: "+key)
		}
	}
	return defects
}

var rowIDKey = regexp.MustCompile(`^(.+?) \[[^\]]* @([a-z,]+)\]$`)

// rowActionKey is the route action table's key for a row id: "METHOD route @listeners".
func rowActionKey(id string) string {
	if m := rowIDKey.FindStringSubmatch(id); m != nil {
		return m[1] + " @" + m[2]
	}
	return id
}

// checkRowAction returns the defects of one protected row's action: empty,
// "unknown", not in the catalog, not the table's, a platform-scope action on a
// resource_resolver other than "none", or a row that still carries an "action:" gap.
func checkRowAction(id, action, resolver string, gaps []string, cat *policycatalog.Catalog, want string) []string {
	var defects []string
	entry, err := cat.Action(action)
	switch {
	case action == unknownValue:
		defects = append(defects, fmt.Sprintf("%s: action is unknown; every query-api protected row names a catalog action (%s)", id, actionMapTicket))
	case err != nil:
		defects = append(defects, fmt.Sprintf("%s: action %q is not in the catalog", id, action))
	case (entry.Scope == policycatalog.ScopePlatform) && !strings.HasPrefix(resolver, "none"):
		defects = append(defects, fmt.Sprintf("%s: action %q has scope platform but resource_resolver is %.40q (a platform action needs resolver none)", id, action, resolver))
	case action != want:
		defects = append(defects, fmt.Sprintf("%s: action %q is not the route action table's %q", id, action, want))
	}
	for _, gap := range gaps {
		if strings.HasPrefix(gap, "action:") && action != unknownValue {
			defects = append(defects, fmt.Sprintf("%s: the row names action %q and still carries an \"action:\" gap", id, action))
		}
	}
	return defects
}

func readServerSource(t *testing.T) func(file string) string {
	return func(file string) string {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
}

func TestQueryAPIRowsAssertOnlyProvenStatementsOrDeclareAGap(t *testing.T) {
	rows := loadQueryAPIRows(t)
	if len(rows) < 40 {
		t.Fatalf("only %d rows: the row load stopped seeing them", len(rows))
	}
	for _, defect := range checkRowFields(rows, policycatalog.Default(), RouteActionTable(), rowClaims, readServerSource(t)) {
		t.Error(defect)
	}
}

func fixtureActions() map[string]string { return map[string]string{"GET /x": "analytics.read"} }

func TestRowFieldGuardCatchesPlantedDefects(t *testing.T) {
	cat := policycatalog.Default()
	src := "package p\nfunc f() { _ = \"needle-a\" /* needle-comment */ }\nfunc g() { _ = \"other\" }\n"
	claims := map[string]map[string]rowClaim{
		"rest": {
			"resource_resolver":   {value: "proven resolver", proofs: []claimProof{{file: "a.go", decl: "f", needles: []string{"needle-a"}}}},
			"disclosure_behavior": {value: "proven disclosure", proofs: []claimProof{{file: "a.go", decl: "f", needles: []string{"needle-a"}}, {file: "a.go", decl: "g", anyOf: []string{"nothing", "other"}}}},
			"primary_validator":   {value: "proven validator", proofs: []claimProof{{file: "a.go", decl: "f", needles: []string{"needle-a"}}}},
		},
	}
	read := func(string) string { return src }
	gap := func(field string) []any { return []any{field + ": declared gap tracked in " + fixtureGapTicket} }
	protected := func() map[string]any {
		gaps := append(gap("current_state_cache_behavior"), gap("impersonation_policy")[0])
		return map[string]any{
			"id": "GET /x", "route": "/x", "classification": "protected", "gaps": gaps,
			"action": "analytics.read", "resource_resolver": "proven resolver", "current_state_cache_behavior": unknownValue,
			"impersonation_policy": unknownValue, "disclosure_behavior": "proven disclosure",
			"primary_validator": map[string]any{"description": "proven validator"},
		}
	}
	public := func() map[string]any {
		return map[string]any{"id": "GET /p", "classification": "public", "gaps": []any{}}
	}
	if defects := checkRowFields([]map[string]any{protected(), public()}, cat, fixtureActions(), claims, read); len(defects) != 0 {
		t.Fatalf("the clean fixture is refused: %v", defects)
	}
	with := func(key string, value any) map[string]any { r := protected(); r[key] = value; return r }
	cases := []struct {
		name   string
		row    map[string]any
		claims map[string]map[string]rowClaim
		read   func(string) string
		want   string
	}{
		{"public row with a field", func() map[string]any { r := public(); r["action"] = "analytics.read"; return r }(), nil, nil, "public row carries action"},
		{"protected row missing a field", func() map[string]any { r := protected(); delete(r, "resource_resolver"); return r }(), nil, nil, "has no resource_resolver"},
		{"null field", with("disclosure_behavior", nil), nil, nil, "is null"},
		{"empty string", with("disclosure_behavior", ""), nil, nil, "not a non-empty string"},
		{"unknown without a gap", with("gaps", gap("current_state_cache_behavior")), nil, nil, "impersonation_policy is unknown without"},
		{"unknown gap without the ticket", with("gaps", []any{"current_state_cache_behavior: x", "impersonation_policy: x"}), nil, nil, "naming an open ticket"},
		{"unknown gap naming the closed check", with("gaps", []any{"current_state_cache_behavior: declared gap tracked in " + retiredGapTicket, "impersonation_policy: declared gap tracked in " + retiredGapTicket}), nil, nil, "naming an open ticket"},
		{"action set back to unknown", with("action", unknownValue), nil, nil, "action is unknown; every query-api protected row names a catalog action"},
		{"action not in the catalog", with("action", "x.nothing"), nil, nil, "is not in the catalog"},
		{"action that is not the table's", with("action", "metrics.read"), nil, nil, "is not the route action table's \"analytics.read\""},
		{"row with an action and a stale action gap", with("gaps", append(gap("action"), gap("current_state_cache_behavior")[0], gap("impersonation_policy")[0])), nil, nil, "still carries an \"action:\" gap"},
		{"platform action on an organization resolver", with("action", "admin.telemetry"), nil, nil, "has scope platform but resource_resolver"},
		{"unproven statement", with("resource_resolver", "a guess"), nil, nil, "not the statement proven"},
		{"statement for a field with no registered proof", with("current_state_cache_behavior", "none"), nil, nil, "no proof is registered"},
		{"needle present only in a comment", protected(), map[string]map[string]rowClaim{"rest": {"resource_resolver": {value: "proven resolver", proofs: []claimProof{{file: "a.go", decl: "f", needles: []string{"needle-comment"}}}}, "disclosure_behavior": claims["rest"]["disclosure_behavior"], "primary_validator": claims["rest"]["primary_validator"]}}, nil, "does not contain"},
		{"needle in another declaration of the file", protected(), map[string]map[string]rowClaim{"rest": {"resource_resolver": {value: "proven resolver", proofs: []claimProof{{file: "a.go", decl: "f", needles: []string{"other"}}}}, "disclosure_behavior": claims["rest"]["disclosure_behavior"], "primary_validator": claims["rest"]["primary_validator"]}}, nil, "does not contain"},
		{"declaration absent", protected(), map[string]map[string]rowClaim{"rest": {"resource_resolver": {value: "proven resolver", proofs: []claimProof{{file: "a.go", decl: "missing", needles: []string{"x"}}}}, "disclosure_behavior": claims["rest"]["disclosure_behavior"], "primary_validator": claims["rest"]["primary_validator"]}}, nil, "holds no declaration"},
		{"needle gone from the source", protected(), nil, func(string) string { return "package p\nfunc f() {}\nfunc g() { _ = \"other\" }\n" }, "does not contain"},
		{"no alternative present", protected(), nil, func(string) string { return "package p\nfunc f() { _ = \"needle-a\" }\nfunc g() {}\n" }, "contains none of"},
		{"bad impersonation value", with("impersonation_policy", "maybe"), nil, nil, "not a schema value"},
		{"validator is not the proven statement", with("primary_validator", map[string]any{"description": "every failure answers the same 401"}), nil, nil, "primary_validator description is not the proven statement"},
	}
	for _, c := range cases {
		reader, table := read, claims
		if c.read != nil {
			reader = c.read
		}
		if c.claims != nil {
			table = c.claims
		}
		joined := strings.Join(checkRowFields([]map[string]any{c.row}, cat, fixtureActions(), table, reader), "\n")
		if !strings.Contains(joined, c.want) {
			t.Errorf("%s: the guard did not report %q, it reported %q", c.name, c.want, joined)
		}
	}
}

// TestRowGuardGoesRedOnTheVerifierPlants plants, in the real source text, the
// defects a verifier named: a status constant changed, and a REST row that
// asserts a catalog action nothing proves.
func TestRowGuardGoesRedOnTheVerifierPlants(t *testing.T) {
	cat := policycatalog.Default()
	real := readServerSource(t)
	rows := loadQueryAPIRows(t)
	if defects := checkRowFields(rows, cat, RouteActionTable(), rowClaims, real); len(defects) != 0 {
		t.Fatalf("the real rows are refused before any plant: %v", defects[:1])
	}
	swapped := func(file, old, repl string) func(string) string {
		return func(f string) string {
			text := real(f)
			if f == file {
				if !strings.Contains(text, old) {
					t.Fatalf("plant anchor %q is not in %s", old, file)
				}
				text = strings.Replace(text, old, repl, 1)
			}
			return text
		}
	}
	forbidden := swapped("rest_access.go", `http.StatusForbidden, "Organization context required"`, `http.StatusUnauthorized, "Organization context required"`)
	if joined := strings.Join(checkRowFields(rows, cat, RouteActionTable(), rowClaims, forbidden), "\n"); !strings.Contains(joined, "Organization context required") {
		t.Errorf("a 403 changed to 401 in authenticateRESTRouted stayed green")
	}
	commented := swapped("rest_access.go", `policy.WriteAuthFailure(`, `/* policy.WriteAuthFailure( */ nothing(`)
	if joined := strings.Join(checkRowFields(rows, cat, RouteActionTable(), rowClaims, commented), "\n"); !strings.Contains(joined, "WriteAuthFailure") {
		t.Errorf("a needle moved into a comment stayed green")
	}
	plant := func(action string) []map[string]any {
		var planted []map[string]any
		for _, row := range rows {
			if row["classification"] == "protected" && row["route"] == "/api/v1/home" && row["method"] == "GET" {
				copyRow := map[string]any{}
				for k, v := range row {
					copyRow[k] = v
				}
				copyRow["action"] = action
				planted = append(planted, copyRow)
			}
		}
		return planted
	}
	table := RouteActionTable()
	for key := range table {
		if key != "GET /api/v1/home @query,internal" {
			delete(table, key)
		}
	}
	if joined := strings.Join(checkRowFields(plant(unknownValue), cat, table, rowClaims, real), "\n"); !strings.Contains(joined, "action is unknown; every query-api protected row names a catalog action") {
		t.Errorf("a real row set back to unknown stayed green: %q", joined)
	}
	if joined := strings.Join(checkRowFields(plant("git.read"), cat, table, rowClaims, real), "\n"); !strings.Contains(joined, "is not the route action table's") {
		t.Errorf("a real row whose action is not the table's stayed green: %q", joined)
	}
}

// legacyGuard is what today's query-api code lets through for a group of rows. Parity rule (pm, answers-12657-1): a row's
// action, with the catalog's role grants for it, allows exactly what the legacy guard allows. orgRoles is the organization
// roles the legacy guard admits; a platform guard names the one action its code requires. Each guard is proven by needles
// in the declaration that implements it.
type legacyGuard struct {
	orgRoles       []string
	platformAction string
	proofs         []claimProof
}

var allOrgRoles = []string{"admin", "member", "owner", "viewer"}

var legacyGuards = map[string]legacyGuard{
	"rest-org": {orgRoles: allOrgRoles, proofs: []claimProof{
		{file: "rest_access.go", decl: "authenticateRESTRouted", needles: []string{"return authctx.Claims{OrgID: claims.OrgID, Scope: scope}, true"}},
	}},
	"internal-org": {orgRoles: allOrgRoles, proofs: []claimProof{
		{file: "internal_auth.go", decl: "authenticateInternalRequest", needles: []string{"return withLegacyScope(claims), true"}},
	}},
	"graphql-edge": {orgRoles: allOrgRoles, proofs: []claimProof{
		{file: "../../api/policy/scope.go", decl: "Scope.mayUseOrg", needles: []string{"s.auth.IsMember(ctx, user.UserID, orgID)"}},
	}},
	"mcp": {orgRoles: []string{"member", "viewer"}, proofs: []claimProof{
		{file: "mcp_route.go", decl: "mcpAuthenticate", needles: []string{"mcpOperatorRoles[role]", "http.StatusForbidden, mcpReasonElevatedClaim"}},
		{file: "mcp_route.go", decl: "mcpOperatorRoles", needles: []string{"\"admin\"", "\"owner\"", "\"operator\""}},
	}},
	"graphql-arg": {orgRoles: allOrgRoles, proofs: []claimProof{
		{file: "../graph/operation_org_guard.go", decl: "operationOrgDecision", needles: []string{"claims.OrgID == first"}},
	}},
	"graphql-cred": {orgRoles: allOrgRoles, proofs: []claimProof{
		{file: "../graph/orgauth.go", decl: "requestOrg", needles: []string{"claims.OrgID == \"\""}},
	}},
	"graphql-mutation": {orgRoles: allOrgRoles, proofs: []claimProof{
		{file: "../graph/reports_mutations.go", decl: "requireMutationOrg", needles: []string{"claims.OrgID != orgID"}},
	}},
	"datahealth-operator": {orgRoles: []string{"admin", "owner"}, proofs: []claimProof{
		{file: "../datahealth/datahealth.go", decl: "RequireOperator", needles: []string{"case \"admin\", \"owner\", \"operator\":", "p.Holds(rule.ActionAdminStats)"}},
	}},
	"platform-telemetry": {platformAction: "admin.telemetry", proofs: []claimProof{
		{file: "../producttelemetry/platform.go", decl: "RequirePlatformAdmin", needles: []string{"p.Holds(rule.ActionAdminTelemetry)"}},
	}},
}

// legacyGuardOf names the legacy guard of a REST table key ("METHOD route @listeners") or a GraphQL field key
// ("Query.name"); credentialResolver reports a GraphQL field whose organization comes from the credential.
func legacyGuardOf(key string, credentialResolver map[string]bool) string {
	switch {
	case strings.HasPrefix(key, "Query.") || strings.HasPrefix(key, "Mutation."):
		switch {
		case key == "Query.dataHealth":
			return "datahealth-operator"
		case key == "Query.productTelemetryPlatformDashboard":
			return "platform-telemetry"
		case strings.HasPrefix(key, "Mutation."):
			return "graphql-mutation"
		case credentialResolver[key]:
			return "graphql-cred"
		}
		return "graphql-arg"
	case strings.HasSuffix(key, " @mcp"):
		return "mcp"
	case strings.Contains(key, " /graphql "):
		return "graphql-edge"
	case strings.Contains(key, " /query") || strings.Contains(key, " /buildinfo "):
		return "internal-org"
	}
	return "rest-org"
}

// parityGap is a row where the catalog's grants differ from the legacy guard's, listed as a deliberate change candidate:
// the exact roles the catalog grants beyond legacy (extra) and the legacy roles the catalog does not grant (missing).
type parityGap struct {
	extra, missing []string
	candidate      string
}

var parityGaps = map[string]parityGap{
	"POST /query @mcp": {extra: []string{"admin", "owner"}, candidate: "the MCP caller class refuses an identity carrying an operator, admin or owner role (mcpAuthenticate, mcp_route.go); the catalog grants analytics.read to admin and owner"},
}

func roleSet(roles []string) string {
	out := append([]string(nil), roles...)
	sort.Strings(out)
	return strings.Join(out, ",")
}

func minus(a, b []string) []string {
	var out []string
	for _, x := range a {
		found := false
		for _, y := range b {
			found = found || x == y
		}
		if !found {
			out = append(out, x)
		}
	}
	return out
}

// parityDefects returns every key whose action, with its catalog role grants, does not allow exactly what its legacy
// guard allows and is not a listed gap, every listed gap that is not exact or no longer needed, and every legacy guard
// whose proof is missing from the declaration it names.
func parityDefects(actions map[string]string, guardOf func(key string) string, cat *policycatalog.Catalog, gaps map[string]parityGap, read func(file string) string) []string {
	var defects []string
	keys := make([]string, 0, len(actions))
	for key := range actions {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	used := map[string]bool{}
	for _, key := range keys {
		action := actions[key]
		name := guardOf(key)
		guard, known := legacyGuards[name]
		if !known {
			defects = append(defects, fmt.Sprintf("%s: no legacy guard %q is registered", key, name))
			continue
		}
		used[name] = true
		if guard.platformAction != "" {
			if action != guard.platformAction {
				defects = append(defects, fmt.Sprintf("%s: the legacy guard %s requires %q, the table says %q", key, name, guard.platformAction, action))
			}
			continue
		}
		granted := cat.RolesHolding(action, policycatalog.ScopeOrganization)
		extra, missing := minus(granted, guard.orgRoles), minus(guard.orgRoles, granted)
		gap, listed := gaps[key]
		switch {
		case len(extra) == 0 && len(missing) == 0 && listed:
			defects = append(defects, fmt.Sprintf("%s: listed as a parity gap but the catalog grants of %q equal the legacy roles", key, action))
		case len(extra) == 0 && len(missing) == 0:
		case !listed:
			defects = append(defects, fmt.Sprintf("%s: parity: action %q is granted to roles [%s] but the legacy guard %s admits [%s]; a difference is a deliberate change candidate, list it in parityGaps", key, action, roleSet(granted), name, roleSet(guard.orgRoles)))
		case roleSet(extra) != roleSet(gap.extra) || roleSet(missing) != roleSet(gap.missing) || gap.candidate == "":
			defects = append(defects, fmt.Sprintf("%s: the listed parity gap (extra [%s], missing [%s]) is not the real one (extra [%s], missing [%s])", key, roleSet(gap.extra), roleSet(gap.missing), roleSet(extra), roleSet(missing)))
		}
	}
	for key := range gaps {
		if _, tabled := actions[key]; !tabled {
			defects = append(defects, "parity gap for a key the action table does not hold: "+key)
		}
	}
	names := make([]string, 0, len(used))
	for name := range used {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		for _, proof := range legacyGuards[name].proofs {
			defects = append(defects, proofDefects("legacy guard "+name, proof, read, proof.decl)...)
		}
	}
	return defects
}

func graphCredentialResolvers(t *testing.T) map[string]bool {
	t.Helper()
	raw, err := os.ReadFile("../../../contracts/auth/v1/graphql-field-actions.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Rows []struct {
			Type, Field      string
			ResourceResolver string `json:"resource_resolver"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	out := map[string]bool{}
	for _, row := range doc.Rows {
		if strings.HasPrefix(row.ResourceResolver, "organization_from_credential") {
			out[row.Type+"."+row.Field] = true
		}
	}
	return out
}

func allActions(t *testing.T) map[string]string {
	all := RouteActionTable()
	for key, action := range graph.FieldActionTable() {
		if _, clash := all[key]; clash {
			t.Fatalf("key %q is in both action tables", key)
		}
		all[key] = action
	}
	return all
}

func TestEveryQueryAPIActionAllowsExactlyWhatTheLegacyGuardAllows(t *testing.T) {
	credential := graphCredentialResolvers(t)
	guardOf := func(key string) string { return legacyGuardOf(key, credential) }
	all := allActions(t)
	if len(all) != 89 {
		t.Fatalf("%d actions in the two tables, want 37 REST rows and 52 GraphQL fields", len(all))
	}
	for _, defect := range parityDefects(all, guardOf, policycatalog.Default(), parityGaps, readServerSource(t)) {
		t.Error(defect)
	}
}

func TestParityCellCatchesAWideningAndANarrowing(t *testing.T) {
	cat := policycatalog.Default()
	credential := graphCredentialResolvers(t)
	guardOf := func(key string) string { return legacyGuardOf(key, credential) }
	real := readServerSource(t)
	all := allActions(t)
	with := func(key, action string) map[string]string {
		out := map[string]string{}
		for k, v := range all {
			out[k] = v
		}
		out[key] = action
		return out
	}
	narrowed := strings.Join(parityDefects(with("Query.sourceHealth", "integrations.read"), guardOf, cat, parityGaps, real), "\n")
	if !strings.Contains(narrowed, "Query.sourceHealth: parity: action \"integrations.read\" is granted to roles [admin,owner] but the legacy guard graphql-arg admits [admin,member,owner,viewer]") {
		t.Errorf("sourceHealth narrowed to integrations.read stayed green: %q", narrowed)
	}
	widened := strings.Join(parityDefects(with("Query.dataHealth", "analytics.read"), guardOf, cat, parityGaps, real), "\n")
	if !strings.Contains(widened, "Query.dataHealth: parity: action \"analytics.read\" is granted to roles [admin,member,owner,viewer] but the legacy guard datahealth-operator admits [admin,owner]") {
		t.Errorf("dataHealth widened to analytics.read stayed green: %q", widened)
	}
	platform := strings.Join(parityDefects(with("Query.productTelemetryPlatformDashboard", "admin.stats"), guardOf, cat, parityGaps, real), "\n")
	if !strings.Contains(platform, "the legacy guard platform-telemetry requires \"admin.telemetry\"") {
		t.Errorf("the platform field moved to another platform action stayed green: %q", platform)
	}
	stale := map[string]parityGap{"Query.home": {extra: []string{"admin"}, candidate: "x"}}
	if joined := strings.Join(parityDefects(all, guardOf, cat, stale, real), "\n"); !strings.Contains(joined, "Query.home: listed as a parity gap but the catalog grants") {
		t.Errorf("a parity gap that is not needed stayed green: %q", joined)
	}
	mcp := map[string]parityGap{"POST /query @mcp": {extra: []string{"admin"}, candidate: "x"}}
	if joined := strings.Join(parityDefects(all, guardOf, cat, mcp, real), "\n"); !strings.Contains(joined, "POST /query @mcp: the listed parity gap") {
		t.Errorf("an inexact parity gap stayed green: %q", joined)
	}
	gone := func(file string) string {
		text := real(file)
		if file == "../datahealth/datahealth.go" {
			text = strings.Replace(text, "case \"admin\", \"owner\", \"operator\":", "case \"admin\", \"owner\":", 1)
		}
		return text
	}
	if joined := strings.Join(parityDefects(all, guardOf, cat, parityGaps, gone), "\n"); !strings.Contains(joined, "legacy guard datahealth-operator") {
		t.Errorf("a legacy guard whose proof is gone stayed green: %q", joined)
	}
}

// queryAPIRoleReads counts, per file under internal/queryapi, the selector reads that decide access by role or action:
// .Role, .Holds, RequireOperator and RequirePlatformAdmin. The parity rule rests on these being the only gates beyond the
// organization match; a new one fails here until it is reviewed against the action tables.
func queryAPIRoleReads(t *testing.T) map[string]int {
	t.Helper()
	counts := map[string]int{}
	watched := map[string]bool{"Role": true, "Holds": true, "RequireOperator": true, "RequirePlatformAdmin": true}
	err := filepath.WalkDir("..", func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return err
		}
		file, perr := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if perr != nil {
			return perr
		}
		ast.Inspect(file, func(node ast.Node) bool {
			if sel, ok := node.(*ast.SelectorExpr); ok && watched[sel.Sel.Name] {
				counts[strings.TrimPrefix(filepath.ToSlash(path), "../")]++
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return counts
}

// Each count was read against its file: the legacy gates are datahealth.RequireOperator (admin.stats or the admin, owner,
// operator roles), producttelemetry.RequirePlatformAdmin (admin.telemetry, no impersonation), the org-less platform
// admission and the cross-org superuser branch in operation_org_guard.go (admin.orgs), the MCP elevated-role refusal in
// mcp_route.go, the /buildinfo platform-admin bound in buildinfo_route.go (admin.orgs, a user access token only; the
// identity headers keep the any-role answer), /query's live scope applying an impersonation row to a holder of
// admin.impersonate only (internal_auth.go liveScope, as go-api's Scope.Impersonation), and the plumbing that copies
// the role into the claims or logs it. graph/field_precheck.go and
// graph/operation_shadow.go only copy the verified claims into the shadow's pre-checks and observations (the shadow
// changes no decision).
var knownQueryAPIRoleReads = map[string]int{
	"authctx/authctx.go":                   1,
	"datahealth/datahealth.go":             3,
	"graph/field_precheck.go":              3,
	"graph/operation_org_guard.go":         2,
	"graph/operation_shadow.go":            2,
	"graph/schema.resolvers.go":            3,
	"internalidentity/internalidentity.go": 1,
	"producttelemetry/platform.go":         2,
	"server/buildinfo_route.go":            1,
	"server/graphql_edge_route.go":         1,
	"server/internal_auth.go":              2,
	"server/mcp_route.go":                  1,
}

func TestQueryAPIRoleAndActionReadsAreExactlyTheKnownLegacyGates(t *testing.T) {
	got := queryAPIRoleReads(t)
	for file, n := range got {
		if knownQueryAPIRoleReads[file] != n {
			t.Errorf("%s: %d role or action reads, the reviewed count is %d: a new gate changes what the action tables must allow", file, n, knownQueryAPIRoleReads[file])
		}
	}
	for file, n := range knownQueryAPIRoleReads {
		if got[file] != n {
			t.Errorf("%s: reviewed %d role or action reads, found %d", file, n, got[file])
		}
	}
}

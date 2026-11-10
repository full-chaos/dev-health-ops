package apiservice

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/routescan"
)

const goAPIRows = "../../contracts/auth/v1/endpoint-profiles.go-api.json"

var rowListener = regexp.MustCompile(`@([a-z-]+)\]$`)

func loadGoAPIRows(t *testing.T) []map[string]any {
	t.Helper()
	raw, err := os.ReadFile(goAPIRows)
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Rows []map[string]any `json:"rows"`
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	return document.Rows
}

// rowKey is the "METHOD path@listener" a row stands for.
func rowKey(t *testing.T, row map[string]any) string {
	t.Helper()
	id, _ := row["id"].(string)
	match := rowListener.FindStringSubmatch(id)
	if match == nil {
		t.Fatalf("row id %q does not end in @listener]", id)
	}
	method, _ := row["method"].(string)
	route, _ := row["route"].(string)
	return method + " " + route + "@" + match[1]
}

// rowGateProblems compares what the listeners mount with the rows: a mounted
// route with no row is missing, a row for a route nothing mounts is stale.
func rowGateProblems(mounted []mountedRoute, rowKeys []string) (missing, stale []string) {
	served := map[string]bool{}
	for _, route := range mounted {
		served[route.key()] = true
	}
	have := map[string]bool{}
	for _, key := range rowKeys {
		have[key] = true
		if !served[key] {
			stale = append(stale, key)
		}
	}
	for key := range served {
		if !have[key] {
			missing = append(missing, key)
		}
	}
	sort.Strings(missing)
	sort.Strings(stale)
	return missing, stale
}

// TestEveryMountedRouteHasAnEndpointProfileRowAndNoRowIsStale: a route a
// go-api listener mounts with no row fails here, and so does a row for a route
// nothing mounts. The mounted set is the real route tables of the api, internal
// and billing-edge listeners.
func TestEveryMountedRouteHasAnEndpointProfileRowAndNoRowIsStale(t *testing.T) {
	mounted := mountedRoutes(t)
	var keys []string
	seen := map[string]bool{}
	for _, row := range loadGoAPIRows(t) {
		key := rowKey(t, row)
		if seen[key] {
			t.Errorf("two rows for %s", key)
		}
		seen[key] = true
		keys = append(keys, key)
	}
	missing, stale := rowGateProblems(mounted, keys)
	if len(missing) > 0 {
		t.Errorf("mounted with no endpoint-profile row (contracts/auth/v1/endpoint-profiles.go-api.json): %v", missing)
	}
	if len(stale) > 0 {
		t.Errorf("endpoint-profile rows for routes nothing mounts: %v", stale)
	}
	if len(mounted) < 200 {
		t.Fatalf("only %d mounted routes found: the route tables stopped mounting", len(mounted))
	}
}

func TestTheRowGateFlagsAPlantedRouteAndAPlantedStaleRow(t *testing.T) {
	mounted := []mountedRoute{
		{Listener: "api", Method: "GET", Pattern: "/api/v1/a"},
		{Listener: "api", Method: "POST", Pattern: "/api/v1/planted-without-a-row"},
		{Listener: "internal", Method: "GET", Pattern: "/api/v1/b"},
	}
	keys := []string{"GET /api/v1/a@api", "GET /api/v1/b@internal", "GET /api/v1/removed@api", "GET /api/v1/b@api"}
	missing, stale := rowGateProblems(mounted, keys)
	if strings.Join(missing, ",") != "POST /api/v1/planted-without-a-row@api" {
		t.Fatalf("missing = %v", missing)
	}
	if strings.Join(stale, ",") != "GET /api/v1/b@api,GET /api/v1/removed@api" {
		t.Fatalf("stale = %v", stale)
	}
	if missing, stale := rowGateProblems(mounted[:1], []string{"GET /api/v1/a@api"}); len(missing)+len(stale) != 0 {
		t.Fatalf("a matching table reported %v %v", missing, stale)
	}
}

// treePath turns a scanned path ("../api/x.go" or "./admin/x.go") into the
// repo-relative form rows carry.
func treePath(scanned string) string {
	if strings.HasPrefix(scanned, "../") {
		return "internal/" + strings.TrimPrefix(scanned, "../")
	}
	return "internal/apiservice/" + strings.TrimPrefix(scanned, "./")
}

// dispatchedLevels are the routes whose registration hands the request to a
// dispatcher that applies the guard level itself, so the registration names no
// policy level; each is pinned here with the level its dispatch applies.
var dispatchedLevels = map[string]string{
	"PATCH /api/v1/auth/oauth/{first}/{second}@api": "Authenticated",
	"POST /api/v1/admin/teams/{team_id}@api":        "AdminOrg",
}

// TestEndpointProfileRowsSitAtTheirRegistrationAndNameTheirGuard: each row's
// source file holds the one registration (method + route) that mounts its
// route, and the row agrees with the guard level that registration names. A
// route behind policy.Admin, AdminOrg, Authenticated, Superuser or Optional
// accepts the user access token with no HS256 opt-in; no row lists the retired
// ops access token. The row's source line is a citation: it is not compared.
func TestEndpointProfileRowsSitAtTheirRegistrationAndNameTheirGuard(t *testing.T) {
	problems, checked, drift := rowRegistrationProblems(t, scanRegistrations(t, "../api", "."), loadGoAPIRows(t), mountedRoutes(t), treePath)
	for _, problem := range problems {
		t.Error(problem)
	}
	if checked < 200 {
		t.Fatalf("only %d rows were checked against their registrations", checked)
	}
	t.Logf("citation note: %d of %d rows cite a line other than their registration's", drift, checked)
}

// rowRegistrationProblems ties each mounted route's row to the registration that mounts it, by symbol, and to the
// guard level that registration names. rel turns a scanned path into the repo-relative form rows carry. drift counts
// the rows whose cited line is not the registration's.
func rowRegistrationProblems(t *testing.T, regs []registration, rows []map[string]any, mounted []mountedRoute, rel func(string) string) (problems []string, checked, drift int) {
	t.Helper()
	bad := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }
	byKey := map[string]map[string]any{}
	for _, row := range rows {
		byKey[rowKey(t, row)] = row
	}
	guarded := map[string]bool{"Admin": true, "AdminOrg": true, "Authenticated": true, "Superuser": true, "Optional": true}
	for _, route := range mounted {
		row := byKey[route.key()]
		if row == nil {
			continue
		}
		reg, ok := registrationFor(regs, route)
		if !ok {
			bad("%s: no registration found in the source", route.key())
			continue
		}
		source := row["source"].(map[string]any)
		if got := source["file"].(string); got != rel(reg.File) {
			bad("%s: row source file %s, the registration is in %s", route.key(), got, rel(reg.File))
		}
		if reg.Pattern != "" {
			if _, err := routescan.Resolve(regs, reg.File, route.Method, route.Pattern); err != nil {
				bad("%s: %v", route.key(), err)
			}
		}
		if int(source["line"].(float64)) != reg.Line {
			drift++
		}
		classes, _ := row["accepted_credential_classes"].([]any)
		hs256, user := false, false
		for _, class := range classes {
			hs256 = hs256 || class == "ops_access_token_hs256"
			user = user || class == "user_access_token"
		}
		validation, _ := row["access_token_validation"].(map[string]any)
		level := reg.Level
		if level == "" {
			level = dispatchedLevels[route.key()]
		}
		reg.Level = level
		switch {
		case guarded[reg.Level]:
			validator, _ := row["primary_validator"].(map[string]any)
			description, _ := validator["description"].(string)
			if row["classification"] != "protected" || hs256 || !user || validation["legacy_hs256"] != false || !strings.HasPrefix(description, "policy."+reg.Level+":") {
				bad("%s: registered at policy.%s, row says %v %v %v %q", route.key(), reg.Level, row["classification"], classes, validation, description)
			}
		case hs256:
			bad("%s: the row accepts the retired ops access token (registration level %q)", route.key(), reg.Level)
		}
		checked++
	}
	return problems, checked, drift
}

// TestEndpointProfileAnchorsStillPointAtTheirSymbols: every validator anchor
// carries a note, and the note resolves in its file to one place: the one line
// that holds it, or, for a note on several lines (the weak "Pattern:"), the
// row's own registration. The anchor's line is a citation: it is not compared.
func TestEndpointProfileAnchorsStillPointAtTheirSymbols(t *testing.T) {
	problems, checked, drift := anchorNoteProblems(t, "../..", loadGoAPIRows(t))
	for _, problem := range problems {
		t.Error(problem)
	}
	if checked < 200 {
		t.Fatalf("only %d anchors were checked: the walk stopped seeing them", checked)
	}
	t.Logf("citation note: %d of %d validator anchors cite a line other than the one their note resolves to", drift, checked)
}

// anchorNoteProblems resolves the validator anchors of rows in the source under root. drift counts the anchors whose
// cited line is not the resolved one.
func anchorNoteProblems(t *testing.T, root string, rows []map[string]any) (problems []string, checked, drift int) {
	t.Helper()
	sources := map[string][]byte{}
	read := func(path string) []byte {
		if raw, ok := sources[path]; ok {
			return raw
		}
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatalf("anchor file %s: %v", path, err)
		}
		sources[path] = raw
		return raw
	}
	scanned := map[string][]registration{}
	ownRegistration := func(row map[string]any, path string) (registration, error) {
		dir := filepath.Join(root, filepath.Dir(path))
		if _, ok := scanned[dir]; !ok {
			scanned[dir] = scanRegistrations(t, dir)
		}
		method, _ := row["method"].(string)
		route, _ := row["route"].(string)
		return routescan.Resolve(scanned[dir], filepath.ToSlash(filepath.Join(root, path)), method, route)
	}
	rowFile := func(row map[string]any) string {
		source, _ := row["source"].(map[string]any)
		file, _ := source["file"].(string)
		return file
	}
	var walk func(node any, where string, row map[string]any)
	walk = func(node any, where string, row map[string]any) {
		switch value := node.(type) {
		case map[string]any:
			path, hasPath := value["path"].(string)
			line, hasLine := value["line"].(float64)
			if hasPath && hasLine {
				checked++
				note, _ := value["note"].(string)
				if note == "" {
					problems = append(problems, fmt.Sprintf("%s: anchor %s:%d carries no note naming what its line holds", where, path, int(line)))
				} else if problem, drifted := routescan.ResolveNote(read(path), path, note, int(line), rowFile(row), func() (registration, error) { return ownRegistration(row, path) }); problem != "" {
					problems = append(problems, where+": "+problem)
				} else if drifted {
					drift++
				}
			}
			for key, child := range value {
				walk(child, where+"/"+key, row)
			}
		case []any:
			for index, child := range value {
				walk(child, where+"["+strconv.Itoa(index)+"]", row)
			}
		}
	}
	for _, row := range rows {
		walk(row["primary_validator"], rowKey(t, row)+"/primary_validator", row)
		walk(row["reachable_validators"], rowKey(t, row)+"/reachable_validators", row)
	}
	return problems, checked, drift
}

// dispatchProbeValues give the wildcard values of the routes whose segment
// selects the handler; any other value is a 405 responder. The handler is
// called without a mux, so the values are set on the request.
var dispatchProbeValues = map[string]map[string]string{
	"POST /api/v1/admin/ip-allowlist/{entry_id}": {"entry_id": "check"},
	"POST /api/v1/admin/teams/{team_id}":         {"team_id": "import"},
	"PATCH /api/v1/auth/oauth/{first}/{second}":  {"first": "providers", "second": "x"},
}

// TestEveryGuardedRowRefusesAnAnonymousCaller is the behavioural half of the
// row/guard agreement: each route a row says is behind a policy guard
// (Admin, AdminOrg, Authenticated, Superuser) answers 401 to a request with no
// credential, through the real mounted handler. A row that claims a guard the
// route does not run fails here whatever the source scan says.
func TestEveryGuardedRowRefusesAnAnonymousCaller(t *testing.T) {
	byKey := map[string]map[string]any{}
	for _, row := range loadGoAPIRows(t) {
		byKey[rowKey(t, row)] = row
	}
	params := regexp.MustCompile(`\{([^}]*)\}`)
	checked := 0
	for _, route := range mountedRoutes(t) {
		row := byKey[route.key()]
		validator, _ := row["primary_validator"].(map[string]any)
		description, _ := validator["description"].(string)
		if row == nil || !strings.HasPrefix(description, "policy.") || strings.HasPrefix(description, "policy.Optional:") {
			continue
		}
		values := dispatchProbeValues[route.Method+" "+route.Pattern]
		target := params.ReplaceAllStringFunc(route.Pattern, func(match string) string {
			if value, ok := values[match[1:len(match)-1]]; ok {
				return value
			}
			return "x"
		})
		request := httptest.NewRequest(route.Method, target, strings.NewReader(`{}`))
		request.Header.Set("Content-Type", "application/json")
		for _, match := range params.FindAllStringSubmatch(route.Pattern, -1) {
			value := "x"
			if v, ok := values[match[1]]; ok {
				value = v
			}
			request.SetPathValue(match[1], value)
		}
		recorder := httptest.NewRecorder()
		route.Handler.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusUnauthorized {
			t.Errorf("%s: a row behind %s answered %d to an anonymous request, want 401: %s", route.key(), strings.SplitN(description, ":", 2)[0], recorder.Code, recorder.Body.String())
		}
		checked++
	}
	if checked < 150 {
		t.Fatalf("only %d guarded rows were probed", checked)
	}
}

// scopeValidators are the two middlewares the api listener wraps around every
// route (service.go configureWith): OrgScope and Impersonation authenticate any
// Bearer token a request carries, whatever the route's own guard is, so they
// are validators that can observe the credential on every api-listener row. The
// internal and billing-edge listeners install neither.
var scopeValidators = []string{"func (s *Scope) OrgScope", "func (s *Scope) Impersonation"}

func TestEveryAPIListenerRowNamesTheScopeMiddlewaresAsReachableValidators(t *testing.T) {
	checked := 0
	seenExempt := map[string]bool{}
	for _, row := range loadGoAPIRows(t) {
		key := rowKey(t, row)
		reachable, _ := row["reachable_validators"].([]any)
		if !strings.HasSuffix(key, "@api") {
			if len(reachable) != 0 {
				t.Errorf("%s: a listener without the scope middlewares lists %d reachable validators", key, len(reachable))
			}
			continue
		}
		if wantClass, exempt := signatureOnlyRoutes[strings.Replace(key, "@api", "", 1)]; exempt {
			if len(reachable) != 0 {
				t.Errorf("%s skips the scope middlewares but lists %d reachable validators", key, len(reachable))
			}
			classes, _ := row["accepted_credential_classes"].([]any)
			if len(classes) != 1 || classes[0] != wantClass {
				t.Errorf("%s: signature-only route accepts %v, want exactly [%s]", key, classes, wantClass)
			}
			if row["classification"] != "protected" {
				t.Errorf("%s: signature-only route is not classified protected", key)
			}
			seenExempt[key] = true
			continue
		}
		notes := map[string]bool{}
		for _, item := range reachable {
			entry := item.(map[string]any)
			anchor, _ := entry["anchor"].(map[string]any)
			note, _ := anchor["note"].(string)
			notes[note] = true
			if entry["reachable_but_not_owner"] != true || entry["is_intended_validator"] != false {
				t.Errorf("%s: reachable validator %q must be marked reachable_but_not_owner and not intended: %v", key, note, entry)
			}
		}
		for _, want := range scopeValidators {
			if !notes[want] {
				t.Errorf("%s: no reachable validator anchored at %q", key, want)
			}
		}
		checked++
	}
	for route := range signatureOnlyRoutes {
		if !seenExempt[route+"@api"] {
			t.Errorf("signature-only route %q has no api-listener row", route)
		}
	}
	if checked < 200 {
		t.Fatalf("only %d api-listener rows were checked", checked)
	}
}

// TestTheScopeMiddlewaresAreWrappedAroundTheAPIListener pins the fact the
// reachable validators state: the api listener is built with OrgScope and
// Impersonation, the internal and billing-edge listeners with neither.
func TestTheScopeMiddlewaresAreWrappedAroundTheAPIListener(t *testing.T) {
	raw, err := os.ReadFile("service.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	file, err := parser.ParseFile(token.NewFileSet(), "service.go", raw, parser.SkipObjectResolution)
	if err != nil {
		t.Fatal(err)
	}
	var lists [][]string
	ast.Inspect(file, func(node ast.Node) bool {
		if lit, ok := node.(*ast.CompositeLit); ok {
			var names []string
			for _, elt := range lit.Elts {
				if sel, ok := elt.(*ast.SelectorExpr); ok {
					names = append(names, sel.Sel.Name)
				}
			}
			if slices.Contains(names, "OrgScope") {
				lists = append(lists, names)
			}
		}
		return true
	})
	if len(lists) != 1 || !slices.Equal(lists[0], []string{"ReadOnce", "Principal", "OrgScope", "Impersonation"}) {
		t.Fatalf("api listener scope lists %v, want one [ReadOnce Principal OrgScope Impersonation]: the rows' reachable validators are stale", lists)
	}
	if !strings.Contains(text, "newServer(cfg, logger, Routes(deps, logger), gate, deps.AuthProxy.ScopeExemptRoutes(), scope...)") {
		t.Fatal("the api listener is no longer built with the scope middlewares")
	}
	for _, listener := range []string{"func NewInternalServer", "func NewEdgeServer"} {
		start := strings.Index(text, listener)
		if start < 0 {
			t.Fatalf("%s not found", listener)
		}
		end := strings.Index(text[start:], "\n}\n")
		body := text[start : start+end]
		if strings.Contains(body, "OrgScope") || strings.Contains(body, "Impersonation") || strings.Contains(body, "scope") {
			t.Errorf("%s now takes scope middleware: the rows' reachable validators for its listener are stale", listener)
		}
	}
}

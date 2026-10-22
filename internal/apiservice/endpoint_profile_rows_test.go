package apiservice

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
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

// handlerValidatedAccessToken are the routes with no bearer guard whose
// handler itself verifies a legacy access token; they are the only unguarded
// routes whose row may accept the ops access token.
var handlerValidatedAccessToken = map[string]bool{
	"POST /api/v1/auth/validate@api": true,
	"POST /api/v1/auth/logout@api":   true,
}

// TestEndpointProfileRowsSitAtTheirRegistrationAndNameTheirGuard: each row's
// source line is the line that registers its route, and the row agrees with the
// guard level that registration names. A route behind policy.Admin,
// AdminOrg, Authenticated, Superuser or Optional accepts the ops access token
// and states its HS256 opt-in; a route with no policy guard never lists that
// class, so the legacy token is impossible there.
func TestEndpointProfileRowsSitAtTheirRegistrationAndNameTheirGuard(t *testing.T) {
	regs := scanRegistrations(t, "../api", ".")
	byKey := map[string]map[string]any{}
	for _, row := range loadGoAPIRows(t) {
		byKey[rowKey(t, row)] = row
	}
	guarded := map[string]bool{"Admin": true, "AdminOrg": true, "Authenticated": true, "Superuser": true, "Optional": true}
	checked := 0
	for _, route := range mountedRoutes(t) {
		row := byKey[route.key()]
		if row == nil {
			continue
		}
		reg, ok := registrationFor(regs, route)
		if !ok {
			t.Errorf("%s: no registration found in the source", route.key())
			continue
		}
		source := row["source"].(map[string]any)
		if got := source["file"].(string); got != treePath(reg.File) {
			t.Errorf("%s: row source file %s, the registration is in %s", route.key(), got, treePath(reg.File))
		}
		if got := int(source["line"].(float64)); got != reg.Line {
			t.Errorf("%s: row source line %d, the registration is on line %d", route.key(), got, reg.Line)
		}
		classes, _ := row["accepted_credential_classes"].([]any)
		hs256 := false
		for _, class := range classes {
			hs256 = hs256 || class == "ops_access_token_hs256"
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
			if row["classification"] != "protected" || !hs256 || validation["legacy_hs256"] != true || !strings.HasPrefix(description, "policy."+reg.Level+":") {
				t.Errorf("%s: registered at policy.%s, row says %v %v %v %q", route.key(), reg.Level, row["classification"], classes, validation, description)
			}
		case hs256 && !(handlerValidatedAccessToken[route.key()] && validation["legacy_hs256"] == true):
			t.Errorf("%s: no policy guard on its registration (%q) but the row accepts the ops access token", route.key(), reg.Level)
		}
		checked++
	}
	if checked < 200 {
		t.Fatalf("only %d rows were checked against their registrations", checked)
	}
}

// TestEndpointProfileAnchorsStillPointAtTheirSymbols: every anchor carries
// the text its line must still hold, so a pinned file:line cannot drift.
func TestEndpointProfileAnchorsStillPointAtTheirSymbols(t *testing.T) {
	lineOf := func(path string, line int) string {
		raw, err := os.ReadFile(filepath.Join("../..", path))
		if err != nil {
			t.Fatalf("anchor file %s: %v", path, err)
		}
		lines := strings.Split(string(raw), "\n")
		if line < 1 || line > len(lines) {
			t.Fatalf("anchor %s:%d is outside the file (%d lines)", path, line, len(lines))
		}
		return lines[line-1]
	}
	checked := 0
	var walk func(node any, where string)
	walk = func(node any, where string) {
		switch value := node.(type) {
		case map[string]any:
			path, hasPath := value["path"].(string)
			line, hasLine := value["line"].(float64)
			if hasPath && hasLine {
				note, _ := value["note"].(string)
				if note == "" {
					t.Errorf("%s: anchor %s:%d carries no note naming what its line holds", where, path, int(line))
				} else if !strings.Contains(lineOf(path, int(line)), note) {
					t.Errorf("%s: %s:%d no longer holds %q (it holds %q)", where, path, int(line), note, strings.TrimSpace(lineOf(path, int(line))))
				}
				checked++
			}
			for key, child := range value {
				walk(child, where+"/"+key)
			}
		case []any:
			for index, child := range value {
				walk(child, where+"["+strconv.Itoa(index)+"]")
			}
		}
	}
	for _, row := range loadGoAPIRows(t) {
		walk(row["primary_validator"], rowKey(t, row)+"/primary_validator")
		walk(row["reachable_validators"], rowKey(t, row)+"/reachable_validators")
	}
	if checked < 200 {
		t.Fatalf("only %d anchors were checked: the walk stopped seeing them", checked)
	}
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
	if !strings.Contains(text, "protected.scope.OrgScope, protected.scope.Impersonation}") {
		t.Fatal("the api listener no longer installs OrgScope and Impersonation: the rows' reachable validators are stale")
	}
	if !strings.Contains(text, "NewServer(cfg, logger, Routes(deps, logger), scope...)") {
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

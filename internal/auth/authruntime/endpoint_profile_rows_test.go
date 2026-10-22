package authruntime

import (
	"context"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

const authServiceRows = "../../../contracts/auth/v1/endpoint-profiles.auth-service.json"

// probeMethods are the methods each registered path is probed with.
var probeMethods = []string{
	http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut,
	http.MethodPatch, http.MethodDelete, http.MethodOptions,
}

// servedAuthRoutes returns every "METHOD path@listener" auth-service SERVES:
// each path registered on the API route table (sessionRoutes, with the session
// secrets, the compatibility switch and the lockout key set) and on the operator mux (internal/platform/health) is probed
// with every method in probeMethods through the handlers configure builds, and
// every answer other than 404 or 405 is a served route. The database is
// unreachable, so no probe reaches a store.
func servedAuthRoutes(t *testing.T) map[string]bool {
	t.Helper()
	cfg, err := authconfig.Load(authconfig.Spec{LookupEnv: lookupFrom(compatEnvironment(map[string]string{
		authconfig.EnvDatabaseURI:        "postgres://auth@127.0.0.1:1/devhealth",
		authconfig.EnvSigningKeyFile:     "/nonexistent",
		authconfig.EnvSigningKeyID:       "rows-test",
		authconfig.EnvRateLimitPerSecond: "100000",
		authconfig.EnvRateLimitBurst:     "100000",
		authconfig.EnvAPIAddress:         "127.0.0.1:0",
		authconfig.EnvOperatorAddress:    "127.0.0.1:1",
	}))})
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	components, err := configure(context.Background(), cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("configure: %v", err)
	}
	t.Cleanup(func() {
		for _, component := range components {
			_ = component.Shutdown(context.Background())
		}
	})
	handlers := map[string]http.Handler{}
	for _, component := range components {
		announced, ok := component.(announcedListener)
		if !ok {
			continue
		}
		switch server := announced.listener.(type) {
		case *httpapi.Server:
			handlers["api"] = server.Handler()
		case *health.Server:
			handlers["operator"] = server.Handler()
		}
	}
	if handlers["api"] == nil || handlers["operator"] == nil {
		t.Fatalf("configure built listeners %v; want api and operator", handlers)
	}

	registered := map[string][]string{}
	pool, err := pgxpool.New(context.Background(), "postgres://auth@127.0.0.1:1/devhealth")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	routes, err := sessionRoutes(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), pool, schema)
	if err != nil {
		t.Fatalf("sessionRoutes: %v", err)
	}
	for _, route := range routes {
		registered["api"] = append(registered["api"], route.Pattern)
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "../../platform/health/server.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || len(call.Args) == 0 {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || (selector.Sel.Name != "HandleFunc" && selector.Sel.Name != "Handle") {
			return true
		}
		if receiver, ok := selector.X.(*ast.Ident); !ok || receiver.Name != "mux" {
			return true
		}
		if literal, ok := call.Args[0].(*ast.BasicLit); ok && literal.Kind == token.STRING {
			pattern, _ := strconv.Unquote(literal.Value)
			if _, path, found := strings.Cut(pattern, " "); found {
				pattern = path
			}
			registered["operator"] = append(registered["operator"], pattern)
		}
		return true
	})

	served := map[string]bool{}
	for listener, paths := range registered {
		for _, path := range paths {
			for _, method := range probeMethods {
				recorder := httptest.NewRecorder()
				handlers[listener].ServeHTTP(recorder, httptest.NewRequest(method, path, strings.NewReader("{}")))
				switch recorder.Code {
				case http.StatusNotFound, http.StatusMethodNotAllowed:
				case http.StatusTooManyRequests:
					t.Fatalf("%s %s@%s answered 429: the probe hit the rate limit and measured nothing", method, path, listener)
				default:
					served[method+" "+path+"@"+listener] = true
				}
			}
		}
	}
	return served
}

var authRowID = regexp.MustCompile(`^([A-Z]+) (\S+) \[dev-health-auth-service @([a-z,]+)\]$`)

func loadAuthServiceRows(t *testing.T) []map[string]any {
	t.Helper()
	raw, err := os.ReadFile(authServiceRows)
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

// TestEveryServedAuthRouteHasAnEndpointProfileRowAndNoRowIsStale: a route
// auth-service serves with no row fails here, and so does a row for a route
// nothing serves, per method and listener.
func TestEveryServedAuthRouteHasAnEndpointProfileRowAndNoRowIsStale(t *testing.T) {
	served := servedAuthRoutes(t)
	missing, stale := compareServedToRows(t, served, loadAuthServiceRows(t))
	if len(missing) > 0 {
		t.Errorf("served with no endpoint-profile row (contracts/auth/v1/endpoint-profiles.auth-service.json): %v", missing)
	}
	if len(stale) > 0 {
		t.Errorf("endpoint-profile rows for routes nothing serves: %v", stale)
	}
	if len(served) != servedAuthRouteCount {
		t.Fatalf("%d served routes found, want %d: a mount was dropped or added without a row", len(served), servedAuthRouteCount)
	}
}

// servedAuthRouteCount is every route the probe serves: four session routes, two social link routes,
// four compatibility routes, the session issue route and the three operator
// paths on GET and HEAD.
const servedAuthRouteCount = 17

// compareServedToRows returns the served routes with no row and the rows for
// routes nothing serves, per method and listener.
func compareServedToRows(t *testing.T, served map[string]bool, rowDocs []map[string]any) (missing, stale []string) {
	t.Helper()
	rows := map[string]bool{}
	for _, row := range rowDocs {
		id, _ := row["id"].(string)
		match := authRowID.FindStringSubmatch(id)
		if match == nil {
			t.Fatalf("row id %q is not \"METHOD route [dev-health-auth-service @listener]\"", id)
		}
		if match[1] != row["method"] || match[2] != row["route"] {
			t.Errorf("row id %q disagrees with its method %v and route %v", id, row["method"], row["route"])
		}
		for _, listener := range strings.Split(match[3], ",") {
			rows[match[1]+" "+match[2]+"@"+listener] = true
		}
	}
	for key := range served {
		if !rows[key] {
			missing = append(missing, key)
		}
	}
	for key := range rows {
		if !served[key] {
			stale = append(stale, key)
		}
	}
	sort.Strings(missing)
	sort.Strings(stale)
	return missing, stale
}

var compatServedRoutes = []string{
	"POST /api/v1/auth/login@api", "POST /api/v1/auth/refresh@api", "POST /api/v1/auth/logout@api",
	"POST /api/v1/auth/social-login@api", "POST /internal/v1/sessions@api",
}

// TestTheRowGateRefusesAPlantedMissingRowForEachCompatibilityRoute deletes each
// compatibility row from the real rows file in memory and runs the comparison
// the gate runs: it must name exactly that route.
func TestTheRowGateRefusesAPlantedMissingRowForEachCompatibilityRoute(t *testing.T) {
	served := servedAuthRoutes(t)
	for _, key := range compatServedRoutes {
		if !served[key] {
			t.Fatalf("probe does not serve %s: the compatibility mounts are missing from the probe", key)
		}
		var kept []map[string]any
		for _, row := range loadAuthServiceRows(t) {
			id, _ := row["id"].(string)
			match := authRowID.FindStringSubmatch(id)
			if match != nil && match[1]+" "+match[2]+"@"+match[3] == key {
				continue
			}
			kept = append(kept, row)
		}
		missing, stale := compareServedToRows(t, served, kept)
		if len(missing) != 1 || missing[0] != key || len(stale) != 0 {
			t.Errorf("rows without %s: missing %v stale %v, want exactly it missing", key, missing, stale)
		}
	}
}

// TestTheRowGateRefusesARowForARouteNothingServes is the stale-row control: a
// row for an unmounted route is reported.
func TestTheRowGateRefusesARowForARouteNothingServes(t *testing.T) {
	served := servedAuthRoutes(t)
	rows := loadAuthServiceRows(t)
	ghost := map[string]any{"id": "POST /api/v1/auth/unmounted [dev-health-auth-service @api]", "method": "POST", "route": "/api/v1/auth/unmounted"}
	_, stale := compareServedToRows(t, served, append(rows, ghost))
	if len(stale) != 1 || stale[0] != "POST /api/v1/auth/unmounted@api" {
		t.Errorf("stale = %v, want the unmounted route", stale)
	}
}

// TestAuthEndpointProfileAnchorsStillPointAtTheirSymbols: a row's source line
// still registers its route, and every anchor with a note still holds the
// symbol the note names, so a pinned file:line cannot drift silently.
func TestAuthEndpointProfileAnchorsStillPointAtTheirSymbols(t *testing.T) {
	lineOf := func(path string, line int) string {
		raw, err := os.ReadFile(filepath.Join("../../..", path))
		if err != nil {
			t.Fatalf("anchor file %s: %v", path, err)
		}
		lines := strings.Split(string(raw), "\n")
		if line < 1 || line > len(lines) {
			t.Fatalf("anchor %s:%d is outside the file (%d lines)", path, line, len(lines))
		}
		return lines[line-1]
	}
	constants := map[string]string{
		"/v1/token/refresh": "PatternRefresh", "/v1/sessions/current": "PatternCurrent",
		"/v1/sessions/revoke": "PatternRevoke", "/v1/sessions/revoke-all": "PatternRevokeAll",
		"/api/v1/auth/login": "PatternCompatLogin", "/api/v1/auth/refresh": "PatternCompatRefresh",
		"/api/v1/auth/logout": "PatternCompatLogout", "/api/v1/auth/social-login": "PatternCompatSocialLogin",
		"/internal/v1/sessions": "PatternIssueSession",
		"/v1/identities/social": "PatternSocialLink", "/v1/identities/social/{id}": "PatternSocialUnlink",
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
					t.Errorf("%s: anchor %s:%d carries no note naming the symbol its line holds", where, path, int(line))
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
	for _, row := range loadAuthServiceRows(t) {
		id, _ := row["id"].(string)
		route, _ := row["route"].(string)
		source := row["source"].(map[string]any)
		text := lineOf(source["file"].(string), int(source["line"].(float64)))
		if !strings.Contains(text, `"`+route+`"`) && (constants[route] == "" || !strings.Contains(text, "Pattern: "+constants[route]+",")) {
			t.Errorf("row %q: %v:%v holds no registration of %s: %q", id, source["file"], source["line"], route, strings.TrimSpace(text))
		}
		checked++
		walk(row["primary_validator"], id+"/primary_validator")
		walk(row["issued_credential"], id+"/issued_credential")
	}
	if checked < 10 {
		t.Fatalf("only %d anchors were checked: the walk stopped seeing them", checked)
	}
}

// TestClassConsumersNameEveryAuthRouteThatAcceptsThem: every auth-service row
// that lists a class in accepted_credential_classes has its route named in that
// class's consumers in credential-classes.json.
func TestClassConsumersNameEveryAuthRouteThatAcceptsThem(t *testing.T) {
	raw, err := os.ReadFile("../../../contracts/auth/v1/credential-classes.json")
	if err != nil {
		t.Fatal(err)
	}
	var registry struct {
		Classes []struct {
			ClassID   string `json:"class_id"`
			Consumers []struct {
				Description string `json:"description"`
			} `json:"consumers"`
		} `json:"classes"`
	}
	if err := json.Unmarshal(raw, &registry); err != nil {
		t.Fatal(err)
	}
	described := map[string]string{}
	for _, class := range registry.Classes {
		described[class.ClassID] += " "
		for _, consumer := range class.Consumers {
			described[class.ClassID] += consumer.Description + " "
		}
	}
	checked := 0
	for _, row := range loadAuthServiceRows(t) {
		route, _ := row["route"].(string)
		classes, _ := row["accepted_credential_classes"].([]any)
		for _, item := range classes {
			class, _ := item.(string)
			text, registered := described[class]
			if !registered {
				t.Errorf("route %s lists class %q, which the registry does not define", route, class)
				continue
			}
			checked++
			if !strings.Contains(text, " "+route+" ") && !strings.Contains(text, " "+route+",") && !strings.Contains(text, " "+route+")") {
				t.Errorf("route %s accepts %s but that class's consumers do not name it: %s", route, class, strings.TrimSpace(text))
			}
		}
	}
	if checked == 0 {
		t.Fatal("no (route, class) pair was checked: the test measured nothing")
	}
}

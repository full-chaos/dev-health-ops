package authruntime

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"fmt"
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

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/workloadexchange"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/routescan"
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
// wellKnownIssuer is the issuer the probe configures: with one set, the discovery document is mounted beside the JWKS.
const wellKnownIssuer = "https://issuer.example.test"

func servedAuthRoutes(t *testing.T) map[string]bool {
	return servedAuthRoutesWithIssuer(t, wellKnownIssuer)
}

// servedAuthRoutesWithIssuer is servedAuthRoutes with AUTH_TOKEN_ISSUER_URL set to issuer ("" leaves it unset).
func servedAuthRoutesWithIssuer(t *testing.T, issuer string) map[string]bool {
	t.Helper()
	env := map[string]string{
		authconfig.EnvDatabaseURI:        "postgres://auth@127.0.0.1:1/devhealth",
		authconfig.EnvSigningKeyFile:     "/nonexistent",
		authconfig.EnvSigningKeyID:       "rows-test",
		authconfig.EnvRateLimitPerSecond: "100000",
		authconfig.EnvRateLimitBurst:     "100000",
		authconfig.EnvAPIAddress:         "127.0.0.1:0",
		authconfig.EnvOperatorAddress:    "127.0.0.1:1",
		authconfig.EnvSocialLinkRoutes:   "true",
		authconfig.EnvUsersOwned:         "true",
	}
	if issuer != "" {
		env[authconfig.EnvTokenIssuerURL] = issuer
		// The workload-token exchange needs the token issuer: the probe turns it
		// on whenever an issuer is set, and the go-api workload subject, which
		// mounts the session issue routes, with it.
		exchangeEnvironment(t, env)
		workloadIssueEnvironment(env)
	}
	cfg, err := authconfig.Load(authconfig.Spec{LookupEnv: lookupFrom(provisioningEnvironment(ssoEnvironment(compatEnvironment(env))))})
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
	signer := keySigner(t)
	routes, err := sessionRoutes(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), pool, schema, signer, mustLimits(t), nil)
	if err != nil {
		t.Fatalf("sessionRoutes: %v", err)
	}
	credentials, _, err := credentialRoutes(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), pool, schema, mustLimits(t), signer)
	if err != nil {
		t.Fatalf("credentialRoutes: %v", err)
	}
	for _, route := range append(routes, credentials...) {
		registered["api"] = append(registered["api"], route.Pattern)
	}
	if cfg.WorkloadExchange.Mounted() {
		registered["api"] = append(registered["api"], workloadexchange.Pattern)
	}
	registered["api"] = append(registered["api"], authapi.PatternAuthorize, authapi.PatternAuthorizeBatch)
	// The OAuth authorization server's routes (O3b) mount only with the issuer, the key set and every endpoint-profile row.
	registered["api"] = append(registered["api"], authapi.PatternOAuthAuthorize, authapi.PatternOAuthMetadata,
		authapi.PatternOAuthDiscovery, authapi.PatternOAuthJWKS)
	seenWellKnown := map[string]bool{}
	for _, route := range authapi.WellKnownRoutes(authapi.WellKnownDeps{IssuerURL: issuer}) {
		if !seenWellKnown[route.Pattern] {
			seenWellKnown[route.Pattern] = true
			registered["api"] = append(registered["api"], route.Pattern)
		}
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
	rows := loadAuthServiceRows(t)
	missing, stale := compareServedToRows(t, served, rows)
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

func TestAPlantedMissingRowForARegisteredRouteIsNamedAlone(t *testing.T) {
	served := servedAuthRoutes(t)
	for _, other := range []string{"POST /api/v1/auth/login@api", "POST /api/v1/auth/register@api"} {
		plantedMissingRow(t, served, other)
	}
}

func plantedMissingRow(t *testing.T, served map[string]bool, other string) {
	t.Helper()
	var kept []map[string]any
	for _, row := range loadAuthServiceRows(t) {
		id, _ := row["id"].(string)
		if match := authRowID.FindStringSubmatch(id); match != nil && match[1]+" "+match[2]+"@"+match[3] == other {
			continue
		}
		kept = append(kept, row)
	}
	missing, _ := compareServedToRows(t, served, kept)
	if len(missing) != 1 || missing[0] != other {
		t.Fatalf("missing = %v, want exactly %s", missing, other)
	}
}

// TestTheRowGateRefusesAPlantedMissingRowForEveryServedRoute takes every route
// from the probed route table, removes its row in memory and requires the
// comparison to name exactly that route: no other route missing, no row stale.
func TestTheRowGateRefusesAPlantedMissingRowForEveryServedRoute(t *testing.T) {
	served := servedAuthRoutes(t)
	if len(served) != servedAuthRouteCount {
		t.Fatalf("%d served routes found, want %d: a mount was dropped or added without a row", len(served), servedAuthRouteCount)
	}
	keys := make([]string, 0, len(served))
	for key := range served {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	rows := loadAuthServiceRows(t)
	for _, key := range keys {
		var kept []map[string]any
		for _, row := range rows {
			id, _ := row["id"].(string)
			if match := authRowID.FindStringSubmatch(id); match != nil && match[1]+" "+match[2]+"@"+match[3] == key {
				continue
			}
			kept = append(kept, row)
		}
		if len(kept) != len(rows)-1 {
			t.Errorf("%s: %d rows removed, want exactly one: no row, or more than one, carries this route", key, len(rows)-len(kept))
			continue
		}
		missing, stale := compareServedToRows(t, served, kept)
		if len(missing) != 1 || missing[0] != key || len(stale) != 0 {
			t.Errorf("rows without %s: missing %v stale %v, want exactly it missing", key, missing, stale)
		}
	}
}

// servedAuthRouteCount is every route the probe serves: four session routes, two social link routes,
// four compatibility routes, the validate route, the switch-org route, the session issue route, the session state route, the two delegated session routes, the eight federated sign-in routes, the six credentials routes,
// the twenty-five provisioning routes, the eight read routes, the four user-admin routes, the two authorize routes and the three operator paths on GET and HEAD, GET and
// HEAD of the JWKS and the discovery document, and the five OAuth authorization server routes.
const servedAuthRouteCount = 85

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

// TestTheRowGateRefusesAPlantedMissingRowForEachProvisioningRoute: each
// provisioning route is served by the probe, and its row removed in memory is
// named alone.
func TestTheRowGateRefusesAPlantedMissingRowForEachProvisioningRoute(t *testing.T) {
	served := servedAuthRoutes(t)
	for _, key := range provisioningServedRoutes {
		if !served[key] {
			t.Fatalf("probe does not serve %s: the provisioning mounts are missing from the probe", key)
		}
		plantedMissingRow(t, served, key)
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

// TestAuthEndpointProfileAnchorsStillPointAtTheirSymbols: a row's source names
// its route by symbol (routescan.RowSymbol: the registration of its method and
// route, the registration of its pattern constant, or that constant's spec),
// and every anchor's note resolves to one place (routescan.ResolveNote). The
// cited lines are citations: a drift count is logged, never compared.
func TestAuthEndpointProfileAnchorsStillPointAtTheirSymbols(t *testing.T) {
	problems, checked, drift := authRowAnchorProblems(t, "../../..", loadAuthServiceRows(t))
	for _, problem := range problems {
		t.Error(problem)
	}
	if checked < 10 {
		t.Fatalf("only %d anchors were checked: the walk stopped seeing them", checked)
	}
	t.Logf("citation note: %d of %d auth-service sources and anchors cite a line other than the one they resolve to", drift, checked)
}

// authRowAnchorProblems resolves the auth-service rows in the source under root, by symbol. drift counts the cited
// lines that are not the resolved ones.
func authRowAnchorProblems(t *testing.T, root string, rows []map[string]any) (problems []string, checked, drift int) {
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
	scanned := map[string][]routescan.Registration{}
	trim := func(err error) string { return strings.ReplaceAll(err.Error(), filepath.ToSlash(root)+"/", "") }
	for _, row := range rows {
		id, _ := row["id"].(string)
		method, _ := row["method"].(string)
		route, _ := row["route"].(string)
		source := row["source"].(map[string]any)
		file, line := source["file"].(string), int(source["line"].(float64))
		dir := filepath.Join(root, filepath.Dir(file))
		if _, ok := scanned[dir]; !ok {
			regs, err := routescan.ScanWithMux(dir)
			if err != nil {
				t.Fatal(err)
			}
			scanned[dir] = regs
		}
		at, err := routescan.RowSymbol(scanned[dir], read(file), filepath.ToSlash(filepath.Join(root, file)), method, route, authPatternConstants[route])
		checked++
		if err != nil {
			problems = append(problems, fmt.Sprintf("row %q: %s", id, trim(err)))
		} else if at != line {
			drift++
		}
		own := func() (routescan.Registration, error) {
			if err != nil {
				return routescan.Registration{}, err
			}
			return routescan.Registration{File: file, Line: at}, nil
		}
		var walk func(node any, where string)
		walk = func(node any, where string) {
			switch value := node.(type) {
			case map[string]any:
				path, hasPath := value["path"].(string)
				anchorLine, hasLine := value["line"].(float64)
				if hasPath && hasLine {
					checked++
					note, _ := value["note"].(string)
					if note == "" {
						problems = append(problems, fmt.Sprintf("%s: anchor %s:%d carries no note naming the symbol its line holds", where, path, int(anchorLine)))
					} else if problem, drifted := routescan.ResolveNote(read(path), path, note, int(anchorLine), file, own); problem != "" {
						problems = append(problems, where+": "+problem)
					} else if drifted {
						drift++
					}
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
		walk(row["primary_validator"], id+"/primary_validator")
		walk(row["issued_credential"], id+"/issued_credential")
	}
	return problems, checked, drift
}

// authPatternConstants are the pattern constants the auth-service registrations name, by route: a constant from another
// package (PatternJWKS) resolves only through its name.
var authPatternConstants = map[string]string{
	"/v1/token/refresh": "PatternRefresh", "/v1/sessions/current": "PatternCurrent",
	"/v1/sessions/revoke": "PatternRevoke", "/v1/sessions/revoke-all": "PatternRevokeAll",
	"/api/v1/auth/login": "PatternCompatLogin", "/api/v1/auth/refresh": "PatternCompatRefresh",
	"/api/v1/auth/logout": "PatternCompatLogout", "/api/v1/auth/social-login": "PatternCompatSocialLogin",
	"/internal/v1/sessions":       "PatternIssueSession",
	"/internal/v1/token/exchange": "Pattern",
	"/.well-known/jwks.json":      "PatternJWKS", "/.well-known/openid-configuration": "PatternDiscovery",
	"/v1/identities/social": "PatternSocialLink", "/v1/identities/social/{id}": "PatternSocialUnlink",
	"/api/v1/auth/verify": "PatternVerifyEmail", "/api/v1/auth/resend-verification": "PatternResendVerification",
	"/api/v1/auth/forgot-password": "PatternForgotPassword", "/api/v1/auth/reset-password": "PatternResetPassword",
	"/api/v1/auth/change-password": "PatternChangePassword", "/api/v1/auth/register": "PatternRegister",
	"/internal/v1/memberships/add": "PatternMembershipAdd", "/internal/v1/memberships/role": "PatternMembershipRole",
	"/internal/v1/memberships/remove": "PatternMembershipRemove", "/internal/v1/memberships/transfer": "PatternMembershipTransfer",
	"/internal/v1/invites/create": "PatternInviteCreate", "/internal/v1/invites/accept": "PatternInviteAccept",
	"/internal/v1/organizations/deprovision":        "PatternOrgDeprovision",
	"/internal/v1/organizations/reactivate":         "PatternOrgReactivate",
	"/internal/v1/invites/revoke":                   "PatternInviteRevoke",
	"/internal/v1/organizations/suspend":            "PatternOrgSuspend",
	"/internal/v1/organizations/state":              "PatternOrgState",
	"/internal/v1/organizations/onboard":            "PatternOrgOnboard",
	"/internal/v1/organizations/create":             "PatternOrgCreate",
	"/oauth/authorize":                              "PatternOAuthAuthorize",
	"/.well-known/oauth-authorization-server/oauth": "PatternOAuthMetadata",
	"/oauth/.well-known/openid-configuration":       "PatternOAuthDiscovery",
	"/oauth/.well-known/jwks.json":                  "PatternOAuthJWKS",
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

// TestRowsThatAcceptNoCredentialListNoClass: a row whose rationale says the
// route accepts no credential lists no accepted class, and the other way
// round, so a pre-session start route cannot claim a credential it never reads.
func TestRowsThatAcceptNoCredentialListNoClass(t *testing.T) {
	checked := 0
	for _, row := range loadAuthServiceRows(t) {
		why, _ := row["public_rationale"].(string)
		classes, _ := row["accepted_credential_classes"].([]any)
		if strings.Contains(why, "accepts no credential") {
			checked++
			if len(classes) != 0 {
				t.Errorf("row %v says it accepts no credential but lists %v", row["id"], classes)
			}
		}
	}
	if checked == 0 {
		t.Fatal("no row says it accepts no credential: the test measured nothing")
	}
}

var wellKnownServedRoutes = []string{
	"GET /.well-known/jwks.json@api", "HEAD /.well-known/jwks.json@api",
	"GET /.well-known/openid-configuration@api", "HEAD /.well-known/openid-configuration@api",
}

// TestTheRowGateRefusesAPlantedMissingRowForEachWellKnownRoute deletes each
// well-known row from the real rows file in memory and runs the comparison the
// gate runs: it must name exactly that route.
func TestTheRowGateRefusesAPlantedMissingRowForEachWellKnownRoute(t *testing.T) {
	served := servedAuthRoutes(t)
	for _, key := range wellKnownServedRoutes {
		if !served[key] {
			t.Fatalf("probe does not serve %s: the well-known mounts are missing from the probe", key)
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

// TestAWellKnownRowWithNoRouteIsStale is the stale-row control for the new rows.
func TestAWellKnownRowWithNoRouteIsStale(t *testing.T) {
	served := servedAuthRoutes(t)
	ghost := map[string]any{"id": "GET /.well-known/unmounted [dev-health-auth-service @api]", "method": "GET", "route": "/.well-known/unmounted"}
	_, stale := compareServedToRows(t, served, append(loadAuthServiceRows(t), ghost))
	if len(stale) != 1 || stale[0] != "GET /.well-known/unmounted@api" {
		t.Errorf("stale = %v, want the unmounted route", stale)
	}
}

// exchangeEnvironment turns the workload-token exchange on for the probe, with
// both subjects: a throwaway CA bundle and reviewer token file, so configure
// builds the route. The probe sends no valid request, so nothing reaches
// TokenReview or the bootstrap registry.
func exchangeEnvironment(t *testing.T, env map[string]string) {
	t.Helper()
	tls := httptest.NewTLSServer(http.NotFoundHandler())
	certificate := tls.Certificate().Raw
	tls.Close()
	dir := t.TempDir()
	ca := filepath.Join(dir, "ca.crt")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate}), 0o600); err != nil {
		t.Fatal(err)
	}
	reviewer := filepath.Join(dir, "token")
	if err := os.WriteFile(reviewer, []byte("probe-reviewer"), 0o600); err != nil {
		t.Fatal(err)
	}
	env[authconfig.EnvWorkloadExchange] = "true"
	env[authconfig.EnvWorkloadBootstrap] = "true"
	env[authconfig.EnvWorkloadTrustDomain] = "cluster.example.test"
	env[authconfig.EnvWorkloadClusterIssuer] = "https://kubernetes.default.svc.cluster.local"
	env[authconfig.EnvWorkloadKubeAPIURL] = "https://kubernetes.default.svc"
	env[authconfig.EnvWorkloadKubeCAFile] = ca
	env[authconfig.EnvWorkloadReviewerTokenFile] = reviewer
}

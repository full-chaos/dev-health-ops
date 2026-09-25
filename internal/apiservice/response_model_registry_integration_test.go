//go:build integration

package apiservice

import (
	"encoding/json"
	"github.com/full-chaos/dev-health-ops/internal/api/session"
	"github.com/full-chaos/dev-health-ops/internal/api/sso"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/jackc/pgx/v5/pgxpool"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/billing"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/teamsidentity"
	"github.com/full-chaos/dev-health-ops/internal/apiservice/admin"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// pythonRouteTableProgram prints the live FastAPI app's route table: for
// each APIRoute method and path, whether FastAPI writes its success body
// through the response_model fast path (fastapi/routing.py use_dump_json:
// a response field and the default response class).
const pythonRouteTableProgram = `
import json
from fastapi.routing import APIRoute
from fastapi.datastructures import DefaultPlaceholder
from dev_health_ops.api.main import app
out = []
for route in app.routes:
    if not isinstance(route, APIRoute):
        continue
    model = route.response_field is not None and isinstance(route.response_class, DefaultPlaceholder)
    for method in sorted(route.methods):
        out.append([method, route.path, model])
print("ROUTES " + json.dumps(out))
`

// goRoutesWithoutPython are Go route patterns with no FastAPI route of
// their own, by ruling or because they only dispatch literal Python
// routes (which responseModelRoutes names) and answer 405 otherwise.
var goRoutesWithoutPython = map[string]string{
	"POST /api/v1/admin/orgs/{org_id}/transfer-ownership": "Go serves the web's shape; the ruled intentional divergence",
	"POST /api/v1/admin/teams/{team_id}":                  "dispatches POST /api/v1/admin/teams/import; any other id is 405",
	"POST /api/v1/admin/ip-allowlist/{entry_id}":          "dispatches POST /api/v1/admin/ip-allowlist/check; any other id is 405",
	"GET /buildinfo":                              "Go-only build stamp; the Python api has no such route",
	"POST /api/v1/billing/plans/{plan_id}":        "answers 405 for every id; POST /api/v1/billing/plans/pull-stripe is its own pattern",
	"HEAD /api/v1/billing/plans/pull-stripe":      "answers the 405 FastAPI gives a HEAD there (Allow: POST); without it the GET /plans/{plan_id} route's 405 would answer",
	"GET /api/v1/auth/oauth/{first}/{second}":     "dispatches the four overlapping /oauth routes (PATCH providers/{id}, POST {id}/authorize, POST {id}/callback, GET {type}/authorize) as Starlette does",
	"HEAD /api/v1/auth/oauth/{first}/{second}":    "dispatches the four overlapping /oauth routes (PATCH providers/{id}, POST {id}/authorize, POST {id}/callback, GET {type}/authorize) as Starlette does",
	"POST /api/v1/auth/oauth/{first}/{second}":    "dispatches the four overlapping /oauth routes (PATCH providers/{id}, POST {id}/authorize, POST {id}/callback, GET {type}/authorize) as Starlette does",
	"PUT /api/v1/auth/oauth/{first}/{second}":     "dispatches the four overlapping /oauth routes (PATCH providers/{id}, POST {id}/authorize, POST {id}/callback, GET {type}/authorize) as Starlette does",
	"PATCH /api/v1/auth/oauth/{first}/{second}":   "dispatches the four overlapping /oauth routes (PATCH providers/{id}, POST {id}/authorize, POST {id}/callback, GET {type}/authorize) as Starlette does",
	"DELETE /api/v1/auth/oauth/{first}/{second}":  "dispatches the four overlapping /oauth routes (PATCH providers/{id}, POST {id}/authorize, POST {id}/callback, GET {type}/authorize) as Starlette does",
	"OPTIONS /api/v1/auth/oauth/{first}/{second}": "dispatches the four overlapping /oauth routes (PATCH providers/{id}, POST {id}/authorize, POST {id}/callback, GET {type}/authorize) as Starlette does",
}

var routeParameter = regexp.MustCompile(`\{[^}]+\}`)

func routeShape(method, path string) string {
	trimmed := strings.TrimRight(path, "/")
	if trimmed == "" {
		trimmed = "/"
	}
	return method + " " + routeParameter.ReplaceAllString(trimmed, "{}")
}

// TestVenueOracleRouteResponseModels pins every dho api route's
// ResponseModel flag against the live FastAPI app: a route whose Python
// counterpart writes its success body as a response_model must be
// flagged (its handlers then write with policy.WriteModel, and the venues
// fail on a WriteJSON success body there), every other route must not be,
// and each named exception must still be what it says.
func TestVenueOracleRouteResponseModels(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	python := pyoracle.Resolve(t, root)
	command := exec.Command(python, "-c", pythonRouteTableProgram)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	output, err := command.Output()
	if err != nil {
		t.Fatalf("live FastAPI route table: %v", pyoracle.RunError(python, err, output))
	}
	var table [][3]any
	for _, line := range strings.Split(string(output), "\n") {
		if rest, ok := strings.CutPrefix(line, "ROUTES "); ok {
			if err := json.Unmarshal([]byte(rest), &table); err != nil {
				t.Fatalf("decode route table: %v", err)
			}
		}
	}
	if len(table) == 0 {
		t.Fatal("the live FastAPI route table is empty; the comparison measured nothing")
	}
	pythonModel := map[string]bool{}
	for _, row := range table {
		pythonModel[routeShape(row[0].(string), row[1].(string))] = row[2].(bool)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	guard := &policy.Guard{}
	// admin and teamsidentity mount only with a pool and a ClickHouse
	// connection; their route lists do not need either to be built.
	routes := Routes(Deps{Guard: guard}, logger)
	routes = append(routes, markResponseModels(admin.Routes(admin.Deps{Guard: guard, Logger: logger, Write: WriteError}))...)
	routes = append(routes, markResponseModels(teamsidentity.Routes(nil, guard, logger, nil, nil))...)
	// billing mounts only with a pool; its route list needs neither a pool
	// nor a Stripe client to be built.
	routes = append(routes, markResponseModels(billing.Routes(billing.Deps{Guard: guard, Logger: logger}))...)
	// session mounts only with a pool, an authenticator and the token keys;
	// its route list needs none of them to work.
	sessionKey := "response-model-registry-signing-key-0123456789"
	verifier, err := edgetoken.New(sessionKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	signer, err := edgetoken.NewSigner(sessionKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	routes = append(routes, markResponseModels(session.Routes(session.Deps{Pool: &pgxpool.Pool{}, Guard: guard,
		Auth: &policy.Authenticator{}, Verifier: verifier, Signer: signer}))...)
	// sso mounts only with a pool; its route list does not use it.
	routes = append(routes, markResponseModels(sso.Routes(sso.Deps{Pool: &pgxpool.Pool{}, Guard: guard, Logger: logger}))...)
	patterns := map[string]httpapi.Route{}
	for _, route := range routes {
		patterns[route.Method+" "+route.Pattern] = route
	}

	// Every Go route pattern is in the table or named as having no
	// FastAPI route of its own.
	for key := range patterns {
		_, inTable := responseModelRoutes[key]
		_, without := goRoutesWithoutPython[key]
		switch {
		case inTable && without:
			t.Errorf("%s is both in responseModelRoutes and named as having no FastAPI route", key)
		case !inTable && !without:
			t.Errorf("%s is in neither responseModelRoutes nor goRoutesWithoutPython", key)
		}
		if without {
			if _, ok := pythonModel[routeShape(strings.SplitN(key, " ", 2)[0], strings.SplitN(key, " ", 2)[1])]; ok {
				t.Errorf("%s is named as having no FastAPI route, but FastAPI has one", key)
			}
		}
	}

	// Every table entry matches FastAPI, is served by a Go route, and is
	// what that route reports for the request.
	compared, flagged := 0, 0
	for key, flag := range responseModelRoutes {
		method, path, _ := strings.Cut(key, " ")
		model, ok := pythonModel[routeShape(method, path)]
		if !ok {
			t.Errorf("responseModelRoutes names %s, which FastAPI has no route for", key)
			continue
		}
		compared++
		if flag {
			flagged++
		}
		if reason, exception := jsonResponseRoutes[key]; exception {
			if !model || flag {
				t.Errorf("%s is a named JSONResponse exception (%s): FastAPI response_model=%v, table=%v; want true and false", key, reason, model, flag)
			}
		} else if flag != model {
			t.Errorf("%s: table says %v, FastAPI writes it as a response_model=%v", key, flag, model)
		}
		route, served := servingRoute(patterns, method, path)
		if !served {
			t.Errorf("responseModelRoutes names %s, which no Go route serves", key)
			continue
		}
		request := httptest.NewRequest(method, strings.NewReplacer("{", "x", "}", "x").Replace(path), nil)
		if got := route.ResponseModelFor(request); got != flag {
			t.Errorf("%s: the Go route %s %s reports ResponseModel=%v for it, the table says %v", key, route.Method, route.Pattern, got, flag)
		}
	}
	for key := range jsonResponseRoutes {
		if _, ok := responseModelRoutes[key]; !ok {
			t.Errorf("jsonResponseRoutes names %s, which responseModelRoutes does not", key)
		}
	}
	if compared == 0 || flagged == 0 {
		t.Fatalf("compared %d routes, %d flagged: the comparison measured nothing", compared, flagged)
	}
	venueoracle.WriteProof(t)
	t.Logf("%d Python routes the dho api serves compared with the live FastAPI table; %d write a response_model body", compared, flagged)
}

// servingRoute is the Go route that serves method and path: the route
// registered for exactly that pattern, or a wildcard route whose pattern
// matches the literal path segment by segment.
func servingRoute(patterns map[string]httpapi.Route, method, path string) (httpapi.Route, bool) {
	if route, ok := patterns[method+" "+path]; ok {
		return route, true
	}
	segments := strings.Split(path, "/")
	for _, route := range patterns {
		if route.Method != method {
			continue
		}
		candidate := strings.Split(route.Pattern, "/")
		if len(candidate) != len(segments) {
			continue
		}
		matched := true
		for index, segment := range candidate {
			if segment != segments[index] && !(strings.HasPrefix(segment, "{") && strings.HasSuffix(segment, "}")) {
				matched = false
				break
			}
		}
		if matched {
			return route, true
		}
	}
	return httpapi.Route{}, false
}

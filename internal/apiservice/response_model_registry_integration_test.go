//go:build integration

package apiservice

import (
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"

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

// goRoutesWithoutPython are Go routes with no FastAPI counterpart, by
// ruling: they are not compared.
var goRoutesWithoutPython = map[string]string{
	"POST /api/v1/admin/orgs/{org_id}/transfer-ownership": "Go serves the web's shape; the ruled intentional divergence",
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
	routes = append(routes, markResponseModels(teamsidentity.Routes(nil, guard, logger))...)
	seen := map[string]httpapi.Route{}
	for _, route := range routes {
		seen[route.Method+" "+route.Pattern] = route
	}

	compared, flagged := 0, 0
	for key, route := range seen {
		model, ok := pythonModel[routeShape(route.Method, route.Pattern)]
		if _, divergent := goRoutesWithoutPython[key]; divergent {
			if ok {
				t.Errorf("%s is listed as having no FastAPI route, but FastAPI has one", key)
			}
			continue
		}
		if !ok {
			t.Errorf("%s has no FastAPI route (name it in goRoutesWithoutPython with the ruling, or fix the pattern)", key)
			continue
		}
		compared++
		if reason, exception := jsonResponseRoutes[key]; exception {
			if !model {
				t.Errorf("%s is a named JSONResponse exception (%s), but FastAPI no longer declares a response_model for it", key, reason)
			}
			if route.ResponseModel {
				t.Errorf("%s is a named JSONResponse exception but is flagged ResponseModel", key)
			}
			continue
		}
		if route.ResponseModel {
			flagged++
		}
		if route.ResponseModel != model {
			t.Errorf("%s: ResponseModel=%v, FastAPI writes it as a response_model=%v", key, route.ResponseModel, model)
		}
	}
	for key := range responseModelRoutes {
		if _, ok := seen[key]; !ok {
			t.Errorf("responseModelRoutes names %s, which no Go route registers", key)
		}
	}
	for key := range jsonResponseRoutes {
		if _, ok := seen[key]; !ok {
			t.Errorf("jsonResponseRoutes names %s, which no Go route registers", key)
		}
	}
	if compared == 0 || flagged == 0 {
		t.Fatalf("compared %d routes, %d flagged: the comparison measured nothing", compared, flagged)
	}
	venueoracle.WriteProof(t)
	t.Logf("%d Go routes compared with the live FastAPI table; %d write a response_model body", compared, flagged)
}

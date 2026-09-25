//go:build integration

package metricsvenue

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jackc/pgx/v5/pgxpool"
	valkeygo "github.com/valkey-io/valkey-go"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// sweepPlugin is a pytest plugin that records, for every prometheus_client
// counter increment, histogram observation and gauge change, the FastAPI
// route whose handler was running ("<no request>" outside one). Each
// process (xdist worker) writes <COUNTER_SWEEP_OUT>.<pid>.json at exit.
const sweepPlugin = `
import atexit, contextvars, json, os
import fastapi.routing
from prometheus_client.metrics import Counter, Gauge, Histogram

_current = contextvars.ContextVar("counter_sweep_route", default=None)
_fired = {}

def _wrap(cls, name):
    original = getattr(cls, name)
    def wrapped(self, *args, **kwargs):
        family = getattr(self, "_name", None) or "<unknown>"
        _fired.setdefault(family, set()).add(_current.get() or "<no request>")
        return original(self, *args, **kwargs)
    setattr(cls, name, wrapped)

_wrap(Counter, "inc")
_wrap(Histogram, "observe")
_wrap(Gauge, "set")
_wrap(Gauge, "inc")
_wrap(Gauge, "dec")

_original_handler = fastapi.routing.APIRoute.get_route_handler

def _get_route_handler(self):
    handler = _original_handler(self)
    label = ",".join(sorted(self.methods or [])) + " " + self.path
    async def wrapped(request):
        token = _current.set(label)
        try:
            return await handler(request)
        finally:
            _current.reset(token)
    return wrapped

fastapi.routing.APIRoute.get_route_handler = _get_route_handler

def _dump():
    out = os.environ.get("COUNTER_SWEEP_OUT")
    if out:
        with open(out + "." + str(os.getpid()) + ".json", "w") as handle:
            json.dump({family: sorted(routes) for family, routes in _fired.items()}, handle)

atexit.register(_dump)
`

// sweepMigrateProgram runs the Alembic heads on POSTGRES_URI, as the
// venue's "migrate" mode does.
const sweepMigrateProgram = `
from alembic import command
from dev_health_ops.migrate import _make_alembic_config
command.upgrade(_make_alembic_config(), "heads")
`

// noRequest is the plugin's label for a metric recorded outside any route.
const noRequest = "<no request>"

// TestPythonMetricsTableSweepVenueOracle proves python_metrics.tsv's
// statuses and routes by execution. It runs the Python api's own route
// tests under sweepPlugin, which records the route every metric family
// fires under, and it builds the dho api's production route set
// (apiservice.Routes) to tell which of those routes the Go api serves.
// Then each row must list exactly the routes its family fired under, and
// its status must fit them:
//
//   - ported: every listed route is one the dho api serves;
//   - partial, route-missing: at least one listed route is;
//   - unreached: no route is listed (the note names the source's route);
//   - every other status: no listed route is one the dho api serves;
//   - unused: besides, the family did not fire anywhere in the run.
//
// A counter the parity oracle compares also fired under the route the
// oracle drives it through (routeCounters), which is added to its swept
// routes. A family the run fired that has no row fails too, so a lazily
// registered family cannot go unlisted. What the Python tests do not
// reach is not proven: a family fired only on an untested path reads as
// not firing under that route.
func TestPythonMetricsTableSweepVenueOracle(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("the sweep runs the live Python api's tests; run with DEV_HEALTH_LIVE_PYTHON_ORACLES=1")
	}
	checkSweptTable(t, runSweep(t), dhoAPIServes(t), parsePythonMetricsTable(t))
	venueoracle.WriteProof(t)
}

// checkSweptTable applies the rules above to a sweep's result (family ->
// the routes it fired under, noRequest for outside one).
func checkSweptTable(t *testing.T, fired map[string]map[string]bool, served func(string) bool, rows []tableRow) {
	t.Helper()
	for _, counter := range routeCounters {
		family := strings.TrimSuffix(counter.metric, "_total")
		if fired[family] == nil {
			fired[family] = map[string]bool{}
		}
		fired[family][counter.route] = true
	}

	named := map[string]bool{}
	for _, row := range rows {
		named[row.family] = true
		seen, ran := fired[row.family]
		var underRoutes []string
		for route := range seen {
			if route != noRequest {
				underRoutes = append(underRoutes, route)
			}
		}
		sort.Strings(underRoutes)
		listed := append([]string(nil), row.routes...)
		sort.Strings(listed)
		if strings.Join(listed, ";") != strings.Join(underRoutes, ";") {
			t.Errorf("%s lists routes %q; the Python api fired it under %q", row.family, listed, underRoutes)
		}
		servedCount := 0
		for _, route := range underRoutes {
			if served(route) {
				servedCount++
			}
		}
		switch row.status {
		case "ported":
			if len(underRoutes) == 0 || servedCount != len(underRoutes) {
				t.Errorf("%s is ported, but the dho api serves %d of the %d routes it fired under", row.family, servedCount, len(underRoutes))
			}
		case "partial", "route-missing":
			if servedCount == 0 {
				t.Errorf("%s is %s, but the dho api serves none of the routes it fired under %q", row.family, row.status, underRoutes)
			}
		case "unreached":
			if len(underRoutes) != 0 {
				t.Errorf("%s is unreached, but it fired under %q", row.family, underRoutes)
			}
		case "unused":
			if ran {
				t.Errorf("%s is unused, but the Python api fired it under %q", row.family, sortedKeys(seen))
			}
		default:
			if servedCount != 0 {
				t.Errorf("%s is %s, but it fired under %d routes the dho api serves", row.family, row.status, servedCount)
			}
		}
	}
	for family, seen := range fired {
		if !named[family] {
			t.Errorf("the Python api fired %s (under %q), which python_metrics.tsv does not name", family, sortedKeys(seen))
		}
	}
}

// runSweep runs the Python api's route tests under sweepPlugin and returns
// family -> the set of route labels it fired under. It fails, rather than
// returning an empty map, when the run fails or fires nothing: an empty
// sweep would read as "fires under no route" for every row.
//
// The tests that need a live PostgreSQL read DEV_HEALTH_POSTGRES_TEST_URI,
// and under CI they fail without it rather than skip. The sweep gives them
// a PostgreSQL of its own, as the Python test job does, so every test runs
// in every environment and none is silently skipped out of the sweep. It
// migrates that database to the Alembic heads first, as the venue does:
// some of these tests expect the app schema, which the Python test job
// only has because its migration tests (outside tests/api) ran first.
func runSweep(t *testing.T) map[string]map[string]bool {
	t.Helper()
	root := venueRoot()
	python := pyoracle.Resolve(t, root)
	postgres, err := containers.StartPostgres(context.Background())
	if err != nil {
		t.Fatalf("start the sweep's PostgreSQL: %v", err)
	}
	t.Cleanup(func() { _ = postgres.Close(context.Background()) })
	postgresURI, err := url.Parse(postgres.URI)
	if err != nil {
		t.Fatal(err)
	}
	postgresURI.Scheme, postgresURI.RawQuery = "postgresql+asyncpg", ""
	migrate := exec.Command(python, "-c", sweepMigrateProgram)
	migrate.Dir = root
	migrate.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"),
		"POSTGRES_URI="+postgresURI.String(), "DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1", "OTEL_SDK_DISABLED=true")
	if migrated, err := migrate.CombinedOutput(); err != nil {
		t.Fatalf("migrate the sweep's PostgreSQL: %v\n%s", err, lastLines(string(migrated), 20))
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "counter_sweep_plugin.py"), []byte(sweepPlugin), 0o600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(python, "-m", "pytest", "tests/api", "-p", "counter_sweep_plugin",
		"-m", "not benchmark and not clickhouse", "-n", "4", "-q", "--no-header",
		"-p", "no:warnings", "-p", "no:cacheprovider")
	command.Dir = root
	command.Env = append(os.Environ(),
		"PYTHONPATH="+filepath.Join(root, "src")+string(os.PathListSeparator)+dir,
		"COUNTER_SWEEP_OUT="+filepath.Join(dir, "out"),
		"DEV_HEALTH_POSTGRES_TEST_URI="+postgresURI.String())
	var output bytes.Buffer
	command.Stdout, command.Stderr = &output, &output
	started := time.Now()
	err = command.Run()
	elapsed := time.Since(started).Round(time.Second)
	summary := lastLines(output.String(), 5)
	if err != nil {
		t.Fatalf("the Python api's tests failed under the sweep (%s): %v\n%s\n%s", elapsed, err, failureLines(output.String()), summary)
	}
	t.Logf("sweep: the Python api's route tests ran in %s\n%s", elapsed, summary)

	files, err := filepath.Glob(filepath.Join(dir, "out.*.json"))
	if err != nil {
		t.Fatal(err)
	}
	fired := map[string]map[string]bool{}
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		var part map[string][]string
		if err := json.Unmarshal(data, &part); err != nil {
			t.Fatalf("%s: %v", file, err)
		}
		for family, routes := range part {
			if fired[family] == nil {
				fired[family] = map[string]bool{}
			}
			for _, route := range routes {
				fired[family][route] = true
			}
		}
	}
	underRoute := 0
	for _, routes := range fired {
		for route := range routes {
			if route != noRequest {
				underRoute++
				break
			}
		}
	}
	if len(fired) == 0 || underRoute == 0 {
		t.Fatalf("the sweep recorded %d families from %d process files, %d of them under a route; the plugin did not record", len(fired), len(files), underRoute)
	}
	t.Logf("sweep: %d families fired, %d of them under a route, from %d processes", len(fired), underRoute, len(files))
	return fired
}

type stubClickHouse struct{ driver.Conn }

type stubValkey struct{ valkeygo.Client }

// dhoAPIServes builds the dho api's route set with apiservice.Routes, the
// function production mounts, and returns whether a Python route label
// ("GET,HEAD /api/v1/x/{id}") names a route it serves. Routes mounts an
// area only when its store is configured, so every store is given: a pool
// that never dials, and ClickHouse and Valkey stubs no route builder calls.
func dhoAPIServes(t *testing.T) func(string) bool {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	pool, err := pgxpool.New(context.Background(), "postgres://sweep:sweep@127.0.0.1:1/sweep")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	verifier, err := edgetoken.New(strings.Repeat("k", 48), "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	auth, err := policy.NewAuthenticator(verifier, policy.PGStore{Pool: pool}, logger)
	if err != nil {
		t.Fatal(err)
	}
	deps := apiservice.Deps{Pool: pool, Auth: auth, Guard: policy.NewGuard(auth, logger), Verifier: verifier,
		ClickHouse: stubClickHouse{}, Valkey: stubValkey{}}
	mux := http.NewServeMux()
	routes := apiservice.Routes(deps, logger)
	for _, route := range routes {
		mux.Handle(route.Method+" "+route.Pattern, http.NotFoundHandler())
	}
	t.Logf("dho api: %d routes", len(routes))
	parameter := regexp.MustCompile(`\{[^}]*\}`)
	return func(label string) bool {
		methods, path, ok := strings.Cut(label, " ")
		if !ok {
			t.Fatalf("route label %q is not <methods> <path>", label)
		}
		for _, method := range strings.Split(methods, ",") {
			if method == "HEAD" {
				continue
			}
			request := httptest.NewRequest(method, parameter.ReplaceAllString(path, "sweep"), nil)
			if _, pattern := mux.Handler(request); pattern != "" && !strings.HasSuffix(pattern, "/") {
				return true
			}
		}
		return false
	}
}

// failureLines keeps pytest's short-summary lines (FAILED and ERROR, one
// per test), which the last lines of a long run do not always hold.
func failureLines(text string) string {
	var kept []string
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, "FAILED ") || strings.HasPrefix(line, "ERROR ") {
			kept = append(kept, line)
		}
	}
	return strings.Join(kept, "\n")
}

func lastLines(text string, n int) string {
	lines := strings.Split(strings.TrimRight(text, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

func sortedKeys(set map[string]bool) []string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

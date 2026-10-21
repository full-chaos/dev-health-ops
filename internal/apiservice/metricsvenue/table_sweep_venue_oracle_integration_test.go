//go:build integration

package metricsvenue

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jackc/pgx/v5/pgxpool"
	valkeygo "github.com/valkey-io/valkey-go"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// noRequest is the frozen sweep's label for a metric recorded outside any route.
const noRequest = "<no request>"

// TestPythonMetricsTableSweepVenueOracle proves python_metrics.tsv's
// statuses and routes against the frozen sweep of the Python api's own route
// tests, which recorded the route every metric family
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
	golden := venueoracle.OpenGolden(t, sweepGoldenSpec(t.Name(), "0136d988806c3ac67a419b4822044730e368d05cd8ae49b84f8d3df1dfdbf2f0"))
	// The sweep was one Python run (the migrations, then the api's route tests);
	// its answer is the map of metric families to the routes
	// they fired under, executed once on the pinned build and frozen.
	request := venueoracle.ProgramRequest("python api route sweep", nil)
	answers := golden.Produce(t, []venueoracle.Request{request})
	golden.Consumed(t, answers...)
	var recorded map[string][]string
	if err := json.Unmarshal([]byte(answers[0].Body), &recorded); err != nil {
		t.Fatalf("decode the frozen sweep: %v", err)
	}
	fired := map[string]map[string]bool{}
	for family, routes := range recorded {
		fired[family] = map[string]bool{}
		for _, route := range routes {
			fired[family][route] = true
		}
	}
	checkSweptTable(t, fired, dhoAPIServes(t), parsePythonMetricsTable(t))
	golden.SkipDiff(t)
	venueoracle.WriteGoOnlyProof(t, "the Go api's counter table against the frozen sweep of the Python api's route tests")
	golden.Finish(t)
}

// sweepPythonBuild is the build whose Python api tests the sweep ran: the one
// the table python_metrics.tsv describes.
const sweepPythonBuild = "a4847c5e93607451a0c987b314d37e02fc43ce85"

// sweepGoldenSpec is the GoldenSpec of the sweep's golden; digest is the
// SHA-256 the test pins ("0136d988806c3ac67a419b4822044730e368d05cd8ae49b84f8d3df1dfdbf2f0" until its first recording).
func sweepGoldenSpec(test, digest string) venueoracle.GoldenSpec {
	return venueoracle.GoldenSpec{
		Path:        "testdata/table-sweep.golden.json",
		PythonBuild: sweepPythonBuild,
		SHA256:      digest,
	}
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

func sortedKeys(set map[string]bool) []string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

//go:build integration

package server

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/explain"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/investment"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/people"
	chclickhouse "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// pythonDictOrderProgram runs the REAL Python service behind each case
// (the service the route called before the route moved to the query-api)
// on the venue's Python ClickHouse database, and writes its return value
// as FastAPI does on that route's response_model path: the route's
// response field validates it and serialize_json writes it with the
// route's response_model_* settings (fastapi/routing.py
// serialize_response, dump_json=True). A service that raises is the
// route's `except Exception: raise HTTPException(503, "Data
// unavailable")`.
const pythonDictOrderProgram = `
import asyncio, base64, datetime, json, os, sys, traceback
from fastapi.routing import APIRoute
from dev_health_ops.api.main import app
from dev_health_ops.api.models.filters import MetricFilter
from dev_health_ops.core.cache import create_cache, epoch_scoped
from dev_health_ops.api.services.explain import build_explain_response
from dev_health_ops.api.services.home import build_home_response
from dev_health_ops.api.services.investment import build_investment_response
from dev_health_ops.api.services.investment_flow import build_investment_flow_response
from dev_health_ops.api.services.aggregated_flame import build_aggregated_flame_response
from dev_health_ops.api.services.people import build_person_summary_response

routes = {}
for route in app.routes:
    if isinstance(route, APIRoute):
        for method in route.methods:
            routes[method + " " + route.path] = route
db_url = os.environ["CLICKHOUSE_URI"]
home_cache = epoch_scoped(create_cache(ttl_seconds=60))
explain_cache = epoch_scoped(create_cache(ttl_seconds=120))

async def call(case):
    kind, org_id, args = case["kind"], case["org_id"], case["args"]
    if kind == "explain":
        return await build_explain_response(db_url=db_url, metric=args["metric"], filters=MetricFilter(**args["filters"]), cache=explain_cache, org_id=org_id)
    if kind == "home":
        return await build_home_response(db_url=db_url, filters=MetricFilter(**args["filters"]), cache=home_cache, org_id=org_id, semantic_session=None)
    if kind == "investment":
        return await build_investment_response(db_url=db_url, filters=MetricFilter(**args["filters"]), org_id=org_id)
    if kind == "flow":
        return await build_investment_flow_response(db_url=db_url, filters=MetricFilter(**args["filters"]), flow_mode=args.get("flow_mode"), org_id=org_id)
    if kind == "aggflame":
        return await build_aggregated_flame_response(
            db_url=db_url, org_id=org_id, mode=args["mode"],
            start_day=datetime.date.fromisoformat(args["start_day"]), end_day=datetime.date.fromisoformat(args["end_day"]),
            team_id=args.get("team_id"), repo_id=args.get("repo_id"), provider=args.get("provider"), work_scope_id=args.get("work_scope_id"))
    if kind == "people_summary":
        return await build_person_summary_response(db_url=db_url, person_id=args["person_id"], range_days=args["range_days"], compare_days=args["compare_days"], org_id=org_id)
    raise SystemExit("unknown case kind " + kind)

out = []
for case in json.loads(sys.stdin.read()):
    route = routes[case["route"]]
    try:
        value = asyncio.run(call(case))
    except Exception:
        out.append({"status": 503, "body": base64.b64encode(b'{"detail":"Data unavailable"}').decode(), "error": traceback.format_exc()[-2000:]})
        continue
    field = route.response_field
    validated, errors = field.validate(value, {}, loc=("response",))
    if errors:
        raise SystemExit(case["route"] + ": " + repr(errors)[:3000])
    body = field.serialize_json(
        validated,
        include=route.response_model_include,
        exclude=route.response_model_exclude,
        by_alias=route.response_model_by_alias,
        exclude_unset=route.response_model_exclude_unset,
        exclude_defaults=route.response_model_exclude_defaults,
        exclude_none=route.response_model_exclude_none,
    )
    out.append({"status": 200, "body": base64.b64encode(body).decode()})
print("RESULT " + json.dumps(out))
`

// dictOrderCase is one route request, answered by the Go handler and by
// the Python service the route called.
type dictOrderCase struct {
	Name   string         `json:"name"`
	Route  string         `json:"route"`
	Kind   string         `json:"kind"`
	OrgID  string         `json:"org_id"`
	Args   map[string]any `json:"args"`
	method string
	path   string
	body   string
}

// freshnessObject returns body's "freshness" object as the bytes pyjson
// writes it, in the body's own key order.
func freshnessObject(t *testing.T, body string) string {
	t.Helper()
	decoded, err := pyjson.Decode([]byte(body))
	if err != nil {
		t.Fatalf("decode %s: %v", body, err)
	}
	object, ok := decoded.(*pyjson.Object)
	if !ok {
		t.Fatalf("body is not an object: %s", body)
	}
	freshness, ok := object.Get("freshness")
	if !ok {
		t.Fatalf("body has no freshness: %s", body)
	}
	out, err := pyjson.Marshal(freshness)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

type dictOrderPythonAnswer struct {
	Status int    `json:"status"`
	Body   string `json:"body"`
	Error  string `json:"error"`
}

// TestVenueOracleQueryAPIDictOrder compares the query-api's response bodies
// with the Python service's, byte for byte, for the routes whose response
// holds a dict the Python code builds in a fixed key order (a Python dict
// keeps insertion order; a Go map is written sorted). Python runs its real
// service on the venue's Python ClickHouse database; the Go handler
// answers on the Go database, which holds the same rows. Nothing is fed
// from one side to the other, so a dict the Go port writes in another
// order than the Python service builds is a DIFF. A case both sides answer
// with an error fails too: it compares no dict.
func TestVenueOracleQueryAPIDictOrder(t *testing.T) {
	ctx := context.Background()
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	orgID := uuid.NewString()
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root: root,
		// A key made per run: no route here checks a token, and a fixed
		// literal would read as a secret to the secret scan.
		JWTKey: uuid.NewString() + uuid.NewString(),
	})

	// One identity with a work-item metrics row on both databases, so the
	// person summary finds its person (person_lookup.sql reads
	// user_metrics_daily and work_item_user_metrics_daily; the person id is
	// md5 of the identity).
	identity := "venue-dict-order@example.com"
	personID := fmt.Sprintf("%x", md5.Sum([]byte(identity)))
	for _, database := range []string{venue.PythonClickHouseDB, venue.GoClickHouseDB} {
		conn, err := chclickhouse.Open(ctx, chclickhouse.DefaultConfig(venue.AdminClickHouseURI(t, database)))
		if err != nil {
			t.Fatal(err)
		}
		if err := conn.Exec(ctx, "INSERT INTO work_item_user_metrics_daily (day, provider, user_identity, org_id, computed_at) VALUES (today() - 1, 'github', ?, ?, now())",
			identity, orgID); err != nil {
			t.Fatalf("seed %s: %v", database, err)
		}
		_ = conn.Close()
	}

	// The Go side reads with the admin login: the venue provisions the api
	// login with the dho api's grants, not the query-api's.
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(newUnrestrictedReadClickHouseOptions(venue.AdminClickHouseURI(t, venue.GoClickHouseDB)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	pgPool, err := pgxpool.New(ctx, venue.GoAPIDatabaseURI(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pgPool.Close)
	explainReader, err := explain.NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	investmentReader, err := investment.NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	peopleReader, err := people.NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	for pattern, handler := range map[string]http.HandlerFunc{
		"GET /api/v1/explain":                    newExplainGetHandler(explainReader),
		"GET /api/v1/home":                       newHomeGetHandler(client, pgPool),
		"GET /api/v1/investment":                 newInvestmentGetHandler(investmentReader),
		"POST /api/v1/investment/flow":           newInvestmentFlowHandler(client),
		"GET /api/v1/flame/aggregated":           newFlameAggregatedWorkHandler(client),
		"GET /api/v1/people/{person_id}/summary": newPeopleSummaryHandler(peopleReader),
	} {
		key, handler := pattern, handler
		mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
			routeWriter := httpapi.NewRouteWriter(w, key, responseModelRoutes[key])
			handler(routeWriter, r.WithContext(authctx.WithClaims(r.Context(), authctx.Claims{OrgID: orgID, Role: "owner"})))
		})
	}

	today := time.Now().UTC()
	start := today.AddDate(0, 0, -7).Format(time.DateOnly)
	end := today.Format(time.DateOnly)
	repoID := uuid.NewString()
	window := map[string]any{"time": map[string]any{"range_days": 7}}
	cases := []dictOrderCase{
		{Name: "explain drilldown_links", Route: "GET /api/v1/explain", Kind: "explain",
			Args: map[string]any{"metric": "cycle_time", "filters": window}, method: http.MethodGet, path: "/api/v1/explain?metric=cycle_time&range_days=7"},
		{Name: "home tiles", Route: "GET /api/v1/home", Kind: "home",
			Args: map[string]any{"filters": window}, method: http.MethodGet, path: "/api/v1/home?range_days=7"},
		{Name: "investment band_counts", Route: "GET /api/v1/investment", Kind: "investment",
			Args: map[string]any{"filters": window}, method: http.MethodGet, path: "/api/v1/investment?range_days=7"},
		{Name: "investment flow coverage", Route: "POST /api/v1/investment/flow", Kind: "flow",
			Args: map[string]any{"filters": window, "flow_mode": "team_category_repo"}, method: http.MethodPost, path: "/api/v1/investment/flow",
			body: `{"filters":{"time":{"range_days":7}},"flow_mode":"team_category_repo"}`},
		// No cycle_breakdown case: both sides read work_item_cycle_milestones_daily,
		// which no ClickHouse migration creates, so both answer 503 on a
		// migrated database. Its filters order is pinned by a unit test.
		{Name: "aggregated flame throughput filters", Route: "GET /api/v1/flame/aggregated", Kind: "aggflame",
			Args:   map[string]any{"mode": "throughput", "start_day": start, "end_day": end, "team_id": "team-a", "repo_id": repoID},
			method: http.MethodGet, path: "/api/v1/flame/aggregated?mode=throughput&start_date=" + start + "&end_date=" + end + "&team_id=team-a&repo_id=" + repoID},
		{Name: "people summary sources", Route: "GET /api/v1/people/{person_id}/summary", Kind: "people_summary",
			Args:   map[string]any{"person_id": personID, "range_days": 7, "compare_days": 7},
			method: http.MethodGet, path: "/api/v1/people/" + personID + "/summary?range_days=7&compare_days=7"},
	}
	for index := range cases {
		cases[index].OrgID = orgID
	}

	python := runDictOrderPython(t, venue, root, cases)
	same := 0
	for index, tc := range cases {
		request := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
		if tc.body != "" {
			request.Header.Set("Content-Type", "application/json")
		}
		recorder := httptest.NewRecorder()
		mux.ServeHTTP(recorder, request)
		answer := python[index]
		goBody, pythonBody := recorder.Body.String(), answer.Body
		if tc.Kind == "people_summary" && recorder.Code == http.StatusOK && answer.Status == http.StatusOK {
			// The person summary is compared on its freshness object only
			// (it holds sources, the dict under test). Its other lists
			// (sections.flow_breakdown, sections.collaboration) come from
			// a UNION ALL with no ORDER BY in the Python query itself, so
			// their row order is ClickHouse's, run to run, on either
			// side; and its spark timestamps are CHAOS-6605.
			goBody, pythonBody = freshnessObject(t, goBody), freshnessObject(t, pythonBody)
		}
		if recorder.Code != answer.Status || goBody != pythonBody {
			t.Errorf("%s: DIFF\n python %d %s %s\n go     %d %s", tc.Name, answer.Status, pythonBody, answer.Error, recorder.Code, goBody)
			continue
		}
		if answer.Status != http.StatusOK {
			t.Errorf("%s: both answered %d, so the case compares no dict (python: %s)", tc.Name, answer.Status, answer.Error)
			continue
		}
		same++
	}
	t.Logf("%d of %d cases byte-identical to the Python service's response_model body", same, len(cases))
	venueoracle.WriteProof(t)
}

func runDictOrderPython(t *testing.T, venue *venueoracle.Venue, root string, cases []dictOrderCase) []dictOrderPythonAnswer {
	t.Helper()
	payload, err := json.Marshal(cases)
	if err != nil {
		t.Fatal(err)
	}
	// venueoracle.Start put the chosen interpreter first on PATH.
	command := exec.Command("python3", "-c", pythonDictOrderProgram)
	command.Dir = root
	command.Env = append(os.Environ(),
		"PYTHONPATH="+filepath.Join(root, "src"),
		"CLICKHOUSE_URI="+venue.AdminClickHouseHTTPURI(t, venue.PythonClickHouseDB))
	command.Stdin = bytes.NewReader(payload)
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Run(); err != nil {
		t.Fatalf("python: %v\n%s", err, stderr.String())
	}
	var result []dictOrderPythonAnswer
	for _, line := range strings.Split(stdout.String(), "\n") {
		if rest, ok := strings.CutPrefix(line, "RESULT "); ok {
			if err := json.Unmarshal([]byte(rest), &result); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(result) != len(cases) {
		t.Fatalf("python answered %d of %d cases\n%s", len(result), len(cases), stderr.String())
	}
	for index := range result {
		raw, err := base64.StdEncoding.DecodeString(result[index].Body)
		if err != nil {
			t.Fatal(err)
		}
		result[index].Body = string(raw)
	}
	return result
}

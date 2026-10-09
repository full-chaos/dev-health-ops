//go:build integration

package server

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/explain"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/investment"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/investmentexplain"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/people"
	chclickhouse "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
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
import sys
# The answer is the only thing on stdout: whatever the services (or the libraries they import) print goes to stderr.
answer_stream, sys.stdout = sys.stdout, sys.stderr
import asyncio, datetime, json, os, traceback
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
from dev_health_ops.api.services.work_units import build_work_unit_investments
from dev_health_ops.api.services.work_unit_explain import explain_work_unit
from dev_health_ops.llm import get_provider

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
    if kind == "work_units":
        return await build_work_unit_investments(db_url=db_url, filters=MetricFilter(**args["filters"]), org_id=org_id, limit=args["limit"], include_text=True)
    if kind == "work_unit_explain":
        investments = await build_work_unit_investments(db_url=db_url, filters=MetricFilter(**args["filters"]), org_id=org_id, limit=1, include_text=True, work_unit_id=args["work_unit_id"])
        if not investments:
            raise LookupError("work unit not found")
        provider = get_provider("mock", model=None, org_id=org_id)
        return await explain_work_unit(investment=investments[0], llm_provider="mock", llm_model=None, provider=provider, org_id=org_id, db_url=db_url)
    raise SystemExit("unknown case kind " + kind)

out = []
for case in json.loads(sys.stdin.read()):
    route = routes[case["route"]]
    try:
        value = asyncio.run(call(case))
    except Exception:
        sys.stderr.write(case["name"] + ": " + traceback.format_exc()[-2000:] + "\n")
        out.append({"status": 503, "body": '{"detail":"Data unavailable"}'})
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
    text = body.decode("utf-8")
    if case["kind"] == "people_summary":
        # Only the freshness object (it holds sources, the dict under test): the summary's other lists (sections.flow_breakdown,
        # sections.collaboration) come from a UNION ALL with no ORDER BY in the Python query itself, so their row order is ClickHouse's,
        # run to run, and a recording of the whole body would differ from the next one. The spark timestamps are CHAOS-6605.
        text = json.dumps(json.loads(text)["freshness"], separators=(",", ":"), ensure_ascii=False)
    out.append({"status": 200, "body": text})
answer_stream.write("RESULT " + json.dumps(out) + "\n")
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

// NOT pinned by this oracle (stated, not hidden):
//   - date and time values in the answers: every date or time the planes read from the clock during the run (the last 30 days up to now) is scrubbed to
//     a placeholder, so a one-day difference in such a value is not seen; the aggregated-flame window is a FIXED one (2026-01-01 to 2026-01-08) and
//     is compared by value. A scrubbed value keeps its shape (fractional or not, which offset form), which IS compared.
//   - people_summary: only its freshness object is held and compared (as the live oracle did); the order of sections.* lists and the spark
//     timestamps (CHAOS-6605) are NOT measured.
//   - the cycle_breakdown mode of the flame route (both planes answer 503 on a migrated database; its filters order is pinned by a unit test).
//
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
	_, file, _, _ := moduleroot.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	// The ids are named, not random: the recording and every frozen run seed and send the same ones.
	orgID := stableVenueID("dict-order/org").String()
	golden := venueoracle.OpenGolden(t, dictOrderGolden(t.Name(), dictOrderPin))
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Golden: golden, Root: golden.PythonRoot(t, root),
		// No route here checks a token; the key is the one the other venue oracles of this package use.
		JWTKey: oracleJWTKey,
	})

	// One identity with a work-item metrics row on both databases, so the
	// person summary finds its person (person_lookup.sql reads
	// user_metrics_daily and work_item_user_metrics_daily; the person id is
	// md5 of the identity).
	identity := "venue-dict-order@example.com"
	personID := fmt.Sprintf("%x", md5.Sum([]byte(identity)))
	for _, database := range dictOrderDatabases(venue, golden) {
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

	// The aggregated-flame case takes explicit dates, so its window is FIXED and its rows are seeded inside it with fixed dates (a work item
	// completed on 2026-01-03, attributed to team-a): the case compares two non-empty answers, not two empty ones.
	flameRepo := stableVenueID("dict-order/repo").String()
	for _, database := range dictOrderDatabases(venue, golden) {
		conn, err := chclickhouse.Open(ctx, chclickhouse.DefaultConfig(venue.AdminClickHouseURI(t, database)))
		if err != nil {
			t.Fatal(err)
		}
		for _, statement := range []string{
			fmt.Sprintf(`INSERT INTO work_item_cycle_times (work_item_id, provider, day, work_scope_id, team_id, team_name, assignee, type,
				status, created_at, started_at, completed_at, cycle_time_hours, lead_time_hours, computed_at, org_id)
			VALUES ('wi-flame', 'github', '2026-01-03', 'scope-1', 'team-a', 'Team A', 'dev', 'issue', 'done',
				toDateTime('2026-01-01 09:00:00'), toDateTime('2026-01-02 09:00:00'), toDateTime('2026-01-03 09:00:00'), 24.0, 48.0, toDateTime('2026-01-04 00:00:00'), '%s')`, orgID),
			fmt.Sprintf(`INSERT INTO work_item_team_attributions (org_id, repo_id, work_item_id, provider, team_id, team_name, source, is_primary, confidence, evidence, computed_at)
			VALUES ('%s', '%s', 'wi-flame', 'github', 'team-a', 'Team A', 'native_team', 1, 'high', 'seed', toDateTime64('2026-01-04 00:00:00', 3))`, orgID, flameRepo),
		} {
			if err := conn.Exec(ctx, statement); err != nil {
				t.Fatalf("seed flame %s: %v\n%s", database, err, statement)
			}
		}
		_ = conn.Close()
	}

	// One work unit whose dicts are not in sorted order: its theme and
	// subcategory maps, its stored structural JSON (nested too) and one
	// evidence quote. The Python services keep each of those orders.
	workUnitID := "wu-dict-order"
	seedNow := time.Now().UTC()
	fromTS := seedNow.AddDate(0, 0, -3).Format("2006-01-02 15:04:05")
	toTS := seedNow.AddDate(0, 0, -1).Format("2006-01-02 15:04:05")
	for _, database := range dictOrderDatabases(venue, golden) {
		conn, err := chclickhouse.Open(ctx, chclickhouse.DefaultConfig(venue.AdminClickHouseURI(t, database)))
		if err != nil {
			t.Fatal(err)
		}
		for _, statement := range []string{
			fmt.Sprintf(`INSERT INTO work_unit_investments
				(work_unit_id, work_unit_type, work_unit_name, from_ts, to_ts, repo_id, provider,
				 effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json,
				 structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status,
				 categorization_errors_json, categorization_model_version, categorization_input_hash,
				 categorization_run_id, computed_at, org_id)
			VALUES ('%s', 'issue', 'Dict order unit', toDateTime64('%s',3), toDateTime64('%s',3), NULL, 'github',
				 'churn_loc', 10.0, map('quality', 0.6, 'feature_delivery', 0.4),
				 map('quality.bugfix', 0.6, 'feature_delivery.customer', 0.4),
				 '{"work_items":["b-2","a-1"],"prs":[],"summary":{"zeta":1,"alpha":2.5}}',
				 0.5, 'moderate', 'ok', '', 'v1', 'hash', 'run-1', now64(3), '%s')`, workUnitID, fromTS, toTS, orgID),
			// Two more stored payloads the work units list must write as
			// Python does: one with its own "type" member (which replaces
			// the value but keeps "type" first), and JSON null (not a
			// dict, so no structural entry).
			fmt.Sprintf(`INSERT INTO work_unit_investments
				(work_unit_id, work_unit_type, work_unit_name, from_ts, to_ts, repo_id, provider,
				 effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json,
				 structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status,
				 categorization_errors_json, categorization_model_version, categorization_input_hash,
				 categorization_run_id, computed_at, org_id)
			VALUES ('%s-typed', 'issue', 'Typed payload unit', toDateTime64('%s',3), toDateTime64('%s',3), NULL, 'github',
				 'churn_loc', 5.0, map('quality', 1.0), map('quality.bugfix', 1.0),
				 '{"zeta":1,"type":"custom","alpha":2}',
				 0.5, 'moderate', 'ok', '', 'v1', 'hash', 'run-2', now64(3), '%s'),
				('%s-null', 'issue', 'Null payload unit', toDateTime64('%s',3), toDateTime64('%s',3), NULL, 'github',
				 'churn_loc', 4.0, map('quality', 1.0), map('quality.bugfix', 1.0),
				 'null',
				 0.5, 'moderate', 'ok', '', 'v1', 'hash', 'run-3', now64(3), '%s')`,
				workUnitID, fromTS, toTS, orgID, workUnitID, fromTS, toTS, orgID),
			fmt.Sprintf(`INSERT INTO work_unit_investment_quotes (work_unit_id, quote, source_type, source_id, computed_at, categorization_run_id, org_id)
			VALUES ('%s', 'Fix the flaky login test.', 'pr_body', 'pr-7', now64(3), 'run-1', '%s')`, workUnitID, orgID),
		} {
			if err := conn.Exec(ctx, statement); err != nil {
				t.Fatalf("seed %s: %v\n%s", database, err, statement)
			}
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
	workUnitsReader, err := investmentexplain.NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("LLM_PROVIDER", "mock")
	peopleReader, err := people.NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	for pattern, handler := range map[string]http.HandlerFunc{
		"GET /api/v1/explain":                            newExplainGetHandler(explainReader),
		"GET /api/v1/home":                               newHomeGetHandler(client, pgPool),
		"GET /api/v1/investment":                         newInvestmentGetHandler(investmentReader),
		"POST /api/v1/investment/flow":                   newInvestmentFlowHandler(client),
		"GET /api/v1/flame/aggregated":                   newFlameAggregatedWorkHandler(client),
		"GET /api/v1/people/{person_id}/summary":         newPeopleSummaryHandler(peopleReader),
		"GET /api/v1/work-units":                         newWorkUnitsGetHandler(workUnitsReader),
		"POST /api/v1/work-units/{work_unit_id}/explain": newWorkUnitExplainHandler(workUnitsReader, nil, nil),
	} {
		key, handler := pattern, handler
		mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
			routeWriter := httpapi.NewRouteWriter(w, key, responseModelRoutes[key])
			handler(routeWriter, r.WithContext(authctx.WithClaims(r.Context(), authctx.Claims{OrgID: orgID, Role: "owner"})))
		})
	}

	// A FIXED window (the route takes explicit dates): its answer holds them as written, the same in every run, so they are pinned and compared by value.
	start, end := "2026-01-01", "2026-01-08"
	repoID := stableVenueID("dict-order/repo").String()
	window := map[string]any{"time": map[string]any{"range_days": 7}}
	cases := []dictOrderCase{
		{Name: "explain drilldown_links", Route: "GET /api/v1/explain", Kind: "explain",
			Args: map[string]any{"metric": "cycle_time", "filters": window}, method: http.MethodGet, path: "/api/v1/explain?metric=cycle_time&range_days=7"},
		{Name: "home tiles", Route: "GET /api/v1/home", Kind: "home",
			Args: map[string]any{"filters": window}, method: http.MethodGet, path: "/api/v1/home?range_days=7"},
		{Name: "investment band_counts and distributions", Route: "GET /api/v1/investment", Kind: "investment",
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
	workUnitWindow := map[string]any{"time": map[string]any{"range_days": 7, "compare_days": 7}}
	cases = append(cases,
		dictOrderCase{Name: "work units themes and evidence", Route: "GET /api/v1/work-units", Kind: "work_units",
			Args: map[string]any{"filters": workUnitWindow, "limit": 200}, method: http.MethodGet, path: "/api/v1/work-units?range_days=7"},
		dictOrderCase{Name: "work unit explain category_rationale", Route: "POST /api/v1/work-units/{work_unit_id}/explain", Kind: "work_unit_explain",
			Args:   map[string]any{"filters": workUnitWindow, "work_unit_id": workUnitID},
			method: http.MethodPost, path: "/api/v1/work-units/" + workUnitID + "/explain?range_days=7&llm_provider=mock"},
	)
	for index := range cases {
		cases[index].OrgID = orgID
	}

	python := runDictOrderPython(t, golden, venue, root, cases)
	byteIdentical := 0
	ledgerValidated := 0
	for index, tc := range cases {
		request := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
		if tc.body != "" {
			request.Header.Set("Content-Type", "application/json")
		}
		recorder := httptest.NewRecorder()
		mux.ServeHTTP(recorder, request)
		answer := python[index]
		// The golden holds its answers projected (the run values of a plane become placeholders), and the Go answer is projected
		// the same way before the two are compared.
		goWritten := recorder.Body.String()
		if tc.Kind == "explain" && recorder.Code == http.StatusOK {
			// The explain body ends with the Go-only fields of CHAOS-8103,
			// which the frozen Python model never had: compared without
			// them, and only them (withoutExplainGoOnlyFields).
			stripped, err := withoutExplainGoOnlyFields(goWritten)
			if err != nil {
				t.Fatalf("%s: %v", tc.Name, err)
			}
			goWritten = stripped
		}
		if tc.Kind == "home" && recorder.Code == http.StatusOK {
			// Every REST Home delta ends with the three Go-only keys of
			// CHAOS-9044, which the frozen Python model never had: compared
			// without them, and only them (withoutHomeDeltaGoOnlyFields;
			// a delta that lacks one fails).
			stripped, err := withoutHomeDeltaGoOnlyFields(goWritten)
			if err != nil {
				t.Fatalf("%s: %v", tc.Name, err)
			}
			goWritten = stripped
		}
		if tc.Kind == "work_units" && recorder.Code == http.StatusOK {
			// Each evidence quote ends with the Go-only source_title of
			// CHAOS-8959 (null here: the seeded source has no stored
			// title), which the frozen Python model never had. The seeded
			// quote must carry it, so the strip checks the key is served.
			const goOnlyTitle = `,"source_title":null`
			if !strings.Contains(goWritten, goOnlyTitle) {
				t.Fatalf("%s: the seeded quote carries no source_title key: %s", tc.Name, goWritten)
			}
			goWritten = strings.ReplaceAll(goWritten, goOnlyTitle, "")
		}
		goBody := golden.Project(t, venueoracle.Response{Status: recorder.Code, Body: goWritten}).Body
		pythonBody := golden.Project(t, venueoracle.Response{Status: answer.Status, Body: answer.Body}).Body
		if tc.Kind == "people_summary" && recorder.Code == http.StatusOK && answer.Status == http.StatusOK {
			// The person summary is compared on its freshness object only
			// (it holds sources, the dict under test). Its other lists
			// (sections.flow_breakdown, sections.collaboration) come from
			// a UNION ALL with no ORDER BY in the Python query itself, so
			// their row order is ClickHouse's, run to run, on either
			// side; and its spark timestamps are CHAOS-6605.
			goBody = freshnessObject(t, goBody)
		}
		if tc.Kind == "home" {
			if recorder.Code != answer.Status {
				t.Errorf("%s: status DIFF\n python %d %s\n go     %d", tc.Name, answer.Status, answer.Error, recorder.Code)
				continue
			}
			// CHAOS-8169 / GWC D4834: the frozen a484 Python response keeps
			// executing. CHAOS-8509 composes the unchanged ledger with the
			// approved nullable coverage leaves and makes every other JSON
			// leaf strict.
			captureCHAOS8509HomePair(t, chaos8169DictOrderHomeLedgerKey, pythonBody, goBody)
			policy, ok := chaos8509HomeCapturePolicies[chaos8169DictOrderHomeLedgerKey]
			if !ok {
				t.Fatal("CHAOS-8509 has no dict-order Home policy")
			}
			assertCHAOS8509HomeCapturePolicy(t, chaos8169DictOrderHomeLedgerKey, policy, pythonBody, goBody)
			ledgerValidated++
			continue
		}
		if recorder.Code != answer.Status || goBody != pythonBody {
			t.Errorf("%s: DIFF\n python %d %s %s\n go     %d %s", tc.Name, answer.Status, pythonBody, answer.Error, recorder.Code, goBody)
			continue
		}
		if tc.Kind == "aggflame" && !strings.Contains(pythonBody, "Team A") {
			t.Errorf("%s: the recorded Python answer holds no seeded row (it would compare two empty answers): %s", tc.Name, pythonBody)
		}
		if answer.Status != http.StatusOK {
			t.Errorf("%s: both answered %d, so the case compares no dict (python: %s)", tc.Name, answer.Status, answer.Error)
			continue
		}
		byteIdentical++
	}
	t.Logf("%d of %d cases byte-identical to the Python service's response_model body; %d CHAOS-8169 D4834 ledger-validated", byteIdentical, len(cases), ledgerValidated)
	golden.SkipDiff(t)
	venueoracle.WriteGoOnlyProof(t, "the Go query-api's response bodies against the frozen answers of the Python services")
	golden.Finish(t)
}

// dictOrderDeclared holds the environment entries that shape the Python program's answers. The address of the run's own ClickHouse database is
// not one of them: it is made for the run and passed as a per-run entry (CLICKHOUSE_URI).
var dictOrderDeclared = map[string]string{"OTEL_SDK_DISABLED": "true", "ENVIRONMENT": "test"}

// dictOrderDatabases are the databases a test seeds: the Go plane's always, the Python plane's only while its answers are recorded.
func dictOrderDatabases(venue *venueoracle.Venue, golden *venueoracle.Golden) []string {
	if golden.Recording() {
		return []string{venue.PythonClickHouseDB, venue.GoClickHouseDB}
	}
	return []string{venue.GoClickHouseDB}
}

func runDictOrderPython(t *testing.T, golden *venueoracle.Golden, venue *venueoracle.Venue, root string, cases []dictOrderCase) []dictOrderPythonAnswer {
	t.Helper()
	payload, err := json.Marshal(cases)
	if err != nil {
		t.Fatal(err)
	}
	// The launcher form: the harness's own producer starts the program in the closed environment; the address of the run's own Python
	// ClickHouse database is the one per-run entry, passed as an extra (it is not part of the request's key).
	request := venueoracle.ProgramRequest("dict order services", pythonDictOrderProgram, payload, dictOrderDeclared)
	answers := golden.Produce(t, golden.PythonRoot(t, root), []venueoracle.Request{request},
		func(producer *venueoracle.Producer, _ []venueoracle.Request) []venueoracle.Response {
			command, err := producer.Command(context.Background(), dictOrderDeclared,
				[]string{"CLICKHOUSE_URI=" + venue.AdminClickHouseHTTPURI(t, venue.PythonClickHouseDB)}, "-c", pythonDictOrderProgram)
			if err != nil {
				t.Fatal(err)
			}
			command.Dir = producer.Root
			command.Stdin = bytes.NewReader(payload)
			var stdout, stderr bytes.Buffer
			command.Stdout, command.Stderr = &stdout, &stderr
			if err := command.Run(); err != nil {
				t.Fatalf("the python program failed: %v\n%s", err, dictOrderWithoutAddress(stderr.String(), venue, t))
			}
			return []venueoracle.Response{{Status: 0, Body: stdout.String()}}
		})
	golden.Consumed(t, answers...)
	if answers[0].Status != 0 {
		t.Fatalf("the python program exited %d when it was recorded", answers[0].Status)
	}
	if golden.Recording() && strings.Contains(answers[0].Body, venue.AdminClickHouseHTTPURI(t, venue.PythonClickHouseDB)) {
		t.Fatal("the recorded answer holds the address of the run's own database: a golden cannot hold a value of one run")
	}
	var result []dictOrderPythonAnswer
	for _, line := range strings.Split(answers[0].Body, "\n") {
		if rest, ok := strings.CutPrefix(line, "RESULT "); ok {
			if err := json.Unmarshal([]byte(rest), &result); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(result) != len(cases) {
		t.Fatalf("python answered %d of %d cases", len(result), len(cases))
	}
	return result
}

// The golden's pin is the digest the record verb writes; PIN: names the golden until it does.
const dictOrderPin = "3c7b4dde602aba9f20e31c5ebdb3fb1e485e1eb55014e96f58d1a2689f29f0a2"

// dictOrderGolden is the spec of the frozen dict-order oracle. Its Scrub also turns the dates and the times a plane reads from the clock
// during the run (every one in [dictOrderClockFloor, 2100)) into placeholders: the windows of these routes are "the last 7 days", so the recorded
// answers hold the recording day's dates and a later run would not.
func dictOrderGolden(test, pin string) venueoracle.GoldenSpec {
	spec := venueGolden("dict-order", test, pin)
	spec.Scrub = dictOrderScrub
	return spec
}

// dictOrderClockFloor is the oldest instant a plane reads from the clock for these routes: 30 days before the run (their windows are 7 and 14 days).
// It is taken at the run, so the scrub is as narrow as the answers allow: a date or time before it stays as written and is compared by value.
func dictOrderClockFloor() time.Time {
	return time.Now().UTC().AddDate(0, 0, -30).Truncate(24 * time.Hour)
}

var dictOrderDate = regexp.MustCompile(`\b(20\d\d)-(\d\d)-(\d\d)\b`)

// dictOrderScrub keeps the placeholders of runValueScrub (and its fractional/offset shapes) for the times at or after the run-value floor, extends
// them to the times from dictOrderClockFloor on, and turns a plain date of that window into "<date>". It is deterministic and idempotent.
func dictOrderScrub(text string) string {
	text = oracleTime.ReplaceAllStringFunc(text, func(stamp string) string {
		parsed, err := time.Parse(time.RFC3339Nano, stamp)
		if err != nil || parsed.Before(dictOrderClockFloor()) || !parsed.Before(runValueFloor) {
			return stamp
		}
		shape := "nofrac"
		if strings.Contains(stamp, ".") {
			shape = "frac"
		}
		offset := "Z"
		if strings.HasSuffix(stamp, "+00:00") {
			offset = "+00:00"
		} else if !strings.HasSuffix(stamp, "Z") {
			offset = "other"
		}
		return "<ts:" + shape + ":" + offset + ">"
	})
	text = runValueScrub(text)
	return dictOrderDate.ReplaceAllStringFunc(text, func(date string) string {
		parsed, err := time.Parse(time.DateOnly, date)
		if err != nil || parsed.Before(dictOrderClockFloor()) || !parsed.Before(runValueCeiling) {
			return date
		}
		return "<date>"
	})
}

// dictOrderWithoutAddress is text with the run's own database address taken out, for a failure message.
func dictOrderWithoutAddress(text string, venue *venueoracle.Venue, t *testing.T) string {
	return strings.ReplaceAll(text, venue.AdminClickHouseHTTPURI(t, venue.PythonClickHouseDB), "<CLICKHOUSE_URI>")
}

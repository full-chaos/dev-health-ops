//go:build integration

package server

import (
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
	// retired: the route is no longer served; the recorded Python request
	// keeps the case so the golden stays valid until it is recorded again.
	retired bool
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
		Golden: golden, Root: root,
		// No route here checks a token; the key is the one the other venue oracles of this package use.
		JWTKey: oracleJWTKey,
	})

	// One identity with a work-item metrics row on both databases, so the
	// person summary finds its person (person_lookup.sql reads
	// user_metrics_daily and work_item_user_metrics_daily; the person id is
	// md5 of the identity).
	identity := "venue-dict-order@example.com"
	personID := fmt.Sprintf("%x", md5.Sum([]byte(identity)))
	for _, database := range []string{venue.GoClickHouseDB} {
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
	for _, database := range []string{venue.GoClickHouseDB} {
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
	// CHAOS-9695 (D5999, D6014): the list is ordered by the work-unit count, then the end of the unit, then the id (it was
	// ordered by effort_value, 10 / 5 / 4 here). Every seeded unit counts 1, so the later two end one and two seconds
	// earlier: the order stays the one the frozen Python answer has, and the span of each unit stays the same.
	shifted := func(base time.Time, seconds int) string {
		return base.Add(-time.Duration(seconds) * time.Second).Format("2006-01-02 15:04:05")
	}
	fromTyped, toTyped := shifted(seedNow.AddDate(0, 0, -3), 1), shifted(seedNow.AddDate(0, 0, -1), 1)
	fromNull, toNull := shifted(seedNow.AddDate(0, 0, -3), 2), shifted(seedNow.AddDate(0, 0, -1), 2)
	seedWorkUnits := func(databases []string) {
		for _, database := range databases {
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
					workUnitID, fromTyped, toTyped, orgID, workUnitID, fromNull, toNull, orgID),
				fmt.Sprintf(`INSERT INTO work_unit_investment_quotes (work_unit_id, quote, source_type, source_id, computed_at, categorization_run_id, org_id)
			VALUES ('%s', 'Fix the flaky login test.', 'pr_body', 'pr-7', now64(3), 'run-1', '%s')`, workUnitID, orgID),
			} {
				if err := conn.Exec(ctx, statement); err != nil {
					t.Fatalf("seed %s: %v\n%s", database, err, statement)
				}
			}
			_ = conn.Close()
		}
	}
	goUnitsSeeded := false

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
		"GET /api/v1/flame/aggregated":                   newFlameAggregatedWorkHandler(client),
		"GET /api/v1/people/{person_id}/summary":         newPeopleSummaryHandler(peopleReader),
		"GET /api/v1/work-units":                         newWorkUnitsGetHandler(workUnitsReader),
		"POST /api/v1/work-units/{work_unit_id}/explain": newWorkUnitExplainHandler(workUnitsReader, nil, nil, nil, testPlatformGate{}),
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
		{Name: "investment flow coverage", Route: "POST /api/v1/investment/flow", Kind: "flow", retired: true,
			Args: map[string]any{"filters": window, "flow_mode": "team_category_repo"}},
		// No cycle_breakdown case: this oracle's Python side, run on a migrated
		// database, answers 503 for an empty window (its fallback read of
		// work_item_cycle_milestones_daily, a table no ClickHouse migration
		// creates), while query-api answers the empty tree (CHAOS-6606), so there
		// is nothing to compare here. The recorded Python scenario of that
		// fallback (testdata/cycle_breakdown_milestone_fallback.json in aggflame)
		// is kept and its one declared difference is held by
		// TestGoldenCycleBreakdownMilestoneFallbackIsADeclaredDifference. The
		// filters order is pinned by a unit test.
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
	// The Home cases run first: Go Home reads the canonical categorization and the frozen Python
	// Home its legacy table, so the Go work units are seeded only after every Home case.
	order := make([]int, 0, len(cases))
	for index, tc := range cases {
		if tc.Kind == "home" {
			order = append(order, index)
		}
	}
	for index, tc := range cases {
		if tc.Kind != "home" {
			order = append(order, index)
		}
	}
	for _, index := range order {
		tc := cases[index]
		if tc.retired {
			continue
		}
		if !goUnitsSeeded && tc.Kind != "home" {
			seedWorkUnits([]string{venue.GoClickHouseDB})
			goUnitsSeeded = true
		}
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
			stripped, err := withoutExplainGoOnlyFields(venueGoBody(goWritten))
			if err != nil {
				t.Fatalf("%s: %v", tc.Name, err)
			}
			goWritten = stripped
		}
		if tc.Kind == "home" && recorder.Code == http.StatusOK {
			// Every REST Home delta ends with the Go-only keys of CHAOS-9044,
			// and the body itself ends with filter_empty_reason (CHAOS-9098),
			// which the frozen Python model never had: compared without
			// them, and only them (withoutHomeDeltaGoOnlyFields; a delta
			// or a body that lacks one fails).
			stripped, err := withoutHomeDeltaGoOnlyFields(venueGoBody(goWritten))
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
			// CHAOS-9695 (D5999, D6014): the frozen Python answer carries the unit's effort as lines of code
			// ({"metric": "churn_loc", "value": 10.0}); Go serves the work-unit count. Both are replaced by one
			// placeholder, and the seeded unit must be served as one work unit, so the comparison still reads every
			// other key and the key order.
			const goEffort = `"effort":{"metric":"work_units","value":1.0}`
			if !strings.Contains(goWritten, goEffort) {
				t.Fatalf("%s: the seeded unit is not served as one work unit (%s): %s", tc.Name, goEffort, goWritten)
			}
			goWritten = strings.ReplaceAll(goWritten, goEffort, declaredWorkUnitEffort)
			// The lines-of-code fact (CHAOS-9695, Go-only, last key of each unit): the three seeded units stored churn 10, 5
			// and 4, the split is not stored per unit. The frozen Python model never had the key: compared without it,
			// and only it.
			if got := len(goOnlyChangeSet.FindAllString(goWritten, -1)); got != 3 || !strings.Contains(goWritten, `,"change_set":{"additions":null,"deletions":null,"total":10}`) {
				t.Fatalf("%s: want a change_set on each of the three seeded units, the first with total 10: %s", tc.Name, goWritten)
			}
			goWritten = goOnlyChangeSet.ReplaceAllString(goWritten, "")
		}
		answerBody := answer.Body
		if tc.Kind == "work_units" {
			answerBody = pythonLinesOfCodeEffort.ReplaceAllString(answerBody, declaredWorkUnitEffort)
		}
		if tc.Kind == "investment" && recorder.Code == http.StatusOK && answer.Status == http.StatusOK {
			// CHAOS-9622 (D5999): Go reads the COUNT of the units, the frozen Python answer weighs them by
			// effort_value. The key order of the answer is compared; the values of the two distributions are
			// zeroed on both sides, and Go's own are asserted against the COUNT of the seeded units: the stored
			// distribution of the seeded unit (quality 0.6, feature_delivery 0.4) plus the two quality-only units.
			goOut, pythonOut, values, err := investmentForOrderComparison(goWritten, answerBody)
			if err != nil {
				t.Errorf("%s: %v", tc.Name, err)
				continue
			}
			for distribution, wants := range map[string]map[string]float64{
				"theme_distribution":       {"quality": 2.6, "feature_delivery": 0.4},
				"subcategory_distribution": {"quality.bugfix": 2.6, "feature_delivery.customer": 0.4},
			} {
				for key, want := range wants {
					if got := values.Distribution[distribution][key]; got < want-1e-3 || got > want+1e-3 {
						t.Errorf("%s: Go %s[%s] = %v, want the COUNT %v", tc.Name, distribution, key, got, want)
					}
				}
			}
			if values.Denominator < 3-1e-3 || values.Denominator > 3+1e-3 {
				t.Errorf("%s: Go denominator = %v, want 3", tc.Name, values.Denominator)
			}
			goWritten, answerBody = goOut, pythonOut
		}
		goBody := golden.Project(t, venueoracle.Response{Status: recorder.Code, Body: goWritten}).Body
		pythonBody := golden.Project(t, venueoracle.Response{Status: answer.Status, Body: answerBody}).Body
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

func runDictOrderPython(t *testing.T, golden *venueoracle.Golden, venue *venueoracle.Venue, root string, cases []dictOrderCase) []dictOrderPythonAnswer {
	t.Helper()
	payload, err := json.Marshal(cases)
	if err != nil {
		t.Fatal(err)
	}
	// The launcher form: the harness's own producer starts the program in the closed environment; the address of the run's own Python
	// ClickHouse database is the one per-run entry, passed as an extra (it is not part of the request's key).
	request := venueoracle.ProgramRequest("dict order services", payload)
	answers := golden.Produce(t, []venueoracle.Request{request})
	golden.Consumed(t, answers...)
	if answers[0].Status != 0 {
		t.Fatalf("the python program exited %d when it was recorded", answers[0].Status)
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

// declaredWorkUnitEffort stands for a work unit's effort on both sides of the work-units comparison (CHAOS-9695,
// D6014): Python's lines of code and Go's work-unit count are different quantities by ruling.
const declaredWorkUnitEffort = `"effort":"<declared D6014: work-unit count>"`

var pythonLinesOfCodeEffort = regexp.MustCompile(`"effort":\{"metric":"(?:churn_loc|active_hours)","value":[0-9.eE+-]+\}`)

var goOnlyChangeSet = regexp.MustCompile(`,"change_set":\{"additions":null,"deletions":null,"total":[0-9]+\}`)

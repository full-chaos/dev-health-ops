package server

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/aggflame"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/drilldown"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/explain"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/filteroptions"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/flame"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/heatmap"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/investment"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/investmentexplain"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/meta"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/opportunities"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/people"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/quadrant"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/sankey"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/workunitexplain"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

// pythonResponseModelProgram reads the FastAPI app. Its input is {"mode",
// "input"}. "table" prints
// every APIRoute's response_model flag (fastapi/routing.py use_dump_json)
// and, for the keys on stdin, the response field's validation JSON schema.
// "render" validates each [key, JSON data] pair with the route's response
// field and writes it as FastAPI does on the response_model path:
// field.serialize_json with the route's response_model_* settings
// (fastapi/routing.py serialize_response, dump_json=True).
const pythonResponseModelProgram = `
import base64, contextlib, json, sys
from fastapi.routing import APIRoute
from fastapi.datastructures import DefaultPlaceholder
from pydantic import TypeAdapter
# The app logs a line with the time of the run while it is imported: it goes
# to stderr, so the answer is the RESULT line alone.
with contextlib.redirect_stdout(sys.stderr):
    from dev_health_ops.api.main import app
routes = {}
for route in app.routes:
    if isinstance(route, APIRoute):
        for method in route.methods:
            routes[(method, route.path)] = route
spec = json.loads(sys.stdin.read())
mode, request = spec["mode"], spec["input"]
SENTINELS = {"\u0000NaN": float("nan"), "\u0000Infinity": float("inf"), "\u0000-Infinity": float("-inf")}
if mode == "table":
    table = [[m, p, r.response_field is not None and isinstance(r.response_class, DefaultPlaceholder)] for (m, p), r in routes.items()]
    schemas = {}
    for key in request:
        route = routes.get(tuple(key.split(" ", 1)))
        if route is not None and route.response_field is not None:
            schemas[key] = TypeAdapter(route.response_field.field_info.annotation).json_schema(mode="validation")
    print("RESULT " + json.dumps({"routes": table, "schemas": schemas}))
else:
    out = []
    for key, data in request:
        route = routes[tuple(key.split(" ", 1))]
        field = route.response_field
        def restore(item):
            if isinstance(item, str) and item in SENTINELS:
                return SENTINELS[item]
            if isinstance(item, list):
                return [restore(x) for x in item]
            if isinstance(item, dict):
                return {k: restore(v) for k, v in item.items()}
            return item
        value, errors = field.validate(restore(json.loads(data)), {}, loc=("response",))
        if errors:
            out.append({"error": repr(errors)[:3000]})
            continue
        body = field.serialize_json(
            value,
            include=route.response_model_include,
            exclude=route.response_model_exclude,
            by_alias=route.response_model_by_alias,
            exclude_unset=route.response_model_exclude_unset,
            exclude_defaults=route.response_model_exclude_defaults,
            exclude_none=route.response_model_exclude_none,
        )
        out.append({"body": base64.b64encode(body).decode()})
    print("RESULT " + json.dumps(out))
`

// responseModelOracleRoute is one response_model route's Go response type
// and how the production code writes it.
type responseModelOracleRoute struct {
	// response is a nil pointer to (or zero value of) the type the route's
	// handler hands its writer.
	response any
	// write, when set, replaces writeModelResponse (the investment
	// explain stream's final body).
	write func(value any) ([]byte, error)
	// data, when set, is the JSON the Python model reads, in place of the
	// value itself (the investment explain body, whose encoder reshapes
	// the Go value).
	data func(value any) (string, error)
}

// opportunitiesPythonCard and opportunitiesPythonResponse are the shape the
// frozen FastAPI model of /api/v1/opportunities has: the five fields the
// Python reference served. The Go response (opportunities.Card) has four more
// since CHAOS-8109 (change_percent, direction, range_days, compare_days), which
// the Python model never had. The frozen program was recorded for the Python
// shape and nothing is recorded again, so the oracle below still checks THAT
// shape, written by the production writer: the five shared fields are byte
// identical to FastAPI's. It says nothing about the four Go-only fields.
//
// TestOpportunitiesCardIsThePythonCardPlusTheDeclaredGoOnlyFields ties this
// shape to the production type: the production card must be exactly these
// five fields, in this order, followed by exactly the four declared ones. A
// field added, removed, renamed or moved in either type fails it.
type opportunitiesPythonCard struct {
	ID                   string   `json:"id"`
	Title                string   `json:"title"`
	Rationale            string   `json:"rationale"`
	EvidenceLinks        []string `json:"evidence_links"`
	SuggestedExperiments []string `json:"suggested_experiments"`
}

type opportunitiesPythonResponse struct {
	Items []opportunitiesPythonCard `json:"items"`
}

// issuesPythonResponse is the frozen FastAPI response shape for the issue
// drilldown. The Go-only Count field is emitted only for the new
// filters.how.blocked=true contract (CHAOS-8106); ordinary issue drilldowns
// retain this Python shape. The declaration test below makes that widening
// explicit instead of treating the frozen model as an API ceiling.
type issuesPythonResponse struct {
	Items []drilldown.IssueItem `json:"items"`
}

func TestIssuesResponseIsThePythonResponsePlusBlockedCount(t *testing.T) {
	type field struct{ name, goType, tag string }
	fieldsOf := func(typ reflect.Type) []field {
		fields := make([]field, 0, typ.NumField())
		for index := range typ.NumField() {
			f := typ.Field(index)
			fields = append(fields, field{f.Name, f.Type.String(), string(f.Tag)})
		}
		return fields
	}

	want := append(fieldsOf(reflect.TypeOf(issuesPythonResponse{})), field{"Count", "*uint64", `json:"count,omitempty"`})
	if got := fieldsOf(reflect.TypeOf(drilldown.IssuesResponse{})); !reflect.DeepEqual(got, want) {
		t.Errorf("drilldown.IssuesResponse fields =\n %v\nwant the Python fields followed by blocked-only Count:\n %v", got, want)
	}

	count := uint64(1)
	recorder := httptest.NewRecorder()
	if err := writeModelResponse(recorder, &drilldown.IssuesResponse{Items: []drilldown.IssueItem{}, Count: &count}); err != nil {
		t.Fatalf("writeModelResponse: %v", err)
	}
	if body := recorder.Body.String(); !strings.Contains(body, `"count":1`) {
		t.Errorf("the production body has no blocked-only count: %s", body)
	}
}

// opportunitiesGoOnlyCardFields are the fields of opportunities.Card the
// Python model does not have (CHAOS-8109), in declaration order.
var opportunitiesGoOnlyCardFields = []struct{ name, goType, tag string }{
	{"ChangePercent", "*float64", `json:"change_percent"`},
	{"Direction", "*string", `json:"direction"`},
	{"RangeDays", "int", `json:"range_days"`},
	{"CompareDays", "int", `json:"compare_days"`},
}

func TestOpportunitiesCardIsThePythonCardPlusTheDeclaredGoOnlyFields(t *testing.T) {
	type field struct{ name, goType, tag string }
	fieldsOf := func(typ reflect.Type) []field {
		out := make([]field, 0, typ.NumField())
		for index := range typ.NumField() {
			f := typ.Field(index)
			out = append(out, field{f.Name, f.Type.String(), string(f.Tag)})
		}
		return out
	}

	want := fieldsOf(reflect.TypeOf(opportunitiesPythonCard{}))
	for _, goOnly := range opportunitiesGoOnlyCardFields {
		want = append(want, field{goOnly.name, goOnly.goType, goOnly.tag})
	}
	if got := fieldsOf(reflect.TypeOf(opportunities.Card{})); !reflect.DeepEqual(got, want) {
		t.Errorf("opportunities.Card fields =\n %v\nwant the five Python fields, then the four Go-only ones:\n %v", got, want)
	}

	response := fieldsOf(reflect.TypeOf(opportunities.Response{}))
	if len(response) != 1 || response[0] != (field{"Items", "[]opportunities.Card", `json:"items"`}) {
		t.Errorf("opportunities.Response fields = %v, want Items alone", response)
	}

	// The production writer does write the Go-only fields: the oracle's
	// Python shape must not be read as "the route serves five fields".
	change, direction := 12.5, opportunities.DirectionUp
	recorder := httptest.NewRecorder()
	if err := writeModelResponse(recorder, &opportunities.Response{Items: []opportunities.Card{{
		ID: "opp-1", Title: "Reduce Cycle Time", Rationale: "r", EvidenceLinks: []string{}, SuggestedExperiments: []string{},
		ChangePercent: &change, Direction: &direction, RangeDays: 30, CompareDays: 14,
	}}}); err != nil {
		t.Fatalf("writeModelResponse: %v", err)
	}
	body := recorder.Body.String()
	for _, part := range []string{`"change_percent":12.5`, `"direction":"up"`, `"range_days":30`, `"compare_days":14`} {
		if !strings.Contains(body, part) {
			t.Errorf("the production body has no %s: %s", part, body)
		}
	}
}

// explainPythonResponse is the typed input for the existing frozen Python
// response-model differential. TestQueryAPIResponseModelsMatchFrozenPython
// validates it against the FastAPI response model. It does not define the
// prefix that withoutExplainGoOnlyFields may remove; that comes directly from
// the frozen producer schema below.
type explainPythonResponse struct {
	Metric         string                    `json:"metric"`
	Label          string                    `json:"label"`
	Unit           string                    `json:"unit"`
	Value          float64                   `json:"value"`
	DeltaPct       float64                   `json:"delta_pct"`
	Drivers        []explain.Contributor     `json:"drivers"`
	Contributors   []explain.Contributor     `json:"contributors"`
	DrilldownLinks pyjson.OrderedMap[string] `json:"drilldown_links"`
}

// explainGoOnlyResponseFields are the fields of explain.Response the Python
// model does not have, in declaration order.
var explainGoOnlyResponseFields = []struct{ name, goType, tag string }{
	{"Repositories", "*[]explain.Repository", `json:"repositories"`},
	{"SourceURL", "*string", `json:"source_url"`},
	{"HasData", "bool", `json:"has_data"`},
	{"HasPriorData", "bool", `json:"has_prior_data"`},
}

func TestExplainResponseIsThePythonResponsePlusTheDeclaredGoOnlyFields(t *testing.T) {
	type field struct{ name, goType, tag string }
	fieldsOf := func(typ reflect.Type) []field {
		out := make([]field, 0, typ.NumField())
		for index := range typ.NumField() {
			f := typ.Field(index)
			out = append(out, field{f.Name, f.Type.String(), string(f.Tag)})
		}
		return out
	}
	pythonFields := fieldsOf(reflect.TypeOf(explainPythonResponse{}))
	want := append([]field(nil), pythonFields...)
	for _, goOnly := range explainGoOnlyResponseFields {
		want = append(want, field{goOnly.name, goOnly.goType, goOnly.tag})
	}
	got := fieldsOf(reflect.TypeOf(explain.Response{}))
	if !reflect.DeepEqual(got, want) {
		t.Errorf("explain.Response fields =\n %v\nwant the typed differential fields, then the declared Go-only tail:\n %v", got, want)
	}

	// The production writer does write the Go-only fields, null included:
	// the oracle's Python shape must not be read as "the route serves eight
	// fields".
	name, sourceURL := "webapp", "https://github.com/acme/webapp"
	repositories := []explain.Repository{{ID: "repo-a", Name: &name, Value: 3, SourceURL: &sourceURL}, {ID: "repo-b", Value: 1}}
	for _, testCase := range []struct {
		response explain.Response
		parts    []string
	}{
		{explain.Response{Drivers: []explain.Contributor{}, Contributors: []explain.Contributor{}, Repositories: &repositories, SourceURL: &sourceURL, HasData: true, HasPriorData: true}, []string{
			`"repositories":[{"id":"repo-a","name":"webapp","value":3.0,"source_url":"https://github.com/acme/webapp"},{"id":"repo-b","name":null,"value":1.0,"source_url":null}]`,
			`"source_url":"https://github.com/acme/webapp","has_data":true,"has_prior_data":true}`,
		}},
		{explain.Response{Drivers: []explain.Contributor{}, Contributors: []explain.Contributor{}}, []string{`"repositories":null,"source_url":null,"has_data":false,"has_prior_data":false}`}},
	} {
		recorder := httptest.NewRecorder()
		if err := writeModelResponse(recorder, &testCase.response); err != nil {
			t.Fatalf("writeModelResponse: %v", err)
		}
		body := recorder.Body.String()
		for _, part := range testCase.parts {
			if !strings.Contains(body, part) {
				t.Errorf("the production body has no %s: %s", part, body)
			}
		}
	}
}

func explainPythonResponseFieldNames() []string {
	typ := reflect.TypeOf(explainPythonResponse{})
	fields := make([]string, 0, typ.NumField())
	for index := range typ.NumField() {
		tag := typ.Field(index).Tag.Get("json")
		name, _, _ := strings.Cut(tag, ",")
		fields = append(fields, name)
	}
	return fields
}

// assertExplainTailMatchesProducerModel derives the legacy prefix directly
// from the frozen FastAPI ExplainResponse schema recorded by this test's real
// producer. The typed Go differential input and the tail stripper must agree
// with that model. A changed or undeclared legacy field is rejected before
// any Go-only field can be stripped.
func assertExplainTailMatchesProducerModel(t *testing.T, rawSchema json.RawMessage) {
	t.Helper()
	var schema struct {
		Properties pyjson.OrderedMap[json.RawMessage] `json:"properties"`
	}
	if err := json.Unmarshal(rawSchema, &schema); err != nil {
		t.Fatalf("decode frozen FastAPI ExplainResponse schema: %v", err)
	}
	producerFields := schema.Properties.Keys()
	if len(producerFields) == 0 {
		t.Fatalf("frozen FastAPI ExplainResponse schema has no ordered properties: %s", rawSchema)
	}
	if legacyFields := explainPythonResponseFieldNames(); !reflect.DeepEqual(legacyFields, producerFields) {
		t.Fatalf("typed legacy fields = %v, want frozen FastAPI ExplainResponse fields %v", legacyFields, producerFields)
	}

	var prefix strings.Builder
	prefix.WriteByte('{')
	for index, name := range producerFields {
		if index > 0 {
			prefix.WriteByte(',')
		}
		fmt.Fprintf(&prefix, "%q:null", name)
	}
	valid := prefix.String() + `,"repositories":null,"source_url":null,"has_data":false,"has_prior_data":false}`
	if got, err := withoutExplainGoOnlyFields(valid); err != nil || got != prefix.String()+`}` {
		t.Fatalf("withoutExplainGoOnlyFields(valid producer body) = %s, %v; want %s", got, err, prefix.String()+`}`)
	}
	for _, body := range []string{
		strings.Replace(valid, fmt.Sprintf(`"%s":null`, producerFields[0]), `"changed_legacy_field":null`, 1),
		strings.Replace(valid, `,"repositories":`, `,"undeclared_legacy_field":null,"repositories":`, 1),
	} {
		if got, err := withoutExplainGoOnlyFields(body); err == nil {
			t.Fatalf("withoutExplainGoOnlyFields(%s) = %s, want an error", body, got)
		}
	}
}

// withoutExplainGoOnlyFields returns an explain body as the production writer
// writes it, without its declared Go-only fields (explainGoOnlyResponseFields),
// so that it can be compared byte for byte with a frozen Python body. The
// prefix must be exactly the typed legacy response used by the frozen
// producer differential, and the tail must hold exactly the declared Go-only
// fields, both in order. The differential verifies that legacy shape against
// the frozen FastAPI producer model.
func withoutExplainGoOnlyFields(body string) (string, error) {
	jsonName := func(tag string) string {
		name, _, _ := strings.Cut(strings.TrimSuffix(strings.TrimPrefix(tag, `json:"`), `"`), ",")
		return name
	}
	first := jsonName(explainGoOnlyResponseFields[0].tag)
	marker := `,"` + first + `":`
	at := strings.LastIndex(body, marker)
	if at < 0 || !strings.HasSuffix(body, "}") {
		return "", fmt.Errorf("explain body has no %s tail: %s", first, body)
	}
	prefix := body[:at] + "}"
	decoder := json.NewDecoder(strings.NewReader(prefix))
	opening, err := decoder.Token()
	if err != nil {
		return "", fmt.Errorf("decode explain body prefix: %w", err)
	}
	if opening != json.Delim('{') {
		return "", fmt.Errorf("explain body prefix opens with %v, want {", opening)
	}
	var prefixFields []string
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			return "", err
		}
		name, ok := key.(string)
		if !ok {
			return "", fmt.Errorf("explain body prefix key is %T, want string", key)
		}
		prefixFields = append(prefixFields, name)
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return "", err
		}
	}
	closing, err := decoder.Token()
	if err != nil {
		return "", fmt.Errorf("decode explain body prefix closing: %w", err)
	}
	if closing != json.Delim('}') {
		return "", fmt.Errorf("explain body prefix closes with %v, want }", closing)
	}
	if legacyFields := explainPythonResponseFieldNames(); !reflect.DeepEqual(prefixFields, legacyFields) {
		return "", fmt.Errorf("explain body legacy fields = %v, want typed frozen-producer differential fields %v", prefixFields, legacyFields)
	}
	tail := "{" + body[at+1:]
	decoder = json.NewDecoder(strings.NewReader(tail))
	if _, err := decoder.Token(); err != nil {
		return "", err
	}
	for index, field := range explainGoOnlyResponseFields {
		key, err := decoder.Token()
		if err != nil {
			return "", err
		}
		if want := jsonName(field.tag); key != want {
			return "", fmt.Errorf("explain body tail field %d is %v, want %q: %s", index, key, jsonName(field.tag), tail)
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return "", err
		}
	}
	if closing, err := decoder.Token(); err != nil || closing != json.Delim('}') || decoder.More() {
		return "", fmt.Errorf("explain body tail holds more than the Go-only fields: %s", tail)
	}
	return prefix, nil
}

func TestWithoutExplainGoOnlyFieldsLeavesThePythonShapeOrFails(t *testing.T) {
	producerFields := explainPythonResponseFieldNames()
	var prefix strings.Builder
	prefix.WriteByte('{')
	for index, name := range producerFields {
		if index > 0 {
			prefix.WriteByte(',')
		}
		fmt.Fprintf(&prefix, "%q:null", name)
	}
	valid := prefix.String() + `,"repositories":null,"source_url":null,"has_data":false,"has_prior_data":false}`
	for _, testCase := range []struct{ body, want string }{
		{valid, prefix.String() + `}`},
	} {
		got, err := withoutExplainGoOnlyFields(testCase.body)
		if err != nil || got != testCase.want {
			t.Fatalf("withoutExplainGoOnlyFields(%s) = %s, %v; want %s", testCase.body, got, err, testCase.want)
		}
	}
	for _, body := range []string{
		strings.Replace(valid, fmt.Sprintf(`"%s":null`, producerFields[0]), `"changed_legacy_field":null`, 1),
		strings.Replace(valid, `,"repositories":`, `,"undeclared_legacy_field":null,"repositories":`, 1),
		strings.Replace(valid, `,"has_prior_data":false`, ``, 1),
		strings.Replace(valid, `,"repositories":null,"source_url":null`, `,"source_url":null,"repositories":null`, 1),
	} {
		if got, err := withoutExplainGoOnlyFields(body); err == nil {
			t.Fatalf("withoutExplainGoOnlyFields(%s) = %s, want an error", body, got)
		}
	}
}

func responseModelOracleRoutes() map[string]responseModelOracleRoute {
	plain := func(response any) responseModelOracleRoute { return responseModelOracleRoute{response: response} }
	sankeyRoute := plain((*sankey.Response)(nil))
	return map[string]responseModelOracleRoute{
		"GET /api/v1/meta":                                plain(meta.Response{}),
		"GET /api/v1/home":                                plain(homeRESTResponse{}),
		"POST /api/v1/home":                               plain(homeRESTResponse{}),
		"GET /api/v1/explain":                             plain((*explainPythonResponse)(nil)),
		"POST /api/v1/explain":                            plain((*explainPythonResponse)(nil)),
		"GET /api/v1/heatmap":                             plain((*heatmap.Response)(nil)),
		"GET /api/v1/work-units":                          plain([]workUnitInvestmentWire(nil)),
		"POST /api/v1/work-units":                         plain([]workUnitInvestmentWire(nil)),
		"POST /api/v1/work-units/{work_unit_id}/explain":  plain((*workunitexplain.Explanation)(nil)),
		"GET /api/v1/flame":                               plain((*flame.Response)(nil)),
		"GET /api/v1/flame/aggregated":                    plain((*aggflame.Response)(nil)),
		"GET /api/v1/quadrant":                            plain((*quadrant.Response)(nil)),
		"GET /api/v1/drilldown/prs":                       plain((*drilldown.PRsResponse)(nil)),
		"POST /api/v1/drilldown/prs":                      plain((*drilldown.PRsResponse)(nil)),
		"GET /api/v1/drilldown/issues":                    plain((*issuesPythonResponse)(nil)),
		"POST /api/v1/drilldown/issues":                   plain((*issuesPythonResponse)(nil)),
		"GET /api/v1/people":                              plain([]people.SearchResult(nil)),
		"GET /api/v1/people/{person_id}/summary":          plain(people.SummaryResponse{}),
		"GET /api/v1/people/{person_id}/metric":           plain(people.MetricResponse{}),
		"GET /api/v1/people/{person_id}/drilldown/prs":    plain((*people.DrilldownPRsResponse)(nil)),
		"GET /api/v1/people/{person_id}/drilldown/issues": plain((*people.DrilldownIssuesResponse)(nil)),
		"GET /api/v1/opportunities":                       plain((*opportunitiesPythonResponse)(nil)),
		"POST /api/v1/opportunities":                      plain((*opportunitiesPythonResponse)(nil)),
		"GET /api/v1/investment":                          plain((*investment.Response)(nil)),
		"POST /api/v1/investment":                         plain((*investment.Response)(nil)),
		"GET /api/v1/investment/sunburst":                 plain([]investment.SunburstSlice(nil)),
		"POST /api/v1/investment/explain": {
			response: investmentexplain.InvestmentMixExplanation{},
			data: func(value any) (string, error) {
				return investmentexplain.EncodeInvestmentMixExplanation(value.(investmentexplain.InvestmentMixExplanation))
			},
			write: func(value any) ([]byte, error) {
				body, err := investmentexplain.EncodeInvestmentMixExplanation(value.(investmentexplain.InvestmentMixExplanation))
				if err != nil {
					return nil, err
				}
				var out bytes.Buffer
				if err := writeStreamedModelBody(&out, []byte(body)); err != nil {
					return nil, err
				}
				return bytes.TrimSuffix(out.Bytes(), []byte("\n")), nil
			},
		},
		"POST /api/v1/investment/flow":           sankeyRoute,
		"POST /api/v1/investment/flow/repo-team": sankeyRoute,
		"GET /api/v1/sankey":                     sankeyRoute,
		"POST /api/v1/sankey":                    sankeyRoute,
		"GET /api/v1/filters/options":            plain(filteroptions.Response{}),
	}
}

// TestQueryAPIResponseModelsMatchFrozenPython checks two things against the
// frozen answers of the FastAPI app of the pinned build. The route table: responseModelRoutes names exactly the
// response_model routes on the paths the query-api serves. The bytes: for
// each such route, a value of its Go response type, filled from the
// route's pydantic schema with floats at the format edges, is written by
// the production writer and by FastAPI's own response_model path from the
// same data, and the two bodies must be byte-identical. That also pins
// field order, int-versus-float field types, and fields the model drops.
func TestQueryAPIResponseModelsMatchFrozenPython(t *testing.T) {
	// Each mode is its own golden: the input of "render" is built from the
	// answer of "table".
	runPython := func(mode string, input any) json.RawMessage {
		t.Helper()
		payload, err := json.Marshal(map[string]any{"mode": mode, "input": input})
		if err != nil {
			t.Fatal(err)
		}
		output := frozenHomeResponseModelPython(t, "query-response-models-"+mode+".golden.json", programoracle.Program{Name: "response models " + mode, Text: pythonResponseModelProgram, Stdin: payload})[0]
		for _, line := range strings.Split(output, "\n") {
			if rest, ok := strings.CutPrefix(line, "RESULT "); ok {
				return json.RawMessage(rest)
			}
		}
		t.Fatalf("python %s printed no RESULT line when it was recorded:\n%s", mode, output)
		return nil
	}

	oracleRoutes := responseModelOracleRoutes()
	var keys []string
	for key := range responseModelRoutes {
		keys = append(keys, key)
		if _, ok := oracleRoutes[key]; !ok {
			t.Errorf("%s is a response_model route with no Go response type in responseModelOracleRoutes", key)
		}
	}
	sort.Strings(keys)
	tableBody := runPython("table", keys)
	var table struct {
		Routes  [][3]any                  `json:"routes"`
		Schemas map[string]map[string]any `json:"schemas"`
	}
	if err := json.Unmarshal(tableBody, &table); err != nil {
		t.Fatal(err)
	}
	var rawTable struct {
		Schemas map[string]json.RawMessage `json:"schemas"`
	}
	if err := json.Unmarshal(tableBody, &rawTable); err != nil {
		t.Fatal(err)
	}
	explainSchema, ok := rawTable.Schemas["GET /api/v1/explain"]
	if !ok {
		t.Fatal("frozen FastAPI response-model table has no GET /api/v1/explain schema")
	}
	assertExplainTailMatchesProducerModel(t, explainSchema)

	// The route table, both directions, on the paths the query-api serves.
	served := map[string]bool{}
	for _, route := range RESTRoutes() {
		served[route.Pattern] = true
	}
	pythonModel := map[string]bool{}
	for _, row := range table.Routes {
		key := row[0].(string) + " " + row[1].(string)
		model := row[2].(bool)
		pythonModel[key] = model
		if model && served[row[1].(string)] && !responseModelRoutes[key] {
			t.Errorf("%s: FastAPI writes it as a response_model and the query-api serves the path, but responseModelRoutes does not name it", key)
		}
	}
	for _, key := range keys {
		if !pythonModel[key] {
			t.Errorf("%s: responseModelRoutes names it, but the frozen FastAPI app has no response_model route for it", key)
		}
	}
	if len(served) == 0 || len(table.Routes) == 0 {
		t.Fatalf("empty comparison: %d served paths, %d FastAPI routes", len(served), len(table.Routes))
	}

	// A nil slice or map means null only on a field the model declares
	// Optional: every plain slice or map field's pyjson:"nullable" tag must
	// say what the model says (FromGoModel writes nil as empty otherwise).
	nullableFields := 0
	for _, key := range keys {
		if schema := table.Schemas[key]; schema != nil {
			nullableFields += checkNullableTags(t, key, reflect.TypeOf(oracleRoutes[key].response), schema, &schemaFiller{defs: definitions(schema)}, 0)
		}
	}
	if nullableFields == 0 {
		t.Fatalf("no field the models declare as an Optional list or dict was found; the nullable check compared nothing")
	}

	// The bytes.
	type pair struct {
		key     string
		variant int
		goBody  []byte
		data    string
	}
	var pairs []pair
	for _, key := range keys {
		route := oracleRoutes[key]
		schema := table.Schemas[key]
		if schema == nil {
			t.Errorf("%s: the frozen app gave no response schema", key)
			continue
		}
		for variant := range len(responseModelEdgeFloats) + 2 {
			mode := fillFull
			switch variant {
			case len(responseModelEdgeFloats):
				mode = fillEmpty
			case len(responseModelEdgeFloats) + 1:
				mode = fillNil
			}
			filler := &schemaFiller{mode: mode, defs: definitions(schema), offset: variant}
			value := filler.fill(reflect.TypeOf(route.response), schema).Interface()
			var data string
			if route.data != nil {
				text, err := route.data(value)
				if err != nil {
					t.Fatalf("%s: data: %v", key, err)
				}
				data = text
			} else {
				// The Go value's meaning, as the writer reads it (a nil
				// slice is an empty list).
				converted, err := pyjson.FromGoModel(value)
				if err != nil {
					t.Fatalf("%s: FromGoModel: %v", key, err)
				}
				// In the Go value's own key order (a dict field keeps its
				// order through the model), with the non-finite floats
				// json.dumps(allow_nan=False) refuses as sentinels.
				encoded, err := pyjson.Marshal(withNonFiniteSentinels(converted))
				if err != nil {
					t.Fatalf("%s: data: %v", key, err)
				}
				data = string(encoded)
			}
			var goBody []byte
			var err error
			if route.write != nil {
				goBody, err = route.write(value)
				if err != nil {
					t.Fatalf("%s: production writer: %v", key, err)
				}
			} else {
				recorder := httptest.NewRecorder()
				if err := writeModelResponse(recorder, value); err != nil {
					t.Fatalf("%s: writeModelResponse: %v", key, err)
				}
				goBody = recorder.Body.Bytes()
			}
			pairs = append(pairs, pair{key, variant, goBody, data})
		}
	}
	input := make([][2]string, len(pairs))
	for index, item := range pairs {
		input[index] = [2]string{item.key, item.data}
	}
	var rendered []struct {
		Body  string `json:"body"`
		Error string `json:"error"`
	}
	if err := json.Unmarshal(runPython("render", input), &rendered); err != nil {
		t.Fatal(err)
	}
	if len(rendered) != len(pairs) {
		t.Fatalf("python rendered %d of %d bodies", len(rendered), len(pairs))
	}
	same := 0
	for index, item := range pairs {
		if rendered[index].Error != "" {
			t.Errorf("%s variant %d: the pydantic model rejects the Go response's data: %s\n data %s", item.key, item.variant, rendered[index].Error, truncateOracleBody(item.data))
			continue
		}
		want, err := base64.StdEncoding.DecodeString(rendered[index].Body)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(item.goBody, want) {
			at := 0
			for at < len(item.goBody) && at < len(want) && item.goBody[at] == want[at] {
				at++
			}
			from := max(0, at-200)
			t.Errorf("%s variant %d: first difference at byte %d:\n go     …%s\n python …%s", item.key, item.variant, at,
				truncateOracleBody(string(item.goBody[from:])), truncateOracleBody(string(want[from:])))
			continue
		}
		same++
	}
	if same == 0 {
		t.Fatalf("no body compared equal")
	}
	t.Logf("%d routes, %d bodies byte-identical to FastAPI's response_model path", len(keys), same)
}

func truncateOracleBody(text string) string {
	if len(text) > 1500 {
		return text[:1500] + "…"
	}
	return text
}

// responseModelEdgeFloats are the values where Go's, Python's and
// pydantic-core's float spellings part: the positional/exponent cut-overs,
// negative zero, an integral float, long digits, and the non-finite values
// pydantic writes as null.
var responseModelEdgeFloats = []float64{
	0.00001, 0.0001, 0.000123456789, 1e-7, 1e16, 1e21, 123456789012345680000,
	math.Copysign(0, -1), 3, 0.1 + 0.2, 1.5, math.NaN(), math.Inf(1),
}

func definitions(schema map[string]any) map[string]any {
	defs, _ := schema["$defs"].(map[string]any)
	return defs
}

// schemaFiller builds a value of a Go response type, choosing each value so
// the route's pydantic schema accepts it: formats and enums for strings,
// bounds for numbers, and edge floats for every float field.
type schemaFiller struct {
	// mode is how lists, maps, pointers and omitempty fields are filled:
	// fillFull gives each two entries or a value, fillEmpty gives empty
	// lists and maps and nil pointers where the schema allows null, and
	// fillNil leaves every list, map and pointer nil. Those are where
	// encoding/json and pydantic part: null against [] against omitted.
	mode   fillMode
	defs   map[string]any
	offset int
	floats int
	texts  int
	depth  int
}

type fillMode int

const (
	fillFull fillMode = iota
	fillEmpty
	fillNil
)

func (m fillMode) String() string { return [...]string{"full", "empty", "nil"}[m] }

// nullable reports whether the schema (before resolving) admits null.
func nullable(schema map[string]any) bool {
	for _, key := range []string{"anyOf", "oneOf"} {
		if options, ok := schema[key].([]any); ok {
			for _, option := range options {
				if candidate, _ := option.(map[string]any); candidate["type"] == "null" {
					return true
				}
			}
		}
	}
	return schema["type"] == "null"
}

var responseModelFixedTime = time.Date(2026, 1, 2, 3, 4, 5, 123456000, time.UTC)

func (f *schemaFiller) resolve(schema map[string]any) map[string]any {
	for schema != nil {
		ref, ok := schema["$ref"].(string)
		if !ok {
			break
		}
		schema, _ = f.defs[strings.TrimPrefix(ref, "#/$defs/")].(map[string]any)
	}
	for _, key := range []string{"anyOf", "oneOf"} {
		if options, ok := schema[key].([]any); ok {
			for _, option := range options {
				candidate, _ := option.(map[string]any)
				candidate = f.resolve(candidate)
				if candidate["type"] != "null" {
					return candidate
				}
			}
		}
	}
	if all, ok := schema["allOf"].([]any); ok && len(all) == 1 {
		one, _ := all[0].(map[string]any)
		return f.resolve(one)
	}
	return schema
}

func (f *schemaFiller) fill(t reflect.Type, schema map[string]any) reflect.Value {
	out := reflect.New(t).Elem()
	// The response itself is always a value; the modes apply inside it.
	switch kind := t.Kind(); {
	case f.depth == 0:
	case kind == reflect.Pointer:
		if f.mode == fillNil || (f.mode == fillEmpty && nullable(schema)) {
			return out
		}
	case kind == reflect.Slice || kind == reflect.Map:
		if f.mode == fillNil {
			return out
		}
		if f.mode == fillEmpty {
			if t.Kind() == reflect.Slice {
				out.Set(reflect.MakeSlice(t, 0, 0))
			} else {
				out.Set(reflect.MakeMap(t))
			}
			return out
		}
	}
	schema = f.resolve(schema)
	// A recursive type (a tree node's children) stops at an empty list.
	f.depth++
	defer func() { f.depth-- }()
	if f.depth > 12 && (t.Kind() == reflect.Slice || t.Kind() == reflect.Map) {
		if t.Kind() == reflect.Slice {
			out.Set(reflect.MakeSlice(t, 0, 0))
		} else {
			out.Set(reflect.MakeMap(t))
		}
		return out
	}
	timeType := reflect.TypeFor[time.Time]()
	switch {
	case t.ConvertibleTo(timeType) && t.Kind() == reflect.Struct:
		out.Set(reflect.ValueOf(responseModelFixedTime).Convert(t))
		return out
	}
	switch t.Kind() {
	case reflect.Pointer:
		out.Set(f.fill(t.Elem(), schema).Addr())
	case reflect.Struct:
		properties, _ := schema["properties"].(map[string]any)
		for index := range t.NumField() {
			field := t.Field(index)
			if !field.IsExported() {
				continue
			}
			name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
			if name == "-" {
				continue
			}
			if name == "" {
				name = field.Name
			}
			var fieldSchema map[string]any
			if field.Anonymous && field.Tag.Get("json") == "" {
				fieldSchema = schema
			} else {
				fieldSchema, _ = properties[name].(map[string]any)
			}
			out.Field(index).Set(f.fill(field.Type, fieldSchema))
		}
	case reflect.Slice:
		items, _ := schema["items"].(map[string]any)
		slice := reflect.MakeSlice(t, 2, 2)
		for index := range 2 {
			slice.Index(index).Set(f.fill(t.Elem(), items))
		}
		out.Set(slice)
	case reflect.Array:
		items, _ := schema["items"].(map[string]any)
		for index := range t.Len() {
			out.Index(index).Set(f.fill(t.Elem(), items))
		}
	case reflect.Map:
		values, _ := schema["additionalProperties"].(map[string]any)
		result := reflect.MakeMap(t)
		key := reflect.New(t.Key()).Elem()
		if key.Kind() == reflect.String {
			key.SetString("k")
		}
		result.SetMapIndex(key, f.fill(t.Elem(), values))
		out.Set(result)
	case reflect.Interface:
		if value := f.dynamic(schema); value != nil {
			out.Set(reflect.ValueOf(value))
		}
	case reflect.String:
		out.SetString(f.text(schema))
	case reflect.Bool:
		out.SetBool(true)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		out.SetInt(f.integer(schema))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		out.SetUint(uint64(f.integer(schema)))
	case reflect.Float32, reflect.Float64:
		out.SetFloat(f.float(schema))
	}
	return out
}

// dynamic is a value for an `any` field, from the schema's type.
func (f *schemaFiller) dynamic(schema map[string]any) any {
	switch schema["type"] {
	case "number":
		return f.float(schema)
	case "integer":
		return f.integer(schema)
	case "boolean":
		return true
	case "array":
		items, _ := schema["items"].(map[string]any)
		return []any{f.dynamic(f.resolve(items))}
	case "object":
		values, _ := schema["additionalProperties"].(map[string]any)
		return map[string]any{"k": f.dynamic(f.resolve(values))}
	case "string":
		return f.text(schema)
	}
	return "v"
}

func (f *schemaFiller) text(schema map[string]any) string {
	if value, ok := schema["const"].(string); ok {
		return value
	}
	if values, ok := schema["enum"].([]any); ok && len(values) > 0 {
		if value, ok := values[f.offset%len(values)].(string); ok {
			return value
		}
	}
	switch schema["format"] {
	case "date-time":
		return "2026-01-02T03:04:05.123456Z"
	case "date":
		return "2026-01-02"
	case "uuid":
		return "5f0c6a38-1d5f-4d6e-9b8e-3a4b5c6d7e8f"
	}
	// Distinct per field, so a list the production code turns into a dict
	// keeps every entry.
	f.texts++
	text := fmt.Sprintf("s<&>é%d", f.texts)
	if minimum, ok := schema["minLength"].(float64); ok && float64(len(text)) < minimum {
		text += strings.Repeat("s", int(minimum)-len(text))
	}
	return text
}

func (f *schemaFiller) integer(schema map[string]any) int64 {
	value := int64(3)
	if minimum, ok := schema["minimum"].(float64); ok && float64(value) < minimum {
		value = int64(math.Ceil(minimum))
	}
	if maximum, ok := schema["maximum"].(float64); ok && float64(value) > maximum {
		value = int64(math.Floor(maximum))
	}
	return value
}

// float is the next edge value the schema's bounds allow, rotated by the
// variant so each float field sees every edge across the variants.
func (f *schemaFiller) float(schema map[string]any) float64 {
	defer func() { f.floats++ }()
	for step := range len(responseModelEdgeFloats) {
		candidate := responseModelEdgeFloats[(f.offset+f.floats+step)%len(responseModelEdgeFloats)]
		if withinBounds(candidate, schema) {
			return candidate
		}
	}
	return f.bounded(schema)
}

func withinBounds(value float64, schema map[string]any) bool {
	_, bounded := schema["minimum"]
	_, boundedAbove := schema["maximum"]
	_, exclusive := schema["exclusiveMinimum"]
	_, exclusiveAbove := schema["exclusiveMaximum"]
	if (bounded || boundedAbove || exclusive || exclusiveAbove) && (math.IsNaN(value) || math.IsInf(value, 0)) {
		return false
	}
	if minimum, ok := schema["minimum"].(float64); ok && value < minimum {
		return false
	}
	if maximum, ok := schema["maximum"].(float64); ok && value > maximum {
		return false
	}
	if minimum, ok := schema["exclusiveMinimum"].(float64); ok && value <= minimum {
		return false
	}
	if maximum, ok := schema["exclusiveMaximum"].(float64); ok && value >= maximum {
		return false
	}
	return true
}

func (f *schemaFiller) bounded(schema map[string]any) float64 {
	if minimum, ok := schema["minimum"].(float64); ok {
		return minimum
	}
	if maximum, ok := schema["maximum"].(float64); ok {
		return maximum
	}
	panic(fmt.Sprintf("no float fits %v", schema))
}

// withNonFiniteSentinels replaces NaN and the infinities with strings the
// Python side turns back into floats.
func withNonFiniteSentinels(value pyjson.Value) pyjson.Value {
	switch typed := value.(type) {
	case pyjson.Float:
		switch {
		case math.IsNaN(float64(typed)):
			return "\x00NaN"
		case math.IsInf(float64(typed), 1):
			return "\x00Infinity"
		case math.IsInf(float64(typed), -1):
			return "\x00-Infinity"
		}
	case []pyjson.Value:
		out := make([]pyjson.Value, len(typed))
		for index, item := range typed {
			out[index] = withNonFiniteSentinels(item)
		}
		return out
	case *pyjson.Object:
		out := pyjson.NewObject()
		for _, key := range typed.Keys() {
			item, _ := typed.Get(key)
			out.Set(key, withNonFiniteSentinels(item))
		}
		return out
	}
	return value
}

// checkNullableTags walks a Go response type beside its pydantic schema and
// fails on a plain slice or map field whose pyjson:"nullable" tag differs
// from the schema's nullability. It returns how many nullable fields it
// matched.
func checkNullableTags(t *testing.T, path string, goType reflect.Type, schema map[string]any, filler *schemaFiller, depth int) int {
	t.Helper()
	if depth > 12 || goType == nil {
		return 0
	}
	resolved := filler.resolve(schema)
	switch goType.Kind() {
	case reflect.Pointer:
		return checkNullableTags(t, path, goType.Elem(), schema, filler, depth+1)
	case reflect.Slice, reflect.Array:
		items, _ := resolved["items"].(map[string]any)
		return checkNullableTags(t, path+"[]", goType.Elem(), items, filler, depth+1)
	case reflect.Map:
		values, _ := resolved["additionalProperties"].(map[string]any)
		return checkNullableTags(t, path+"{}", goType.Elem(), values, filler, depth+1)
	case reflect.Struct:
		properties, _ := resolved["properties"].(map[string]any)
		count := 0
		for index := range goType.NumField() {
			field := goType.Field(index)
			if !field.IsExported() {
				continue
			}
			name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
			if name == "-" {
				continue
			}
			if name == "" {
				name = field.Name
			}
			fieldSchema, _ := properties[name].(map[string]any)
			kind := field.Type.Kind()
			if (kind == reflect.Slice || kind == reflect.Map) && fieldSchema != nil {
				tagged := field.Tag.Get("pyjson") == "nullable"
				if tagged != nullable(fieldSchema) {
					t.Errorf("%s.%s: pyjson:\"nullable\" tag is %v, the model's nullability is %v", path, name, tagged, nullable(fieldSchema))
				}
				if tagged {
					count++
				}
			}
			count += checkNullableTags(t, path+"."+name, field.Type, fieldSchema, filler, depth+1)
		}
		return count
	}
	return 0
}

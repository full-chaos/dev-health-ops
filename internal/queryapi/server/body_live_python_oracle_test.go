package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/investmentexplain"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// pythonBodyModelsProgram serves each query-api POST route's request model
// (api/models/filters.py) from a bare FastAPI app: the body is validated
// by FastAPI exactly as the real route does it (the real routes' own
// bodies are Go-served stubs, and FastAPI validates the body before the
// endpoint runs), then answered 200.
const pythonBodyModelsProgram = `
import json, sys
from fastapi import FastAPI
from fastapi.testclient import TestClient
from dev_health_ops.api.models.filters import (
    DrilldownRequest, ExplainRequest, HomeRequest, InvestmentExplainRequest,
    InvestmentFlowRequest, SankeyRequest, WorkUnitRequest,
)
app = FastAPI()
@app.post("/DrilldownRequest")
def drilldown(payload: DrilldownRequest): return {"ok": True}
@app.post("/ExplainRequest")
def explain(payload: ExplainRequest): return {"ok": True}
@app.post("/HomeRequest")
def home(payload: HomeRequest): return {"ok": True}
@app.post("/InvestmentExplainRequest")
def investment_explain(payload: InvestmentExplainRequest): return {"ok": True}
@app.post("/InvestmentFlowRequest")
def investment_flow(payload: InvestmentFlowRequest): return {"ok": True}
@app.post("/SankeyRequest")
def sankey(payload: SankeyRequest): return {"ok": True}
@app.post("/WorkUnitRequest")
def work_units(payload: WorkUnitRequest): return {"ok": True}
client = TestClient(app, raise_server_exceptions=False)
out = []
for model, body in json.loads(sys.stdin.read()):
    r = client.post("/" + model, content=body.encode("utf-8"), headers={"content-type": "application/json"})
    out.append([r.status_code, r.text])
print(json.dumps(out))
`

// bodyOracleValues is every JSON value the oracle puts at each field:
// numbers at and past each representation edge (integral floats, the
// int64 bounds, big ints, overflowing exponents, NaN/Infinity), strings
// the lax rules read, and every other JSON type.
var bodyOracleValues = []string{
	`0`, `1`, `2`, `-1`, `2.0`, `1.0`, `0.0`, `0.5`, `1e3`, `1E400`, `-1e9999`, `1.5e308`,
	`9223372036854775807`, `9223372036854775808`, `-9223372036854775808`, `-9223372036854775809`,
	`12345678901234567890123`, `NaN`, `Infinity`, `-Infinity`, `1700000000`, `86400`,
	`true`, `false`, `null`, `"x"`, `""`, `"5"`, `" 5 "`, `"5.0"`, `"true"`, `"org"`, `"team"`,
	`"investment"`, `"team_category_repo"`, `"pr"`, `"2024-01-01"`, `"2024-13-01"`, `"1700000000"`,
	`"é"`, `[]`, `["a"]`, `[1]`, `["pr", "x"]`, `{}`, `{"entity_id": 1}`,
}

// bodyOracleModel is one request model: a valid body, its top-level
// fields, and the Go handlers that serve it.
type bodyOracleModel struct {
	name     string
	valid    string
	fields   []string
	handlers map[string]func(t *testing.T) http.HandlerFunc
}

// metricFilterPaths are MetricFilter's fields (api/models/filters.py),
// each set inside an otherwise valid "filters".
var metricFilterPaths = []string{
	"time.range_days", "time.compare_days", "time.start_date", "time.end_date",
	"scope.level", "scope.ids", "who.developers", "who.roles", "what.repos", "what.services",
	"what.artifacts", "why.work_category", "why.issue_type", "why.initiative",
	"how.flow_stage", "how.blocked", "how.wip_state",
	"time", "scope", "who", "what", "why", "how",
}

func bodyOracleModels() []bodyOracleModel {
	emptyInvestmentExplain := func(t *testing.T) http.HandlerFunc {
		reader, err := investmentexplain.NewReader(emptyRowsQueryClient{})
		if err != nil {
			t.Fatalf("NewReader: %v", err)
		}
		return newInvestmentExplainWorkHandler(reader, nil, nil)
	}
	filters := `{"time": {"range_days": 7}, "scope": {"level": "org", "ids": []}}`
	return []bodyOracleModel{
		{"DrilldownRequest", `{"filters": ` + filters + `, "sort": "x", "limit": 5}`, []string{"filters", "sort", "limit"},
			map[string]func(t *testing.T) http.HandlerFunc{
				"drilldown/prs": func(t *testing.T) http.HandlerFunc { return newDrilldownPRsPostHandler(newEmptyRowsDrilldownReader(t)) },
				"drilldown/issues": func(t *testing.T) http.HandlerFunc {
					return newDrilldownIssuesPostHandler(newEmptyRowsDrilldownIssuesReader(t))
				},
			}},
		{"WorkUnitRequest", `{"filters": ` + filters + `, "limit": 5, "include_textual": true}`, []string{"filters", "limit", "include_textual"},
			map[string]func(t *testing.T) http.HandlerFunc{
				"work-units": func(t *testing.T) http.HandlerFunc { return newWorkUnitsPostHandler(newTestWorkUnitsReader(t)) },
			}},
		{"HomeRequest", `{"filters": ` + filters + `}`, []string{"filters"},
			map[string]func(t *testing.T) http.HandlerFunc{
				"home":          func(t *testing.T) http.HandlerFunc { return newHomePostHandler(emptyRowsHomeClient{}, nil) },
				"investment":    func(t *testing.T) http.HandlerFunc { return newInvestmentPostHandler(newEmptyRowsInvestmentReader(t)) },
				"opportunities": func(t *testing.T) http.HandlerFunc { return newOpportunitiesPostHandler(emptyRowsHomeClient{}) },
			}},
		{"ExplainRequest", `{"metric": "cycle_time", "filters": ` + filters + `}`, []string{"metric", "filters"},
			map[string]func(t *testing.T) http.HandlerFunc{
				"explain": func(t *testing.T) http.HandlerFunc { return newExplainPostHandler(newEmptyRowsExplainReader(t)) },
			}},
		{"InvestmentFlowRequest", `{"filters": ` + filters + `, "theme": "x", "flow_mode": "team_category_repo", "drill_category": "x", "top_n_repos": 3}`,
			[]string{"filters", "theme", "flow_mode", "drill_category", "top_n_repos"},
			map[string]func(t *testing.T) http.HandlerFunc{
				"investment/flow": func(t *testing.T) http.HandlerFunc { return newInvestmentFlowHandler(emptyRowsInvestmentFlowClient{}) },
				"investment/flow/repo-team": func(t *testing.T) http.HandlerFunc {
					return newInvestmentFlowRepoTeamHandler(emptyRowsInvestmentFlowClient{})
				},
			}},
		{"SankeyRequest", `{"mode": "investment", "filters": ` + filters + `, "context": {"entity_id": "e", "entity_label": "l"}, "window_start": "2024-01-01", "window_end": "2024-01-31"}`,
			[]string{"mode", "filters", "context", "context.entity_id", "context.entity_label", "window_start", "window_end"},
			map[string]func(t *testing.T) http.HandlerFunc{
				"sankey": func(t *testing.T) http.HandlerFunc { return newSankeyPostHandler(emptyRowsSankeyClient{}) },
			}},
		{"InvestmentExplainRequest", `{"theme": "x", "subcategory": "y", "filters": ` + filters + `, "llm_model": "m"}`,
			[]string{"theme", "subcategory", "filters", "llm_model"},
			map[string]func(t *testing.T) http.HandlerFunc{"investment/explain": emptyInvestmentExplain}},
	}
}

// setPath returns body with the value at a dotted path replaced (created
// when absent), or the key removed when value is "".
func setPath(t *testing.T, body string, path []string, value string) string {
	t.Helper()
	root, err := pyjson.DecodeString(body)
	if err != nil {
		t.Fatalf("decode %s: %v", body, err)
	}
	object := root.(*pyjson.Object)
	for _, key := range path[:len(path)-1] {
		next, ok := object.Get(key)
		child, isObject := next.(*pyjson.Object)
		if !ok || !isObject {
			child = pyjson.NewObject()
			object.Set(key, child)
		}
		object = child
	}
	marker := `"__oracle_value__"`
	object.Set(path[len(path)-1], "__oracle_value__")
	rendered, err := pyjson.Marshal(root)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return strings.Replace(string(rendered), marker, value, 1)
}

// bodyOracleCorpus is every model's body: the valid one; each field and
// each MetricFilter field set to each oracle value; each top-level field
// removed; the whole body and "filters" replaced; and malformed JSON.
func bodyOracleCorpus(t *testing.T, model bodyOracleModel) []string {
	corpus := []string{model.valid, ``, `null`, `{}`, `[]`, `1`, `"x"`, `{"filters": {}}`, `{`, `{"filters": 1,}`, `{"filters": }`, `  `,
		strings.Replace(model.valid, "{", `{"extra": 1, `, 1)}
	for _, value := range bodyOracleValues {
		corpus = append(corpus, value)
		for _, field := range model.fields {
			corpus = append(corpus, setPath(t, model.valid, strings.Split(field, "."), value))
		}
		for _, path := range metricFilterPaths {
			corpus = append(corpus, setPath(t, model.valid, append([]string{"filters"}, strings.Split(path, ".")...), value))
		}
	}
	for _, field := range model.fields {
		if strings.Contains(field, ".") {
			continue
		}
		root, _ := pyjson.DecodeString(model.valid)
		object := root.(*pyjson.Object)
		trimmed := pyjson.NewObject()
		for _, key := range object.Keys() {
			if key != field {
				value, _ := object.Get(key)
				trimmed.Set(key, value)
			}
		}
		rendered, _ := pyjson.Marshal(trimmed)
		corpus = append(corpus, string(rendered))
	}
	return corpus
}

// canonicalJSON re-renders a response body with its key order, int/float
// distinction and float spelling kept, so a comparison sees the JSON
// document, not the writer's whitespace or HTML escaping.
func canonicalJSON(text string) string {
	value, err := pyjson.DecodeString(strings.TrimSpace(text))
	if err != nil {
		return "undecodable: " + text
	}
	rendered, err := pyjson.Marshal(value)
	if err != nil {
		return "unrenderable: " + text
	}
	return string(rendered)
}

// goBodyAnswer runs one body through a real Go route handler, backed by a
// reader that returns no rows: its status and body.
func goBodyAnswer(handler http.HandlerFunc, path, body string) (status int, text string) {
	defer func() {
		if recovered := recover(); recovered != nil {
			status, text = 999, fmt.Sprint("panic: ", recovered)
		}
	}()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/"+path, bytes.NewReader([]byte(body)))
	request.Header.Set("Content-Type", "application/json")
	request = request.WithContext(authctx.WithClaims(request.Context(), authctx.Claims{OrgID: "org-1"}))
	recorder := httptest.NewRecorder()
	handler(recorder, request)
	return recorder.Code, recorder.Body.String()
}

// TestQueryAPIBodiesMatchLiveFastAPI compares every query-api POST route's
// body validation with FastAPI's, per request model. A body FastAPI
// accepts must pass Go's validation (any status but 422 and 500); a 422
// must match in status and JSON document (key order, number spelling and
// every error field); a 500 (a body FastAPI cannot render its error for)
// must be a 500 on Go too.
func TestQueryAPIBodiesMatchLiveFastAPI(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	models := bodyOracleModels()
	var inputs [][2]string
	type item struct {
		model *bodyOracleModel
		body  string
	}
	var items []item
	for index := range models {
		for _, body := range bodyOracleCorpus(t, models[index]) {
			inputs = append(inputs, [2]string{models[index].name, body})
			items = append(items, item{&models[index], body})
		}
	}
	payload, _ := json.Marshal(inputs)
	command := exec.Command(python, "-c", pythonBodyModelsProgram)
	command.Stdin = bytes.NewReader(payload)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want [][2]any
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(want) != len(items) {
		t.Fatalf("python returned %d results for %d bodies", len(want), len(items))
	}
	handlers := map[string]http.HandlerFunc{}
	mismatches, compared, classes := 0, 0, map[string]int{}
	byteDiffs, knownNullGap := 0, 0
	for index, it := range items {
		wantStatus := int(want[index][0].(float64))
		wantText := want[index][1].(string)
		paths := make([]string, 0, len(it.model.handlers))
		for path := range it.model.handlers {
			paths = append(paths, path)
		}
		sort.Strings(paths)
		for _, path := range paths {
			handler, ok := handlers[path]
			if !ok {
				handler = it.model.handlers[path](t)
				handlers[path] = handler
			}
			status, text := goBodyAnswer(handler, path, it.body)
			compared++
			var problem string
			switch {
			case wantStatus == 200 && (status == 422 || status >= 500):
				problem = "python accepts"
			case wantStatus == 422 && status != 422 && nullGap(wantText):
				knownNullGap++
			case wantStatus == 422 && status != 422:
				problem = "python 422"
			case wantStatus == 422 && canonicalJSON(text) != canonicalJSON(wantText):
				problem = "422 document"
			case wantStatus == 500 && status != 500:
				problem = "python 500"
			}
			if wantStatus == 422 && status == 422 && strings.TrimSpace(text) != wantText && canonicalJSON(text) == canonicalJSON(wantText) {
				byteDiffs++
			}
			if problem != "" {
				mismatches++
				classes[path+": "+problem]++
				if mismatches <= 40 {
					t.Errorf("%s %s:\n  go     %d %s\n  python %d %s", path, it.body, status, strings.TrimSpace(text), wantStatus, wantText)
				}
			}
		}
	}
	if mismatches > 0 {
		t.Fatalf("%d of %d route answers differ; by class: %v", mismatches, compared, classes)
	}
	if byteDiffs > 0 {
		t.Fatalf("%d 422 bodies equal FastAPI's as JSON but not byte for byte", byteDiffs)
	}
	if knownNullGap == 0 {
		t.Fatal("the named null gap is closed (every null FastAPI refuses is refused by Go): remove nullGap and compare those bodies directly")
	}
	proof := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proof == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	if err := os.WriteFile(filepath.Join(proof, "query-api-bodies"), []byte("executed"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d bodies, %d route answers compared; 0 mismatches beyond the named null gap (%d answers); every matching 422 byte for byte",
		len(items), compared, knownNullGap)
}

// nullGap is the named gap the query-api validators carry apart from
// number fidelity, ticketed separately: a JSON null for a field that is
// not Optional (a model field with a default, such as filters.time or
// time.range_days, or a whole null or empty investment-explain body) is
// refused by FastAPI and accepted by Go. It matches a FastAPI 422 whose
// every error echoes a null input.
func nullGap(pythonText string) bool {
	value, err := pyjson.DecodeString(pythonText)
	if err != nil {
		return false
	}
	detail, _ := objectField(value, "detail")
	list, ok := detail.([]pyjson.Value)
	if !ok || len(list) == 0 {
		return false
	}
	for _, item := range list {
		input, has := objectField(item, "input")
		if !has || input != nil {
			return false
		}
	}
	return true
}

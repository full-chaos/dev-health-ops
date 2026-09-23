package customerpush

import (
	"encoding/json"
	"math/rand"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// The three request models the write routes take, each behind a FastAPI
// route that echoes the validated model, so the oracle sees FastAPI's own
// decode, 422 and dump.
const pythonBodiesProgram = `
import json, sys
from fastapi import FastAPI
from fastapi.testclient import TestClient
from dev_health_ops.api.admin.schemas.customer_push import IngestSourceCreate, IngestSourcePatch, IngestTokenCreate
app = FastAPI()
@app.post("/create")
def create(p: IngestSourceCreate): return p.model_dump(mode="json")
@app.post("/patch")
def patch(p: IngestSourcePatch): return p.model_dump(mode="json")
@app.post("/token")
def token(p: IngestTokenCreate): return p.model_dump(mode="json")
client = TestClient(app, raise_server_exceptions=False)
out = []
for model, body in json.loads(sys.stdin.read()):
    r = client.post("/" + model, content=body.encode("utf-8"), headers={"content-type": "application/json"})
    out.append([r.status_code, r.text])
print(json.dumps(out))
`

func bodyFields() map[string][]string {
	strs := []string{`"github"`, `"GitHub"`, `""`, `" "`, `"  acme/api  "`, `"x"`, `"é"`, `null`, `1`, `1.5`, `true`, `[]`, `{}`, `["a"]`}
	return map[string][]string{
		"system":        strs,
		"instance":      strs,
		"entity_family": {`"legacy"`, `"operational"`, `"Legacy"`, `"operational_incident"`, `null`, `0`, `""`, `["legacy"]`},
		"display_name":  strs,
		"mode":          append([]string{`"customer_push"`, `"fullchaos_sync"`, `"disabled"`, `"bogus"`}, strs...),
		"webhook_mode":  {`"disabled"`, `"customer_relay"`, `"fullchaos_hosted"`, `"Disabled"`, `""`, `null`, `1`, `true`},
		"enabled":       {`true`, `false`, `null`, `0`, `1`, `2`, `0.0`, `1.0`, `0.5`, `"yes"`, `"off"`, `"TRUE"`, `" true"`, `"maybe"`, `[]`},
		"name":          strs,
		"scopes": {`["schema:read"]`, `["ingest:write","ingest:status"]`, `[]`, `["x"]`, `["b","a","schema:read","a"]`, `[1]`, `[1,"x"]`,
			`"schema:read"`, `null`, `{}`, `["é","\u00e9"]`, `["it's","say \"hi\""]`, `[null]`},
		"expires_at": {`null`, `"2026-10-01T00:00:00Z"`, `"2026-10-01T00:00:00.123456+02:00"`, `"2026-10-01T00:00:00"`, `"2026-10-01"`,
			`"2026-13-01"`, `"yesterday"`, `1700000000`, `1700000000123`, `1.7e9`, `"1700000000"`, `true`, `[]`, `""`, `-1`, `1e20`},
	}
}

var modelFields = map[string][]string{
	"create": {"system", "instance", "entity_family", "display_name", "mode", "webhook_mode"},
	"patch":  {"display_name", "mode", "enabled", "webhook_mode"},
	"token":  {"name", "scopes", "expires_at"},
}

// bodiesCorpus is fixed edge bodies plus seeded random objects over each
// model's fields (a field is absent, or one of its value pool).
func bodiesCorpus() [][2]string {
	var corpus [][2]string
	for model := range modelFields {
		for _, body := range []string{``, `null`, `[]`, `"x"`, `1`, `{}`, `{`, `{"a":1}`, `{"x": NaN}`} {
			corpus = append(corpus, [2]string{model, body})
		}
	}
	fields := bodyFields()
	random := rand.New(rand.NewSource(63190))
	for _, model := range []string{"create", "patch", "token"} {
		for range 1500 {
			var parts []string
			for _, field := range modelFields[model] {
				if random.Intn(5) == 0 {
					continue
				}
				pool := fields[field]
				parts = append(parts, `"`+field+`":`+pool[random.Intn(len(pool))])
			}
			corpus = append(corpus, [2]string{model, "{" + strings.Join(parts, ",") + "}"})
		}
	}
	valid := map[string][]string{
		"system": {`"github"`, `"GitHub"`, `"x"`}, "instance": {`"acme/api"`, `"  acme/api  "`, `"é"`},
		"entity_family": {`"legacy"`, `"operational"`}, "display_name": {`null`, `"Acme"`, `""`},
		"mode": {`"customer_push"`, `"disabled"`, `"bogus"`}, "webhook_mode": {`"disabled"`, `"customer_relay"`, `"fullchaos_hosted"`},
		"enabled": {`true`, `false`, `null`, `0`, `1.0`, `"off"`, `"Y"`}, "name": {`"ci"`, `"é"`},
		"scopes": {`["schema:read"]`, `["ingest:write","ingest:status","ingest:write"]`},
		"expires_at": {`null`, `"2026-10-01T00:00:00Z"`, `"2026-10-01T00:00:00.123456+02:00"`, `"2026-10-01T00:00:00"`,
			`"2026-10-01"`, `1700000000`, `1700000000123`, `1.7e9`, `"1700000000"`, `-1`, `"2026-10-01T00:00:00-05:30"`, `1700000000.5`},
	}
	for _, model := range []string{"create", "patch", "token"} {
		for range 500 {
			var parts []string
			for _, field := range modelFields[model] {
				if random.Intn(4) == 0 {
					continue
				}
				pool := valid[field]
				parts = append(parts, `"`+field+`":`+pool[random.Intn(len(pool))])
			}
			corpus = append(corpus, [2]string{model, "{" + strings.Join(parts, ",") + "}"})
		}
	}
	return corpus
}

func optionalText(value *string) pyjson.Value {
	if value == nil {
		return nil
	}
	return *value
}

// goBodyAnswer is what the Go routes answer for body before any database
// work: FastAPI's decode failure, its 422, or (for the oracle) the dump of
// the validated model.
func goBodyAnswer(model, body string) (int, string) {
	request := httptest.NewRequest("POST", "/", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	read, outcome, failure, err := pybody.Read(request)
	if err != nil {
		return 0, err.Error()
	}
	render := func(value pyjson.Value) string {
		text, err := pyjson.Marshal(value)
		if err != nil {
			return "render failed"
		}
		return string(text)
	}
	answer := func(status int, value pyjson.Value) (int, string) {
		text := render(value)
		if text == "render failed" {
			// The Python api cannot render it either: an unhandled 500.
			return 500, text
		}
		return status, text
	}
	switch outcome {
	case pybody.DecodeFailed:
		return answer(422, pybody.Detail([]pybody.Error{*failure}))
	case pybody.ParseFailed:
		detail := pyjson.NewObject()
		detail.Set("detail", "There was an error parsing the body")
		return 400, render(detail)
	}
	dump := pyjson.NewObject()
	var errs []pybody.Error
	switch model {
	case "create":
		var parsed sourceCreate
		parsed, errs = parseSourceCreate(read)
		dump.Set("system", parsed.System)
		dump.Set("instance", parsed.Instance)
		dump.Set("entity_family", parsed.EntityFamily)
		dump.Set("display_name", optionalText(parsed.DisplayName))
		dump.Set("mode", parsed.Mode)
		dump.Set("webhook_mode", parsed.WebhookMode)
	case "patch":
		var parsed sourcePatch
		parsed, errs = parseSourcePatch(read)
		dump.Set("display_name", optionalText(parsed.DisplayName))
		dump.Set("mode", optionalText(parsed.Mode))
		if parsed.Enabled == nil {
			dump.Set("enabled", nil)
		} else {
			dump.Set("enabled", *parsed.Enabled)
		}
		dump.Set("webhook_mode", optionalText(parsed.WebhookMode))
	case "token":
		var parsed tokenCreate
		parsed, errs = parseTokenCreate(read)
		dump.Set("name", parsed.Name)
		scopes := make([]pyjson.Value, len(parsed.Scopes))
		for index, scope := range parsed.Scopes {
			scopes[index] = scope
		}
		dump.Set("scopes", scopes)
		if parsed.ExpiresAt == nil {
			dump.Set("expires_at", nil)
		} else {
			dump.Set("expires_at", pytime.Pydantic(*parsed.ExpiresAt))
		}
	}
	if len(errs) > 0 {
		return answer(422, pybody.Detail(errs))
	}
	return answer(200, dump)
}

// TestCustomerPushBodiesMatchLiveFastAPI compares the write routes' body
// validation with FastAPI's on the real request models.
func TestCustomerPushBodiesMatchLiveFastAPI(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	corpus := bodiesCorpus()
	input, _ := json.Marshal(corpus)
	command := exec.Command(python, "-c", pythonBodiesProgram)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	command.Stdin = strings.NewReader(string(input))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want [][2]any
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(want) != len(corpus) {
		t.Fatalf("python returned %d results for %d bodies", len(want), len(corpus))
	}
	mismatches, statuses := 0, map[int]int{}
	for index, item := range corpus {
		status, text := goBodyAnswer(item[0], item[1])
		statuses[status]++
		wantStatus := int(want[index][0].(float64))
		wantText := want[index][1].(string)
		if status != wantStatus || (status != 500 && text != wantText) {
			mismatches++
			if mismatches <= 15 {
				t.Errorf("%s %s:\n  go     %d %s\n  python %d %s", item[0], item[1], status, text, wantStatus, wantText)
			}
		}
	}
	if mismatches > 0 {
		t.Fatalf("%d of %d bodies differ", mismatches, len(corpus))
	}
	proof := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proof == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	if err := os.WriteFile(filepath.Join(proof, "api-customerpush-bodies"), []byte("executed"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d bodies compared (status counts %v); 0 mismatches", len(corpus), statuses)
}

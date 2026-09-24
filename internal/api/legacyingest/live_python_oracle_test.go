package legacyingest

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// pythonProgram runs each case through the real Python router (the FastAPI
// app with api.ingest.router mounted) in-process: the credentials and the
// environment name from the case, the stream write captured instead of sent.
// It reports, per case, the status, the body, and what would be streamed.
const pythonProgram = `
import base64, json, os, sys
from fastapi import FastAPI
from fastapi.testclient import TestClient
import importlib
ingest_router = importlib.import_module("dev_health_ops.api.ingest.router")

captured = []
ingest_router.get_redis_client = lambda: object()
ingest_router.write_to_stream = lambda rc, name, data: captured.append([name, data["payload"]]) or True
app = FastAPI()
app.include_router(ingest_router.router)
client = TestClient(app, raise_server_exceptions=False)
managed = ["INGEST_API_KEYS", "INGEST_SIGNING_SECRET", "ENVIRONMENT", "APP_ENV", "ENV", "REDIS_URL"]
out = []
for case in json.loads(sys.stdin.read()):
    for key in managed:
        os.environ.pop(key, None)
    os.environ.update(case["env"])
    del captured[:]
    headers = {"content-type": "application/json"}
    headers.update({k: base64.b64decode(v) for k, v in case["headers"].items()})
    body = base64.b64decode(case["body"])
    r = client.post("/api/v1/ingest/" + case["route"], content=body, headers=headers)
    out.append([r.status_code, r.text, list(captured)])
print(json.dumps(out))
`

// oracleCase is one request; its bytes go to Python base64-encoded, so a
// header or body that is not valid UTF-8 reaches both planes as it is.
type oracleCase struct {
	Route   string
	Env     map[string]string
	Headers map[string]string
	Body    string
}

type oracleWire struct {
	Route   string            `json:"route"`
	Env     map[string]string `json:"env"`
	Headers map[string]string `json:"headers"`
	Body    string            `json:"body"`
}

type fakeStore struct{ streams [][2]string }

func (f *fakeStore) Append(_ context.Context, stream, _ string, payload string) error {
	f.streams = append(f.streams, [2]string{stream, payload})
	return nil
}
func (f *fakeStore) Claim(context.Context, string) (bool, error) { return true, nil }

var ingestionID = regexp.MustCompile(`"ingestion_id":"[0-9a-f-]{36}"`)

// goAnswer is the Go routes' answer for one case, in the Python program's
// terms.
func goAnswer(t *testing.T, item oracleCase) (int, string, [][2]string) {
	t.Helper()
	store := &fakeStore{}
	routes := Routes(Deps{Store: store, Getenv: func(key string) string { return item.Env[key] }, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	var handler http.Handler
	for _, route := range routes {
		if route.Pattern == "/api/v1/ingest/"+item.Route {
			handler = route.Handler
		}
	}
	if handler == nil {
		t.Fatalf("no route %s", item.Route)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/"+item.Route, strings.NewReader(item.Body))
	request.Header.Set("Content-Type", "application/json")
	for key, value := range item.Headers {
		request.Header.Set(key, value)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder.Code, recorder.Body.String(), store.streams
}

func credentials(secret, body string) string { return sign(secret, body) }

func authCorpus() []oracleCase {
	const body = `{"org_id":"org-1","repo_url":"u","items":[{"incident_id":"i","status":"open","started_at":"2026-01-01T00:00:00Z"}]}`
	envs := []map[string]string{
		{}, {"ENVIRONMENT": "development"}, {"ENVIRONMENT": " DEV "}, {"ENVIRONMENT": "local"}, {"ENVIRONMENT": "prod"}, {"ENVIRONMENT": "", "APP_ENV": "dev"},
		{"ENVIRONMENT": "prod", "APP_ENV": "dev"}, {"ENV": "local"}, {"ENVIRONMENT": "Développement"},
		{"INGEST_API_KEYS": "k1,k2"}, {"INGEST_API_KEYS": " k1 , ,\tk2\x1f"}, {"INGEST_API_KEYS": " , "}, {"INGEST_API_KEYS": "é"},
		{"INGEST_SIGNING_SECRET": "s"}, {"INGEST_SIGNING_SECRET": "é"},
		{"INGEST_API_KEYS": "k1", "INGEST_SIGNING_SECRET": "s"},
		{"INGEST_API_KEYS": "k1", "ENVIRONMENT": "dev"}, {"INGEST_SIGNING_SECRET": "s", "ENVIRONMENT": "dev"},
	}
	headerSets := []map[string]string{
		nil, {"X-API-Key": "k1"}, {"X-API-Key": "k2"}, {"X-API-Key": "k3"}, {"X-API-Key": ""}, {"X-API-Key": "k1 "}, {"X-API-Key": "\xc3\xa9"}, {"X-API-Key": "é"},
		{"X-Signature-256": sign("s", body)}, {"X-Signature-256": strings.ToUpper(sign("s", body))}, {"X-Signature-256": strings.TrimPrefix(sign("s", body), "sha256=")},
		{"X-Signature-256": sign("other", body)}, {"X-Signature-256": "sha256="}, {"X-Signature-256": "sha256=\xe9"}, {"X-Signature-256": sign("\xc3\xa9", body)},
		{"X-API-Key": "k1", "X-Signature-256": sign("s", body)}, {"X-API-Key": "k1", "X-Signature-256": sign("s", body+" ")},
		{"X-Idempotency-Key": "abc"}, {"X-API-Key": "k1", "X-Idempotency-Key": ""},
	}
	var corpus []oracleCase
	for _, env := range envs {
		for _, headers := range headerSets {
			for _, route := range []string{"incidents"} {
				corpus = append(corpus, oracleCase{Route: route, Env: env, Headers: headers, Body: body})
			}
		}
	}
	return corpus
}

func TestLegacyIngestMatchesLiveFastAPI(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	corpus := append(authCorpus(), bodyCorpus()...)
	wire := make([]oracleWire, len(corpus))
	for index, item := range corpus {
		headers := map[string]string{}
		for key, value := range item.Headers {
			headers[key] = base64.StdEncoding.EncodeToString([]byte(value))
		}
		wire[index] = oracleWire{Route: item.Route, Env: item.Env, Headers: headers, Body: base64.StdEncoding.EncodeToString([]byte(item.Body))}
	}
	input, _ := json.Marshal(wire)
	command := exec.Command(python, "-c", pythonProgram)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	command.Stdin = strings.NewReader(string(input))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want []json.RawMessage
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil {
		t.Fatalf("decode: %v\n%s", err, output)
	}
	if len(want) != len(corpus) {
		t.Fatalf("python answered %d of %d", len(want), len(corpus))
	}
	mismatches, statuses := 0, map[int]int{}
	for index, item := range corpus {
		var answer struct {
			Status int
			Body   string
			Stream [][2]string
		}
		var raw []json.RawMessage
		_ = json.Unmarshal(want[index], &raw)
		_ = json.Unmarshal(raw[0], &answer.Status)
		_ = json.Unmarshal(raw[1], &answer.Body)
		_ = json.Unmarshal(raw[2], &answer.Stream)
		status, body, stream := goAnswer(t, item)
		statuses[status]++
		wantBody, gotBody := ingestionID.ReplaceAllString(answer.Body, `"ingestion_id":"<id>"`), ingestionID.ReplaceAllString(body, `"ingestion_id":"<id>"`)
		if status == http.StatusInternalServerError {
			// Starlette's bare-app 500 is plain text; the api's JSON 500 is the
			// venue's to compare.
			wantBody, gotBody = "", ""
		}
		if status != answer.Status || gotBody != wantBody || fmt.Sprint(stream) != fmt.Sprint(answer.Stream) {
			mismatches++
			if mismatches <= 12 {
				t.Errorf("%s env=%v headers=%v body=%.300s:\n  go     %d %.400s %v\n  python %d %.400s %v", item.Route, item.Env, item.Headers, item.Body,
					status, body, stream, answer.Status, answer.Body, answer.Stream)
			}
		}
	}
	if mismatches > 0 {
		t.Fatalf("%d of %d cases differ", mismatches, len(corpus))
	}
	writeProof(t, "api-legacyingest")
	t.Logf("%d cases compared (statuses %v); 0 mismatches", len(corpus), statuses)
}

func writeProof(t *testing.T, name string) {
	t.Helper()
	dir := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if dir == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	if err := os.WriteFile(filepath.Join(dir, name), []byte("executed"), 0o600); err != nil {
		t.Fatal(err)
	}
}

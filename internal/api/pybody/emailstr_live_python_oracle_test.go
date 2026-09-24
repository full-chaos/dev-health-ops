package pybody

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// emailStrProgram serves each raw JSON body to a real FastAPI route per
// field shape (required EmailStr, optional EmailStr) and returns the
// status and the exact response bytes.
const emailStrProgram = `
import base64, json, sys
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import BaseModel, EmailStr

class Required(BaseModel):
    email: EmailStr

class Optional_(BaseModel):
    email: EmailStr | None = None

app = FastAPI()

@app.post("/required")
def required(payload: Required):
    return {"email": payload.email}

@app.post("/optional")
def optional(payload: Optional_):
    return {"email": payload.email}

client = TestClient(app, raise_server_exceptions=False)
out = []
for case in json.load(sys.stdin):
    r = client.post(case["path"], content=base64.b64decode(case["body"]), headers={"content-type": "application/json"})
    out.append({"status": r.status_code, "body": base64.b64encode(r.content).decode()})
print(json.dumps(out))
`

var emailStrBodies = []string{
	`{}`, `{"email": null}`, `{"email": 5}`, `{"email": true}`, `{"email": []}`, `{"email": {}}`, `{"email": ""}`,
	`{"email": "a@b.com"}`, `{"email": "Admin@Example.COM"}`, `{"email": " a@b.com "}`, `{"email": "Name <a@b.com>"}`,
	`{"email": "a@b"}`, `{"email": "a..b@c.com"}`, `{"email": "\u00fc@b\u00fccher.DE"}`, `{"email": "a@[1.2.3.4]"}`,
	`{"email": "INFO@x.com"}`, `{"email": "\"q\"@x.com"}`, `{"email": "a\u0000@b.com"}`, `{"email": "a@localhost"}`,
	`{"email": "\ud800@b.com"}`, `{"email": "a@b.com", "email": "bad"}`, `{"email": "e\u0301@b.com"}`,
	`{"email": "` + strings.Repeat("a", 2049) + `"}`, `{"email": "a@b.com", "other": 1}`, `[]`, `"a@b.com"`, `null`,
}

// TestEmailStrMatchesLiveFastAPI compares RequiredEmailStr and
// OptionalEmailStr, rendered as a route renders them, with FastAPI's own
// 422 body (and the handler's view of the value on success), byte for byte.
func TestEmailStrMatchesLiveFastAPI(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	type request struct {
		Path string `json:"path"`
		Body string `json:"body"`
	}
	var requests []request
	for _, path := range []string{"/required", "/optional"} {
		for _, body := range emailStrBodies {
			requests = append(requests, request{Path: path, Body: base64.StdEncoding.EncodeToString([]byte(body))})
		}
	}
	payload, _ := json.Marshal(requests)
	command := exec.Command(python, "-c", emailStrProgram)
	command.Stdin = bytes.NewReader(payload)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		t.Fatalf("live fastapi: %v", pyoracle.RunError(python, err, stderr.Bytes()))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want []struct {
		Status int    `json:"status"`
		Body   string `json:"body"`
	}
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil {
		t.Fatalf("decode: %v", err)
	}
	for index, req := range requests {
		body, _ := base64.StdEncoding.DecodeString(req.Body)
		status, got := serveEmailStr(t, req.Path, body)
		expected, _ := base64.StdEncoding.DecodeString(want[index].Body)
		if status != want[index].Status || got != string(expected) {
			t.Errorf("%s %s:\n  go     %d %s\n  python %d %s", req.Path, body, status, got, want[index].Status, expected)
		}
	}
	t.Logf("%d requests compared", len(requests))
	proof := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proof == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	if err := os.WriteFile(filepath.Join(proof, "api-pybody-emailstr"), []byte("executed"), 0o600); err != nil {
		t.Fatal(err)
	}
}

// serveEmailStr is what a route does with the helpers: 422 with the
// detail list, else 200 with the value the handler received. A body that
// cannot be rendered (a lone surrogate) is Starlette's plain 500.
func serveEmailStr(t *testing.T, path string, body []byte) (int, string) {
	t.Helper()
	request := httptest.NewRequest("POST", path, bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	parsed, outcome, decodeErr, err := Read(request)
	if err != nil || outcome != Ready {
		t.Fatalf("read %s: outcome=%v err=%v", body, outcome, err)
	}
	var errs Errors
	if decodeErr != nil {
		errs = append(errs, *decodeErr)
	}
	var value pyjson.Value
	if object, ok := errs.Object(parsed); ok {
		var email string
		var present bool
		if path == "/required" {
			email, present = errs.RequiredEmailStr(object, "email")
		} else {
			email, present = errs.OptionalEmailStr(object, "email")
		}
		if present {
			value = email
		}
	}
	status := 200
	out := pyjson.NewObject()
	out.Set("email", value)
	rendered := pyjson.Value(out)
	if len(errs) > 0 {
		status, rendered = 422, Detail(errs)
	}
	encoded, err := pyjson.Marshal(rendered)
	if err != nil {
		return 500, "Internal Server Error"
	}
	return status, string(encoded)
}

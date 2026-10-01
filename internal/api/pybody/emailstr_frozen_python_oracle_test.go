package pybody_test

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
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

// TestEmailStrMatchesFrozenFastAPI compares RequiredEmailStr and
// OptionalEmailStr, rendered as a route renders them, with FastAPI's own
// 422 body (and the handler's view of the value on success), byte for byte.
func TestEmailStrMatchesFrozenFastAPI(t *testing.T) {
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
	output := frozenPython(t, "email-str.golden.json",
		programoracle.Program{Name: "email-str", Text: emailStrProgram, Stdin: payload})[0]
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
		status, got := pybody.ServeEmailStr(t, req.Path, body)
		expected, _ := base64.StdEncoding.DecodeString(want[index].Body)
		if status != want[index].Status || got != string(expected) {
			t.Errorf("%s %s:\n  go     %d %s\n  python %d %s", req.Path, body, status, got, want[index].Status, expected)
		}
	}
	t.Logf("%d requests compared", len(requests))
}

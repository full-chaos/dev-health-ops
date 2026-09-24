package apiservice

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// pythonCORSProgram builds the api's own CORSMiddleware (its keyword
// arguments read from register_middleware's app, never restated) around a
// route that sets 0, 1 or 2 Vary headers, per origin configuration, and
// answers every case through the installed Starlette.
const pythonCORSProgram = `
import json, os, sys
from fastapi import FastAPI
from starlette.applications import Starlette
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import Response
from starlette.routing import Route
from starlette.testclient import TestClient
import starlette

def vary_route(request):
    count = int(request.query_params.get("vary", "0"))
    response = Response("ok", media_type="text/plain")
    for value in ["Accept-Encoding", "X-Two"][:count]:
        response.raw_headers.append((b"vary", value.encode()))
    return response

payload = json.loads(sys.stdin.read())
out = {"starlette": starlette.__version__, "configs": {}}
for name, origins in payload["configs"].items():
    if origins is None:
        os.environ.pop("CORS_ALLOWED_ORIGINS", None)
    else:
        os.environ["CORS_ALLOWED_ORIGINS"] = origins
    import importlib, dev_health_ops.api._middleware as middleware
    from dev_health_ops.api.middleware.rate_limit import limiter
    importlib.reload(middleware)
    api = FastAPI()
    api.state.limiter = limiter
    middleware.register_middleware(api)
    kwargs = [m for m in api.user_middleware if m.cls is CORSMiddleware][0].kwargs
    app = Starlette(routes=[Route("/v", vary_route, methods=["GET", "POST", "OPTIONS"])])
    app.add_middleware(CORSMiddleware, **kwargs)
    client = TestClient(app)
    answers = []
    for case in payload["cases"]:
        r = client.request(case["method"], "/v?vary=%d" % case["vary"], headers=case["headers"])
        headers = {}
        for key, value in r.headers.multi_items():
            key = key.lower()
            if key == "vary" or key.startswith("access-control-"):
                headers.setdefault(key, []).append(value)
        answers.append({"status": r.status_code, "headers": headers})
    out["configs"][name] = {"allow_origins": list(kwargs["allow_origins"]), "answers": answers}
print(json.dumps(out))
`

type corsCase struct {
	Method  string            `json:"method"`
	Vary    int               `json:"vary"`
	Headers map[string]string `json:"headers"`
}

// TestCORSVenueOracleMatchesLiveStarlette pins the Go CORS middleware against the
// installed Starlette CORSMiddleware built with the api's own arguments:
// every Origin shape (absent, empty, allowed, disallowed) crossed with 0, 1
// or 2 handler-set Vary headers, on a simple request and on a preflight, for
// the default, list and "*" origin configurations. Compared: status, every
// Vary value and every Access-Control-* header. It imports the api's own
// middleware module, so it needs the full project environment: it runs in
// the venue-oracles job, which discovers it by name and requires its proof.
func TestCORSVenueOracleMatchesLiveStarlette(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("the CORS oracle needs the full project Python environment; ci/check_go.sh venue-oracles runs it")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	python := pyoracle.Resolve(t, root)

	configs := map[string]*string{"default": nil, "list": ptr("https://a.example, https://b.example,,"), "star": ptr("*")}
	var cases []corsCase
	for _, origin := range []*string{nil, ptr(""), ptr("https://a.example"), ptr("http://localhost:3000"), ptr("https://evil.example")} {
		for vary := 0; vary <= 2; vary++ {
			for _, preflight := range []bool{false, true} {
				headers := map[string]string{}
				if origin != nil {
					headers["Origin"] = *origin
				}
				method := http.MethodGet
				if preflight {
					method = http.MethodOptions
					headers["Access-Control-Request-Method"] = "POST"
				}
				cases = append(cases, corsCase{Method: method, Vary: vary, Headers: headers})
			}
			cases = append(cases, corsCase{Method: http.MethodPost, Vary: vary, Headers: withOrigin(origin)})
		}
	}
	input, _ := json.Marshal(map[string]any{"configs": configs, "cases": cases})
	command := exec.Command(python, "-c", pythonCORSProgram)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	command.Stdin = strings.NewReader(string(input))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live Starlette: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want struct {
		Starlette string `json:"starlette"`
		Configs   map[string]struct {
			AllowOrigins []string `json:"allow_origins"`
			Answers      []struct {
				Status  int                 `json:"status"`
				Headers map[string][]string `json:"headers"`
			} `json:"answers"`
		} `json:"configs"`
	}
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil {
		t.Fatalf("decode: %v\n%s", err, output)
	}
	compared := 0
	for name, config := range want.Configs {
		cors := NewCORS(config.AllowOrigins)
		for index, c := range cases {
			vary := c.Vary
			handler := cors.Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				for _, value := range []string{"Accept-Encoding", "X-Two"}[:vary] {
					w.Header().Add("Vary", value)
				}
				w.Header().Set("Content-Type", "text/plain; charset=utf-8")
				_, _ = w.Write([]byte("ok"))
			}))
			request := httptest.NewRequest(c.Method, "/v", nil)
			for key, value := range c.Headers {
				request.Header.Set(key, value)
			}
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			got := map[string][]string{}
			for key, values := range recorder.Result().Header {
				key = strings.ToLower(key)
				if key == "vary" || strings.HasPrefix(key, "access-control-") {
					got[key] = values
				}
			}
			expected := config.Answers[index]
			if recorder.Code != expected.Status || !reflect.DeepEqual(got, normalizeEmpty(expected.Headers)) {
				t.Errorf("config=%s %s vary=%d headers=%v:\n go     %d %v\n python %d %v", name, c.Method, c.Vary,
					sortedHeaders(c.Headers), recorder.Code, got, expected.Status, expected.Headers)
			}
			compared++
		}
	}
	if compared != len(cases)*len(configs) || !strings.HasPrefix(want.Starlette, "1.7.") {
		t.Fatalf("compared %d of %d cases against starlette %s", compared, len(cases)*len(configs), want.Starlette)
	}
	venueoracle.WriteProof(t)
}

func ptr(value string) *string { return &value }

func withOrigin(origin *string) map[string]string {
	if origin == nil {
		return map[string]string{}
	}
	return map[string]string{"Origin": *origin}
}

func normalizeEmpty(headers map[string][]string) map[string][]string {
	if headers == nil {
		return map[string][]string{}
	}
	return headers
}

func sortedHeaders(headers map[string]string) []string {
	out := make([]string, 0, len(headers))
	for key, value := range headers {
		out = append(out, key+"="+value)
	}
	sort.Strings(out)
	return out
}

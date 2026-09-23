package httpapi

import (
	"crypto/tls"
	"encoding/json"
	"net"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// The scheme uvicorn's ProxyHeadersMiddleware leaves in an http scope.
const pythonForwardedProgram = `
import asyncio, json, sys
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware
out = []
for case in json.loads(sys.stdin.read()):
    seen = {}
    async def app(scope, receive, send):
        seen["scheme"] = scope["scheme"]
    headers = [(b"x-forwarded-proto", value.encode("latin-1")) for value in (case["headers"] or [])]
    host, port = case["peer"]
    scope = {"type": "http", "scheme": "https" if case["tls"] else "http", "client": (host, port), "headers": headers}
    asyncio.run(ProxyHeadersMiddleware(app, trusted_hosts=case["allow"])(scope, None, None))
    out.append(seen["scheme"])
print(json.dumps(out))
`

func TestForwardedSchemeMatchesLiveUvicorn(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	type forwardedCase struct {
		Allow   string   `json:"allow"`
		Peer    [2]any   `json:"peer"`
		Headers []string `json:"headers"`
		TLS     bool     `json:"tls"`
	}
	var cases []forwardedCase
	for _, allow := range []string{"127.0.0.1", "*", "", "10.0.0.0/8", "10.0.0.1/8", " 10.0.0.1 , 127.0.0.1", "::1", "unix-socket", "fe80::/10"} {
		for _, peer := range []string{"127.0.0.1", "10.9.9.9", "10.0.0.1", "::1", "::ffff:127.0.0.1", "unix-socket", ""} {
			for _, headers := range [][]string{nil, {"https"}, {"http"}, {"https", "http"}, {" \x1chttps\u00a0"}, {"HTTPS"}, {"wss"}, {"ws"}, {""}} {
				for _, secure := range []bool{false, true} {
					cases = append(cases, forwardedCase{Allow: allow, Peer: [2]any{peer, 5000}, Headers: headers, TLS: secure})
				}
			}
		}
	}
	input, _ := json.Marshal(cases)
	command := exec.Command(python, "-c", pythonForwardedProgram)
	command.Stdin = strings.NewReader(string(input))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live uvicorn: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want []string
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil || len(want) != len(cases) {
		t.Fatalf("decode: %v (%d of %d)", err, len(want), len(cases))
	}
	for index, c := range cases {
		request := httptest.NewRequest("GET", "http://h/x/", nil)
		host := c.Peer[0].(string)
		request.RemoteAddr = net.JoinHostPort(host, "5000")
		if host == "unix-socket" || host == "" {
			request.RemoteAddr = host
		}
		for _, value := range c.Headers {
			// The Python side sends value.encode("latin-1"); the header
			// carries those bytes.
			latin1 := make([]byte, 0, len(value))
			for _, r := range value {
				latin1 = append(latin1, byte(r))
			}
			request.Header.Add("X-Forwarded-Proto", string(latin1))
		}
		if c.TLS {
			request.TLS = &tls.ConnectionState{}
		}
		if got := ParseForwardedTrust(c.Allow).scheme(request); got != want[index] {
			t.Errorf("allow=%q peer=%q headers=%q tls=%v: go %q, uvicorn %q", c.Allow, host, c.Headers, c.TLS, got, want[index])
		}
	}
	proof := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proof == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	if err := os.WriteFile(filepath.Join(proof, "httpapi-forwarded-scheme"), []byte("executed"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d forwarded-scheme cases compared", len(cases))
}

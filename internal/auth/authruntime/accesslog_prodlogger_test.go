package authruntime

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/clientaddr"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

// wireIDLogKey is the access-line key go-api also uses for the auth-service wire id, so one filter finds both lines.
const wireIDLogKey = "upstream_request_id"

// newProdLoggerStack builds a listener the way configure does: logging.NewJSON (the redacting prod logger) and
// apiServerOptions, or internalServerOptions for the internal listener.
func newProdLoggerStack(t *testing.T, internal bool) *accessStack {
	t.Helper()
	trust, err := clientaddr.ParseTrust("192.0.2.0/28")
	if err != nil {
		t.Fatal(err)
	}
	cfg := authconfig.Config{TrustedProxies: trust}
	routes := []httpapi.Route{
		{Method: http.MethodGet, Pattern: "/v1/access-ok", Unthrottled: true,
			Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })},
		{Method: http.MethodGet, Pattern: "/v1/access-reshaped", Unthrottled: true,
			Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set(requestid.Header, reshapedWireID)
				w.WriteHeader(http.StatusNoContent)
			})},
	}
	logs := &bytes.Buffer{}
	logger := logging.NewJSON(logs, slog.LevelInfo)
	options := apiServerOptions(cfg, logger, routes)
	if internal {
		options = internalServerOptions(cfg, logger, routes)
	}
	options.Address, options.RequestTimeout, options.MaxBodyBytes = "127.0.0.1:0", 2*time.Second, 64
	server, err := httpapi.NewServer(options)
	if err != nil {
		t.Fatal(err)
	}
	return &accessStack{logs: logs, handler: server.Handler()}
}

func TestTheProdLoggerAccessLineCarriesTheUnredactedWireID(t *testing.T) {
	cases := []struct {
		name     string
		internal bool
		target   string
		status   int
	}{
		{"api listener", false, "/v1/access-ok", http.StatusNoContent},
		{"internal listener", true, "/v1/access-none", http.StatusNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stack := newProdLoggerStack(t, tc.internal)
			request := httptest.NewRequest(http.MethodGet, tc.target, nil)
			request.Header.Set(httpapi.RequestIDHeader, accessCallerID)
			response := stack.serve(request)
			if response.Code != tc.status {
				t.Fatalf("status = %d, want %d", response.Code, tc.status)
			}
			wire := response.Header().Get(requestid.Header)
			if requestid.LoggableWire(wire) != wire || wire == "" {
				t.Fatalf("answer %s = %q, want a minted wire id", requestid.Header, wire)
			}
			line := stack.onlyLine(t)
			if line[wireIDLogKey] != wire {
				t.Errorf("%s = %v, want the answer's %s %q; line: %s", wireIDLogKey, line[wireIDLogKey], requestid.Header, wire, stack.logs.String())
			}
			for key, value := range line {
				if value == "[REDACTED]" {
					t.Errorf("access line field %q is [REDACTED] under the prod logger", key)
				}
			}
		})
	}
}

// reshapedWireID is an answer X-Auth-Request-ID outside the wire form.
const reshapedWireID = "req_not-the-wire-form"

// The access line logs the answer header only through requestid.LoggableWire: a value outside the wire form is
// "invalid", never the raw text.
func TestTheProdLoggerAccessLineLogsAReshapedWireIDAsInvalid(t *testing.T) {
	stack := newProdLoggerStack(t, false)
	response := stack.serve(httptest.NewRequest(http.MethodGet, "/v1/access-reshaped", nil))
	if got := response.Header().Get(requestid.Header); got != reshapedWireID {
		t.Fatalf("answer %s = %q, want the handler's %q", requestid.Header, got, reshapedWireID)
	}
	if got := stack.onlyLine(t)[wireIDLogKey]; got != "invalid" {
		t.Errorf("%s = %v, want \"invalid\"", wireIDLogKey, got)
	}
	if strings.Contains(stack.logs.String(), reshapedWireID) {
		t.Errorf("the raw answer header reached the log: %s", stack.logs.String())
	}
}

// Control: on the same prod logger, credential-like keys stay masked, including the old auth_request_id key.
func TestTheProdLoggerStillRedactsCredentialKeys(t *testing.T) {
	logs := &bytes.Buffer{}
	logger := logging.NewJSON(logs, slog.LevelInfo)
	wire := requestid.WireOf([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	keys := []string{"auth_request_id", "auth_token", "authorization", "session_id", "access_token", "api_key", "client_secret"}
	attrs := make([]slog.Attr, 0, len(keys)+1)
	for _, key := range keys {
		attrs = append(attrs, slog.String(key, wire))
	}
	attrs = append(attrs, slog.String(wireIDLogKey, wire))
	logger.LogAttrs(t.Context(), slog.LevelInfo, "http request", attrs...)
	var line map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(logs.Bytes()), &line); err != nil {
		t.Fatalf("log line %q is not JSON: %v", logs.String(), err)
	}
	for _, key := range keys {
		if line[key] != "[REDACTED]" {
			t.Errorf("%s = %v, want [REDACTED]", key, line[key])
		}
	}
	if line[wireIDLogKey] != wire {
		t.Errorf("%s = %v, want %q", wireIDLogKey, line[wireIDLogKey], wire)
	}
	if strings.Count(logs.String(), wire) != 1 {
		t.Errorf("the wire id appears %d times, want once (under %s only): %s", strings.Count(logs.String(), wire), wireIDLogKey, logs.String())
	}
}

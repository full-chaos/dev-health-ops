package authruntime

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/clientaddr"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
)

const (
	// httptest.NewRequest's RemoteAddr is 192.0.2.1:1234.
	accessTrustedPeer   = "192.0.2.1:1234"
	accessUntrustedPeer = "198.51.100.7:1234"
)

// accessCallerID is a canonical UUID, built at run time.
var accessCallerID = uuid.NewSHA1(uuid.NameSpaceURL, []byte("access-log-caller")).String()

type accessStack struct {
	logs    *bytes.Buffer
	handler http.Handler
}

func newAccessStack(t *testing.T) *accessStack {
	t.Helper()
	return newAccessStackTrusting(t, "192.0.2.0/28")
}

func newAccessStackTrusting(t *testing.T, trusted string) *accessStack {
	t.Helper()
	trust, err := clientaddr.ParseTrust(trusted)
	if err != nil {
		t.Fatal(err)
	}
	cfg := authconfig.Config{TrustedProxies: trust}
	routes := append([]httpapi.Route(nil), authapi.NotOwnedCredentialRoutes()...)
	routes = append(routes,
		httpapi.Route{Method: http.MethodGet, Pattern: "/v1/access-ok", Unthrottled: true,
			Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })},
		httpapi.Route{Method: http.MethodPost, Pattern: "/v1/access-body", RateLimitPerSecond: 1000, RateLimitBurst: 1000,
			Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if _, err := io.Copy(io.Discard, r.Body); err != nil {
					return
				}
				w.WriteHeader(http.StatusNoContent)
			})},
		httpapi.Route{Method: http.MethodGet, Pattern: "/v1/access-limited",
			Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })},
		httpapi.Route{Method: http.MethodGet, Pattern: "/v1/access-panic", Unthrottled: true,
			Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("access-panic-detail") })},
		httpapi.Route{Method: http.MethodGet, Pattern: "/v1/access-abort", Unthrottled: true,
			Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusAccepted)
				panic(http.ErrAbortHandler)
			})},
	)
	logs := &bytes.Buffer{}
	options := apiServerOptions(cfg, slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo})), routes)
	options.Address, options.RequestTimeout, options.MaxBodyBytes = "127.0.0.1:0", 2*time.Second, 64
	options.RateLimit, options.RateLimitBurst, options.MaxInFlight = 0.001, 1, 8
	server, err := httpapi.NewServer(options)
	if err != nil {
		t.Fatal(err)
	}
	return &accessStack{logs: logs, handler: server.Handler()}
}

func (s *accessStack) serve(request *http.Request) (recorder *httptest.ResponseRecorder) {
	recorder = httptest.NewRecorder()
	defer func() {
		if recovered := recover(); recovered != nil && recovered != http.ErrAbortHandler {
			panic(recovered)
		}
	}()
	s.handler.ServeHTTP(recorder, request)
	return recorder
}

func (s *accessStack) lines(t *testing.T) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, raw := range strings.Split(strings.TrimSpace(s.logs.String()), "\n") {
		if raw == "" {
			continue
		}
		var line map[string]any
		if err := json.Unmarshal([]byte(raw), &line); err != nil {
			t.Fatalf("log line %q is not JSON: %v", raw, err)
		}
		if line["msg"] == "http request" {
			out = append(out, line)
		}
	}
	return out
}

func (s *accessStack) onlyLine(t *testing.T) map[string]any {
	t.Helper()
	lines := s.lines(t)
	if len(lines) != 1 {
		t.Fatalf("got %d access lines, want 1:\n%s", len(lines), s.logs.String())
	}
	return lines[0]
}

func TestTheAccessLineCarriesTheWireIDAndTheTrustedCallerIDBesideTheTransportID(t *testing.T) {
	stack := newAccessStack(t)
	request := httptest.NewRequest(http.MethodGet, "/v1/access-ok", nil)
	request.Header.Set(httpapi.RequestIDHeader, accessCallerID)
	response := stack.serve(request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", response.Code)
	}
	line := stack.onlyLine(t)
	wire := response.Header().Get(requestid.Header)
	if wire == "" || line["auth_request_id"] != wire {
		t.Errorf("auth_request_id = %v, want the answer's %s %q", line["auth_request_id"], requestid.Header, wire)
	}
	if line["caller_request_id"] != accessCallerID {
		t.Errorf("caller_request_id = %v, want %q", line["caller_request_id"], accessCallerID)
	}
	transport := response.Header().Get(httpapi.RequestIDHeader)
	if line["request_id"] != transport || transport == accessCallerID {
		t.Errorf("request_id = %v, answer X-Request-ID %q: want the minted transport id, never the caller's", line["request_id"], transport)
	}
}

func TestTheCredentialStubAccessLineCarriesTheWireID(t *testing.T) {
	stack := newAccessStack(t)
	request := httptest.NewRequest(http.MethodPost, authapi.PatternRegister, strings.NewReader("{}"))
	request.Header.Set(httpapi.RequestIDHeader, accessCallerID)
	response := stack.serve(request)
	if response.Code != http.StatusServiceUnavailable || response.Header().Get("Retry-After") != "1" {
		t.Fatalf("stub = %d Retry-After %q, want 503 and 1", response.Code, response.Header().Get("Retry-After"))
	}
	if !strings.HasPrefix(response.Body.String(), `{"detail":{"message":`) {
		t.Fatalf("stub body = %q, want go-api's refusal shape", response.Body.String())
	}
	line := stack.onlyLine(t)
	if wire := response.Header().Get(requestid.Header); wire == "" || line["auth_request_id"] != wire {
		t.Errorf("auth_request_id = %v, want %q", line["auth_request_id"], wire)
	}
	if line["caller_request_id"] != accessCallerID {
		t.Errorf("caller_request_id = %v, want %q", line["caller_request_id"], accessCallerID)
	}
}

// Every answer class the API listener produces writes one access line, and
// each carries both ids.
func TestEveryAccessLineCarriesBothIDs(t *testing.T) {
	stack := newAccessStack(t)
	cases := []struct {
		name, method, target, body string
		status                     int
	}{
		{"routed", http.MethodGet, "/v1/access-ok", "", http.StatusNoContent},
		{"stub", http.MethodPost, authapi.PatternRegister, "{}", http.StatusServiceUnavailable},
		{"unrouted", http.MethodGet, "/v1/access-none", "", http.StatusNotFound},
		{"method", http.MethodDelete, "/v1/access-ok", "", http.StatusMethodNotAllowed},
		{"body bound", http.MethodPost, "/v1/access-body", strings.Repeat("x", 4096), http.StatusRequestEntityTooLarge},
		{"rate limit first", http.MethodGet, "/v1/access-limited", "", http.StatusNoContent},
		{"rate limit", http.MethodGet, "/v1/access-limited", "", http.StatusTooManyRequests},
		{"panic", http.MethodGet, "/v1/access-panic", "", http.StatusInternalServerError},
		{"abort", http.MethodGet, "/v1/access-abort", "", http.StatusAccepted},
	}
	var wires []string
	for _, tc := range cases {
		request := httptest.NewRequest(tc.method, tc.target, strings.NewReader(tc.body))
		request.Header.Set(httpapi.RequestIDHeader, accessCallerID)
		response := stack.serve(request)
		if response.Code != tc.status {
			t.Fatalf("%s: status = %d, want %d", tc.name, response.Code, tc.status)
		}
		wires = append(wires, response.Header().Get(requestid.Header))
	}
	lines := stack.lines(t)
	if len(lines) != len(cases) {
		t.Fatalf("got %d access lines for %d requests:\n%s", len(lines), len(cases), stack.logs.String())
	}
	for index, line := range lines {
		if wires[index] == "" || line["auth_request_id"] != wires[index] {
			t.Errorf("%s: auth_request_id = %v, want %q", cases[index].name, line["auth_request_id"], wires[index])
		}
		if line["caller_request_id"] != accessCallerID {
			t.Errorf("%s: caller_request_id = %v, want %q", cases[index].name, line["caller_request_id"], accessCallerID)
		}
	}
}

func TestTheCallerIDIsTrustedOnlyFromATrustedProxyPeer(t *testing.T) {
	cases := []struct {
		name, peer string
		header     bool
		want       string
	}{
		{"untrusted peer with an id", accessUntrustedPeer, true, "untrusted"},
		{"trusted peer without an id", accessTrustedPeer, false, ""},
		{"untrusted peer without an id", accessUntrustedPeer, false, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stack := newAccessStack(t)
			request := httptest.NewRequest(http.MethodGet, "/v1/access-ok", nil)
			request.RemoteAddr = tc.peer
			if tc.header {
				request.Header.Set(httpapi.RequestIDHeader, accessCallerID)
			}
			stack.serve(request)
			line := stack.onlyLine(t)
			if got, ok := line["caller_request_id"]; !ok || got != tc.want {
				t.Errorf("caller_request_id = %v (present %v), want %q", got, ok, tc.want)
			}
			if strings.Contains(stack.logs.String(), accessCallerID) && tc.want != accessCallerID {
				t.Errorf("an untrusted caller id reached the log:\n%s", stack.logs.String())
			}
		})
	}
}

// An excluded address inside the trusted range (the node's own address) is not a proxy: its caller id is not believed.
func TestTheCallerIDFromAnExcludedPeerIsUntrusted(t *testing.T) {
	stack := newAccessStackTrusting(t, "192.0.2.0/28,!192.0.2.1")
	request := httptest.NewRequest(http.MethodGet, "/v1/access-ok", nil)
	request.RemoteAddr = accessTrustedPeer
	request.Header.Set(httpapi.RequestIDHeader, accessCallerID)
	stack.serve(request)
	if got := stack.onlyLine(t)["caller_request_id"]; got != "untrusted" {
		t.Errorf("caller_request_id = %v, want \"untrusted\"", got)
	}
	if strings.Contains(stack.logs.String(), accessCallerID) {
		t.Errorf("the excluded peer's caller id reached the log:\n%s", stack.logs.String())
	}
}

func TestACallerIDOutsideTheCorrelationFormIsLoggedAsInvalid(t *testing.T) {
	cases := map[string][]string{
		"crlf":            {"6f1c2d3e-4b5a-4c6d-8e7f-0a1b2c3d4e5f\r\nlevel=ERROR"},
		"nul":             {"6f1c2d3e\x00"},
		"del":             {"6f1c2d3e-4b5a-4c6d-8e7f-0a1b2c3d4e5\x7f"},
		"invalid utf-8":   {"\xff\xfe6f1c2d3e"},
		"lone surrogate":  {"\xed\xa0\x806f1c2d3e"},
		"37 chars":        {"6f1c2d3e-4b5a-4c6d-8e7f-0a1b2c3d4e5f0"},
		"8 KiB":           {strings.Repeat("a", 8192)},
		"token shaped":    {"eyJhbGciOiJFZERTQSJ9.eyJzdWIiOiJ1In0.c2ln"},
		"two values":      {accessCallerID, accessCallerID},
		"hyphen misplace": {"6f1c2d3e4-b5a-4c6d-8e7f-0a1b2c3d4e5f"},
	}
	for name, values := range cases {
		t.Run(name, func(t *testing.T) {
			stack := newAccessStack(t)
			request := httptest.NewRequest(http.MethodGet, "/v1/access-ok", nil)
			for _, value := range values {
				request.Header.Add(httpapi.RequestIDHeader, value)
			}
			stack.serve(request)
			line := stack.onlyLine(t)
			if line["caller_request_id"] != "invalid" {
				t.Errorf("caller_request_id = %v, want \"invalid\"", line["caller_request_id"])
			}
			if len(values) == 1 && strings.Contains(stack.logs.String(), values[0]) {
				t.Errorf("the raw caller id reached the log:\n%s", stack.logs.String())
			}
		})
	}
}

func TestAnInboundAuthRequestIDIsNeverReadOrLogged(t *testing.T) {
	stack := newAccessStack(t)
	inbound := "req_AAAAAAAAAAAAAAAAAAAAAAAAAA"
	request := httptest.NewRequest(http.MethodGet, "/v1/access-ok", nil)
	request.Header.Set(requestid.Header, inbound)
	response := stack.serve(request)
	line := stack.onlyLine(t)
	wire := response.Header().Get(requestid.Header)
	if wire == "" || wire == inbound || line["auth_request_id"] != wire {
		t.Errorf("auth_request_id = %v, answer header %q: want the minted wire id", line["auth_request_id"], wire)
	}
	if strings.Contains(stack.logs.String(), inbound) {
		t.Errorf("the inbound %s reached the log:\n%s", requestid.Header, stack.logs.String())
	}
}

// Trust is decided first: an untrusted peer's header is never parsed or
// counted, so repeated values from it log "untrusted"; from a trusted peer
// they log "invalid".
func TestTrustIsDecidedBeforeTheCallerIDIsCounted(t *testing.T) {
	cases := []struct {
		name, peer, want string
	}{
		{"untrusted peer, two values", accessUntrustedPeer, "untrusted"},
		{"trusted peer, two values", accessTrustedPeer, "invalid"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stack := newAccessStack(t)
			request := httptest.NewRequest(http.MethodGet, "/v1/access-ok", nil)
			request.RemoteAddr = tc.peer
			request.Header.Add(httpapi.RequestIDHeader, accessCallerID)
			request.Header.Add(httpapi.RequestIDHeader, accessCallerID)
			stack.serve(request)
			line := stack.onlyLine(t)
			if line["caller_request_id"] != tc.want {
				t.Errorf("caller_request_id = %v, want %q", line["caller_request_id"], tc.want)
			}
			if strings.Contains(stack.logs.String(), accessCallerID) {
				t.Errorf("a repeated caller id reached the log:\n%s", stack.logs.String())
			}
		})
	}
}

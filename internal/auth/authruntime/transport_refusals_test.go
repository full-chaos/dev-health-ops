package authruntime

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

var wireRequestID = regexp.MustCompile(`^req_[A-Za-z0-9_-]{26}$`)

// sessionStack is the API handler configure builds with the session routes
// mounted. The database is unreachable: every request below is answered by
// the transport before a handler reads the store.
func sessionStack(t *testing.T, logs io.Writer) http.Handler {
	t.Helper()
	cfg, err := authconfig.Load(authconfig.Spec{LookupEnv: lookupFrom(sessionEnvironment(map[string]string{
		authconfig.EnvDatabaseURI:        "postgres://auth@127.0.0.1:1/devhealth",
		authconfig.EnvSigningKeyFile:     "/nonexistent",
		authconfig.EnvSigningKeyID:       "transport-test",
		authconfig.EnvMaxBodyBytes:       "1024",
		authconfig.EnvRateLimitPerSecond: "0.01",
		authconfig.EnvRateLimitBurst:     "1",
		authconfig.EnvAPIAddress:         "127.0.0.1:0",
		authconfig.EnvOperatorAddress:    "127.0.0.1:1",
	}))})
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	logger := slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	components, err := configure(context.Background(), cfg, logger)
	if err != nil {
		t.Fatalf("configure: %v", err)
	}
	t.Cleanup(func() {
		for _, component := range components {
			_ = component.Shutdown(context.Background())
		}
	})
	for _, component := range components {
		if announced, ok := component.(announcedListener); ok {
			if server, ok := announced.listener.(*httpapi.Server); ok {
				return server.Handler()
			}
		}
	}
	t.Fatal("configure built no API listener")
	return nil
}

// Every answer of the auth API, the transport's own refusals included (an
// unknown path, a wrong method, an oversized body, a rate-limited request),
// carries a server-minted X-Auth-Request-ID, so the go-api proxy can log the
// id of every forwarded request.
func TestTransportRefusalsCarryTheServerRequestID(t *testing.T) {
	front := httptest.NewServer(sessionStack(t, io.Discard))
	defer front.Close()
	client := &http.Client{Timeout: 5 * time.Second}
	for _, c := range []struct {
		name, method, path, body string
		status                   int
	}{
		{"unknown path", http.MethodGet, "/v1/nothing", "", http.StatusNotFound},
		{"wrong method", http.MethodDelete, "/v1/token/refresh", "", http.StatusMethodNotAllowed},
		{"oversized body", http.MethodPost, "/v1/token/refresh", strings.Repeat("x", 4096), http.StatusRequestEntityTooLarge},
		{"rate limited", http.MethodPost, "/v1/token/refresh", "{}", http.StatusTooManyRequests},
	} {
		request, err := http.NewRequest(c.method, front.URL+c.path, strings.NewReader(c.body))
		if err != nil {
			t.Fatal(err)
		}
		response, err := client.Do(request)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		if c.name == "rate limited" && response.StatusCode != c.status {
			// The bucket (burst 1) was spent by the requests above; one more may be needed.
			response, err = client.Do(mustRequest(t, c.method, front.URL+c.path, c.body))
			if err != nil {
				t.Fatal(err)
			}
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
		}
		if response.StatusCode != c.status {
			t.Fatalf("%s: status %d, want %d", c.name, response.StatusCode, c.status)
		}
		if got := response.Header.Get("X-Auth-Request-ID"); !wireRequestID.MatchString(got) {
			t.Errorf("%s: X-Auth-Request-ID = %q, want a server-minted req_ id", c.name, got)
		}
	}
}

func mustRequest(t *testing.T, method, url, body string) *http.Request {
	t.Helper()
	request, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	return request
}

// A client's X-Request-ID never reaches a log line or a response of the auth
// API: the transport id is minted by the server too.
func TestTheClientRequestIDIsNeverLoggedOrEchoed(t *testing.T) {
	logs := &safeBuffer{}
	front := httptest.NewServer(sessionStack(t, logs))
	defer front.Close()
	client := &http.Client{Timeout: 5 * time.Second}
	caller := uuid.NewString()
	for _, path := range []string{"/v1/token/refresh", "/v1/nothing"} {
		request := mustRequest(t, http.MethodPost, front.URL+path, `{}`)
		request.Header.Set(httpapi.RequestIDHeader, caller)
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		_ = response.Body.Close()
		for name, values := range response.Header {
			for _, value := range values {
				if strings.Contains(value, caller) {
					t.Errorf("%s: response header %s echoes the client request id", path, name)
				}
			}
		}
		if strings.Contains(string(body), caller) {
			t.Errorf("%s: response body echoes the client request id", path)
		}
	}
	if strings.Contains(logs.String(), caller) {
		t.Fatalf("a log line carries the client request id: %s", logs.String())
	}
}

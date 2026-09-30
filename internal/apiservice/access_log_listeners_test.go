package apiservice

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
)

// TestEveryListenerLogsItsOwnAccessLabel builds the three api listeners with
// their real transport stacks and checks that a routed and an unknown request
// each write one access line naming the listener and the route pattern, and
// that the unknown path never appears in it.
func TestEveryListenerLogsItsOwnAccessLabel(t *testing.T) {
	route := httpapi.Route{Method: http.MethodGet, Pattern: "/api/v1/probe/{id}", Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})}
	cfg := config.Config{APIAddress: "127.0.0.1:0", APIInternalAddress: "127.0.0.1:0", APIBillingEdgeAddress: "127.0.0.1:0"}
	builders := map[string]func(*slog.Logger) (*httpapi.Server, error){
		"public": func(l *slog.Logger) (*httpapi.Server, error) {
			return NewServer(cfg, l, []httpapi.Route{route})
		},
		"internal": func(l *slog.Logger) (*httpapi.Server, error) {
			return NewInternalServer(cfg, l, []httpapi.Route{route})
		},
		"billing-edge": func(l *slog.Logger) (*httpapi.Server, error) {
			return NewEdgeServer(cfg, l, []httpapi.Route{route})
		},
	}
	for listener, build := range builders {
		t.Run(listener, func(t *testing.T) {
			var logs bytes.Buffer
			server, err := build(slog.New(slog.NewJSONHandler(&logs, nil)))
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			for _, target := range []string{"/api/v1/probe/customer-8841", "/api/v1/never-registered/customer-8841"} {
				server.Handler().ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, target+"?token=hunter2", nil))
			}
			if strings.Contains(logs.String(), "customer-8841") || strings.Contains(logs.String(), "hunter2") {
				t.Errorf("access log carries a raw path or query:\n%s", logs.String())
			}
			var routes []string
			for _, raw := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
				var line map[string]any
				if err := json.Unmarshal([]byte(raw), &line); err != nil {
					t.Fatalf("log line %q: %v", raw, err)
				}
				if line["msg"] != "http request" {
					continue
				}
				if line["listener"] != listener {
					t.Errorf("listener = %v, want %s", line["listener"], listener)
				}
				routes = append(routes, line["route"].(string))
			}
			if strings.Join(routes, " ") != "/api/v1/probe/{id} unmatched" {
				t.Errorf("routes = %v, want the pattern then unmatched", routes)
			}
		})
	}
}

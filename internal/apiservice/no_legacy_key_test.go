package apiservice

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

// TestGoAPIStartsWithADatabaseAndNoLegacyKey: the api's environment carries a database and no JWT_SECRET_KEY.
// The api loads, builds every start-up component and mounts the session routes: logout answers its 200.
func TestGoAPIStartsWithADatabaseAndNoLegacyKey(t *testing.T) {
	env := map[string]string{
		"API_DATABASE_URI":    "postgresql://devhealth_api@127.0.0.1:1/devhealth?connect_timeout=1",
		"API_DATABASE_ROLE":   "devhealth_api",
		"DEV_HEALTH_API_ADDR": "127.0.0.1:0",
	}
	cfg, err := config.Load(config.Spec{Service: config.APIServiceName, LookupEnv: func(name string) (string, bool) {
		value, ok := env[name]
		return value, ok
	}})
	if err != nil {
		t.Fatalf("load with no JWT_SECRET_KEY: %v", err)
	}
	components, err := configureChecked(context.Background(), cfg, health.NewRegistry(time.Second), quietLogger())
	if err != nil {
		t.Fatalf("go-api with a database and no JWT_SECRET_KEY does not start: %v", err)
	}
	defer closeComponents(components)
	var server *httpapi.Server
	for _, component := range components {
		if candidate, ok := component.(*httpapi.Server); ok {
			server = candidate
		}
	}
	if server == nil {
		t.Fatal("no api listener among the start-up components")
	}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/auth/logout", strings.NewReader(`{"refresh_token":"not-a-refresh-token"}`))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK || strings.TrimSpace(recorder.Body.String()) != `{"message":"Logout successful"}` {
		t.Fatalf("logout with no JWT_SECRET_KEY: %d %s, want 200 Logout successful (the session routes are mounted)", recorder.Code, recorder.Body.String())
	}
}

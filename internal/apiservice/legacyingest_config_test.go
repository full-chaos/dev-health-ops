package apiservice

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/legacyingest"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

func lookupOf(values map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		value, ok := values[name]
		return value, ok
	}
}

func hasLegacyIngestRoute(routes []string) bool {
	for _, route := range routes {
		if strings.Contains(route, "/api/v1/ingest/") {
			return true
		}
	}
	return false
}

func TestLegacyIngestFlagDefaultOffMountsNothing(t *testing.T) {
	var off, on []string
	for _, route := range Routes(Deps{}, nil) {
		off = append(off, route.Pattern)
	}
	for _, route := range Routes(Deps{LegacyIngestEnabled: true}, nil) {
		on = append(on, route.Pattern)
	}
	if hasLegacyIngestRoute(off) {
		t.Fatalf("legacy ingest routes mounted by default: %v", off)
	}
	if !hasLegacyIngestRoute(on) {
		t.Fatalf("legacy ingest routes missing with the flag on: %v", on)
	}
	for _, unset := range []map[string]string{nil, {legacyIngestEnabledEnv: ""}, {legacyIngestEnabledEnv: "false"}} {
		enabled, _, _, err := checkLegacyIngest(lookupOf(unset))
		if err != nil || enabled {
			t.Fatalf("enabled=%v err=%v for %v", enabled, err, unset)
		}
	}
}

func TestLegacyIngestStartupRefusedWithoutCredentialOutsideDevelopment(t *testing.T) {
	for name, values := range map[string]map[string]string{
		"staging":           {"ENVIRONMENT": "staging"},
		"production":        {"ENVIRONMENT": "production"},
		"environment unset": {},
		"blank credentials": {"ENVIRONMENT": "prod", "INGEST_API_KEYS": " , ", "INGEST_SIGNING_SECRET": ""},
	} {
		t.Run(name, func(t *testing.T) {
			values[legacyIngestEnabledEnv] = "true"
			_, _, reason, err := checkLegacyIngest(lookupOf(values))
			if err == nil || reason != "legacy_ingest_auth_required" {
				t.Fatalf("reason=%q err=%v", reason, err)
			}
			if err.Error() != "legacy ingest is enabled but neither INGEST_API_KEYS nor INGEST_SIGNING_SECRET is set: refusing to start" {
				t.Fatalf("error text = %q", err.Error())
			}
		})
	}
	t.Run("the whole service refuses to configure", func(t *testing.T) {
		t.Setenv("ENVIRONMENT", "staging")
		t.Setenv("INGEST_API_KEYS", "")
		t.Setenv("INGEST_SIGNING_SECRET", "")
		t.Setenv(legacyIngestEnabledEnv, "true")
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := configure(ctx, config.Config{APIAddress: "127.0.0.1:0"}, health.NewRegistry(time.Second), quietLogger()); err == nil {
			t.Fatal("configure succeeded with legacy ingest enabled and no credential")
		}
	})
}

func TestLegacyIngestStartupAllowedWithCredential(t *testing.T) {
	for name, values := range map[string]map[string]string{
		"api key":     {"ENVIRONMENT": "production", "INGEST_API_KEYS": "k"},
		"secret":      {"ENVIRONMENT": "staging", "INGEST_SIGNING_SECRET": "s"},
		"both":        {"ENVIRONMENT": "production", "INGEST_API_KEYS": "k", "INGEST_SIGNING_SECRET": "s"},
		"development": {"ENVIRONMENT": "development"},
	} {
		t.Run(name, func(t *testing.T) {
			values[legacyIngestEnabledEnv] = "true"
			enabled, options, _, err := checkLegacyIngest(lookupOf(values))
			if err != nil || !enabled || !options.Deprecation || !options.RequireSignatureTimestamp {
				t.Fatalf("enabled=%v options=%+v err=%v", enabled, options, err)
			}
		})
	}
}

func TestLegacyIngestFlagInvalidValueRefused(t *testing.T) {
	for name, values := range map[string]map[string]string{
		"flag not a boolean":   {legacyIngestEnabledEnv: "maybe"},
		"sunset not a date":    {legacyIngestEnabledEnv: "true", "ENVIRONMENT": "development", legacyIngestSunsetEnv: "tomorrow"},
		"sunset iso only":      {legacyIngestEnabledEnv: "true", "ENVIRONMENT": "development", legacyIngestSunsetEnv: "2026-12-01"},
		"sunset with flag off": {legacyIngestSunsetEnv: "not a date"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, reason, err := checkLegacyIngest(lookupOf(values)); err == nil || reason != "legacy_ingest_config_invalid" {
				t.Fatalf("reason=%q err=%v", reason, err)
			}
		})
	}
	t.Run("sunset parsed and unset allowed", func(t *testing.T) {
		_, options, _, err := checkLegacyIngest(lookupOf(map[string]string{legacyIngestEnabledEnv: "true", "ENVIRONMENT": "development", legacyIngestSunsetEnv: "Tue, 01 Dec 2026 00:00:00 GMT"}))
		if err != nil || options.Sunset.UTC().Format("2006-01-02") != "2026-12-01" {
			t.Fatalf("options=%+v err=%v", options, err)
		}
		if _, options, _, err = checkLegacyIngest(lookupOf(map[string]string{legacyIngestEnabledEnv: "true", "ENVIRONMENT": "development"})); err != nil || !options.Sunset.IsZero() {
			t.Fatalf("options=%+v err=%v", options, err)
		}
	})
}

func TestLegacyIngestEveryPathMethodRefusalCarriesTheDeprecationHeaders(t *testing.T) {
	deps := Deps{LegacyIngestEnabled: true, LegacyIngestOptions: legacyingest.Options{Deprecation: true, Sunset: time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC)}}
	server, err := newServerWithGate(config.Config{APIAddress: "127.0.0.1:0"}, quietLogger(), Routes(deps, quietLogger()), nil, legacyIngestLayers(deps))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"commits", "pull-requests", "work-items", "deployments", "incidents", "telemetry"} {
		for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodPut} {
			request := httptest.NewRequest(method, "/api/v1/ingest/"+path, nil)
			recorder := httptest.NewRecorder()
			server.Handler().ServeHTTP(recorder, request)
			if recorder.Code != http.StatusMethodNotAllowed {
				t.Fatalf("%s %s: status %d, want 405", method, path, recorder.Code)
			}
			if recorder.Header().Get("Deprecation") != "true" || recorder.Header().Get("Sunset") != "Tue, 01 Dec 2026 00:00:00 GMT" {
				t.Fatalf("%s %s: headers %v", method, path, recorder.Header())
			}
		}
	}
	other := httptest.NewRecorder()
	server.Handler().ServeHTTP(other, httptest.NewRequest(http.MethodGet, "/health", nil))
	if other.Header().Get("Deprecation") != "" {
		t.Fatalf("a path outside the legacy routes carries Deprecation: %v", other.Header())
	}
}

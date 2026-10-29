package apiservice

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

// The legacy ingest routes are retired (CHAOS-11813): nothing mounts them, so the
// settings that once opened them open nothing.
var retiredLegacyIngestPaths = []string{
	"/api/v1/ingest/commits",
	"/api/v1/ingest/pull-requests",
	"/api/v1/ingest/work-items",
	"/api/v1/ingest/deployments",
	"/api/v1/ingest/incidents",
	"/api/v1/ingest/telemetry",
}

// startRetiredLegacyIngest runs the real start-up (configure, the components,
// the listener) and returns the API base URL, or the configure error.
func startRetiredLegacyIngest(t *testing.T, ctx context.Context) (string, error) {
	t.Helper()
	components, err := configure(ctx, config.Config{APIAddress: "127.0.0.1:0"}, health.NewRegistry(time.Second), quietLogger())
	if err != nil {
		return "", err
	}
	for _, component := range components {
		if err := component.Start(ctx); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		for index := len(components) - 1; index >= 0; index-- {
			_ = components[index].Shutdown(context.Background())
		}
	})
	for _, component := range components {
		if server, ok := component.(interface{ Address() string }); ok {
			return "http://" + server.Address(), nil
		}
	}
	t.Fatal("configure started no HTTP server")
	return "", nil
}

// With every setting that used to mount the routes (the flag on, an API key and
// a signing secret), each of the six paths answers 404 and carries no
// deprecation header: the routes are gone, not refused.
func TestRetiredLegacyIngestRoutesAnswer404(t *testing.T) {
	for _, environment := range []string{"development", "production"} {
		t.Run(environment, func(t *testing.T) {
			t.Setenv("ENVIRONMENT", environment)
			t.Setenv("DEV_HEALTH_LEGACY_INGEST_ENABLED", "true")
			t.Setenv("DEV_HEALTH_LEGACY_INGEST_SUNSET", time.Now().Add(30*24*time.Hour).UTC().Format(http.TimeFormat))
			t.Setenv("INGEST_API_KEYS", "retired-key")
			t.Setenv("INGEST_SIGNING_SECRET", "retired-secret")
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			base, err := startRetiredLegacyIngest(t, ctx)
			if err != nil {
				t.Fatalf("start-up: %v", err)
			}
			for _, path := range retiredLegacyIngestPaths {
				request, err := http.NewRequestWithContext(ctx, http.MethodPost, base+path, strings.NewReader(`{"org_id":"org-1","items":[]}`))
				if err != nil {
					t.Fatal(err)
				}
				request.Header.Set("Content-Type", "application/json")
				request.Header.Set("X-API-Key", "retired-key")
				response, err := http.DefaultClient.Do(request)
				if err != nil {
					t.Fatal(err)
				}
				body, err := io.ReadAll(response.Body)
				_ = response.Body.Close()
				if err != nil {
					t.Fatal(err)
				}
				if response.StatusCode != http.StatusNotFound || string(body) != `{"detail":"Not Found"}` {
					t.Errorf("POST %s = %d %s, want 404 {\"detail\":\"Not Found\"} (retired)", path, response.StatusCode, body)
				}
				for _, header := range []string{"Deprecation", "Sunset"} {
					if value := response.Header.Get(header); value != "" {
						t.Errorf("POST %s carries %s %q: a retired route sets no deprecation header", path, header, value)
					}
				}
			}
		})
	}
}

// The retired settings are read by nothing: values that once refused the start
// (a flag that is not a boolean, a sunset that is not an HTTP-date, the flag on
// with no credential outside development) start the process.
func TestRetiredLegacyIngestSettingsAreUnread(t *testing.T) {
	for name, settings := range map[string]map[string]string{
		"flag not a boolean":      {"DEV_HEALTH_LEGACY_INGEST_ENABLED": "maybe"},
		"sunset not an HTTP-date": {"DEV_HEALTH_LEGACY_INGEST_SUNSET": "garbage"},
		"flag on, no credential":  {"DEV_HEALTH_LEGACY_INGEST_ENABLED": "true"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("ENVIRONMENT", "production")
			t.Setenv("INGEST_API_KEYS", "")
			t.Setenv("INGEST_SIGNING_SECRET", "")
			for key, value := range settings {
				t.Setenv(key, value)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if _, err := startRetiredLegacyIngest(t, ctx); err != nil {
				t.Fatalf("start-up refused on a retired setting: %v", err)
			}
		})
	}
}

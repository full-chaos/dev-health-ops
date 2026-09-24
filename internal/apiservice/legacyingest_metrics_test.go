package apiservice

import (
	"bytes"
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

// TestLegacyIngestRefusalsAreScrapedFromTheOperatorRegistry proves the
// counter is wired where /metrics reads it: every reason is present at zero
// after configure, and a request refused for its credentials moves exactly
// its own series.
func TestLegacyIngestRefusalsAreScrapedFromTheOperatorRegistry(t *testing.T) {
	t.Setenv("INGEST_API_KEYS", "")
	t.Setenv("INGEST_SIGNING_SECRET", "")
	t.Setenv("ENVIRONMENT", "production")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	registry := health.NewRegistry(time.Second)
	components, err := configure(ctx, config.Config{APIAddress: "127.0.0.1:0"}, registry, quietLogger())
	if err != nil {
		t.Fatal(err)
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
	base := ""
	for _, component := range components {
		if server, ok := component.(interface{ Address() string }); ok {
			base = "http://" + server.Address()
		}
	}
	scrape := func() string {
		var out bytes.Buffer
		if err := registry.WriteMetrics(&out); err != nil {
			t.Fatal(err)
		}
		return out.String()
	}
	const series = `dev_health_api_ingest_legacy_auth_rejected_total{reason="no_credential_configured"}`
	if got := scrape(); !strings.Contains(got, series+" 0\n") || !strings.Contains(got, `{reason="invalid_api_key"} 0`) || !strings.Contains(got, `{reason="invalid_signature"} 0`) {
		t.Fatalf("the series are not all present at zero:\n%s", got)
	}
	response, err := http.Post(base+"/api/v1/ingest/commits", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status %d, want 401", response.StatusCode)
	}
	got := scrape()
	if !strings.Contains(got, series+" 1\n") || !strings.Contains(got, `{reason="invalid_api_key"} 0`) {
		t.Fatalf("one refusal did not move exactly its own series:\n%s", got)
	}
}

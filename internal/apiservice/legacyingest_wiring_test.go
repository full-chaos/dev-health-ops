package apiservice

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

// TestLegacyIngestStartupWiresTheHardeningIntoTheMountedRoutes runs the real
// start-up (configure, buildDeps, the routes, the listener) with the flag on
// and a signing secret, and checks over the wire what the process serves: a
// body-only signature is refused, a timestamped one is accepted, and the
// deprecation headers are on both answers.
func TestLegacyIngestStartupWiresTheHardeningIntoTheMountedRoutes(t *testing.T) {
	const secret = "wiring-secret"
	t.Setenv("ENVIRONMENT", "development")
	t.Setenv("INGEST_API_KEYS", "")
	t.Setenv("INGEST_SIGNING_SECRET", secret)
	t.Setenv(legacyIngestEnabledEnv, "true")
	t.Setenv(legacyIngestSunsetEnv, "Tue, 01 Dec 2026 00:00:00 GMT")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	components, err := configure(ctx, config.Config{APIAddress: "127.0.0.1:0"}, health.NewRegistry(time.Second), quietLogger())
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
	const body = `{"org_id":"org-1","repo_url":"https://example.test/r.git","items":[{"hash":"abc","message":"m","author_name":"a","author_email":"a@example.test","author_when":"2026-01-01T00:00:00Z"}]}`
	sign := func(message string) string {
		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write([]byte(message))
		return "sha256=" + hex.EncodeToString(mac.Sum(nil))
	}
	post := func(headers map[string]string) *http.Response {
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/api/v1/ingest/commits", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		for key, value := range headers {
			request.Header.Set(key, value)
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		return response
	}
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	for name, item := range map[string]struct {
		headers map[string]string
		status  int
	}{
		"body-only signature":   {map[string]string{"X-Signature-256": sign(body)}, http.StatusUnauthorized},
		"timestamped signature": {map[string]string{"X-Signature-256": sign(timestamp + "." + body), "X-Signature-Timestamp": timestamp}, http.StatusAccepted},
		"timestamp not covered": {map[string]string{"X-Signature-256": sign(body), "X-Signature-Timestamp": timestamp}, http.StatusUnauthorized},
	} {
		response := post(item.headers)
		if response.StatusCode != item.status {
			t.Errorf("%s: status %d, want %d", name, response.StatusCode, item.status)
		}
		if response.Header.Get("Deprecation") != "true" || response.Header.Get("Sunset") != "Tue, 01 Dec 2026 00:00:00 GMT" {
			t.Errorf("%s: headers %v", name, response.Header)
		}
	}
}

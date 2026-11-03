package devhealth_test

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// The largest authorize batch, read from the service source so the chart check cannot drift from it.
func maxAuthorizeBatchItems(t *testing.T) string {
	t.Helper()
	src, err := os.ReadFile("../../../internal/auth/authapi/authorize_request.go")
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`maxAuthorizationBatchItems\s*=\s*(\d+)`).FindSubmatch(src)
	if m == nil {
		t.Fatal("maxAuthorizationBatchItems not found")
	}
	return string(m[1])
}

func TestAuthPDPRateLimitBurstFloorIsTheLargestBatch(t *testing.T) {
	if got := maxAuthorizeBatchItems(t); got != "100" {
		t.Fatalf("the largest authorize batch is %s: update the chart floor (auth-service.yaml, pdpRateLimit) and its schema note", got)
	}
}

func TestAuthPDPRateLimitDefaultsRender(t *testing.T) {
	docs, out, err := renderAuth(t, authBase...)
	if err != nil {
		t.Fatalf("render: %v\n%.400s", err, out)
	}
	deployment := authDoc(docs, "Deployment", "a-dev-health-auth-service")
	for env, want := range map[string]string{
		"AUTH_SERVICE_PDP_CALLER_RATE_LIMIT_PER_SECOND": "150",
		"AUTH_SERVICE_PDP_CALLER_RATE_LIMIT_BURST":      "300",
		"AUTH_SERVICE_PDP_ROUTE_RATE_LIMIT_PER_SECOND":  "2000",
		"AUTH_SERVICE_PDP_ROUTE_RATE_LIMIT_BURST":       "4000",
	} {
		if got, set := containerEnvValue(t, deployment, env); !set || got != want {
			t.Errorf("%s = %q (set %v), want %q", env, got, set, want)
		}
	}
}

func TestAuthPDPRateLimitOverridesRender(t *testing.T) {
	docs, out, err := renderAuth(t, authWith(
		"--set-json", `authService.pdpRateLimit.callerPerSecond=0.5`, "--set", "authService.pdpRateLimit.callerBurst=100",
		"--set-json", `authService.pdpRateLimit.routePerSecond=100000`, "--set", "authService.pdpRateLimit.routeBurst=100")...)
	if err != nil {
		t.Fatalf("render: %v\n%.400s", err, out)
	}
	deployment := authDoc(docs, "Deployment", "a-dev-health-auth-service")
	for env, want := range map[string]string{
		"AUTH_SERVICE_PDP_CALLER_RATE_LIMIT_PER_SECOND": "0.5",
		"AUTH_SERVICE_PDP_CALLER_RATE_LIMIT_BURST":      "100",
		"AUTH_SERVICE_PDP_ROUTE_RATE_LIMIT_PER_SECOND":  "100000",
		"AUTH_SERVICE_PDP_ROUTE_RATE_LIMIT_BURST":       "100",
	} {
		if got, set := containerEnvValue(t, deployment, env); !set || got != want {
			t.Errorf("%s = %q (set %v), want %q", env, got, set, want)
		}
	}
}

// A burst under the largest batch refuses that batch for ever; the render refuses it, the service start never does (D6465).
func TestAuthPDPRateLimitBurstUnderTheLargestBatchFailsTheRender(t *testing.T) {
	for _, key := range []string{"callerBurst", "routeBurst"} {
		for _, burst := range []string{"99", "1"} {
			_, out, err := renderAuth(t, authWith("--set", "authService.pdpRateLimit."+key+"="+burst)...)
			if err == nil || !strings.Contains(out, "authService.pdpRateLimit."+key+" must be at least 100") {
				t.Errorf("%s=%s: err=%v\n%.400s", key, burst, err, out)
			}
		}
	}
}

func TestAuthPDPRateLimitOutOfRangeFailsTheSchema(t *testing.T) {
	for _, set := range []string{"callerPerSecond=0", "callerBurst=100001", "routePerSecond=100001", "routeBurst=0"} {
		if _, _, err := renderAuth(t, authWith("--set-json", "authService.pdpRateLimit."+set)...); err == nil {
			t.Errorf("%s rendered, want a schema refusal", set)
		}
	}
}

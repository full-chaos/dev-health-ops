package authruntime

import (
	"log/slog"
	"net/http"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// The PDP routes carry the PDP ceiling, never the 20/40 default bucket.
func TestPDPRoutesAreNotOnTheDefaultRouteBucket(t *testing.T) {
	cfg := authconfig.Config{
		RateLimit: authconfig.RateLimit{PerSecond: 20, Burst: 40},
		PDPRoute:  authconfig.RateLimit{PerSecond: 2000, Burst: 4000},
	}
	routes := []httpapi.Route{
		{Method: http.MethodPost, Pattern: authapi.PatternAuthorize},
		{Method: http.MethodPost, Pattern: authapi.PatternAuthorizeBatch},
		{Method: http.MethodPost, Pattern: authapi.PatternAuthorizeRepositories},
		{Method: http.MethodPost, Pattern: "/v1/other"},
	}
	got := apiServerOptions(cfg, slog.Default(), routes).Routes
	for _, r := range got {
		pdp := r.Pattern != "/v1/other"
		if pdp && (r.RateLimitPerSecond != 2000 || r.RateLimitBurst != 4000) {
			t.Errorf("%s: limit %v/%d, want the PDP ceiling 2000/4000", r.Pattern, r.RateLimitPerSecond, r.RateLimitBurst)
		}
		if !pdp && (r.RateLimitPerSecond != 0 || r.RateLimitBurst != 0) {
			t.Errorf("%s: limit %v/%d, want the server default (0/0)", r.Pattern, r.RateLimitPerSecond, r.RateLimitBurst)
		}
	}
}

package authconfig

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/edgetokenmint"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
)

// auth-service verifies go-api's HS256 access token with the audience go-api
// signs it with. Every default of that audience is the one contracts constant.
func TestGoAPIsHS256AudienceDefaultsAreThePlatformAPIAudience(t *testing.T) {
	cfg, err := config.Load(config.Spec{Service: config.APIServiceName, LookupEnv: lookupFrom(map[string]string{})})
	if err != nil {
		t.Fatalf("go-api config: %v", err)
	}
	if cfg.APIJWTAudience != contracts.PlatformAPIAudience {
		t.Errorf("go-api's default JWT audience %q, want %q", cfg.APIJWTAudience, contracts.PlatformAPIAudience)
	}
	if edgetokenmint.DefaultAudience != contracts.PlatformAPIAudience {
		t.Errorf("the edge token mint default audience %q, want %q", edgetokenmint.DefaultAudience, contracts.PlatformAPIAudience)
	}
	if defaultAccessTokenAudience != contracts.PlatformAPIAudience {
		t.Errorf("auth-service's default access token audience %q, want %q", defaultAccessTokenAudience, contracts.PlatformAPIAudience)
	}
}

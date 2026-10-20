package apiservice

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

// go-api reads auth-service sessions in the schema AUTH_DATABASE_SCHEMA names
// and refuses to start on a name that is not a schema.
func TestConfigureRefusesAnUnusableAuthSchema(t *testing.T) {
	t.Setenv("ENVIRONMENT", "development")
	t.Setenv(policy.EnvAuthSchema, "Bad-Name")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	components, err := configure(ctx, reachableDatabase(config.Config{APIAddress: "127.0.0.1:0"}), health.NewRegistry(time.Second), quietLogger())
	closeComponents(components)
	var coded interface{ DependencyReason() string }
	if !errors.As(err, &coded) || coded.DependencyReason() != "api_protection_config_failed" {
		t.Fatalf("err = %v, want reason api_protection_config_failed", err)
	}

	t.Setenv(policy.EnvAuthSchema, "acp")
	components, err = configure(ctx, reachableDatabase(config.Config{APIAddress: "127.0.0.1:0"}), health.NewRegistry(time.Second), quietLogger())
	closeComponents(components)
	if err != nil {
		t.Fatalf("a usable schema name: %v", err)
	}
}

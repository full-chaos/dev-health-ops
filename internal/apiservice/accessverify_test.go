package apiservice

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

func configuredAccessVerify(t *testing.T) (built bool, err error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	components, err := configureWith(ctx, config.Config{APIAddress: "127.0.0.1:0"}, health.NewRegistry(time.Second), quietLogger(),
		func(deps *Deps) { built = deps.AccessVerify != nil })
	for index := len(components) - 1; index >= 0; index-- {
		_ = components[index].Shutdown(context.Background())
	}
	return built, err
}

func TestConfigure_ConstructsAccessVerifyWhenIssuerIsSet(t *testing.T) {
	t.Setenv(envAccessTokenIssuer, "https://auth.example.test")
	built, err := configuredAccessVerify(t)
	if err != nil || !built {
		t.Fatalf("built=%v err=%v", built, err)
	}
}

func TestConfigure_NoAccessVerifyWithoutIssuer(t *testing.T) {
	t.Setenv(envAccessTokenIssuer, "")
	built, err := configuredAccessVerify(t)
	if err != nil || built {
		t.Fatalf("built=%v err=%v", built, err)
	}
}

func TestConfigure_RefusesInvalidAccessTokenIssuer(t *testing.T) {
	t.Setenv(envAccessTokenIssuer, "http://auth.example.test")
	built, err := configuredAccessVerify(t)
	if err == nil || built {
		t.Fatalf("built=%v err=%v", built, err)
	}
}

package apiservice

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
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

func TestLegacyVerifier_StoreOutageIsUnavailableNotRefusal(t *testing.T) {
	verify := legacyVerifier(func(context.Context, string) (*policy.User, error) { return nil, policy.ErrUnavailable })
	_, err := verify(context.Background(), "t")
	if !errors.Is(err, accessverify.ErrUnavailable) || errors.Is(err, accessverify.ErrRefused) {
		t.Fatalf("err=%v", err)
	}
	other := errors.New("rejected")
	if _, err := legacyVerifier(func(context.Context, string) (*policy.User, error) { return nil, other })(context.Background(), "t"); errors.Is(err, accessverify.ErrUnavailable) {
		t.Fatalf("a rejection became unavailable: %v", err)
	}
}

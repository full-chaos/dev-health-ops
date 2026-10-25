package apiservice

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
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
}

// TestLegacyVerifier_ErrorClasses is acp D3: a refusal and a claim of the
// wrong type stay refusals (401); any other store failure is unavailable
// (503), never a refusal.
func TestLegacyVerifier_ErrorClasses(t *testing.T) {
	refusal, claimType := realLegacyErrors(t)
	for name, in := range map[string]error{"refusal": refusal, "claim type": claimType} {
		if _, err := legacyVerifier(func(context.Context, string) (*policy.User, error) { return nil, in })(context.Background(), "t"); err == nil || errors.Is(err, accessverify.ErrUnavailable) {
			t.Errorf("%s became %v", name, err)
		}
	}
	other := errors.New("store: connection reset")
	if _, err := legacyVerifier(func(context.Context, string) (*policy.User, error) { return nil, other })(context.Background(), "t"); !errors.Is(err, accessverify.ErrUnavailable) {
		t.Fatalf("a store failure was not unavailable: %v", err)
	}
}

// realLegacyErrors are the errors the real Authenticator returns for a token
// it refuses and for a sub claim of the wrong type.
func realLegacyErrors(t *testing.T) (refusal, claimType error) {
	t.Helper()
	verifier, err := edgetoken.New(probeKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	auth, err := policy.NewAuthenticator(verifier, newProbeStore(), quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	_, refusal = auth.AuthenticateLegacy(context.Background(), "not-a-token")
	_, claimType = auth.AuthenticateLegacy(context.Background(), probeHS256(t, probeKey, map[string]any{"sub": 7}))
	if !policy.IsRefusal(refusal) || !policy.IsClaimTypeError(claimType) {
		t.Fatalf("refusal %v, claim type %v", refusal, claimType)
	}
	return refusal, claimType
}

// configureForAccessVerify runs the prod construction path with a database
// configured (the pool is lazy) and the issuer set or unset.
func configureForAccessVerify(t *testing.T, issuerSet bool) Deps {
	t.Helper()
	issuer := ""
	if issuerSet {
		issuer = "https://auth.example.test"
	}
	t.Setenv(envAccessTokenIssuer, issuer)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var deps Deps
	components, err := configureWith(ctx, apiTestConfig(), health.NewRegistry(time.Second), quietLogger(), func(built *Deps) { deps = *built })
	for index := len(components) - 1; index >= 0; index-- {
		_ = components[index].Shutdown(context.Background())
	}
	if err != nil {
		t.Fatal(err)
	}
	return deps
}

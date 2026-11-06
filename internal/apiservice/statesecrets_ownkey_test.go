package apiservice

import (
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/githubapp"
)

func TestStatePurposeSecretsNeverReadTheJWTKey(t *testing.T) {
	t.Parallel()
	jwt := strings.Repeat("j", 40)
	sso, ssoFallback, signer := statePurposeSecrets(stateConfig(t, jwt, "", ""))
	if sso != "" || ssoFallback != "" || signer.Secret != "" {
		t.Fatal("with no own key, a state key still comes from JWT_SECRET_KEY")
	}
	sso, ssoFallback, signer = statePurposeSecrets(stateConfig(t, jwt, strings.Repeat("s", 40), strings.Repeat("g", 40)))
	if sso == jwt || signer.Secret == jwt {
		t.Fatal("a state key is JWT_SECRET_KEY")
	}
	if ssoFallback != "" || signer.FallbackSecret != "" {
		t.Fatal("a state verifies under JWT_SECRET_KEY")
	}
}

func TestGitHubInstallStateMintedUnderTheJWTKeyIsRefused(t *testing.T) {
	t.Parallel()
	jwt := strings.Repeat("j", 40)
	cfg := stateConfig(t, jwt, strings.Repeat("s", 40), strings.Repeat("g", 40))
	_, _, signer := statePurposeSecrets(cfg)
	old, err := githubapp.Signer{Secret: jwt, Issuer: cfg.APIJWTIssuer, Audience: cfg.APIJWTAudience}.Mint("org-1", nil, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := signer.Verify(old, time.Now()); err == nil {
		t.Fatal("a state minted under JWT_SECRET_KEY verified")
	}
}

package apiservice

import (
	"github.com/full-chaos/dev-health-ops/internal/api/githubapp"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
)

// statePurposeSecrets returns the SSO login-state key and the GitHub App
// install-state signer. Each reads only its own variable; an unset one yields
// an empty key, and the flow that needs it answers 503 config_missing.
func statePurposeSecrets(cfg config.Config) (ssoState string, githubState githubapp.Signer) {
	githubState = githubapp.Signer{Issuer: cfg.APIJWTIssuer, Audience: cfg.APIJWTAudience}
	if cfg.SSOStateSecret.Configured() {
		ssoState = cfg.SSOStateSecret.Reveal()
	}
	if cfg.GitHubAppStateSecret.Configured() {
		githubState.Secret = cfg.GitHubAppStateSecret.Reveal()
	}
	return ssoState, githubState
}
